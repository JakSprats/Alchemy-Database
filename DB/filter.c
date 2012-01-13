/*
  *
  * This file implements the filtering of columns in SELECTS
  *

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of ALCHEMY_DATABASE

   This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <ctype.h>
#include <assert.h>

#include "adlist.h"
#include "redis.h"

#include "debug.h"
#include "colparse.h"
#include "index.h"
#include "range.h"
#include "find.h"
#include "aobj.h"
#include "common.h"
#include "filter.h"

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

extern char *OP_Desc[];

void initFilter(f_t *flt) {
    bzero(flt, sizeof(f_t));
    flt->jan    = flt->imatch = flt->tmatch = flt->cmatch = -1;
    flt->op     = NONE;
    initAobj(&flt->akey); initAobj(&flt->alow); initAobj(&flt->ahigh);
}
f_t *newEmptyFilter() {
    f_t *flt = malloc(sizeof(f_t)); initFilter(flt); return flt; // FREE 036
}
static void releaseFilterInnards(f_t *flt) {
    if (flt->key)  { sdsfree(flt->key);     flt->key  = NULL; }
    if (flt->low)  { sdsfree(flt->low);     flt->low  = NULL; }
    if (flt->high) { sdsfree(flt->high);    flt->high = NULL; }
    releaseAobj(&flt->akey); releaseAobj(&flt->alow); releaseAobj(&flt->ahigh);
    destroyINLlist(&flt->inl);
}
void releaseFilterD_KL(f_t *flt) {             //printf("releaseFilterD_KL\n");
    releaseFilterInnards(flt);
    destroyFlist(  &flt->klist); /* RANGE QUERIES klist is live */
}
void releaseFilterR_KL(f_t *flt) {             //printf("releaseFilterR_KL\n");
    releaseFilterInnards(flt);
    releaseFlist(  &flt->klist); /* JOINS klist is just a reference list */
}
void destroyFilter(void *v) {
    f_t *flt = (f_t *)v;
    releaseFilterD_KL(flt);
    free(flt);
}
static void *vcloneAobj(void *v) { return cloneAobj((aobj *)v); }
f_t *cloneFilter(f_t *oflt) {
    f_t *flt    = malloc(sizeof(f_t));                   /* FREE ME 067 */
    bzero(flt, sizeof(f_t));
    flt->jan    = oflt->jan;
    flt->imatch = oflt->imatch;
    flt->tmatch = oflt->tmatch; flt->cmatch = oflt->cmatch;
    flt->op     = oflt->op;
    flt->iss    = oflt->iss;
    if (oflt->key)  flt->key  = sdsdup(oflt->key);
    if (oflt->low)  flt->low  = sdsdup(oflt->low);
    if (oflt->high) flt->high = sdsdup(oflt->high);
    aobjClone(&flt->akey,  &oflt->akey);
    aobjClone(&flt->alow,  &oflt->alow);
    aobjClone(&flt->ahigh, &oflt->ahigh);
    if (oflt->inl) {
        oflt->inl->dup = vcloneAobj;
        flt->inl       = listDup(oflt->inl);
    }
    if (oflt->klist) flt->klist = listDup(oflt->klist);
    return flt;
}
inline void *vcloneFilter(void *v) { return cloneFilter((f_t *)v); }

void convertFilterListToAobj(list *flist) {
    if (!flist) return;
    listNode *ln;
    listIter *li = listGetIterator(flist, AL_START_HEAD);
    while((ln = listNext(li))) {
        f_t *flt = ln->value;
        if (flt->inl) continue;
        uchar ctype = CTYPE_FROM_FLT(flt)
        if (flt->key) {
            initAobjFromStr(&flt->akey,  flt->key,  sdslen(flt->key),  ctype);
        }
        if (flt->low) {
            initAobjFromStr(&flt->alow,  flt->low,  sdslen(flt->low),  ctype);
            initAobjFromStr(&flt->ahigh, flt->high, sdslen(flt->high), ctype);
        }
    } listReleaseIterator(li);
}

// SERIALISE_FLT SERIALISE_FLT SERIALISE_FLT SERIALISE_FLT SERIALISE_FLT
int getSizeFLT(f_t *flt) { //NOTE: only single NUM lookup compilation supported 
    if (flt->inl || flt->low)           return -1; //TODO support [IN, RQ]
    convertFilterSDStoAobj(flt); //NOTE: "$1" will become aobj[0]
    if (flt->tmatch == -1)              return  0;
    int ksize = getSizeAobj(&flt->akey);
    if (ksize == -1)                    return -1;
    return (sizeof(int) * 3) + sizeof(uchar) + ksize; // [t, c, j], type + aobj
}

static uchar SerialiseFLT_Buf[32];
uchar *serialiseFLT(f_t *flt) {
    if (flt->inl || flt->low) return NULL; //TODO support [IN, RQ] compilation
    if (flt->tmatch == -1)    return NULL;
    uchar *x     = (uchar *)&SerialiseFLT_Buf;
    memcpy(x, &flt->tmatch,  sizeof(int));   x += sizeof(int);
    memcpy(x, &flt->cmatch,  sizeof(int));   x += sizeof(int);
    memcpy(x, &flt->jan,     sizeof(int));   x += sizeof(int);
    uchar  ctype = CTYPE_FROM_FLT(flt)
    memcpy(x, &ctype,        sizeof(uchar)); x += sizeof(uchar);
    if      C_IS_I(ctype) memcpy(x, &flt->akey.i, sizeof(uint32));
    else if C_IS_L(ctype) memcpy(x, &flt->akey.l, sizeof(ulong));
    else if C_IS_X(ctype) memcpy(x, &flt->akey.x, sizeof(uint128));
    else                  assert(!"serialiseFLT ERROR\n");
    return (uchar *)&SerialiseFLT_Buf;
}
int deserialiseFLT(uchar *x, f_t *flt) {
    uchar *ox = x;
    memcpy(&flt->tmatch, x, sizeof(int));   x += sizeof(int);
    memcpy(&flt->cmatch, x, sizeof(int));   x += sizeof(int);
    memcpy(&flt->jan   , x, sizeof(int));   x += sizeof(int);
    flt->imatch = find_index(flt->tmatch, flt->cmatch);
    flt->op     = EQ;
    uchar ctype;
    memcpy(&ctype,       x, sizeof(uchar)); x += sizeof(uchar);
    if        C_IS_I(ctype) {
        memcpy(&flt->akey.i, x, sizeof(uint32));  x += sizeof(uint32);
        flt->akey.enc = flt->akey.type = COL_TYPE_INT;
    } else if C_IS_L(ctype) {
        memcpy(&flt->akey.l, x, sizeof(ulong));   x += sizeof(ulong);
        flt->akey.enc = flt->akey.type = COL_TYPE_LONG;
    } else if C_IS_X(ctype) {
        memcpy(&flt->akey.x, x, sizeof(uint128)); x += sizeof(uint128);
        flt->akey.enc = flt->akey.type = COL_TYPE_U128;
    }
    flt->akey.empty = 0;
    return (int)(x - ox);
}

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpFilter(printer *prn, f_t *flt, char *prfx) {
    if (!flt) return;
    int t = flt->tmatch; int c = flt->cmatch; int i = flt->imatch;
    (*prn)("\t%sSTART dumpFilter: (%p) iss: %d\n", prfx, (void *)flt, flt->iss);
    (*prn)("\t%s\tjan:    %d (%s)\n", prfx, flt->jan, getJoinAlias(flt->jan));
    (*prn)("\t%s\ttmatch: %d (%s)\n", prfx, t, (t == -1) ? "" : Tbl[t].name);
    if (c < -1) (*prn)("\t%s\tcmatch: %d (%s.pos())\n", prfx, c, 
                                           Index[getImatchFromOCmatch(c)].name);
                                                          
    else (*prn)("\t%s\tcmatch: %d (%s)\n", prfx, c, (c == -1) ? "" :
                                                            Tbl[t].col[c].name);
    (*prn)("\t%s\timatch: %d (%s)\n", prfx, i, (i == -1) ? "" : Index[i].name);
    (*prn)("\t%s\top:     %d (%s)\n", prfx, flt->op, OP_Desc[flt->op]);
    if (flt->key) {
        (*prn)("\t%s\tkey:    %s\n",      prfx, flt->key);
        (*prn)("\t%s\t", prfx);                  dumpAobj(prn, &flt->akey);
    } else if (flt->akey.type != COL_TYPE_NONE) {
        (*prn)("\t%s\takey:   ", prfx);          dumpAobj(prn, &flt->akey);
    }
    if (flt->low) {
        (*prn)("\t%s\tlow:    %s\n",      prfx, flt->low);
        (*prn)("\t%s\t", prfx);                  dumpAobj(prn, &flt->alow);
    } else if (flt->alow.type != COL_TYPE_NONE) {
        (*prn)("\t%s\talow:   ", prfx);          dumpAobj(prn, &flt->alow);
    }
    if (flt->high) {
        (*prn)("\t%s\thigh:   %s\n",      prfx, flt->high);
        (*prn)("\t%s\t", prfx);                  dumpAobj(prn, &flt->ahigh);
    } else if (flt->ahigh.type != COL_TYPE_NONE) {
        (*prn)("\t%s\tahigh:   ", prfx);          dumpAobj(prn, &flt->ahigh);
    }
    if (flt->inl) {
        (*prn)("\t%s\tinl len: %d\n", prfx, flt->inl->len);
        listNode *ln;
        listIter *li = listGetIterator(flt->inl, AL_START_HEAD);
        while((ln = listNext(li))) {
            aobj *a = ln->value;
            (*prn)("\t%s\t\t", prfx); dumpAobj(prn, a);
        } listReleaseIterator(li);
    }
    if (flt->op == LFUNC) {
        (*prn)("\t%s\tlua_function: fname: %s args: %d\n",
               prfx, flt->le.fname, flt->le.ncols);
        //TODO get function body from lua
    }
    dumpFL(prn, "\t\t\t", "KLIST", flt->klist);
}

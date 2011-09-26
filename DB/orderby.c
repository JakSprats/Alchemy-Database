/*
 * This file implements "SELECT ... ORDER BY col LIMIT X OFFSET Y"
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
#include <float.h>

#include "redis.h"
#include "adlist.h"

#include "row.h"
#include "rpipe.h"
#include "parser.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "orderby.h"

extern r_tbl_t  Tbl[MAX_NUM_TABLES];

//GLOBALS
uint32 OB_nob   = 0;                 // TODO push into cswc_t
bool   OB_asc  [MAX_ORDER_BY_COLS];  // TODO push into cswc_t
uchar  OB_ctype[MAX_ORDER_BY_COLS];  // TODO push into cswc_t

static float  Fmin = FLT_MIN;
#define FSIZE sizeof(float)

static int intOrderBySort(const void *s1, const void *s2) {
    return (long)s1 - (long)s2;
}
static int intOrderByRevSort(const void *s1, const void *s2) {
    return (long)s2 - (long)s1;
}
static int ulongOrderBySort(const void *s1, const void *s2) {
    return ((ulong)s1 == (ulong)s2)  ? 0 : 
           (((ulong)s1 >  (ulong)s2) ? 1 : -1);
}
static int ulongOrderByRevSort(const void *s1, const void *s2) {
    return ((ulong)s1 == (ulong)s2)  ? 0 : 
           (((ulong)s1 >  (ulong)s2) ? -1 : 1);
}
static int floatOrderBySort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, FSIZE);
    memcpy(&f2, &s2, FSIZE);
    float   f  = f1 - f2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
static int floatOrderByRevSort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, FSIZE);
    memcpy(&f2, &s2, FSIZE);
    float   f  = f2 - f1;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
static int stringOrderBySort(const void *s1, const void *s2) {
    char *c1 = (char *)s1;
    char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c1, c2) : c1 - c2; /* strcmp() not ok w/ NULLs */
}
static int stringOrderByRevSort(const void *s1, const void *s2) {
    char *c1 = (char *)s1;
    char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c2, c1) : c2 - c1; /* strcmp() not ok w/ NULLs */
}

typedef int ob_cmp(const void *, const void *);
/* slot = OB_ctype[j] * 2 + OB_asc[j] */
ob_cmp (*OB_cmp[8]) = { intOrderByRevSort,    intOrderBySort,     /* CTYPE: 0 */
                        ulongOrderByRevSort,  ulongOrderBySort,   /* CTYPE: 1 */
                        stringOrderByRevSort, stringOrderBySort,  /* CTYPE: 2 */
                        floatOrderByRevSort,  floatOrderBySort }; /* CTYPE: 3 */

int genOBsort(const void *s1, const void *s2) {
    int     ret = 0;
    obsl_t *o1  = *(obsl_t **)s1;
    obsl_t *o2  = *(obsl_t **)s2;
    for (uint32 j = 0; j < OB_nob; j++) {
        ob_cmp *compar = OB_cmp[OB_ctype[j] * 2 + OB_asc[j]];
        ret            = (*compar)(o1->keys[j], o2->keys[j]);
        if (ret) return ret; /* return the first non-zero compar result */
    }
    return ret;
}

list *initOBsort(bool qed, wob_t *wb, bool rcrud) {
    OB_nob = wb->nob;
    if (OB_nob) {
        for (uint32 i = 0; i < OB_nob; i++) {
            OB_asc[i]   = wb->asc[i];
            OB_ctype[i] = Tbl[wb->obt[i]].col_type[wb->obc[i]];
        }
    }                                                         // \/ DESTROY 009
    if (qed) {                                           return listCreate();
    } else if (rcrud) {
        list *ll = listCreate(); ll->free = destroyAobj; return ll;
    } else                                               return NULL;
}
void releaseOBsort(list *ll) {
    if (ll) listRelease(ll);                             /* DESTROYED 009 */
    OB_nob = 0;
}

obsl_t *create_obsl(void *row, uint32 nob) {
    obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t));       /* FREE ME 001 */
    ob->row    = row;
    ob->keys   = malloc(sizeof(void *) * nob);           /* FREE ME 006 */
    bzero(ob->keys, sizeof(void *) * nob);
    ob->apk    = NULL;
    ob->lruc   = NULL;
    return ob;
}
static void free_obsl_key(obsl_t *ob, int i) {
    if (C_IS_S(OB_ctype[i])) {
        if (ob->keys[i]) free(ob->keys[i]); ob->keys[i] = NULL; /* FREED 003 */
    }
}
void destroy_obsl(obsl_t *ob, bool ofree) {
    for (uint32 i = 0; i < OB_nob; i++) free_obsl_key(ob, i);
    if (ob->row) {
        if (     ofree == OBY_FREE_ROBJ) destroyCloneRobj(ob->row);/*DESTED005*/
        else if (ofree == OBY_FREE_AOBJ) destroyAobj(ob->row); /*DESTROYED 029*/
    }
    if (ob->apk) destroyAobj(ob->apk);                   /* DESTROYED 071 */
    free(ob->keys);                                      /* FREED 006 */
    free(ob);                                            /* FREED 001 */
}
obsl_t * cloneOb(obsl_t *ob, uint32 nob) { /* JOIN's API */
    obsl_t *ob2  = create_obsl(NULL, nob);               /* FREE ME 001 */
    for (uint32 i = 0; i < nob; i++) {
        if (!ob->keys[i]) continue;
        if (C_IS_S(OB_ctype[i])) ob2->keys[i] = _strdup(ob->keys[i]);
        else                     ob2->keys[i] = ob->keys[i];
    }
    if (ob->apk) ob2->apk = cloneAobj(ob->apk);          /* DESTROY ME 071 */
    ob2->lruc = ob->lruc;
    return ob2;
}

void assignObEmptyKey(obsl_t *ob, uchar ctype, int i) {
    if      (C_IS_NUM(ctype)) ob->keys[i] = 0;
    else if (C_IS_F  (ctype)) memcpy(&ob->keys[i], &Fmin, FSIZE);
    else /*  C_IS_S()   */    ob->keys[i] = NULL;
}
void assignObKey(wob_t *wb, bt *btr, void *rrow, aobj *apk, int i, obsl_t *ob) {
    void  *key;
    uchar  ctype = Tbl[wb->obt[i]].col_type[wb->obc[i]];
    aobj   ao    = getCol(btr, rrow, wb->obc[i], apk, wb->obt[i]);
    if        (C_IS_I(ctype)) {
        key = (void *)(long)ao.i;
    } else if (C_IS_L(ctype)) {
        key = (void *)ao.l;
    } else if (C_IS_F(ctype)) {
        memcpy(&(key), &ao.f, FSIZE);
    } else {/* C_IS_S() */
        char *s   = malloc(ao.len + 1);                  /* FREE ME 003 */
        memcpy(s, ao.s, ao.len); /* memcpy needed ao.s maybe decoded(freeme) */
        s[ao.len] = '\0';
        key       = s;
    }
    releaseAobj(&ao);
    ob->keys[i] = key;
}
/* Range Query API */
void addRow2OBList(list   *ll,    wob_t  *wb,   bt     *btr, void   *r,
                   bool    ofree, void   *rrow, aobj   *apk) {
    void   *row;
    int     tmatch = wb->obt[0]; /* function ONLY FOR RANGE_QEURIES */
    if (ofree == OBY_FREE_ROBJ)   row = cloneRobj((robj *)r); /* DEST 005 */
    else /*      OBY_FREE_AOBJ */ row = cloneAobj((aobj *)r); /* DEST 029 */
    obsl_t *ob  = create_obsl(row, wb->nob);             /* FREE ME 001 */
    for (uint32 i = 0; i < wb->nob; i++) assignObKey(wb, btr, rrow, apk, i, ob);
    ob->apk = cloneAobj(apk);                           /* DESTROY ME 071 */
    GET_LRUC ob->lruc = lruc; /* NOTE: updateLRU (SELECT ORDER BY) */
    listAddNodeTail(ll, ob);
}
obsl_t **sortOB2Vector(list *ll) {
    listNode  *ln;
    int        vlen   = listLength(ll);
    obsl_t   **vector = malloc(sizeof(obsl_t *) * vlen); /* FREE ME 004 */
    int        j      = 0;
    listIter *li      = listGetIterator(ll, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        vector[j] = (obsl_t *)ln->value;
        j++;
    } listReleaseIterator(li);
    qsort(vector, vlen, sizeof(obsl_t *), genOBsort);
    return vector;
}
void sortOBCleanup(obsl_t **vector, int vlen, bool ofree) {
    for (int i = 0; i < vlen; i++) {
        destroy_obsl((obsl_t *)vector[i], ofree);
    }
}

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpObKey(printer *prn, int i, void *key, uchar ctype) {
    if        (C_IS_I(ctype)) {
        (*prn)("\t\t%d: i: %d\n", i, (int)(long)key);
    } else if (C_IS_L(ctype)) {
        (*prn)("\t\t%d: i: %ld\n", i, (long)key);
    } else if (C_IS_S(ctype)) {
        (*prn)("\t\t%d: s: %s\n", i, (char *)key);
    } else {/* COL_TYPE_FLOAT */ 
        float f;
        memcpy(&f, &key, FSIZE);
        (*prn)("\t\t%d: f: %f\n", i, f);
    }
}
void dumpOb(printer *prn, obsl_t *ob) {
    (*prn)("\tdumpOB START: nob: %u\n", OB_nob);
    for (uint32 i = 0; i < OB_nob; i++) {
        dumpObKey(prn, i, ob->keys[i], OB_ctype[i]);
    }
    (*prn)("\tEND dumpOB\n");
}

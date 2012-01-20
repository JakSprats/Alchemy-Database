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
#include <assert.h>

#include "redis.h"
#include "adlist.h"

#include "stream.h"
#include "lru.h"
#include "lfu.h"
#include "row.h"
#include "rpipe.h"
#include "parser.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "orderby.h"

extern r_tbl_t *Tbl;
extern robj    *CurrError;  // NOTE: for deeply nested errors
extern long     CurrCard;   // NOTE: to report when nested errors happened


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
static int u128OrderBySort(const void *s1, const void *s2) {
    uint128 x1 = *((uint128 *)s1); uint128 x2 = *((uint128 *)s2);
    return x1 == x2 ? 0 : (x1 > x2) ? 1 : -1;
}
static int u128OrderByRevSort(const void *s1, const void *s2) {
    uint128 x1 = *((uint128 *)s1); uint128 x2 = *((uint128 *)s2);
    return x1 == x2 ? 0 : (x1 > x2) ? -1 : 1;
}
static int floatOrderBySort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, FSIZE); memcpy(&f2, &s2, FSIZE);
    float   f  = f1 - f2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
static int floatOrderByRevSort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, FSIZE); memcpy(&f2, &s2, FSIZE);
    float   f  = f2 - f1;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
static int stringOrderBySort(const void *s1, const void *s2) {
    char *c1 = (char *)s1; char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c1, c2) : c1 - c2; /* strcmp() not ok w/ NULLs */
}
static int stringOrderByRevSort(const void *s1, const void *s2) {
    char *c1 = (char *)s1; char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c2, c1) : c2 - c1; /* strcmp() not ok w/ NULLs */
}

typedef int ob_cmp(const void *, const void *);
/* slot = OB_ctype[j] * 2 + OB_asc[j] */
ob_cmp (*OB_cmp[14]) = { NULL,                 NULL,               // CTYPE: 0
                         intOrderByRevSort,    intOrderBySort,     // CTYPE: 1
                         ulongOrderByRevSort,  ulongOrderBySort,   // CTYPE: 2
                         stringOrderByRevSort, stringOrderBySort,  // CTYPE: 3
                         floatOrderByRevSort,  floatOrderBySort,   // CTYPE: 4
                         u128OrderByRevSort,   u128OrderBySort,    // CTYPE: 5
                         ulongOrderByRevSort,  ulongOrderBySort};  // CTYPE: 6

int genOBsort(const void *s1, const void *s2) {
    int     ret = 0;
    obsl_t *o1  = *(obsl_t **)s1; obsl_t *o2  = *(obsl_t **)s2;
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
            OB_ctype[i] = wb->le[i].yes ? COL_TYPE_LUAO :
                                    Tbl[wb->obt[i]].col[wb->obc[i].cmatch].type;
        }}                                                    // \/ DESTROY 009
    if (qed) {                                           return listCreate();
    } else if (rcrud) {
        list *ll = listCreate(); ll->free = destroyAobj; return ll;
    } else                                               return NULL;
}
void releaseOBsort(list *ll) {
    OB_nob = 0; if (ll) listRelease(ll);                 // DESTROYED 009
}

obsl_t *create_obsl(void *row, uint32 nob) {
    obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t));       /* FREE ME 001 */
    bzero(ob, sizeof(obsl_t));
    ob->row    = row;
    ob->keys   = malloc(sizeof(void *) * nob);           /* FREE ME 006 */
    return ob;
}
static void free_obsl_key(obsl_t *ob, int i) {
    if (C_IS_S(OB_ctype[i]) || C_IS_X(OB_ctype[i])) {
        if (ob->keys[i]) { free(ob->keys[i]); ob->keys[i] = NULL; } //FREED 003
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
        if        C_IS_S(OB_ctype[i]) ob2->keys[i] = _strdup(ob->keys[i]);
          else if C_IS_X(OB_ctype[i]) {
            ob2->keys[i] = malloc(16); memcpy(ob2->keys[i], ob->keys[i], 16);
        } else                        ob2->keys[i] = ob->keys[i];
    }
    if (ob->apk) ob2->apk = cloneAobj(ob->apk);          /* DESTROY ME 071 */
    ob2->lruc = ob->lruc; ob2->lrud = ob->lrud;
    ob2->lfuc = ob->lfuc; ob2->lfu  = ob->lfu;
    return ob2;
}

void assignObEmptyKey(obsl_t *ob, uchar ctype, int i) {
    if      C_IS_X  (ctype) ob->keys[i] = NULL;
    if      C_IS_NUM(ctype) ob->keys[i] = 0; // I or L
    else if C_IS_F  (ctype) memcpy(&ob->keys[i], &Fmin, FSIZE);
    else if C_IS_S  (ctype) ob->keys[i] = NULL;
    else            assert(!"assignObEmptyKey ERROR");
}

static bool assignObKeyLuaFunc(wob_t *wb, bt     *btr, void *rrow,   aobj *apk,
                               int    i,  obsl_t *ob,  int   tmatch) {
printf("OB LE: i: %d fname: %s ncols: %d\n", i, wb->le[i].fname, wb->le[i].ncols);
    void  *key;
    CLEAR_LUA_STACK lua_getglobal(server.lua, wb->le[i].fname);
    for (int k = 0; k < wb->le[i].ncols; k++) {
        pushColumnLua(btr, rrow, tmatch, wb->le[i].as[k], apk);
    }
    bool ret = 1;
    int  r   = lua_pcall(server.lua, wb->le[i].ncols, 1, 0);
    if (r) { ret = 0;
        CURR_ERR_CREATE_OBJ
        "-ERR: running ORDER BY FUNCTION (%s): %s [CARD: %ld]\r\n",
         wb->le[i].fname, lua_tostring(server.lua, -1), CurrCard));
    } else {
        int t = lua_type(server.lua, -1);
        if        (t == LUA_TNUMBER) {
            key = (void *)(long)lua_tonumber (server.lua, -1);
        } else if (t == LUA_TBOOLEAN) {
            key = (void *)(long)lua_toboolean(server.lua, -1);
        } else { ret = 0;
            CURR_ERR_CREATE_OBJ
            "-ERR: ORDER BY FUNCTION (%s): %s [CARD: %ld]\r\n",
             wb->le[i].fname, "use NUMBER or BOOLEAN return types", CurrCard));
        }
    }
    ob->keys[i] = key; CLEAR_LUA_STACK return ret;
}
bool assignObKey(wob_t *wb, bt     *btr, void *rrow,  aobj *apk,
                 int    i,  obsl_t *ob,  int   tmatch) {
    if (wb->le[i].yes) {
        return assignObKeyLuaFunc(wb, btr, rrow, apk, i, ob, tmatch);
    }
    void  *key; bool ret = 1;
    uchar  ctype = Tbl[wb->obt[i]].col[wb->obc[i].cmatch].type;
    //TODO this is a repetitive getCol() call
    aobj   ao    = getCol(btr, rrow, wb->obc[i], apk, wb->obt[i], NULL);
    if        C_IS_I(ctype) key = VOIDINT ao.i;
      else if C_IS_L(ctype) key = (void *)ao.l;
      else if C_IS_X(ctype) { uint128 *x = malloc(16); *x = ao.x; key = x; }
      else if C_IS_F(ctype) memcpy(&(key), &ao.f, FSIZE);
      else if C_IS_S(ctype) {
        char *s   = malloc(ao.len + 1);                  /* FREE ME 003 */
        memcpy(s, ao.s, ao.len); /* memcpy needed ao.s maybe decoded(freeme) */
        s[ao.len] = '\0';   key       = s;
    } else if (C_IS_O(ctype) && wb->obc[i].nlo) {
        CLEAR_LUA_STACK pushLuaVar(tmatch, wb->obc[i], apk);
        if        (lua_isnumber (server.lua, -1)) {
            key = VOIDINT lua_tonumber (server.lua, -1);
        } else if (lua_isboolean(server.lua, -1)) {
            key = VOIDINT lua_toboolean(server.lua, -1);
        } else if (lua_isnil    (server.lua, -1)) {
            key = VOIDINT 0;
        } else {
            CURR_ERR_CREATE_OBJ
            "-ERR: ORDER BY DOT-NOTATION: %s [CARD: %ld]\r\n",
             "unsupported return type", CurrCard)); ret = 0;
        }
        CLEAR_LUA_STACK
    } else assert(!"assignObKey ERROR");
    releaseAobj(&ao);
    ob->keys[i] = key;
    return ret;
}
/* Range Query API */
bool addRow2OBList(list   *ll,    wob_t  *wb,   bt     *btr, void  *r,
                   bool    ofree, void   *rrow, aobj   *apk) {
    void   *row;
    int     tmatch = wb->obt[0]; /* function ONLY FOR RANGE_QEURIES */
printf("addRow2OBList: wb: %p tmatch: %d\n", (void *)wb, tmatch);
    if (ofree == OBY_FREE_ROBJ)   row = cloneRobj((robj *)r); /* DEST 005 */
    else /*      OBY_FREE_AOBJ */ row = cloneAobj((aobj *)r); /* DEST 029 */
    obsl_t *ob  = create_obsl(row, wb->nob);                  /* FREE ME 001 */
    for (uint32 i = 0; i < wb->nob; i++) {
        if (!assignObKey(wb, btr, rrow, apk, i, ob, tmatch)) goto adr2oberr;
    }
    ob->apk = cloneAobj(apk);                                 /* FREED ME 071 */
    GET_LRUC ob->lruc = lruc; ob->lrud = lrud; // updateLRU (SELECT ORDER BY)
    GET_LFUC ob->lfuc = lfuc; ob->lfu  = lfu;  // updateLFU (SELECT ORDER BY)
    listAddNodeTail(ll, ob); return 1;

adr2oberr:
    destroy_obsl(ob, ofree); return 0;
}
obsl_t **sortOB2Vector(list *ll) {
    listNode  *ln;
    int        vlen   = listLength(ll);
    obsl_t   **vector = malloc(sizeof(obsl_t *) * vlen); /* FREE ME 004 */
    int        j      = 0;
    listIter *li      = listGetIterator(ll, AL_START_HEAD);
    while((ln = listNext(li))) {
        vector[j] = (obsl_t *)ln->value; j++;
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
    if        C_IS_I(ctype) (*prn)("\t\t%d: i: %d\n", i, (int)(long)key);
      else if C_IS_L(ctype) (*prn)("\t\t%d: i: %ld\n", i, (long)key);
      else if C_IS_S(ctype) (*prn)("\t\t%d: s: %s\n", i, (char *)key);
      else if C_IS_X(ctype) {
        (*prn)("\t\t%d: x: ", i); DEBUG_U128(prn, *((uint128 *)key))
        (*prn)("\n");
    } else {//C_IS_F
        float f; memcpy(&f, &key, FSIZE); (*prn)("\t\t%d: f: %f\n", i, f);
    }
}
void dumpOb(printer *prn, obsl_t *ob) {
    (*prn)("\tdumpOB START: nob: %u\n", OB_nob);
    for (uint32 i = 0; i < OB_nob; i++) {
        dumpObKey(prn, i, ob->keys[i], OB_ctype[i]);
    }
    (*prn)("\tEND dumpOB\n");
}

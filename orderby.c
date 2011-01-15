/*
 * This file implements "SELECT ... ORDER BY col LIMIT X OFFSET Y"
 *

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <float.h>
//#include <assert.h>

#include "redis.h"
#include "adlist.h"

#include "row.h"
#include "rpipe.h"
#include "parser.h"
#include "range.h"
#include "aobj.h"
#include "common.h"
#include "orderby.h"

// FROM redis.c
extern struct redisServer server;

/* GLOBALS */
extern void    *Order_by_col_val;
extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];

int   OB_nob   = 0;
bool  OB_asc  [MAX_ORDER_BY_COLS];
uchar OB_ctype[MAX_ORDER_BY_COLS];

int intOrderBySort(const void *s1, const void *s2) {
    return (long)s1 - (long)s2;
}
int intOrderByRevSort(const void *s1, const void *s2) {
    return (long)s2 - (long)s1;
}
int floatOrderBySort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, sizeof(float));
    memcpy(&f2, &s2, sizeof(float));
    float   f  = f1 - f2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
int floatOrderByRevSort(const void *s1, const void *s2) {
    float f1, f2;
    memcpy(&f1, &s1, sizeof(float));
    memcpy(&f2, &s2, sizeof(float));
    float   f  = f2 - f1;
    return (f == 0.0) ? 0 : ((f > 0.0) ? -1: 1);
}
int stringOrderBySort(const void *s1, const void *s2) {
    char *c1 = (char *)s1;
    char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c1, c2) : c1 - c2; /* strcmp() not ok w/ NULLs */
}
int stringOrderByRevSort(const void *s1, const void *s2) {
    char *c1 = (char *)s1;
    char *c2 = (char *)s2;
    return (c1 && c2) ? strcmp(c2, c1) : c2 - c1; /* strcmp() not ok w/ NULLs */
}

typedef int ob_cmp(const void *, const void *);
/* COL_TYPE_STRING=0, COL_TYPE_INT=1, COL_TYPE_FLOAT=2 */
/* slot = OB_ctype[j] * 2 + OB_asc[j] */
ob_cmp (*OB_cmp[6]) = {stringOrderByRevSort, stringOrderBySort,
                       intOrderByRevSort,    intOrderBySort,
                       floatOrderByRevSort,  floatOrderBySort};

int genOBsort(const void *s1, const void *s2) {
    int     ret = 0;
    obsl_t *o1  = *(obsl_t **)s1;
    obsl_t *o2  = *(obsl_t **)s2;
    for (int j = 0; j < OB_nob; j++) {
        ob_cmp *compar = OB_cmp[OB_ctype[j] * 2 + OB_asc[j]];
        ret            = (*compar)(o1->keys[j], o2->keys[j]);
        if (ret) return ret; /* return the first non-zero compar result */
    }
    return ret;
}

list *initOBsort(bool qed, cswc_t *w) {
    OB_nob = w->nob;
    if (OB_nob) {
        for (int i = 0; i < OB_nob; i++) {
            OB_asc[i]   = w->asc[i];
            OB_ctype[i] = Tbl[server.dbid][w->tmatch].col_type[w->obc[i]];
        }
    }
    return qed ? listCreate() : NULL ;                   /* DESTROY 009 */
}
void releaseOBsort(list *ll) {
    if (ll) listRelease(ll);                             /* DESTROYED 009 */
    OB_nob = 0;
}

obsl_t *create_obsl(void *row, int nob) {
    obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t));       /* FREE ME 001 */
    ob->row    = row;
    ob->keys   = malloc(sizeof(void *) * nob);           /* FREE ME 006 */
    return ob;
}
static void destroy_obsl(obsl_t *ob, bool ofree) {
    for (int i = 0; i < OB_nob; i++) {
        if (OB_ctype[i] == COL_TYPE_STRING) free(ob->keys[i]); /* FREED 003 */
    }
    if (     ofree == OBY_FREE_ROBJ) decrRefCount(ob->row);  /* DESTROYED 005 */
    else if (ofree == OBY_FREE_AOBJ) destroyAobj(ob->row);   /* DESTROYED 029 */
    free(ob->keys);                                      /* FREED 006 */
    free(ob);                                            /* FREED 001 */
}

void assignObKey(cswc_t *w, void *rrow, aobj *apk, int i, obsl_t *ob) {
    flag   cflag;
    void  *key;
    uchar  ctype = Tbl[server.dbid][w->tmatch].col_type[w->obc[i]];
    aobj   ao    = getRawCol(rrow, w->obc[i], apk, w->tmatch, &cflag, 0);
    if (ctype == COL_TYPE_INT) {
        key = (void *)(long)ao.i;
    } else if (ctype == COL_TYPE_FLOAT) {
        memcpy(&(key), &ao.f, sizeof(float));
    } else { /* memcpy needed here as ao.s may be sixbit encoded */
        char *s   = malloc(ao.len + 1);                  /* FREE ME 003 */
        memcpy(s, ao.s, ao.len);
        s[ao.len] = '\0';
        key       = s;
    }
    releaseAobj(&ao);
    ob->keys[i] = key;
}
void addRow2OBList(list   *ll,
                   cswc_t *w,
                   void   *r,
                   bool    ofree,
                   void   *rrow,
                   aobj   *apk) {
    void   *row;
    if (     ofree == OBY_FREE_ROBJ) row = cloneRobj((robj *)r); /* DEST 005 */
    else if (ofree == OBY_FREE_AOBJ) row = cloneAobj((aobj *)r); /* DEST 029 */
    //else assert(!"OBY_FREE not defined");
    obsl_t *ob  = create_obsl(row, w->nob);              /* FREE ME 001 */
    for (int i = 0; i < w->nob; i++) {
        assignObKey(w, rrow, apk, i, ob);
    }
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
    }
    listReleaseIterator(li);

    qsort(vector, vlen, sizeof(obsl_t *), genOBsort);
    return vector;
}

void sortOBCleanup(obsl_t **vector, int vlen, bool ofree) {
    for (int i = 0; i < vlen; i++) {
        destroy_obsl((obsl_t *)vector[i], ofree);
    }
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
void addJoinOutputRowToList(jrow_reply_t *r, void *resp) {
    obsl_t *ob  = create_obsl(resp, 1);             /* FREE ME 008 */
    ob->keys[0] = Order_by_col_val;
    listAddNodeTail(r->ll, ob);
}

long sortJoinOrderByAndReply(redisClient *c, build_jrow_reply_t *b, cswc_t *w) {
    listNode  *ln;
    int        vlen   = listLength(b->j.ll);
    obsl_t   **vector = malloc(sizeof(obsl_t *) * vlen); /* freed in function */
    int        j      = 0;
    listIter  *li     = listGetIterator(b->j.ll, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        vector[j] = (obsl_t *)ln->value;
        j++;
    }
    listReleaseIterator(li);

    qsort(vector, vlen, sizeof(obsl_t *), genOBsort);

    //TODO this is almost (opSelectOnSort + sortOBCleanup)
    long sent = 0;
    for (int k = 0; k < vlen; k++) {
        if (w->lim != -1 && sent == w->lim) break;
        if (w->ofst > 0) {
            w->ofst--;
        } else {
            sent++;
            obsl_t *ob = vector[k];
            addReplyBulk(c, ob->row);
            decrRefCount(ob->row);
        }
    }
    for (int k = 0; k < vlen; k++) {
        obsl_t *ob = vector[k];
        free(ob);               /* free malloc in addJoinOutputRowToList */
    }
    free(vector);
    return sent;
}

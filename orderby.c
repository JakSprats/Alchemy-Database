/*
 * This file implements "SELECT ... ORDER BY col LIMIT X OFFSET Y" helper funcs
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

#include "redis.h"
#include "adlist.h"

#include "row.h"
#include "rpipe.h"
#include "parser.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "orderby.h"

// FROM redis.c
#define RL4 redisLog(4,

/* GLOBALS */
extern char  *Order_by_col_val;


//TODO there is most likely too much register assignment in these functions
/* ORDER BY START */
int intOrderBySort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    int    *i1 = (int *)(o1->key);
    int    *i2 = (int *)(o2->key);
    return *i1 - *i2;
}
int intOrderByRevSort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    int    *i1 = (int *)(o1->key);
    int    *i2 = (int *)(o2->key);
    return *i2 - *i1;
}

int floatOrderBySort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    float  *i1 = (float *)(o1->key);
    float  *i2 = (float *)(o2->key);
    float   f  = *i1 - *i2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
int floatOrderByRevSort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    float  *i1 = (float *)(o1->key);
    float  *i2 = (float *)(o2->key);
    float   f  = *i1 - *i2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? -1: 1);
}

int stringOrderBySort(const void *s1, const void *s2) {
    obsl_t  *o1 = (obsl_t *)s1;
    obsl_t  *o2 = (obsl_t *)s2;
    char   **c1 = (char **)(o1->key);
    char   **c2 = (char **)(o2->key);
    char    *x1 = *c1;
    char    *x2 = *c2;
    return (x1 && x2) ? strcmp(x1, x2) : x1 - x2; /* strcmp() not ok w/ NULLs */
}
int stringOrderByRevSort(const void *s1, const void *s2) {
    obsl_t  *o1 = (obsl_t *)s1;
    obsl_t  *o2 = (obsl_t *)s2;
    char   **c1 = (char **)(o1->key);
    char   **c2 = (char **)(o2->key);
    char    *x1 = *c1;
    char    *x2 = *c2;
    return (x1 && x2) ? strcmp(x2, x1) : x2 - x1; /* strcmp() not ok w/ NULLs */
}

void addORowToRQList(list  *ll,
                     void  *r,
                     bool   is_robj,
                     void  *rrow,
                     int    obc,
                     aobj  *apk,
                     int    tmatch,
                     uchar  ctype) {
    flag cflag;
    obsl_t *ob  = (obsl_t *)malloc(sizeof(obsl_t));/*freed sortedOrdrByCleanup*/
    ob->row = is_robj ? cloneRobj((robj *)r) :
                        r; /* decRefCount()ed in sortedOrderByCleanup() */
    aobj ao = getRawCol(rrow, obc, apk, tmatch, &cflag, 0);
    if (ctype == COL_TYPE_INT) {
        ob->key   = (void *)(long)ao.i;
    } else if (ctype == COL_TYPE_FLOAT) {
        memcpy(&(ob->key), &ao.f, sizeof(float));
    } else {
        char *s   = malloc(ao.len + 1); /*free()d in sortedOrderByCleanup() */
        memcpy(s, ao.s, ao.len);
        s[ao.len] = '\0';
        ob->key   = s;
    }
    releaseAobj(&ao);
    listAddNodeTail(ll, ob);
}

obsl_t **sortOrderByToVector(list *ll, uchar ctype, bool asc) {
    listNode  *ln;
    listIter   li;
    int        vlen   = listLength(ll);
    obsl_t   **vector = malloc(sizeof(obsl_t *) * vlen); /* freed in function */
    int        j      = 0;
    listRewind(ll, &li);
    while((ln = listNext(&li))) {
        vector[j] = (obsl_t *)ln->value;
        j++;
    }
    if (ctype == COL_TYPE_INT) {
        asc ? qsort(vector, vlen, sizeof(obsl_t *), intOrderBySort) :
              qsort(vector, vlen, sizeof(obsl_t *), intOrderByRevSort);
    } else if (ctype == COL_TYPE_FLOAT) {
        asc ? qsort(vector, vlen, sizeof(obsl_t *), floatOrderBySort) :
              qsort(vector, vlen, sizeof(obsl_t *), floatOrderByRevSort);
    } else {
        asc ? qsort(vector, vlen, sizeof(obsl_t *), stringOrderBySort) :
              qsort(vector, vlen, sizeof(obsl_t *), stringOrderByRevSort);
    }
    return vector;
}

void sortedOrderByCleanup(obsl_t **vector,
                          int      vlen,
                          uchar    ctype,
                          bool     decr_row) {
    for (int k = 0; k < vlen; k++) {
        obsl_t *ob = vector[k];
        if (decr_row)                 decrRefCount(ob->row);
        if (ctype == COL_TYPE_STRING) free(        ob->key);
        free(ob);
    }
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
void addJoinOutputRowToList(jrow_reply_t *r, void *resp) {
    obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t));
    ob->row    = resp;
    if (r->ctype == COL_TYPE_INT) {
        ob->key = Order_by_col_val ? (void *)(long)atoi(Order_by_col_val) :
                                     (void *)-1; /* -1 for UINT */
    } else if (r->ctype == COL_TYPE_FLOAT) {
        float f = Order_by_col_val ? atof(Order_by_col_val) : FLT_MIN;
        memcpy(&(ob->key), &f, sizeof(float));
    } else if (r->ctype == COL_TYPE_STRING) {
        ob->key = Order_by_col_val;
    }
    listAddNodeTail(r->ll, ob);
}

int sortJoinOrderByAndReply(redisClient *c, build_jrow_reply_t *b, cswc_t *w) {
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
    if (       b->j.ctype == COL_TYPE_INT) {
        w->asc ? qsort(vector, vlen, sizeof(obsl_t *), intOrderBySort) :
                 qsort(vector, vlen, sizeof(obsl_t *), intOrderByRevSort);
    } else if (b->j.ctype == COL_TYPE_STRING) {
        w->asc ? qsort(vector, vlen, sizeof(obsl_t *), stringOrderBySort) :
                 qsort(vector, vlen, sizeof(obsl_t *), stringOrderByRevSort);
    } else if (b->j.ctype == COL_TYPE_FLOAT) {
        w->asc ? qsort(vector, vlen, sizeof(obsl_t *), floatOrderBySort) :
                 qsort(vector, vlen, sizeof(obsl_t *), floatOrderByRevSort);
    }
    int sent = 0;
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

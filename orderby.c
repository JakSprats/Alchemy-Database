/*
 * This file implements "SELECT ... ORDER BY col LIMIT X" helper funcs
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
#include "alsosql.h"
#include "store.h"
#include "parser.h"
#include "common.h"
#include "orderby.h"

// FROM redis.c
#define RL4 redisLog(4,

/* GLOBALS */
extern char  *Order_by_col_val;


/* ORDER BY START */
int intOrderBySort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    int    *i1 = (int *)(o1->val);
    int    *i2 = (int *)(o2->val);
    return *i1 - *i2;
}
int intOrderByRevSort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    int    *i1 = (int *)(o1->val);
    int    *i2 = (int *)(o2->val);
    return *i2 - *i1;
}

int floatOrderBySort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    float  *i1 = (float *)(o1->val);
    float  *i2 = (float *)(o2->val);
    float   f  = *i1 - *i2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
int floatOrderByRevSort(const void *s1, const void *s2) {
    obsl_t *o1 = (obsl_t *)s1;
    obsl_t *o2 = (obsl_t *)s2;
    float  *i1 = (float *)(o1->val);
    float  *i2 = (float *)(o2->val);
    float   f  = *i1 - *i2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? -1: 1);
}

int stringOrderBySort(const void *s1, const void *s2) {
    obsl_t  *o1 = (obsl_t *)s1;
    obsl_t  *o2 = (obsl_t *)s2;
    char   **c1 = (char **)(o1->val);
    char   **c2 = (char **)(o2->val);
    char    *x1 = *c1;
    char    *x2 = *c2;
    return (x1 && x2) ? strcmp(x1, x2) : x1 - x2; /* strcmp() not ok w/ NULLs */
}
int stringOrderByRevSort(const void *s1, const void *s2) {
    obsl_t  *o1 = (obsl_t *)s1;
    obsl_t  *o2 = (obsl_t *)s2;
    char   **c1 = (char **)(o1->val);
    char   **c2 = (char **)(o2->val);
    char    *x1 = *c1;
    char    *x2 = *c2;
    return (x1 && x2) ? strcmp(x2, x1) : x2 - x1; /* strcmp() not ok w/ NULLs */
}

void addORowToRQList(list  *ll,
                     robj  *r,
                     robj  *row,
                     int    obc,
                     robj  *pko,
                     int    tmatch,
                     uchar  ctype) {
    flag cflag;
    obsl_t *ob  = (obsl_t *)malloc(sizeof(obsl_t));/*freed sortedOrdrByCleanup*/
    if (r) {
        ob->row = cloneRobj(r); /*decrRefCount()d N sortedOrderByCleanup() */
    } else {
         /* used ONLY in istoreCommit SELECT PK ORDER BY notPK (preserve pk) */
        ob->row = row->ptr;
    }
    aobj    ao  = getRawCol(row, obc, pko, tmatch, &cflag, ctype, 0);
    if (ctype == COL_TYPE_INT) {
        ob->val   = (void *)(long)ao.i;
    } else if (ctype == COL_TYPE_FLOAT) {
        memcpy(&(ob->val), &ao.f, sizeof(float));
    } else {
        char *s   = malloc(ao.len + 1); /*free()d in sortedOrderByCleanup() */
        memcpy(s, ao.s, ao.len);
        s[ao.len] = '\0';
        ob->val   = s;
        if (ao.sixbit) free(ao.s); /* getRawCol() malloc()s sixbit strings */
    }
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
        if (ctype == COL_TYPE_STRING) free(ob->val);
        free(ob);
    }
}

/* ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE */
/* ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE ISTORE */
static robj IstoreOrderByRobj;
static void init_IstoreOrderByRobj() {
    IstoreOrderByRobj.type     = REDIS_ROW;
    IstoreOrderByRobj.encoding = REDIS_ENCODING_RAW;
    IstoreOrderByRobj.refcount = 1;
}

static robj *createObjFromCol(void *col, uchar ctype) {
    robj *r;
    if (ctype == COL_TYPE_INT) {
        r = createObject(REDIS_STRING, NULL);
        r->encoding = REDIS_ENCODING_INT;
        r->ptr      = col;
    } else if (ctype == COL_TYPE_FLOAT) {
        float f;
        memcpy(&f, col, sizeof(float));
        char buf[32];
        snprintf(buf, 31, "%10.10g", f);
        buf[31] = '\0';
        r = createStringObject(buf, strlen(buf));
    } else {
        r = createStringObject(col, strlen(col));
    }
    return r;
}

int sortedOrderByIstore(redisClient  *c,
                        cswc_t       *w,
                        redisClient  *fc,
                        int           tmatch,
                        int           cmatchs[],
                        int           qcols,
                        char         *nname,
                        bool          sub_pk,
                        int           nargc,
                        uchar         ctype,
                        obsl_t      **vector,
                        int           vlen) {
    static bool inited_IstoreOrderByRobj = 0;
    if (!inited_IstoreOrderByRobj) {
        init_IstoreOrderByRobj();
        inited_IstoreOrderByRobj = 1;
    }

    int sent = 0;
    for (int k = 0; k < vlen; k++) {
        if (w->lim != -1 && sent == w->lim) break;
        if (w->ofst > 0) {
            w->ofst--;
        } else {
            sent++;
            obsl_t *ob            = vector[k];
            IstoreOrderByRobj.ptr = ob->row;
            robj *key             = createObjFromCol(ob->val, ctype);
            robj *row             = &IstoreOrderByRobj;
            if (!istoreAction(c, fc, tmatch, cmatchs, qcols, w->sto,
                              key, row, nname, sub_pk, nargc))
                                  return -1;
        }
    }
    return sent;
}


/* JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE */
/* JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE JOIN_STORE */

void addJoinOutputRowToList(jrow_reply_t *r, void *resp) {
    obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t));
    ob->row    = resp;
    if (r->ctype == COL_TYPE_INT) {
        ob->val = Order_by_col_val ? (void *)(long)atoi(Order_by_col_val) :
                                     (void *)-1; /* -1 for UINT */
    } else if (r->ctype == COL_TYPE_FLOAT) {
        float f = Order_by_col_val ? atof(Order_by_col_val) : FLT_MIN;
        memcpy(&(ob->val), &f, sizeof(float));
    } else if (r->ctype == COL_TYPE_STRING) {
        ob->val = Order_by_col_val;
    }
    listAddNodeTail(r->ll, ob);
}

int sortJoinOrderByAndReply(redisClient        *c,
                                    build_jrow_reply_t *b,
                                    cswc_t             *w) {
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
            if (b->j.sto != -1) { /* JSTORE */
                b->j.fc->argv = ob->row; /* argv's in list */
                if (!performStoreCmdOrReply(b->j.c, b->j.fc, b->j.sto, 1))
                    return -1;
            } else if (!b->j.cstar) {
                addReplyBulk(c, ob->row);
                decrRefCount(ob->row);
            }
        }
    }
    for (int k = 0; k < vlen; k++) {
        obsl_t *ob = vector[k];
        free(ob);               /* free malloc in addJoinOutputRowToList */
    }
    free(vector);
    return sent;
}

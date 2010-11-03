/*
 * This file implements "SELECT ... ORDER BY col LIMIT X" helper funcs
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "redis.h"
#include "adlist.h"

#include "row.h"
#include "common.h"
#include "alsosql.h"
#include "orderby.h"

// FROM redis.c
#define RL4 redisLog(4,

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

void addORowToRQList(list *ll,
                     robj *r,
                     robj *row,
                     int   obc,
                     robj *pko,
                     int   tmatch,
                     bool  icol) {
    flag cflag;
    obsl_t *ob  = (obsl_t *)malloc(sizeof(obsl_t));/*freed sortedOrdrByCleanup*/
    if (r) {
        ob->row = cloneRobj(r); /*decrRefCount()d N sortedOrderByCleanup() */
    } else {
        ob->row = row->ptr; /* used ONLY in istoreCommit */
    }
    aobj    ao  = getRawCol(row, obc, pko, tmatch, &cflag, icol, 0);
    if (icol) {
        ob->val   = (void *)(long)ao.i;
    } else {
        char *s   = malloc(ao.len + 1); /*free()d in sortedOrderByCleanup() */
        memcpy(s, ao.s, ao.len);
        s[ao.len] = '\0';
        ob->val   = s;
        if (ao.sixbit) free(ao.s); /* getRawCol() malloc()s sixbit strings */
    }
    listAddNodeTail(ll, ob);
}

obsl_t **sortOrderByToVector(list *ll, bool icol, bool asc) {
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
    if (icol) {
        asc ? qsort(vector, vlen, sizeof(obsl_t *), intOrderBySort) :
              qsort(vector, vlen, sizeof(obsl_t *), intOrderByRevSort);
    } else {
        asc ? qsort(vector, vlen, sizeof(obsl_t *), stringOrderBySort) :
              qsort(vector, vlen, sizeof(obsl_t *), stringOrderByRevSort);
    }
    return vector;
}

void sortedOrderByCleanup(obsl_t **vector,
                          int      vlen,
                          bool     icol,
                          bool     decr_row) {
    for (int k = 0; k < vlen; k++) {
        obsl_t *ob = vector[k];
        if (decr_row) decrRefCount(ob->row);
        if (!icol)    free(ob->val);
        free(ob);
    }
}

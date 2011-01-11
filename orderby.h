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

#ifndef __ORDER_BY__H
#define __ORDER_BY__H

#include "join.h"
#include "aobj.h"

list *initOBsort(bool qed, cswc_t *w);
void releaseOBsort(list *ll);

typedef struct order_by_sort_element { /* INT(4) PTR*3(24) -> 28B */
    void  *row;
    void **keys;
} obsl_t;
obsl_t *create_obsl(void *row, int nob);

void assignObKey(cswc_t *w, void *rrow, aobj *apk, int i, obsl_t *ob);
void addRow2OBList(list    *ll,
                   cswc_t  *w,
                   void    *r,
                   bool     is_robj,
                   void    *rrow,
                   aobj    *apk);

obsl_t **sortOB2Vector(list *ll);

void sortOBCleanup(obsl_t **vector,
                   int      vlen,
                   bool     decr_row);

/* JOIN */
void addJoinOutputRowToList(jrow_reply_t *r, void *resp);
int sortJoinOrderByAndReply(redisClient *c, build_jrow_reply_t *b, cswc_t *w);

#endif /* __ORDER_BY__H */ 

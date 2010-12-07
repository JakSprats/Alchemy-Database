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

typedef struct order_by_sort_element {
    void *val;
    void *row;
} obsl_t;

int intOrderBySort(      const void *s1, const void *s2);
int intOrderByRevSort(   const void *s1, const void *s2);
int floatOrderBySort(    const void *s1, const void *s2);
int floatOrderByRevSort( const void *s1, const void *s2);
int stringOrderBySort(   const void *s1, const void *s2);
int stringOrderByRevSort(const void *s1, const void *s2);

void addORowToRQList(list  *ll,
                     robj  *r,
                     robj  *row,
                     int    obc,
                     robj  *pko,
                     int    tmatch,
                     uchar  ctype);

obsl_t **sortOrderByToVector(list *ll, uchar ctype, bool asc);

void sortedOrderByCleanup(obsl_t **vector,
                          int      vlen,
                          uchar    ctype,
                          bool     decr_row);

/* for ISTORE */
int sortedOrderByIstore(redisClient  *c,
                        cswc_t       *w,    
                        redisClient  *fc,
                        int           cmatchs[],
                        int           qcols, 
                        char         *nname,
                        bool          sub_pk,
                        int           nargc, 
                        uchar         ctype, 
                        obsl_t      **vector,
                        int           vlen);

/* for JoinStore */
void addJoinOutputRowToList(jrow_reply_t *r, void *resp);

int sortJoinOrderByAndReply(redisClient *c, build_jrow_reply_t *b, cswc_t *w);

#endif /* __ORDER_BY__H */ 

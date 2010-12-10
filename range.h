/*
 * This file implements Range OPS (iselect, idelete, iupdate) for AlchemyDB
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

#ifndef __RANGE__H
#define __RANGE__H

#include "redis.h"

#include "alsosql.h"
#include "btreepriv.h"
#include "parser.h"
#include "common.h"

/* NOTE: the range_* structs only hold pointers and ints, i.e. no destructor */
/*       IMPORTANT: range_* structs are just skeletons,
                    i.e. not to be changed after initialization, just derefed */
typedef struct range_store {
    bool         sub_pk;
    redisClient *fc;
    char        *nname;
    int          nargc;
} rs_t;

typedef struct range_select {
    bool cstar;
    int  qcols;
    int  *cmatchs; /*cmatchs[MAX_COLUMN_PER_TABLE] */
} rsel_t;

typedef struct range_common {
    uchar        ctype;
    bool         qed;
    redisClient *c;
    cswc_t      *w;
    list        *ll;
} rcomm_t;

typedef struct range {
    rcomm_t co;
    rsel_t  se;
    rs_t    st;
} range_t;


void init_range(range_t *g, redisClient *c, cswc_t *w, list *ll, uchar ctype);

typedef bool row_op(range_t *g, robj *key, robj *row, bool q);
long rangeOp(range_t *g, row_op *p); /* RangeQuery */
long inOp(range_t *g, row_op *p);    /* InQuery */

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar);

void ideleteAction(redisClient *c,
                   cswc_t      *w);

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]);

#endif /* __RANGE__H */ 

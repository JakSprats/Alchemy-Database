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

#include "btree.h"
#include "row.h"
#include "parser.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

typedef struct queue_range_results {
    bool pk;     /* range query on pk, ORDER BY col not pk */
    bool pk_lim; /* pk LIMIT OFFSET */
    bool pk_lo;  /* WHERE pk BETWEEN x AND Y ORDER BY pk LIMIT OFFSET */
    bool fk;     /* range query on fk, ORDER BY col not fk */
    bool fk_lim; /* fk LIMIT OFFSET */
    bool fk_lo;  /* WHERE fk = x ORDER BY fk LIMIT OFFSET */
    bool qed;    /* an additional sort will be required -> queued */
} qr_t;

/* NOTE: the range_* structs only hold pointers and ints, i.e. no destructor */
/*       IMPORTANT: range_* structs are just skeletons,
                    i.e. not to be changed after initialization, just derefed */
typedef struct range_select {
    bool cstar;
    int  qcols;
    int  *cmatchs; /*cmatchs[MAX_COLUMN_PER_TABLE] */
} rsel_t;

typedef struct range_update {
    bt      *btr;
    int      ncols;
    int      matches;
    int     *indices;
    char   **vals;
    uint32  *vlens;
    uchar   *cmiss;
    ue_t    *ue;
} rup_t;

#define OBY_FREE_ROBJ 1
#define OBY_FREE_AOBJ 2
typedef struct range_common {
    redisClient *c;
    bt          *btr;
    cswc_t      *w;
    list        *ll;
    uchar        ofree; /* order by sorting needs to free [robj,aobj] */
} rcomm_t;

typedef struct range {
    rcomm_t co;
    rsel_t  se;
    rup_t   up;
    qr_t    *q;
} range_t;

void setQueued(cswc_t *w, qr_t *q);

bool passFilters(bt *btr, void *rrow, list *flist, int tmatch);

void opSelectOnSort(redisClient *c,
                    list        *ll,
                    cswc_t      *w,
                    uchar        ofree,
                    long        *sent);
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
                   char        *vals   [],
                   uint32       vlens  [],
                   uchar        cmiss  [],
                   ue_t         ue     []);

#endif /* __RANGE__H */ 

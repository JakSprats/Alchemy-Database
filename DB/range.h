/*
 * This file implements Range OPS (iselect, idelete, iupdate) for ALCHEMY_DATABASE
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

#ifndef __RANGE__H
#define __RANGE__H

#include "redis.h"

#include "btree.h"
#include "row.h"
#include "orderby.h"
#include "colparse.h"
#include "parser.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

void initX_DB_Range(); /* NOTE: called on server startup */

typedef struct queue_range_results {
    bool pk;       /* range query on pk, ORDER BY col not pk */
    bool pk_lim;   /* pk LIMIT OFFSET */
    bool pk_lo;    /* WHERE pk BETWEEN x AND Y ORDER BY pk LIMIT OFFSET */
    bool pk_desc;  /* PK DESC iterator */
    bool fk;       /* range query on fk, ORDER BY col not fk */
    bool fk_lim;   /* fk LIMIT OFFSET */
    bool fk_lo;    /* WHERE fk = x ORDER BY fk LIMIT OFFSET */
    bool fk_desc;  /* FK DESC iterator */
    bool inr_desc; /* ORDER BY FK DESC, PK DESC */
    bool qed;      /* an additional sort will be required -> queued */
} qr_t;

/* NOTE: range_* structs are just skeletons,
               i.e. not to be changed after initialization, just derefed */
typedef struct range_select {
    bool cstar;
    int  qcols;
    int  *cmatchs;
} rsel_t;

typedef struct range_update {
    bt       *btr;
    int       ncols;
    int       matches;
    int      *indices;
    char   **vals;
    uint32   *vlens;
    uchar    *cmiss;
    ue_t     *ue;
    lue_t    *le;
    bool      upi;
} rup_t;

typedef struct range_common {
    redisClient *c;
    bt          *btr;
    cswc_t      *w;
    wob_t       *wb;
    list        *ll;
    uchar        ofree; /* order by sorting needs to free [robj,aobj] */
    uint32       lvl;   /* level of recursion WHILE joining */
} rcomm_t;

typedef struct range {
    rcomm_t co;
    rsel_t  se;
    rup_t   up;
    qr_t    *q;
    jb_t    *jb;
    bool     asc;
} range_t;

void init_range(range_t *g, redisClient *c,  cswc_t *w,     wob_t *wb,
                qr_t    *q, list        *ll, uchar   ofree, jb_t  *jb);

typedef bool row_op(range_t *g, aobj *apk, void *rrow, bool q, long *card);
long keyOp(range_t *g, row_op *p); // Also Used in JOINs
long Op(range_t *g, row_op *p);    // Also Used in JOINs

bool passFilters(bt *btr, aobj *akey, void *rrow, list *flist, int tmatch);

bool opSelectSort(cli  *c,    list *ll,   wob_t *wb,
                  bool ofree, long *sent, int    tmatch);

void iselectAction(cli *c,         cswc_t *w,     wob_t *wb,
                   int  cmatchs[], int     qcols, bool   cstar);

void ideleteAction(cli *c,         cswc_t *w,       wob_t *wb);

void iupdateAction(cli  *c,        cswc_t *w,       wob_t *wb,
                   int   ncols,    int     matches, int    inds[],
                   char *vals[],   uint32  vlens[], uchar  cmiss[],
                   ue_t  ue[],     lue_t  *le,      bool   upi);

void setQueued (              cswc_t *w, wob_t *wb, qr_t *q);
void dumpQueued(printer *prn, cswc_t *w, wob_t *wb, qr_t *q, bool debug);

#endif /* __RANGE__H */ 

/*
 * This file implements LRU Indexes
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

#include "sds.h"
#include "redis.h"

#include "bt.h"
#include "row.h"
#include "stream.h"
#include "index.h"
#include "alsosql.h"
#include "colparse.h"
#include "parser.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "lru.h"

// GLOBALS
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern int      Num_indx;
extern r_ind_t  Index[MAX_NUM_INDICES];

#define DEBUG_GET_LRU \
  printf("t: %u tmod: %u A: %d B: %d\n", \
    t, tmod, (rt->n_intr == ThresAccess), rt->lastts);

/* NOTE: increasing "IntervalSecs" makes sense for low traffic use-cases
         all requests falling into this many seconds will have the same lru */
uint32 IntervalSecs = 5;
/* NOTE: on more requests per interval than "ThresAccess"
         a new interval will be made,
         starting at now and spanning until the end of the old interval */
uint32 ThresAccess  = 55; /* 55 ->memory optimal for the BTREE */
uint32 LastT        = 0;

uint32 getLru(int tmatch) {
    r_tbl_t *rt   = &Tbl[tmatch];
    uint32   t    = time(NULL) - SERVER_BEGINNING_OF_TIME;
    uint32   tmod = t - (t % IntervalSecs);
    if ((LastT + IntervalSecs) < t) rt->n_intr = 0; 
    LastT         = t;
    rt->n_intr++;                                               //DEBUG_GET_LRU
    if (rt->n_intr == ThresAccess) {
        rt->lastts = t;
        rt->nextts = t + IntervalSecs;
        rt->n_intr = 0;                                       return t;
    } else if (rt->lastts) {
        if (t < rt->nextts)                                   return rt->lastts;
        else                { rt->lastts = 0; rt->n_intr = 0; return tmod; }
    } else                                                    return tmod;
}
void createLruIndex(cli *c) {
    if (strcasecmp(c->argv[2]->ptr, "ON")) {
        addReply(c, shared.createsyntax); return;
    }
    int    len   = sdslen(c->argv[3]->ptr);
    char  *tname = rem_backticks(c->argv[3]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    if (OTHER_BT(getBtr(tmatch))) { addReply(c, shared.lru_other); return; }
    r_tbl_t *rt  = &Tbl[tmatch];
    if (rt->lrud) { addReply(c, shared.lru_repeat); return; }
    int  imatch  = Num_indx;
    sds  iname   = P_SDS_EMT "%s_%s", LRUINDEX_DELIM, tname); /* DEST 072 */
    bool ok      = newIndex(c, iname, tmatch, rt->col_count, NULL,
                             CONSTRAINT_NONE, 0, 1, NULL); 
    sdsfree(iname);                                            /*DESTROYED 072*/
    if (!ok) return;
    rt->lrui     = imatch;
    rt->lruc     = rt->col_count;
    addColumn(tmatch, "LRU", COL_TYPE_INT);
    rt->lrud     = (uint32)getLru(tmatch);
    addReply(c, shared.ok);
}
void updateLru(cli *c, int tmatch, aobj *apk, uchar *lruc) {
    if (tmatch == -1)      return; /* from JOIN opSelectOnSort */
    if (!Tbl[tmatch].lrud) return;
    if (c->LruColInSelect) return; /* NOTE: otherwise TOO cyclical */
    r_tbl_t *rt = &Tbl[tmatch];
    if (lruc) {
        uint32  oltime = streamLRUToUInt(lruc);
        uint32  nltime = getLru(tmatch);
        if (oltime == nltime) return;
        overwriteLRUcol(lruc, nltime);
        bt     *ibtr   = getIBtr(rt->lrui);
        int     pktyp  = Tbl[tmatch].col_type[0];
        aobj ocol; initAobjInt(&ocol, oltime);
        aobj ncol; initAobjInt(&ncol, nltime); 
        upIndex(ibtr, apk, &ocol, apk, &ncol, pktyp);
        releaseAobj(&ocol); releaseAobj(&ncol);
    } else { /* LRU empty -> run "UPDATE tbl SET LRU = now WHERE PK = apk" */
        char     LruBuf[32]; snprintf(LruBuf, 32, "%u", getLru(tmatch));
        MATCH_INDICES(tmatch)
        int      ncols = rt->col_count;
        uchar    cmiss[ncols]; ue_t    ue   [ncols];
        char    *vals [ncols]; uint32  vlens[ncols];
        for (int i = 0; i < ncols; i++) {
            ue[i].yes = 0;
            if (i == rt->lruc) {
                cmiss[i] = 0;
                vals [i] = LruBuf; vlens[i] = strlen(LruBuf);
            } else cmiss[i] = 1;
        }
        bt      *btr   = getBtr(tmatch);
        void    *row   = btFind(btr, apk);
        updateRow(c, btr, apk, row, tmatch, ncols, matches, inds,
                  vals, vlens, cmiss, ue); /* NOTE: ALWAYS succeeds */
    }
}

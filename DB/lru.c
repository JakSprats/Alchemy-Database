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
#include "colparse.h"
#include "parser.h"
#include "index.h"
#include "ddl.h"
#include "find.h"
#include "alsosql.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "lru.h"

/* TODO LIST LRU
  1.) LRU indexes should be UNIQUE U128 indexes [lru|pk]
*/

// GLOBALS
extern r_tbl_t *Tbl;
extern int      Num_indx; extern r_ind_t *Index;

#define DEBUG_GET_LRU \
  printf("t: %u tmod: %u A: %d B: %d\n", \
    t, tmod, (rt->n_intr == ThresAccess), rt->lastts);

/* NOTE: increasing "IntervalSecs" makes sense for low traffic use-cases
         all requests falling into this many seconds will have the same lru */
uint32 IntervalSecs = 5;
/* NOTE: on more requests per interval than "ThresAccess"
         a new interval will be made,
         starting at now and spanning until the end of the old interval */
uint32 ThresAccess  = 55; /* 55 -> memory optimal for the BTREE */
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
    int      len   = sdslen(c->argv[3]->ptr);
    char    *tname = rem_backticks(c->argv[3]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    if (OTHER_BT(getBtr(tmatch))) { addReply(c, shared.lru_other); return; }
    r_tbl_t *rt    = &Tbl[tmatch];
    if (rt->lrud) { addReply(c, shared.lru_repeat); return; }

    rt->lrud       = (uint32)getLru(tmatch);
    rt->lruc       = rt->col_count; // -> new Column
    addColumn(tmatch, "LRU", COL_TYPE_INT);

    sds  iname     = P_SDS_EMT "%s_%s", LRUINDEX_DELIM, tname); /* DEST 072 */
    DECLARE_ICOL(lruic, rt->lruc) DECLARE_ICOL(ic, -1)
    rt->lrui       = newIndex(c, iname, tmatch, lruic, NULL, 0, 0, 1, NULL,
                              ic, 0, 0, 0, NULL, NULL, NULL); //Cant fail
    sdsfree(iname);                                            /*DESTROYED 072*/
    addReply(c, shared.ok);
}

#define DEBUG_UPDATE_LRU \
  printf("updateLru: tmatch: %d lruc: %p lrud: %d LruColInSelect: %d apk: ", \
         tmatch, lruc, lrud, c? c->LruColInSelect : 0);                      \
  if (apk) dumpAobj(printf, apk); else printf("\n");

void updateLru(cli *c, int tmatch, aobj *apk, uchar *lruc, bool lrud) {
    //DEBUG_UPDATE_LRU
    if (!lrud)                return;
    if (tmatch == -1)         return; /* from JOIN opSelectSort */
    if (c->LruColInSelect)    return; /* NOTE: otherwise TOO cyclical */
    r_tbl_t *rt     = &Tbl[tmatch];
    int      imatch = rt->lrui;
    if (lruc) { //printf("updateLru: LRU -> lruc update\n");
        uint32  oltime = streamLRUToUInt(lruc);
        uint32  nltime = getLru(tmatch);
        if (oltime == nltime) return;
        overwriteLRUcol(lruc, nltime);
        bt     *ibtr   = getIBtr(rt->lrui);
        int     pktyp  = rt->col[0].type;
        aobj ocol; initAobjInt(&ocol, oltime);
        aobj ncol; initAobjInt(&ncol, nltime); 
        upIndex(c, ibtr, apk, &ocol, apk, &ncol, pktyp, NULL, NULL, imatch);
        releaseAobj(&ocol); releaseAobj(&ncol);
    } else { /* LRU empty -> run "UPDATE tbl SET LRU = now WHERE PK = apk" */
        char     LruBuf[32]; snprintf(LruBuf, 32, "%u", getLru(tmatch));
        MATCH_INDICES(tmatch)
        int      ncols  = rt->col_count;
        uchar    cmiss[ncols]; ue_t    ue   [ncols]; lue_t le[ncols];
        char    *vals [ncols]; uint32  vlens[ncols];
        for (int i = 0; i < ncols; i++) {
            ue[i].yes = 0; le[i].yes = 0;
            if (i == rt->lruc) {
                cmiss[i] = 0; vals [i] = LruBuf; vlens[i] = strlen(LruBuf);
            } else cmiss[i] = 1;
        }
        bt      *btr   = getBtr(tmatch);
        void    *rrow  = btFind(btr, apk); // apk has NOT been evicted
        uc_t uc;
        init_uc(&uc, btr, tmatch, ncols, matches, inds, vals, vlens, cmiss,
                ue,  le);
        updateRow(c, &uc, apk, rrow, 0); /* NOTE: ALWAYS succeeds */
        //NOTE: rrow is no longer valid, updateRow() can change it
        release_uc(&uc);
    }
}

inline bool initLRUCS(int tmatch, icol_t *ics, int qcols) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->lrud) {
        for (int i = 0; i < qcols; i++) if (ics[i].cmatch == rt->lruc) return 1;
    }
    return 0;
}
inline bool initL_LRUCS(int tmatch, list *cs) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->lrud) {
        listIter *li = listGetIterator(cs, AL_START_HEAD); listNode *ln;
        while((ln = listNext(li))) {
            icol_t *ic = ln->value; if (ic->cmatch == rt->lruc) return 1;
        } listReleaseIterator(li);
    }
    return 0;
}
inline bool initLRUCS_J(jb_t *jb) {
    for (int i = 0; i < jb->qcols; i++) {
        if (Tbl[jb->js[i].t].lruc == jb->js[i].c) return 1;
    }
    return 0;
}


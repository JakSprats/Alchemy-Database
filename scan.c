/*
 * This file implements the "SCANSELECT"
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

#include "bt_iterator.h"
#include "row.h"
#include "wc.h"
#include "orderby.h"
#include "index.h"
#include "colparse.h"
#include "range.h"
#include "alsosql.h"
#include "aobj.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern r_tbl_t Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// TODO merge w/ struct range
typedef struct filter_row {
    bool           nowc;
    qr_t          *q;
    int            qcols;
    int           *cmatchs;
    bool           cstar;
    redisClient   *c;
    cswc_t        *w;
    list          *ll;
    bool           orobj;
} fr_t;

static void init_filter_row(fr_t *fr, redisClient *c, cswc_t *w, qr_t *q,
                            int qcols, int *cmatchs, bool nowc,
                            list *ll, bool cstar, bool orobj) {
    fr->c       = c;
    fr->w       = w;
    fr->q       = q;
    fr->qcols   = qcols;
    fr->cmatchs = cmatchs;
    fr->nowc    = nowc;
    fr->ll      = ll;
    fr->cstar   = cstar;
    fr->orobj   = orobj;
}


static void condSelectReply(fr_t *fr, aobj *akey, void *rrow, ulong *card) {
    uchar hit = 0;
    if (fr->nowc) hit = 1;
    else          hit = passFilters(rrow, fr->w->flist, fr->w->tmatch);

    if (hit) {
        *card = *card + 1;
        if (fr->cstar) return; /* just counting */
        robj *r = outputRow(rrow, fr->qcols, fr->cmatchs,
                            akey, fr->w->tmatch, 0);
        if (fr->q->qed) addRow2OBList(fr->ll, fr->w, r, fr->orobj, rrow, akey);
        else            addReplyBulk(fr->c, r);
        decrRefCount(r);
    }
}

/* SYNTAX
   1.) SCANSELECT * FROM tbl
   2.) SCANSELECT * FROM tbl ORDER_BY_CLAUSE
   3.) SCANSELECT * FROM tbl WHERE clause [ORDER_BY_CLAUSE]
*/
void tscanCommand(redisClient *c) {
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool nowc   =  0; /* NO WHERE CLAUSE */
    bool cstar  =  0;
    int  qcols  =  0;
    int  tmatch = -1;
    bool join   =  0;
    sds  where  = (c->argc > 4) ? c->argv[4]->ptr : NULL;
    sds  wc     = (c->argc > 5) ? c->argv[5]->ptr : NULL;
    if ((where && !*where) || (wc && !*wc)) {
        addReply(c, shared.scanselectsyntax);
        return;
    }
    if (!parseSelectReply(c, 1, &nowc, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          c->argv[3]->ptr, where)) return;
    if (join) {
        addReply(c, shared.scan_join);
        return;
    }
    if (!nowc && !wc) {
        addReply(c, shared.scanselectsyntax);
        return;
    }

    cswc_t  w;
    list   *ll  = NULL; /* B4 GOTO */
    init_check_sql_where_clause(&w, tmatch, wc); /* on error: GOTO tscan_end */

    if (nowc && c->argc > 4) { /* ORDER BY or STORE w/o WHERE CLAUSE */
        if (!strncasecmp(where, "ORDER ", 6) ||
            !strncasecmp(where, "STORE ", 6)) {
            if (!parseWCAddtlSQL(c, c->argv[4]->ptr, &w)) goto tscan_end;
            if (w.lvr) {
                w.lvr = sdsnewlen(w.lvr, strlen(w.lvr));
                if (!leftoverParsingReply(c, w.lvr))      goto tscan_end;
            }
            if (w.wtype > SQL_STORE_LOOKUP_MASK) { /* STORE after ORDER BY */
                addReply(c, shared.scan_store);
                goto tscan_end;
            }
        }
    }
    if (nowc && !w.nob && c->argc > 4) { /* argv[4] parse error */
        w.lvr = sdsdup(where);
        leftoverParsingReply(c, w.lvr);
        goto tscan_end;
    }

    if (!nowc && !w.nob) { /* WhereClause exists and no ORDER BY */
        parseWCReply(c, &w, SQL_SCANSELECT, 1);
        if (w.wtype == SQL_ERR_LOOKUP)       goto tscan_end;
        if (!leftoverParsingReply(c, w.lvr)) goto tscan_end;
        if (w.imatch != -1) { /* disallow SCANSELECT on indexed columns */
            addReply(c, shared.scan_on_index);
            goto tscan_end;
        }
        if (w.wtype > SQL_STORE_LOOKUP_MASK) { /* no SCAN STOREs (for now) */
            addReply(c, shared.scan_store);
            goto tscan_end;
        }
    }

    if (cstar && w.nob) { /* SCANSELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);
        goto tscan_end;
    }

    robj *btt  = lookupKeyRead(c->db, Tbl[server.dbid][w.tmatch].name);
    bt   *btr = (bt *)btt->ptr;
    if (cstar && nowc) { /* SCANSELECT COUNT(*) FROM tbl */
        addReplyLongLong(c, (long long)btr->numkeys);
        goto tscan_end;
    }
    // TODO on "fk_lim" iterate on FK (not PK)
    //if (w.nob) w.imatch = find_index(w.tmatch, w.obc);
    fr_t fr;
    qr_t q;
    setQueued(&w, &q);
    ll = initOBsort(q.qed, &w);
    init_filter_row(&fr, c, &w, &q, qcols, cmatchs, nowc, ll, cstar, 1);
    //dumpW(&w, w.wtype);
    LEN_OBJ
    btEntry *be;
    ulong    sent  =  0;
    long     loops = -1;
    btSIter *bi    = q.pk_lo ? btGetFullIteratorXth(btr, w.ofst):
                               btGetFullRangeIterator(btr);
    while ((be = btRangeNext(bi)) != NULL) {
        loops++;
        if (q.pk_lim) {
            if (!q.pk_lo && w.ofst != -1 && loops < w.ofst) continue;
            sent++;
            if ((uint32)w.lim == card) break; /* ORDRBY PK LIM */
        }
        condSelectReply(&fr, be->key, be->val, &card);
    }
    btReleaseRangeIterator(bi);

    if (q.qed && card) opSelectOnSort(c, ll, &w, fr.orobj, &sent);

    if (w.lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

    if (w.ovar) incrOffsetVar(c, &w, card);

tscan_end:
    releaseOBsort(ll);
    destroy_check_sql_where_clause(&w);
}

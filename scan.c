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

typedef struct filter_row {
    redisClient   *c;
    cswc_t        *w;
    qr_t          *q;
    int            tmatch;
    int            qcols;
    int           *cmatchs;
    bool           nowc;
    uchar          ctype;
    list          *ll;
    bool           cstar;
    uchar          ftype;
    uint32         lowi;
    uint32         highi;
    float          lowf;
    float          highf;
    char          *lows;
    char          *highs;
} fr_t;

static bool init_filter_row(fr_t *fr, redisClient *c, cswc_t *w, qr_t *q,
                            int tmatch, int qcols, int *cmatchs, bool nowc,
                            uchar ctype, list *ll, bool cstar, uchar ftype) {
    fr->c       = c;
    fr->w       = w;
    fr->q       = q;
    fr->tmatch  = tmatch;
    fr->qcols   = qcols;
    fr->cmatchs = cmatchs;
    fr->nowc    = nowc;
    fr->ctype   = ctype;
    fr->ll      = ll;
    fr->cstar   = cstar;
    fr->ftype   = ftype;
    fr->lowi    = -1;
    fr->highi   = -1;
    fr->lowf    = FLT_MAX;
    fr->highf   = FLT_MIN;
    fr->lows    = NULL;
    fr->highs   = NULL;

    if (w->low) { /* RANGE_QUERY (or single lookup, currently also a range)*/
         if (ftype == COL_TYPE_INT) {
             uint32 i;
             if (!strToInt(c, w->low->ptr, sdslen(w->low->ptr), &i)) return 0;
             fr->lowi  = i;
             if (!strToInt(c, w->high->ptr, sdslen(w->high->ptr), &i)) return 0;
             fr->highi = i;
         } else if (ftype == COL_TYPE_FLOAT) {
             float f;
             if (!strToFloat(c, w->low->ptr, sdslen(w->low->ptr), &f)) return 0;
             fr->lowf  = f;
             if (!strToFloat(c, w->high->ptr, sdslen(w->high->ptr), &f))
                 return 0;
             fr->highf = f;
         } else {
             fr->lows  = w->low->ptr;
             fr->highs = w->high->ptr;
         }
    }
    return 1;
}

static bool is_hit(fr_t *fr, aobj *a) {
    if (fr->ftype == COL_TYPE_INT) {
        if (a->i >= fr->lowi && a->i <= fr->highi) return 1;
    } else if (fr->ftype == COL_TYPE_FLOAT) {
        if (a->f >= fr->lowf && a->f <= fr->highf) return 1;
    } else {             /* COL_TYPE_STRING */
        if (strcmp(a->s, fr->lows)  >= 0 &&
            strcmp(a->s, fr->highs) <= 0) return 1;
    }
    return 0;
}
static bool is_match(aobj *la, aobj *a, uchar ftype) {
    if (     ftype == COL_TYPE_INT)      return (la->i == a->i);
    else if (ftype == COL_TYPE_FLOAT)    return (la->f == a->f);
    else           /* COL_TYPE_STRING */ return (strcmp(la->s, a->s) == 0);
}

//TODO merge w/ ALL RangeOps and InOps when FILTERS are implemented
static void condSelectReply(fr_t *fr, aobj *akey, void *rrow, ulong *card) {
    uchar hit = 0;
    if (fr->nowc) {
        hit = 1;
    } else {
        aobj acol = getRawCol(rrow, fr->w->cmatch, NULL, fr->tmatch, NULL, 0);
        if (fr->w->low) { /* RANGE_QUERY */
            hit = is_hit(fr, &acol);
        } else {          /* IN_QUERY */
            listNode *ln;
            listIter *li = listGetIterator(fr->w->inl, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *la = ln->value;
                hit      = is_match(la, &acol, fr->ftype);
                if (hit) break;
            }
            listReleaseIterator(li);
        }
        releaseAobj(&acol);
    }

    if (hit) {
        *card = *card + 1;
        if (fr->cstar) return; /* just counting */
        robj *r = outputRow(rrow, fr->qcols, fr->cmatchs,
                            akey, fr->tmatch, 0);
        if (fr->q->qed) {
            addORowToRQList(fr->ll, r, rrow, fr->w->obc, akey,
                            fr->tmatch, fr->ctype);
            decrRefCount(r);
        } else {
            addReplyBulk(fr->c, r);
            decrRefCount(r);
        }
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
    uchar   sop = SQL_SCANSELECT;
    init_check_sql_where_clause(&w, tmatch, wc); /* on error: GOTO tscan_end */

    if (nowc && c->argc > 4) { /* ORDER BY or STORE w/o WHERE CLAUSE */
        if (!strncasecmp(where, "ORDER ", 6) ||
            !strncasecmp(where, "STORE ", 6)) {
            if (!parseWCAddtlSQL(c, c->argv[4]->ptr, &w) ||
                !leftoverParsingReply(c, w.lvr))            goto tscan_end;
            if (w.stor) { /* if STORE comes after ORDER BY */
                addReply(c, shared.scan_store);
                goto tscan_end;
            }
        }
    }
    if (nowc && w.obc == -1 && c->argc > 4) { /* argv[4] parse error */
        w.lvr = where;
        leftoverParsingReply(c, w.lvr);
        goto tscan_end;
    }

    if (!nowc && w.obc == -1) { /* WhereClause exists and no ORDER BY */
        uchar wtype  = checkSQLWhereClauseReply(c, &w, sop, 1);
        if (wtype == SQL_ERR_LOOKUP)         goto tscan_end;
        if (!leftoverParsingReply(c, w.lvr)) goto tscan_end;
        if (w.imatch != -1) { /* disallow SCANSELECT on indexed columns */
            addReply(c, shared.scan_on_index);
            goto tscan_end;
        }
        if (w.stor) { /* disallow SCANSELECT STOREs (for now) */
            addReply(c, shared.scan_store);
            goto tscan_end;
        }

        /* HACK turn key into (low,high) - TODO this should be a SINGLE OP */
        if (wtype != SQL_RANGE_QUERY && wtype != SQL_IN_LOOKUP) {
            w.low  = cloneRobj(w.key);
            w.high = cloneRobj(w.key);
        }
    }

    if (cstar && w.obc != -1) { /* SCANSELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);
        goto tscan_end;
    }

    robj *btt  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr = (bt *)btt->ptr;
    if (cstar && nowc) { /* SCANSELECT COUNT(*) FROM tbl */
        addReplyLongLong(c, (long long)btr->numkeys);
        goto tscan_end;
    }

    // TODO on "fk_lim" iterate on FK (not PK)
    //if (w.obc != -1) w.imatch = find_index(tmatch, w.obc);

    qr_t  q;
    setQueued(&w, &q);
    
    uchar ctype = COL_TYPE_NONE;
    if (q.qed) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][tmatch].col_type[w.obc];
    }

    r_tbl_t *rt     = &Tbl[server.dbid][tmatch];
    uchar    pktype = rt->col_type[0];
    uchar    ftype  = (w.cmatch == -1) ? pktype : rt->col_type[w.cmatch];
    fr_t fr;
    if (!init_filter_row(&fr, c, &w, &q, tmatch, qcols, cmatchs, nowc,
                         ctype, ll, cstar, ftype)) goto tscan_end;

    LEN_OBJ
    btEntry *be;
    int      sent  =  0;
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

    if (q.qed && card) {
        obsl_t **vector = sortOrderByToVector(ll, ctype, w.asc);
        for (int k = 0; k < (int)listLength(ll); k++) {
            if (w.lim != -1 && sent == w.lim) break;
            if (w.ofst > 0) {
                w.ofst--;
            } else {
                sent++;
                obsl_t *ob = vector[k];
                addReplyBulk(c, ob->row);
            }
        }
        sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
        free(vector);
    }

    if (w.lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

    if (w.ovar) incrOffsetVar(c, &w, card);

tscan_end:
    if (ll) listRelease(ll);
    destroy_check_sql_where_clause(&w);
}

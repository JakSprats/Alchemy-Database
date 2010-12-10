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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>

#include "redis.h"
#include "adlist.h"

#include "bt.h"
#include "bt_iterator.h"
#include "orderby.h"
#include "index.h"
#include "alsosql.h"
#include "common.h"
#include "range.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer server;

extern char    *Col_type_defs[];
extern r_tbl_t  Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_DB][MAX_NUM_INDICES];

void init_range(range_t *g, redisClient *c, cswc_t *w, list *ll, uchar ctype) {
    bzero(g, sizeof(range_t));
    g->co.c     = c;
    g->co.w     = w;
    g->co.ll    = ll;
    g->co.ctype = ctype;
}


long rangeOp(range_t *g, row_op *p) {
    btEntry *be, *nbe;
    btSIter *nbi     = NULL; /* B4 GOTO */
    cswc_t  *w       = g->co.w;
    robj    *ind     = Index[server.dbid][w->imatch].obj;
    bool     virt    = Index[server.dbid][w->imatch].virt;
    int      ind_col = (int)Index[server.dbid][w->imatch].column;
    bool     pktype  = Tbl[server.dbid][w->tmatch].col_type[0];
    bool     q_pk    = (!w->asc || (w->obc > 0));
    bool     brk_pk  = (w->asc && w->obc == 0);
    bool     q_fk    = (w->obc > 0 && w->obc != ind_col);
    bool     brk_fk  = (w->asc && !q_fk);
    robj    *o       = lookupKeyRead(g->co.c->db,
                                     Tbl[server.dbid][w->tmatch].name);
    robj    *btt     = virt ? o : lookupKey(g->co.c->db, ind);
    g->co.qed        = virt ? q_pk : q_fk;
    long     loops   = -1;
    long     card    =  0;
    btSIter *bi      = btGetRangeIterator(btt, w->low, w->high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {     /* iterate btree */
        if (virt) {
            loops++;
            if (brk_pk) {
                if (w->ofst != -1 && loops < w->ofst) continue;
                if ((uint32)w->lim == card) break; /* ORDRBY PK LIM */
            }
            robj *key = be->key;
            robj *row = be->val;
            /* PK operation specific code comes here */
            if (!(*p)(g, key, row, q_pk)) goto range_op_err;
            card++;
        } else {
            robj *val = be->val;
            if (g->se.cstar) { /* FK cstar w/o filters is simple */
                bt *nbtr  = (bt *)val->ptr;
                card     += nbtr->numkeys;
            } else {
                nbi       = btGetFullRangeIterator(val, 0, 0);
                while ((nbe = btRangeNext(nbi, 1)) != NULL) {  /* NodeBT */
                    loops++;
                    if (brk_fk) {
                        if (w->ofst != -1 && loops < w->ofst) continue;
                        if ((uint32)w->lim == card) break; /* ORDRBY FK LIM */
                    }
                    robj *key = nbe->key;
                    robj *row = btFindVal(o, key, pktype);
                    /* FK operation specific code comes here */
                    if (!(*p)(g, key, row, q_fk)) goto range_op_err;
                    card++;
                }
                if (brk_fk && (uint32)w->lim == card) break; /*ORDRBY FK LIM*/
            }
            btReleaseRangeIterator(nbi);
            nbi = NULL; /* explicit in case of GOTO in inner loop */
        }
    }
    btReleaseRangeIterator(bi);
    bi = NULL; /* explicit in case of GOTO in inner loop */
    return card;

range_op_err:
    if (nbi)  btReleaseRangeIterator(nbi);
    if (bi)   btReleaseRangeIterator(bi);
    return -1;
}

long inOp(range_t *g, row_op *p) {
    listNode  *ln;
    btSIter *nbi     = NULL; /* B4 GOTO */
    cswc_t  *w       = g->co.w;
    bool     virt    = Index[server.dbid][w->imatch].virt;
    bool     pktype  = Tbl[server.dbid][w->tmatch].col_type[0];
    robj    *o       = lookupKeyRead(g->co.c->db,
                                     Tbl[server.dbid][w->tmatch].name);
    long     card    =  0;
    listIter *li     = listGetIterator(w->inl, AL_START_HEAD);
    if (virt) {
        bool  brk_pk  = (w->asc && w->obc == 0);
        bool  q_pk    = (!w->asc || (w->obc != -1 && w->obc != 0));
        g->co.qed     = q_pk;
        while((ln = listNext(li)) != NULL) {
            if (brk_pk && (uint32)w->lim == card) break; /* ORDRBY PK LIM */
            robj *key = convertRobj(ln->value, pktype);
            robj *row = btFindVal(o, key, pktype);
            if (row) {
                /* PK operation specific code comes here */
                if (!(*p)(g, key, row, q_pk)) goto in_op_err;
                card++;
            }
            decrRefCount(key); /* from addRedisCmdToINList() */
         }
     } else {
        btEntry *nbe;
        robj *ind     = Index[server.dbid][w->imatch].obj;
        robj *ibt     = lookupKey(g->co.c->db, ind);
        int   ind_col = (int)Index[server.dbid][w->imatch].column;
        bool  fktype  = Tbl[server.dbid][w->tmatch].col_type[ind_col];
        bool  brk_fk  = (w->asc  && w->obc != -1 && w->obc == ind_col);
        bool  q_fk    = (w->obc != -1);
        g->co.qed     = q_fk;
        while((ln = listNext(li)) != NULL) {
            robj *ikey = convertRobj(ln->value, fktype);
            robj *val  = btIndFindVal(ibt->ptr, ikey, fktype);
            if (val) {
                if (g->se.cstar) { /* FK cstar w/o filters is simple */
                    bt *nbtr = (bt *)val->ptr;
                    card    += nbtr->numkeys;
                } else {
                    nbi = btGetFullRangeIterator(val, 0, 0);
                    while ((nbe = btRangeNext(nbi, 1)) != NULL) {
                        if (brk_fk && (uint32)w->lim == card) break;
                        robj *key = nbe->key;
                        robj *row = btFindVal(o, key, pktype);
                        /* FK operation specific code comes here */
                        if (!(*p)(g, key, row, q_fk)) goto in_op_err;
                        card++;
                    }
                }
                btReleaseRangeIterator(nbi);
                nbi = NULL; /* explicit in case of GOTO in inner loop */
            }
            decrRefCount(ikey); /* from addRedisCmdToINList() */
        }
    }
    listReleaseIterator(li);
    return card;

in_op_err:
    if (nbi)  btReleaseRangeIterator(nbi);
    return -1;

}

bool select_op(range_t *g, robj *key, robj *row, bool q) {
    if (!g->se.cstar) {
        robj *r = outputRow(row, g->se.qcols, g->se.cmatchs,
                            key, g->co.w->tmatch, 0);
        if (q) addORowToRQList(g->co.ll, r, row, g->co.w->obc,
                               key, g->co.w->tmatch, g->co.ctype);
        else   addReplyBulk(g->co.c, r);
        decrRefCount(r);
    }
    return 1;
}

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar) {
    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (w->obc != -1) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][w->tmatch].col_type[w->obc];
    }

    LEN_OBJ
    range_t g;
    init_range(&g, c, w, ll, ctype);
    g.se.cstar   = cstar;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    if (w->low) { /* RANGE QUERY */
        card = (ulong)rangeOp(&g, select_op);
    } else {    /* IN () QUERY */
        card = (ulong)inOp(&g, select_op);
    }

    int sent = 0;
    if (card) {
        if (g.co.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = v[k];
                    addReplyBulk(c, ob->row);
                }
            }
            sortedOrderByCleanup(v, listLength(ll), ctype, 1);
            free(v);
        } else {
            sent = card;
        }
    }
    if (ll) listRelease(ll);

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}


bool build_rq_op(range_t *g, robj *key, robj *row, bool q) {
    if (q) {
        addORowToRQList(g->co.ll, key, row, g->co.w->obc,
                        key, g->co.w->tmatch, g->co.ctype);
    } else {
        robj *cln  = cloneRobj(key); /* clone orig is BtRobj */
        listAddNodeTail(g->co.ll, cln);
    }
    return 1;
}

#define BUILD_RANGE_QUERY_LIST                                                \
    list *ll    = listCreate();                                               \
    uchar ctype = COL_TYPE_NONE;                                              \
    if (w->obc != -1) {                                                       \
        ctype = Tbl[server.dbid][w->tmatch].col_type[w->obc];                 \
    }                                                                         \
                                                                              \
    range_t g;                                                                \
    init_range(&g, c, w, ll, ctype);                                          \
    ulong   card  = 0;                                                        \
    if (w->low) { /* RANGE QUERY */                                           \
        card = (ulong)rangeOp(&g, build_rq_op);                               \
    } else {    /* IN () QUERY */                                             \
        card = (ulong)inOp(&g, build_rq_op);                                  \
    }


void ideleteAction(redisClient *c,
                   cswc_t      *w) {
    BUILD_RANGE_QUERY_LIST

    MATCH_INDICES(w->tmatch)

    int sent = 0;
    if (card) {
        if (g.co.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = v[k];
                    robj *nkey = ob->row;
                    deleteRow(c, w->tmatch, nkey, matches, indices);
                }
            }
            sortedOrderByCleanup(v, listLength(ll), ctype, 1);
            free(v);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                deleteRow(c, w->tmatch, nkey, matches, indices);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
}

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]) {
    BUILD_RANGE_QUERY_LIST

    bool pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    int  sent   = 0;
    if (card) {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);
        if (g.co.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = v[k];
                    robj *nkey = ob->row;
                    robj *row  = btFindVal(o, nkey, pktype);
                    updateRow(c, o, nkey, row, w->tmatch, ncols,
                              matches, indices, vals, vlens, cmiss);
                }
            }
            sortedOrderByCleanup(v, listLength(ll), ctype, 1);
            free(v);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                robj *row  = btFindVal(o, nkey, pktype);
                updateRow(c, o, nkey, row, w->tmatch, ncols, matches, indices,
                          vals, vlens, cmiss);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
}

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

static void setRangeQueued(cswc_t *w, qr_t *q) {
    bzero(q, sizeof(qr_t));
    r_ind_t *ri     = (w->imatch == -1) ? NULL : &Index[server.dbid][w->imatch];
    bool     virt   = (w->imatch == -1) ? 0    : ri->virt;
    int      indcol = (w->imatch == -1) ? -1   : ri->column;
    //RL4 "asc: %d obc: %d lim: %d ofst: %d", w->asc, w->obc, w->lim, w->ofst);
    q->pk           = (!w->asc || (w->obc > 0));
    q->pk_lim       = (w->asc && w->obc == 0);
    q->pk_lo        = (!q->pk && (w->lim != -1) && (w->ofst != -1));

    q->fk           = (w->obc > 0 && w->obc != indcol);
    q->fk_lim       = (w->asc && !q->fk);
    q->fk_lo        = (!q->fk && (w->lim != -1) && (w->ofst != -1));

    q->qed          = virt ? q->pk : q->fk;
}

static void setInQueued(cswc_t *w, qr_t *q) {
    bzero(q, sizeof(qr_t));
    bool virt = (w->imatch == -1) ? 0 : Index[server.dbid][w->imatch].virt;
    if (virt) {
        q->pk         = (!w->asc || (w->obc != -1 && w->obc != 0));
        q->pk_lim     = (w->asc && w->obc == 0);
        q->qed        = q->pk;
    } else {
        int indcol  = (w->imatch == -1) ? -1 :
                                      (int)Index[server.dbid][w->imatch].column;
        q->fk         = (w->obc != -1);
        q->fk_lim     = (w->asc  && w->obc != -1 && w->obc == indcol);
        q->qed        = q->fk;
    }
}

void setQueued(cswc_t *w, qr_t *q) {
    if (w->inl) setInQueued(   w, q);
    else        setRangeQueued(w, q);
}

void init_range(range_t     *g,
                redisClient *c,
                cswc_t      *w,
                qr_t        *q,
                list        *ll,
                uchar        ctype) {
    bzero(g, sizeof(range_t));
    g->co.c     = c;
    g->co.w     = w;
    g->q        = q;
    g->co.ll    = ll;
    g->co.ctype = ctype;
}

static long rangeOpFK(range_t *g, row_op *p) {
    btEntry *be, *nbe;
    cswc_t  *w      = g->co.w;
    qr_t    *q      = g->q;
    btSIter *nbi    = NULL; /* B4 GOTO */
    robj    *ind    = Index[server.dbid][w->imatch].obj;
    bool     pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj    *tname  = Tbl[server.dbid][w->tmatch].name;
    robj    *tbl    = lookupKeyRead(g->co.c->db, tname);
    robj    *btt    = lookupKey(g->co.c->db, ind);
    long     ofst   = (long)w->ofst;
    long     loops  = -1;
    long     card   =  0;
    btSIter *bi = q->pk_lo ? btGetIteratorXth(btt, w->low, w->high, ofst, 0) :
                             btGetRangeIterator(btt, w->low, w->high, 0);
    while ((be = btRangeNext(bi)) != NULL) {     /* iterate btree */
        robj *val  = be->val;
        bt   *nbtr = (bt *)val->ptr;
        if (g->se.cstar) { /* FK cstar w/o filters is simple */
            card += nbtr->numkeys;
        } else {
            if (q->fk_lo) {
                if (nbtr->numkeys <= ofst) { /* skip IndexNode */
                    ofst -= nbtr->numkeys;
                    continue;
                }
            }
            nbi = (q->fk_lo && ofst) ? btGetFullIteratorXth(val, ofst, 0) :
                                       btGetFullRangeIterator(val, 0);
            while ((nbe = btRangeNext(nbi)) != NULL) {  /* NodeBT */
                loops++;
                if (q->fk_lim) {
                    if (!q->fk_lo && ofst != -1 && loops < ofst) continue;
                    if ((uint32)w->lim == card) break; /* ORDRBY FK LIM */
                }
                robj *key = nbe->key;
                robj *row = btFindVal(tbl, key, pktype);
                if (!(*p)(g, key, row, q->fk)) goto range_op_err;
                card++;
            }
            if (q->fk_lo) ofst = 0; /* OFFSET fulfilled */
            if (q->fk_lim && (uint32)w->lim == card) break; /*ORDRBY FK LIM*/
        }
        btReleaseRangeIterator(nbi);
        nbi = NULL; /* explicit in case of GOTO in inner loop */
    }
    btReleaseRangeIterator(bi);
    bi = NULL; /* explicit in case of GOTO in inner loop */
    return card;

range_op_err:
    if (nbi) btReleaseRangeIterator(nbi);
    if (bi)  btReleaseRangeIterator(bi);
    return -1;
}

static long rangeOpPK(range_t *g, row_op *p) {
    btEntry *be;
    cswc_t  *w     = g->co.w;
    qr_t    *q     = g->q;
    robj    *tname = Tbl[server.dbid][w->tmatch].name;
    robj    *btt   = lookupKeyRead(g->co.c->db, tname);
    long     loops = -1;
    long     card  =  0;
    btSIter *bi = q->pk_lo ? btGetIteratorXth(btt, w->low, w->high, w->ofst, 1):
                             btGetRangeIterator(btt, w->low, w->high, 1);
    while ((be = btRangeNext(bi)) != NULL) {     /* iterate btree */
        loops++;
        if (q->pk_lim) {
            if (!q->pk_lo && w->ofst != -1 && loops < w->ofst) continue;
            if ((uint32)w->lim == card) break; /* ORDRBY PK LIM */
        }
        robj *key = be->key;
        robj *row = be->val;
        if (!(*p)(g, key, row, q->pk)) goto range_op_err;
        card++;
    }
    btReleaseRangeIterator(bi);
    return card;

range_op_err:
    if (bi) btReleaseRangeIterator(bi);
    return -1;
}

long rangeOp(range_t *g, row_op *p) {
    cswc_t  *w      = g->co.w;
    bool     virt   = Index[server.dbid][w->imatch].virt;
    return virt ? rangeOpPK(g, p) : rangeOpFK(g,p);
}
    
static long inOpPK(range_t *g, row_op *p) {
    listNode  *ln;
    cswc_t    *w      = g->co.w;
    qr_t      *q      = g->q;
    bool       pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj      *tbl    = lookupKeyRead(g->co.c->db, tname);
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        if (q->pk_lim && (uint32)w->lim == card) break; /* ORDRBY PK LIM */
        robj *key = convertRobj(ln->value, pktype);
        robj *row = btFindVal(tbl, key, pktype);
        if (row) {
            if (!(*p)(g, key, row, q->pk)) goto in_op_pk_err;
            card++;
        }
        decrRefCount(key); /* from addRedisCmdToINList() */
    }
    listReleaseIterator(li);
    return card;

in_op_pk_err:
    listReleaseIterator(li);
    return -1;
}

static long inOpFK(range_t *g, row_op *p) {
    listNode *ln;
    btEntry  *nbe;
    btSIter  *nbi     = NULL; /* B4 GOTO */
    cswc_t   *w       = g->co.w;
    qr_t     *q       = g->q;
    bool      pktype  = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj     *tbl     = lookupKeyRead(g->co.c->db, tname);
    robj     *ind     = Index[server.dbid][w->imatch].obj;
    robj     *ibt     = lookupKey(g->co.c->db, ind);
    int       indcol = (int)Index[server.dbid][w->imatch].column;
    bool      fktype  = Tbl[server.dbid][w->tmatch].col_type[indcol];
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *ikey = convertRobj(ln->value, fktype);
        robj *val  = btIndFindVal(ibt->ptr, ikey, fktype);
        if (val) {
            if (g->se.cstar) { /* FK cstar w/o filters is simple */
                bt *nbtr  = (bt *)val->ptr;
                card     += nbtr->numkeys;
            } else {
                nbi = btGetFullRangeIterator(val, 0);
                while ((nbe = btRangeNext(nbi)) != NULL) {
                    if (q->fk_lim && (uint32)w->lim == card) break;
                    robj *key = nbe->key;
                    robj *row = btFindVal(tbl, key, pktype);
                    /* FK operation specific code comes here */
                    if (!(*p)(g, key, row, q->fk)) goto in_op_err;
                    card++;
                }
            }
            btReleaseRangeIterator(nbi);
            nbi = NULL; /* explicit in case of GOTO in inner loop */
        }
        decrRefCount(ikey); /* from addRedisCmdToINList() */
    }
    listReleaseIterator(li);
    return card;

in_op_err:
    listReleaseIterator(li);
    if (nbi)  btReleaseRangeIterator(nbi);
    return -1;
}

long inOp(range_t *g, row_op *p) {
    cswc_t   *w       = g->co.w;
    bool      virt    = Index[server.dbid][w->imatch].virt;
    return virt ? inOpPK(g, p) : inOpFK(g, p);
}

/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
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
    qr_t  q;
    setQueued(w, &q);
    
    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (q.qed) {
        ll      = listCreate();
        ctype   = Tbl[server.dbid][w->tmatch].col_type[w->obc];
    }

    range_t g;
    init_range(&g, c, w, &q, ll, ctype);
    g.se.cstar   = cstar;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    LEN_OBJ
    if (w->low) { /* RANGE_QUERY */
        card = (ulong)rangeOp(&g, select_op);
    } else {      /* IN_QUERY */
        card = (ulong)inOp(&g, select_op);
    }

    int sent = 0;
    if (card) {
        if (q.qed) {
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

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

    if (w->ovar) incrOffsetVar(c, w, card);
    if (ll) listRelease(ll);
}


bool build_rq_op(range_t *g, robj *key, robj *row, bool q) {
    if (q) {
        addORowToRQList(g->co.ll, key, row, g->co.w->obc,
                        key, g->co.w->tmatch, g->co.ctype);
    } else {
        robj *cln  = cloneRobj(key); /* clone -> orig is BtRobj */
        listAddNodeTail(g->co.ll, cln);
    }
    return 1;
}

#define BUILD_RANGE_QUERY_LIST                                                 \
    qr_t  q;                                                                   \
    setQueued(w, &q);                                                          \
    list *ll    = listCreate();                                                \
    uchar ctype = (w->obc == -1) ? COL_TYPE_NONE :                             \
                                  Tbl[server.dbid][w->tmatch].col_type[w->obc];\
    range_t g;                                                                 \
    init_range(&g, c, w, &q, ll, ctype);                                       \
    ulong   card  = 0;                                                         \
    if (w->low) { /* RANGE_QUERY */                                            \
        card = (ulong)rangeOp(&g, build_rq_op);                                \
    } else {      /* IN_QUERY */                                               \
        card = (ulong)inOp(&g, build_rq_op);                                   \
    }


void ideleteAction(redisClient *c,
                   cswc_t      *w) {
    BUILD_RANGE_QUERY_LIST

    MATCH_INDICES(w->tmatch)

    int sent = 0;
    if (card) {
        if (q.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob   = v[k];
                    robj   *nkey = ob->row;
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
                decrRefCount(nkey); /* from cloneRobj in build_rq_op() */
                sent++;
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    if (w->ovar) incrOffsetVar(c, w, card);
    listRelease(ll);
}

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals   [],
                   uint32       vlens  [],
                   uchar        cmiss  [],
                   ue_t         ue     []) {
    BUILD_RANGE_QUERY_LIST

    bool pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    int  sent   = 0;
    if (card) {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);
        if (q.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob   = v[k];
                    robj   *nkey = ob->row;
                    robj   *row  = btFindVal(o, nkey, pktype);
                    updateRow(c, o, nkey, row, w->tmatch, ncols,
                              matches, indices, vals, vlens, cmiss, ue);
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
                          vals, vlens, cmiss, ue);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
                sent++;
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    if (w->ovar) incrOffsetVar(c, w, card);
    listRelease(ll);
}

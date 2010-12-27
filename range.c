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
#include "colparse.h"
#include "alsosql.h"
#include "aobj.h"
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

static long rangeOpPK(range_t *g, row_op *p) {
    btEntry *be;
    cswc_t  *w     = g->co.w;
    qr_t    *q     = g->q;
    robj    *tname = Tbl[server.dbid][w->tmatch].name;
    robj    *btt   = lookupKeyRead(g->co.c->db, tname);
    bt      *btr   = (bt *)btt->ptr;
    long     loops = -1;
    long     card  =  0;
    btSIter *bi = q->pk_lo ? btGetIteratorXth(btr, w->low, w->high, w->ofst):
                             btGetRangeIterator(btr, w->low, w->high);
    while ((be = btRangeNext(bi)) != NULL) {     /* iterate btree */
        loops++;
        if (q->pk_lim) {
            if (!q->pk_lo && w->ofst != -1 && loops < w->ofst) continue;
            if ((uint32)w->lim == card) break; /* ORDRBY PK LIM */
        }
        if (!(*p)(g, be->key, be->val, q->pk)) goto range_op_err;
        card++;
    }
    btReleaseRangeIterator(bi);
    return card;

range_op_err:
    if (bi) btReleaseRangeIterator(bi);
    return -1;
}

static long rangeOpFK(range_t *g, row_op *p) {
    btEntry *be, *nbe;
    cswc_t  *w      = g->co.w;
    qr_t    *q      = g->q;
    btSIter *nbi    = NULL; /* B4 GOTO */
    bool     pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj    *tname  = Tbl[server.dbid][w->tmatch].name;
    robj    *btt    = lookupKeyRead(g->co.c->db, tname);
    bt      *btr    = (bt *)btt->ptr;
    robj    *ind    = Index[server.dbid][w->imatch].obj;
    robj    *ibtt   = lookupKey(g->co.c->db, ind);
    bt      *ibtr   = (bt *)ibtt->ptr;
    long     ofst   = (long)w->ofst;
    long     loops  = -1;
    long     card   =  0;
    btSIter *bi     = q->pk_lo ? btGetIteratorXth(ibtr, w->low, w->high, ofst) :
                                 btGetRangeIterator(ibtr, w->low, w->high);
    while ((be = btRangeNext(bi)) != NULL) {
        bt *nbtr = be->val;
        if (g->se.cstar) { /* FK cstar w/o filters is simple */
            card += nbtr->numkeys;
        } else {
            if (q->fk_lo) {
                if (nbtr->numkeys <= ofst) { /* skip IndexNode */
                    ofst -= nbtr->numkeys;
                    continue;
                }
            }
            nbi = (q->fk_lo && ofst) ? btGetFullIteratorXth(nbtr, ofst) :
                                       btGetFullRangeIterator(nbtr);
            while ((nbe = btRangeNext(nbi)) != NULL) {  /* NodeBT */
                loops++;
                if (q->fk_lim) {
                    if (!q->fk_lo && ofst != -1 && loops < ofst) continue;
                    if ((uint32)w->lim == card) break; /* ORDRBY FK LIM */
                }
                void *rrow = btFindVal(btr, nbe->key, pktype);
                if (!(*p)(g, nbe->key, rrow, q->fk)) goto range_op_err;
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

long rangeOp(range_t *g, row_op *p) {
    cswc_t *w    = g->co.w;
    bool    virt = Index[server.dbid][w->imatch].virt;
    return virt ? rangeOpPK(g, p) : rangeOpFK(g,p);
}
    
static long inOpPK(range_t *g, row_op *p) {
    listNode  *ln;
    cswc_t    *w      = g->co.w;
    qr_t      *q      = g->q;
    bool       pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj      *btt    = lookupKeyRead(g->co.c->db, tname);
    bt        *btr    = (bt *)btt->ptr;
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        if (q->pk_lim && (uint32)w->lim == card) break; /* ORDRBY PK LIM */
        aobj *apk  = ln->value;
        void *rrow = btFindVal(btr, apk, pktype);
        if (rrow) {
            if (!(*p)(g, apk, rrow, q->pk)) goto in_op_pk_err;
            card++;
        }
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
    robj     *btt     = lookupKeyRead(g->co.c->db, tname);
    bt       *btr     = (bt *)btt->ptr;
    robj     *ind     = Index[server.dbid][w->imatch].obj;
    robj     *ibtt    = lookupKey(g->co.c->db, ind);
    bt       *ibtr    = (bt *)ibtt->ptr;
    int       indcol = (int)Index[server.dbid][w->imatch].column;
    bool      fktype  = Tbl[server.dbid][w->tmatch].col_type[indcol];
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *afk  = ln->value;
        bt   *nbtr = btIndFindVal(ibtr, afk, fktype);
        if (nbtr) {
            if (g->se.cstar) { /* FK cstar w/o filters is simple */
                card += nbtr->numkeys;
            } else {
                nbi = btGetFullRangeIterator(nbtr);
                while ((nbe = btRangeNext(nbi)) != NULL) {
                    if (q->fk_lim && (uint32)w->lim == card) break;
                    void *rrow  = btFindVal(btr, nbe->key, pktype);
                    /* FK operation specific code comes here */
                    if (!(*p)(g, nbe->key, rrow, q->fk)) goto in_op_err;
                    card++;
                }
            }
            btReleaseRangeIterator(nbi);
            nbi = NULL; /* explicit in case of GOTO in inner loop */
        }
    }
    listReleaseIterator(li);
    return card;

in_op_err:
    listReleaseIterator(li);
    if (nbi)  btReleaseRangeIterator(nbi);
    return -1;
}

long inOp(range_t *g, row_op *p) {
    cswc_t *w    = g->co.w;
    bool    virt = Index[server.dbid][w->imatch].virt;
    return virt ? inOpPK(g, p) : inOpFK(g, p);
}

/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
bool select_op(range_t *g, aobj *akey, void *rrow, bool q) {
    if (!g->se.cstar) {
        robj *r = outputRow(rrow, g->se.qcols, g->se.cmatchs,
                            akey, g->co.w->tmatch, 0);
        if (q) addORowToRQList(g->co.ll, r, rrow, g->co.w->obc,
                               akey, g->co.w->tmatch, g->co.ctype);
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


bool build_rq_op(range_t *g, aobj *akey, void *rrow, bool q) {
    if (q) {
        addORowToRQList(g->co.ll, akey, rrow, g->co.w->obc,
                        akey, g->co.w->tmatch, g->co.ctype);
    } else {
        aobj *cln  = cloneAobj(akey);
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
                    obsl_t *ob  = v[k];
                    aobj   *apk = ob->row;
                    deleteRow(c, w->tmatch, apk, matches, indices);
                }
            }
            sortedOrderByCleanup(v, listLength(ll), ctype, 1);
            free(v);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *apk = ln->value;
                deleteRow(c, w->tmatch, apk, matches, indices);
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

//TODO once btReplace does IN-PLACE replaces, update does NOT need a LIST
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
        robj *btt = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);
        bt   *btr = (bt *)btt->ptr;
        if (q.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob   = v[k];
                    aobj   *apk  = ob->row;
                    void   *rrow = btFindVal(btr, apk, pktype);
                    updateRow(c, btr, apk, rrow, w->tmatch, ncols,
                              matches, indices, vals, vlens, cmiss, ue);
                }
            }
            sortedOrderByCleanup(v, listLength(ll), ctype, 1);
            free(v);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *apk  = ln->value;
                void *rrow = btFindVal(btr, apk, pktype);
                updateRow(c, btr, apk, rrow, w->tmatch, ncols, matches, indices,
                          vals, vlens, cmiss, ue);
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

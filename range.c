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

extern char     *Col_type_defs[];
extern aobj_cmp *OP_CMP[7];
extern r_tbl_t   Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t   Index[MAX_NUM_DB][MAX_NUM_INDICES];

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

static void init_range(range_t     *g,
                       redisClient *c,
                       cswc_t      *w,
                       qr_t        *q,
                       list        *ll,
                       uchar        ctype,
                       bool         orobj) {
    bzero(g, sizeof(range_t));
    g->co.c     = c;
    g->co.w     = w;
    g->q        = q;
    g->co.ll    = ll;
    g->co.ctype = ctype;
    g->co.orobj = orobj;
}

static long rangeOpPK(range_t *g, row_op *p) {
    //printf("rangeOpPK\n");
    btEntry *be;
    cswc_t  *w     = g->co.w;
    qr_t    *q     = g->q;
    robj    *tname = Tbl[server.dbid][w->tmatch].name;
    robj    *btt   = lookupKeyRead(g->co.c->db, tname); // TODO pass into func
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
        if (!(*p)(g, be->key, be->val, q->pk, &card)) goto range_op_err;
    }
    btReleaseRangeIterator(bi);
    return card;

range_op_err:
    if (bi) btReleaseRangeIterator(bi);
    return -1;
}

static long rangeOpFK(range_t *g, row_op *p) {
    //printf("rangeOpFK\n");
    btEntry *be, *nbe;
    cswc_t  *w      = g->co.w;
    qr_t    *q      = g->q;
    btSIter *nbi    = NULL; /* B4 GOTO */
    bool     pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj    *tname  = Tbl[server.dbid][w->tmatch].name;
    robj    *btt    = lookupKeyRead(g->co.c->db, tname); // TODO pass into func
    bt      *btr    = (bt *)btt->ptr;
    robj    *ind    = Index[server.dbid][w->imatch].obj;
    robj    *ibtt   = lookupKey(g->co.c->db, ind);       // TODO pass into func
    bt      *ibtr   = (bt *)ibtt->ptr;
    long     ofst   = (long)w->ofst;
    long     loops  = -1;
    long     card   =  0;
    btSIter *bi     = q->pk_lo ? btGetIteratorXth(ibtr, w->low, w->high, ofst) :
                                 btGetRangeIterator(ibtr, w->low, w->high);
    while ((be = btRangeNext(bi)) != NULL) {
        bt *nbtr = be->val;
        if (g->se.cstar && !g->co.w->flist) { /* FK cstar w/o filters */
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
                if (!(*p)(g, nbe->key, rrow, q->fk, &card)) goto range_op_err;
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

static long singleOpFK(range_t *g, row_op *p) {
    //printf("singleOpFK\n");
    btEntry  *nbe;
    btSIter  *nbi     = NULL; /* B4 GOTO */
    cswc_t   *w       = g->co.w;
    qr_t     *q       = g->q;
    bool      pktype  = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj     *btt     = lookupKeyRead(g->co.c->db, tname); //TODO pass into func
    bt       *btr     = (bt *)btt->ptr;
    robj     *ind     = Index[server.dbid][w->imatch].obj;
    robj     *ibtt    = lookupKey(g->co.c->db, ind);      //TODO pass into func
    bt       *ibtr    = (bt *)ibtt->ptr;
    int       indcol  = (int)Index[server.dbid][w->imatch].column;
    bool      fktype  = Tbl[server.dbid][w->tmatch].col_type[indcol];
    aobj     *afk     = copyRobjToAobj(w->key, fktype);
    bt       *nbtr    = btIndFindVal(ibtr, afk, fktype);
    long      card    =  0;
    if (nbtr) {
        if (g->se.cstar && !g->co.w->flist) { /* FK cstar w/o filters */
            card += nbtr->numkeys;
        } else {
            nbi = btGetFullRangeIterator(nbtr);
            while ((nbe = btRangeNext(nbi)) != NULL) {
                if (q->fk_lim && (uint32)w->lim == card) break;
                void *rrow  = btFindVal(btr, nbe->key, pktype);
                if (!(*p)(g, nbe->key, rrow, q->fk, &card)) goto sing_fk_err;
            }
        }
        btReleaseRangeIterator(nbi);
        nbi = NULL; /* explicit in case of GOTO in inner loop */
    }
    destroyAobj(afk);
    return card;

sing_fk_err:
    destroyAobj(afk);
    if (nbi) btReleaseRangeIterator(nbi);
    return -1;
}

long rangeOp(range_t *g, row_op *p) {
    cswc_t *w    = g->co.w;
    if (w->wtype & SQL_SINGLE_FK_LOOKUP) {
        return singleOpFK(g, p);
    } else {
        return Index[server.dbid][w->imatch].virt ? rangeOpPK(g, p) :
                                                    rangeOpFK(g,p);
    }
}
    
static long inOpPK(range_t *g, row_op *p) {
    //printf("inOpPK\n");
    listNode  *ln;
    cswc_t    *w      = g->co.w;
    qr_t      *q      = g->q;
    bool       pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj      *btt    = lookupKeyRead(g->co.c->db, tname); //TODO pass into func
    bt        *btr    = (bt *)btt->ptr;
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        if (q->pk_lim && (uint32)w->lim == card) break; /* ORDRBY PK LIM */
        aobj *apk  = ln->value;
        void *rrow = btFindVal(btr, apk, pktype);
        if (rrow) {
            if (!(*p)(g, apk, rrow, q->pk, &card)) goto in_op_pk_err;
        }
    }
    listReleaseIterator(li);
    return card;

in_op_pk_err:
    listReleaseIterator(li);
    return -1;
}

static long inOpFK(range_t *g, row_op *p) {
    //printf("inOpFK\n");
    listNode *ln;
    btEntry  *nbe;
    btSIter  *nbi     = NULL; /* B4 GOTO */
    cswc_t   *w       = g->co.w;
    qr_t     *q       = g->q;
    bool      pktype  = Tbl[server.dbid][w->tmatch].col_type[0];
    robj     *tname   = Tbl[server.dbid][w->tmatch].name;
    robj     *btt     = lookupKeyRead(g->co.c->db, tname); //TODO pass into func
    bt       *btr     = (bt *)btt->ptr;
    robj     *ind     = Index[server.dbid][w->imatch].obj;
    robj     *ibtt    = lookupKey(g->co.c->db, ind);       //TODO pass into func
    bt       *ibtr    = (bt *)ibtt->ptr;
    int       indcol  = (int)Index[server.dbid][w->imatch].column;
    bool      fktype  = Tbl[server.dbid][w->tmatch].col_type[indcol];
    long      card    =  0;
    listIter *li      = listGetIterator(w->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *afk  = ln->value;
        bt   *nbtr = btIndFindVal(ibtr, afk, fktype);
        if (nbtr) {
            if (g->se.cstar && !g->co.w->flist) { /* FK cstar w/o filters */
                card += nbtr->numkeys;
            } else {
                nbi = btGetFullRangeIterator(nbtr);
                while ((nbe = btRangeNext(nbi)) != NULL) {
                    if (q->fk_lim && (uint32)w->lim == card) break;
                    void *rrow  = btFindVal(btr, nbe->key, pktype);
                    if (!(*p)(g, nbe->key, rrow, q->fk, &card)) goto in_op_err;
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

bool passFilters(void *rrow, list *flist, int tmatch) {
    if (!flist) return 1; /* no filters always passes */
    listNode *ln, *ln2;
    bool      ret = 1;
    listIter *li  = listGetIterator(flist, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        f_t *flt  = ln->value;
        aobj a    = getRawCol(rrow, flt->cmatch, NULL, tmatch, NULL, 0);
        if (flt->inl) {
            listIter *li2 = listGetIterator(flt->inl, AL_START_HEAD);
            while((ln2 = listNext(li2)) != NULL) {
                aobj *a2 = ln2->value;
                ret      = (*OP_CMP[flt->op])(a2, &a);
                if (ret) break; /* break on hit */
            }
        } else {
            ret = (*OP_CMP[flt->op])(&flt->rhsv, &a);
        }
        releaseAobj(&a);
        if (!ret) break; /* break on miss */
    }
    listReleaseIterator(li);
    return ret;
}

/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
bool select_op(range_t *g, aobj *akey, void *rrow, bool q, long *card) {
    if (!passFilters(rrow, g->co.w->flist, g->co.w->tmatch)) return 1;
    if (!g->se.cstar) {
        robj *r = outputRow(rrow, g->se.qcols, g->se.cmatchs,
                            akey, g->co.w->tmatch, 0);
        if (q) addORowToRQList(g->co.ll, r, g->co.orobj, rrow, g->co.w->obc,
                               akey, g->co.w->tmatch, g->co.ctype);
        else   addReplyBulk(g->co.c, r);
        decrRefCount(r);
    }
    *card = *card + 1;
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
    init_range(&g, c, w, &q, ll, ctype, 1);
    g.se.cstar   = cstar;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    LEN_OBJ
    if (w->wtype == SQL_IN_LOOKUP) { /* IN_QUERY */
        card = (ulong)inOp(&g, select_op);
    } else {                         /* RANGE_QUERY */
        card = (ulong)rangeOp(&g, select_op);
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
            sortedOrderByCleanup(v, listLength(ll), ctype, g.co.orobj);
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

bool build_del_list_op(range_t *g, aobj *akey, void *rrow, bool q, long *card) {
    if (!passFilters(rrow, g->co.w->flist, g->co.w->tmatch)) return 1;
    if (q) {
        addORowToRQList(g->co.ll, akey, g->co.orobj, rrow, g->co.w->obc,
                        akey, g->co.w->tmatch, g->co.ctype);
    } else {
        aobj *cln  = cloneAobj(akey);
        listAddNodeTail(g->co.ll, cln); /* UGLY: build list of PKs to delete */
    }
    *card = *card + 1;
    return 1;
}

void ideleteAction(redisClient *c,
                   cswc_t      *w) {
    qr_t  q;
    setQueued(w, &q);
    list *ll    = listCreate();
    uchar ctype = (w->obc == -1) ? COL_TYPE_NONE :
                                  Tbl[server.dbid][w->tmatch].col_type[w->obc];
    range_t g;
    init_range(&g, c, w, &q, ll, ctype, 0);
    ulong   card  = 0;
    if (w->wtype == SQL_IN_LOOKUP) { /* IN_QUERY */
        card = (ulong)inOp(&g, build_del_list_op);
    } else {                         /* RANGE_QUERY */
        card = (ulong)rangeOp(&g, build_del_list_op);
    }

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
            sortedOrderByCleanup(v, listLength(ll), ctype, g.co.orobj);
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

bool update_op(range_t *g, aobj *akey, void *rrow, bool q, long *card) {
    if (!passFilters(rrow, g->co.w->flist, g->co.w->tmatch)) return 1;
    if (q) {
        addORowToRQList(g->co.ll, akey, g->co.orobj, rrow, g->co.w->obc,
                        akey, g->co.w->tmatch, g->co.ctype);
    } else {
        updateRow(g->co.c, g->up.btr, akey, rrow, g->co.w->tmatch, g->up.ncols,
                  g->up.matches, g->up.indices, g->up.vals, g->up.vlens,
                  g->up.cmiss, g->up.ue);
    }
    *card = *card + 1;
    return 1;
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
    qr_t  q;
    setQueued(w, &q);
    list *ll     = NULL;
    uchar ctype  = COL_TYPE_NONE;
    if (q.qed) {
        ll       = listCreate();
        ctype    = Tbl[server.dbid][w->tmatch].col_type[w->obc];
    }
    robj *tname  = Tbl[server.dbid][w->tmatch].name;
    robj *btt    = lookupKeyRead(c->db, tname);
    bt   *btr    = (bt *)btt->ptr;

    range_t g;
    init_range(&g, c, w, &q, ll, ctype, 0);
    g.up.btr     = btr;
    g.up.ncols   = ncols;
    g.up.matches = matches;
    g.up.indices = indices;
    g.up.vals    = vals;
    g.up.vlens   = vlens;
    g.up.cmiss   = cmiss;
    g.up.ue      = ue;
    ulong   card  = 0;
    if (w->wtype == SQL_IN_LOOKUP) { /* IN_QUERY */
        card = (ulong)inOp(&g, update_op);
    } else {                         /* RANGE_QUERY */
        card = (ulong)rangeOp(&g, update_op);
    }

    int  sent   = 0;
    if (card) {
        if (q.qed) {
            bool  pktype = Tbl[server.dbid][w->tmatch].col_type[0];
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
            sortedOrderByCleanup(v, listLength(ll), ctype, g.co.orobj);
            free(v);
        } else {
            sent = card;
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    if (w->ovar) incrOffsetVar(c, w, card);
    if (ll) listRelease(ll);
}

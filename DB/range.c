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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "redis.h"
#include "adlist.h"

#include "bt.h"
#include "bt_iterator.h"
#include "filter.h"
#include "orderby.h"
#include "index.h"
#include "wc.h"
#include "qo.h"
#include "colparse.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "range.h"

// CONSTANT GLOBALS
extern char     *Col_type_defs[];
extern aobj_cmp *OP_CMP[7];
extern r_tbl_t   Tbl[MAX_NUM_TABLES];
extern r_ind_t   Index[MAX_NUM_INDICES];
extern uchar     OutputMode;

ulong  CurrCard = 0; // TODO remove - after no update on MCI cols FIX

static bool Bdum; /* dummy variable */

#define DEBUG_SINGLE_PK                                       \
  printf("singleOpPK: tmatch: %d\n", g->co.w->wf.tmatch);
#define DEBUG_RANGE_PK                                        \
  printf("rangeOpPK: imatch: %d\n", g->co.w->wf.imatch);
#define DEBUG_NODE_BT                                         \
  printf("nodeBT_Op: nbtr->numkeys: %d\n", d->nbtr->numkeys);
#define DEBUG_MCI_FIND                                        \
  printf("in btMCIFindVal: trgr: %d\n", trgr);
#define DEBUG_MCI_FIND_MID                                    \
  dumpFilter(printf, flt, "\t");
#define DEBUG_RUN_ON_NODE                                     \
  printf("in runOnNode: ibtr: %p still: %u nop: %p\n", ibtr, still, nop);  \
  bt_dumptree(printf, ibtr, 0);
#define DEBUG_SINGLE_FK                                       \
  printf("singleOpFK: imatch: %d key: ", g->co.w->wf.imatch); \
  dumpAobj(printf, &g->co.w->wf.akey);
#define DEBUG_RANGE_FK                                        \
  printf("rangeOpFK: imatch: %d\n", g->co.w->wf.imatch);
#define DEBUG_PASS_FILT_INL                            \
printf("PF: ret: %d a2: ", ret); dumpAobj(printf, a2); \
printf("a: ");dumpAobj(printf, &a);
#define DEBUG_PASS_FILT_LOW                                   \
  printf("PassF: low:  "); dumpAobj(printf, &flt->alow);      \
  printf("PassF: high: "); dumpAobj(printf, &flt->ahigh);     \
  printf("PassF: a:    "); dumpAobj(printf, &a);              \
  printf("ret: %d\n", ret);
#define DEBUG_PASS_FILT_KEY                                   \
  printf("PassF: key: "); dumpAobj(printf, &flt->akey);       \
  printf("PassF: a:   "); dumpAobj(printf, &a);               \
  printf("ret: %d\n", ret);

void init_range(range_t *g, redisClient *c,  cswc_t *w,     wob_t *wb,
                qr_t    *q, list        *ll, uchar   ofree, jb_t  *jb) {
    bzero(g, sizeof(range_t));
    g->co.c     = c;     g->co.w     = w;  g->co.wb = wb; g->co.ll    = ll;
    g->co.ofree = ofree; g->q        = q;  g->jb    = jb;
}

/* NOTE: this struct contains pointers, it is to be used ONLY for derefs */
typedef struct inner_bt_data {
    row_op  *p;
    range_t *g;
    qr_t    *q;
    bt      *btr;
    bt      *nbtr;
    long    *ofst;
    long    *card;
    long    *loops;
    bool    *brkr;
} ibtd_t;
static void init_ibtd(ibtd_t *d,    row_op *p,    range_t *g,     qr_t *q,
                      bt     *nbtr, long   *ofst, long    *card,  long *loops,
                      bool   *brkr) {
    d->p    = p;    d->g    = g;    d->q      = q;     d->nbtr = nbtr;
    d->ofst = ofst; d->card = card; d->loops  = loops; d->brkr = brkr;
}

static void setRangeQueued(cswc_t *w, wob_t *wb, qr_t *q) {
    bzero(q, sizeof(qr_t));
    r_ind_t *ri     = (w->wf.imatch == -1) ? NULL: &Index[w->wf.imatch];
    bool     virt   = (w->wf.imatch == -1) ?  0  : ri->virt;
    int      cmatch = (w->wf.imatch == -1) ? -1  : ri->column ? ri->column : -1;
    if (virt) {
        q->pk       = (wb->nob > 1) ||
                      (wb->nob == 1 && !(wb->asc[0] && !wb->obc[0]));
        q->pk_lim   = (!q->pk    && (wb->lim  != -1));
        q->pk_lo    = (q->pk_lim && (wb->ofst != -1));
        q->qed      = q->pk;
    } else {
        q->fk       = (wb->nob > 1) ||
                      (wb->nob == 1 && !(wb->asc[0] && wb->obc[0] == cmatch));
        q->fk_lim   = (!q->fk    && (wb->lim  != -1));
        q->fk_lo    = (q->fk_lim && (wb->ofst != -1));
        q->qed      = q->fk;
    } //dumpQueued(printf, w, wb, q, 1);
}
static void setInQueued(cswc_t *w, wob_t *wb, qr_t *q) {
    setRangeQueued(w, wb, q);
    q->pk_lo = q->fk_lo = 0; /* NOTE: LIMIT OFFSET optimisations not possible */
}
void setQueued(cswc_t *w, wob_t *wb, qr_t *q) { /* NOTE: NOT for JOINS */
    if (w->wf.inl) setInQueued(   w, wb, q);
    else           setRangeQueued(w, wb, q);
}

static bool pk_row_op(aobj  *apk, void *rrow, range_t *g,    row_op *p,
                      qr_t *q,    long    *card) {
    if (rrow && !(*p)(g, apk, rrow, q->qed, card)) return 0;
    else                                           return 1;
}
static bool pk_op_l(aobj *apk, void *rrow, range_t *g,     row_op *p, wob_t *wb,
                    qr_t *q,   long *card, long    *loops, bool   *brkr) {
    *brkr   = 0;
    INCR(*loops)
    if (q->pk_lim) {
        if (!q->pk_lo && wb->ofst != -1 && *loops < wb->ofst) return 1;
        if (wb->lim == *card) { *brkr = 1;                    return 1; }
    }
    if (!pk_row_op(apk, rrow, g, p, q, card)) return 0;
    else                                      return 1;
}
#define CBRK { card = -1;   break; }
#define NBRK { nbtr = NULL; break; }

static long singleOpPK(range_t *g, row_op *p) {               //DEBUG_SINGLE_PK
    cswc_t  *w     = g->co.w;
    qr_t    *q     = g->q;
    aobj    *apk   = &w->wf.akey;
    g->co.btr      = getBtr(w->wf.tmatch);
    void    *rrow  = btFind(g->co.btr, apk);
    long     card  =  0;
    if (!pk_row_op(apk, rrow, g, p, q, &card)) return -1;
    else                                       return card;
}
static long rangeOpPK(range_t *g, row_op *p) {                 //DEBUG_RANGE_PK
    btEntry *be;
    bool     brkr  = 0;
    cswc_t  *w     = g->co.w;
    wob_t   *wb    = g->co.wb;
    qr_t    *q     = g->q;
    bt      *btr   = getBtr(w->wf.tmatch);
    g->co.btr      = btr;
    long     loops = -1;
    long     card  =  0;
    btSIter *bi    = q->pk_lo ?
                      btGetXthIter(btr,   &w->wf.alow, &w->wf.ahigh, wb->ofst) :
                      btGetRangeIter(btr, &w->wf.alow, &w->wf.ahigh);
    while ((be = btRangeNext(bi)) != NULL) {
        if (!pk_op_l(be->key, be->val, g, p, wb, q, &card, &loops, &brkr)) CBRK
        if (brkr) break;
    }
    btReleaseRangeIterator(bi);
    return card;
}

static aobj  UniqueIndexVal;
void initX_DB_Range() { /* NOTE: called on server startup */
    initAobj(&UniqueIndexVal);
}
typedef bool node_op(ibtd_t *d);
static bool uBT_Op(ibtd_t *d) {
    INCR(*d->loops)
    if (d->g->se.cstar) { INCR(*d->card) return 1; }
    qr_t  *q   = d->g->q;     /* code compaction */
    wob_t *wb  = d->g->co.wb; /* code compaction */
    *d->brkr   = 0;
    void *rrow = btFind(d->g->co.btr, &UniqueIndexVal); /* FK lkp - must work */
    if (q->fk_lim) {
        if      (q->fk_lo && *d->loops < *d->ofst)    return 1;
        else if (wb->lim == *d->card) { *d->brkr = 1; return 1; }
    }
    if (!(*d->p)(d->g, &UniqueIndexVal, rrow, d->g->q->qed, d->card)) return 0;
    return 1;
}
static bool nodeBT_Op(ibtd_t *d) {                              //DEBUG_NODE_BT
    if (d->g->se.cstar && !d->g->co.w->flist) { /* FK cstar w/o filters */
        INCRBY(*d->card, d->nbtr->numkeys) return 1;
    }
    if (d->q->fk_lo && FK_RQ(d->g->co.w->wtype) &&
        d->nbtr->numkeys <= *d->ofst) { /* skip IndexNode */
            DECRBY(*d->ofst,  d->nbtr->numkeys); return 1;
    }
    btEntry *nbe;
    bool     ret      = 1; /* presume success */
    qr_t    *q        = d->g->q;     /* code compaction */
    wob_t   *wb       = d->g->co.wb; /* code compaction */
    *d->brkr          = 0;
    bool     gox      = (q->fk_lo && *d->ofst > 0);
    btSIter *nbi      = gox ? btGetFullXthIter(d->nbtr, *d->ofst) :
                              btGetFullRangeIter(d->nbtr);
    while ((nbe = btRangeNext(nbi)) != NULL) {
        INCR(*d->loops)
        if (q->fk_lim) {
            if      (!q->fk_lo && *d->loops < *d->ofst) continue;
            else if (wb->lim == *d->card)               break;
        }
        void *rrow = btFind(d->g->co.btr, nbe->key);
        if (!(*d->p)(d->g, nbe->key, rrow, q->qed, d->card)) { ret = 0; break; }
    } btReleaseRangeIterator(nbi);
    if (q->fk_lo)                         *d->ofst = 0; /* OFFSET fulfilled */
    if (q->fk_lim && wb->lim == *d->card) *d->brkr = 1; /* ORDERBY FK LIM*/
    return ret;
}
bt *btMCIFindVal(cswc_t *w, bt *nbtr, uint32 *nmatch, r_ind_t *ri) {
    if (nbtr && w->wf.klist) {
        listNode *ln;
        int       trgr = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
        int       i    = 0;                                     //DEBUG_MCI_FIND
        listIter *li   = listGetIterator(w->wf.klist, AL_START_HEAD);
        while ((ln = listNext(li)) != NULL) {
            f_t *flt  = ln->value;                          //DEBUG_MCI_FIND_MID
            if (flt->op == NONE) break; /* MCI Joins can have empty flt's */
            if (i == trgr) {
                ln             = listNext(li);
                aobj *uv       = &UniqueIndexVal;
                if        UU(nbtr) { //TODO refactor this w/ 099
                    uv->enc = uv->type = COL_TYPE_INT;
                    if (!(uv->i = (long)btFind(nbtr, &flt->akey))) NBRK
                } else if UL(nbtr) {
                    uv->enc = uv->type = COL_TYPE_LONG;
                    ulk *ul  = btFind(nbtr, &flt->akey); if (!ul)  NBRK
                    uv->l    = ul->val;
                } else if LU(nbtr) {
                    uv->enc = uv->type = COL_TYPE_INT;
                    luk *lu  = btFind(nbtr, &flt->akey); if (!lu)  NBRK
                    uv->i    = lu->val;
                } else if LL(nbtr) {
                    uv->enc = uv->type = COL_TYPE_LONG;
                    llk *ll  = btFind(nbtr, &flt->akey); if (!ll)  NBRK
                    uv->l    = ll->val;
                }
                INCR(*nmatch)
                break;
            }
            bt  *xbtr = btIndFind(nbtr, &flt->akey);
            if (!xbtr)                                             NBRK/* MISS*/
            INCR(*nmatch)
            nbtr      = xbtr;
            i++;
        }
        listReleaseIterator(li);
    }
    return nbtr;
}
static bool runOnNode(bt      *ibtr, uint32  still,
                      node_op *nop,  ibtd_t *d,     r_ind_t *ri) {
    btEntry *nbe;                                           //DEBUG_RUN_ON_NODE
    still--;
    bool     ret = 1; /* presume success */
    btSIter *nbi = btGetFullRangeIter(ibtr);
    while ((nbe = btRangeNext(nbi)) != NULL) {
        d->nbtr        = nbe->val;
        if (still) { /* Recurse until node*/
            if (!runOnNode(d->nbtr, still, nop, d, ri)) { ret = 0; break; }
        } else {
            if UNIQ(ri->cnstr) {
                d->nbtr        = ibtr;/* Unique 1 step shorter*/
                aobj *uv       = &UniqueIndexVal;
                if        UU(d->nbtr) { //TODO refactor this w/ 099
                    uv->enc = uv->type = COL_TYPE_INT;
                    uv->i = (int)(long)nbe->val;
                } else if UL(d->nbtr) {
                    uv->enc = uv->type = COL_TYPE_LONG;
                    ulk *ul  = nbe->val;
                    uv->l    = ul->val;
                } else if LU(d->nbtr) {
                    uv->enc = uv->type = COL_TYPE_INT;
                    luk *lu  = nbe->val;
                    uv->i    = lu->val;
                } else if LL(d->nbtr) {
                    uv->enc = uv->type = COL_TYPE_LONG;
                    llk *ll  = nbe->val;
                    uv->l    = ll->val;
                }
            }  /* NEXT LINE: When we get to NODE -> run xBT_Op */
            if (!(*nop)(d)) { ret = 0; break; }
        }
        if (*d->brkr) break;
    }
    btReleaseRangeIterator(nbi);
    return ret;
}
static long rangeOpFK(range_t *g, row_op *p) {                 //DEBUG_RANGE_FK
    ibtd_t   d; btEntry *be;
    bool     brkr  = 0;
    cswc_t  *w     = g->co.w;  /* code compaction */
    wob_t   *wb    = g->co.wb; /* code compaction */
    qr_t    *q     = g->q;     /* code compaction */
    g->co.btr      = getBtr(w->wf.tmatch);
    bt      *ibtr  = getIBtr(w->wf.imatch);
    r_ind_t *ri    = &Index[w->wf.imatch];
    node_op *nop   = UNIQ(ri->cnstr) ? uBT_Op : nodeBT_Op;
    uint32   nexpc = ri->clist ? (ri->clist->len - 1) : 0;
    long     ofst  = wb->ofst;
    long     loops = -1;
    long     card  =  0;
    btSIter *bi    = btGetRangeIter(ibtr, &w->wf.alow, &w->wf.ahigh);
    init_ibtd(&d, p, g, q, NULL, &ofst, &card, &loops, &brkr);
    while ((be = btRangeNext(bi)) != NULL) {
        uint32 nmatch = 0;
        d.nbtr        = btMCIFindVal(w, be->val, &nmatch, ri);
        if (d.nbtr) {
            uint32 diff = nexpc - nmatch;
            if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) CBRK }
            else if (!(*nop)(&d))                                       CBRK
        }
        if (brkr) break;
    }
    btReleaseRangeIterator(bi);
    return card;
}
static long singleOpFK(range_t *g, row_op *p) {               //DEBUG_SINGLE_FK
    ibtd_t    d;
    cswc_t   *w      = g->co.w;
    wob_t    *wb     = g->co.wb;
    qr_t     *q      = g->q;
    g->co.btr        = getBtr(w->wf.tmatch);
    bt       *ibtr   = getIBtr(w->wf.imatch);
    aobj     *afk    = &w->wf.akey;
    uint32    nmatch = 0;
    r_ind_t  *ri     = &Index[w->wf.imatch];
    node_op  *nop    = UNIQ(ri->cnstr) ? uBT_Op : nodeBT_Op;
    uint32    nexpc  = ri->clist ? (ri->clist->len - 1) : 0;
    bt       *nbtr   = btMCIFindVal(w, btIndFind(ibtr, afk), &nmatch, ri);
    long      ofst   = wb->ofst;
    long      loops  = -1;
    long      card   =  0;
    init_ibtd(&d, p, g, q, nbtr, &ofst, &card, &loops, &Bdum);
    if (d.nbtr) {
        uint32 diff = nexpc - nmatch;
        if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) return -1; }
        else if (!(*nop)(&d))                                       return -1;
    }
    return card;
}
static long rangeOp(range_t *g, row_op *p) {
    return (g->co.w->wtype & SQL_SINGLE_FK_LKP)   ? singleOpFK(g, p) :
           (Index[g->co.w->wf.imatch].virt)       ? rangeOpPK( g, p) :
                                                    rangeOpFK( g, p);
}
long keyOp(range_t *g, row_op *p) {
    return (g->co.w->wtype & SQL_SINGLE_LKP) ?      singleOpPK(g, p) :
                                                    rangeOp(   g, p);
}
    
static long inOpPK(range_t *g, row_op *p) {                //printf("inOpPK\n");
    listNode  *ln;
    bool       brkr   = 0;
    cswc_t    *w      = g->co.w;
    wob_t     *wb     = g->co.wb;
    qr_t      *q      = g->q;
    g->co.btr         = getBtr(w->wf.tmatch);
    long      loops   = -1;
    long      card    =  0;
    listIter *li      = listGetIterator(w->wf.inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *apk  = ln->value;
        void *rrow = btFind(g->co.btr, apk);
        if (rrow && !pk_op_l(apk, rrow, g, p, wb, q, &card, &loops, &brkr)) CBRK
        if (brkr) break;
    }
    listReleaseIterator(li);
    return card;
}
static long inOpFK(range_t *g, row_op *p) {                //printf("inOpFK\n");
    ibtd_t    d; listNode *ln;
    bool      brkr    = 0;
    cswc_t   *w       = g->co.w;
    wob_t    *wb      = g->co.wb;
    qr_t     *q       = g->q;
    g->co.btr         = getBtr(w->wf.tmatch);
    bt       *ibtr    = getIBtr(w->wf.imatch);
    r_ind_t  *ri      = &Index[w->wf.imatch];
    node_op  *nop     = UNIQ(ri->cnstr) ? uBT_Op : nodeBT_Op;
    uint32    nexpc   = ri->clist ? (ri->clist->len - 1) : 0;
    long      ofst    = wb->ofst;
    long      loops   = -1;
    long      card    =  0;
    init_ibtd(&d, p, g, q, NULL, &ofst, &card, &loops, &brkr);
    listIter *li      = listGetIterator(w->wf.inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        uint32  nmatch = 0;
        aobj   *afk    = ln->value;
        d.nbtr         = btMCIFindVal(w, btIndFind(ibtr, afk), &nmatch, ri);
        if (d.nbtr) {
            uint32 diff = nexpc - nmatch;
            if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) CBRK }
            else if (!(*nop)(&d))                                       CBRK
        }
        if (brkr) break;
    }
    listReleaseIterator(li);
    return card;
}
long inOp(range_t *g, row_op *p) {
    return Index[g->co.w->wf.imatch].virt ? inOpPK(g, p) : inOpFK(g, p);
}
long Op(range_t *g, row_op *p) {
    if (g->co.w->wtype == SQL_IN_LKP) return inOp( g, p);
    else                              return keyOp(g, p);
}

bool passFilters(bt *btr, aobj *akey, void *rrow, list *flist, int tmatch) {
    if (!flist) return 1; /* no filters always passes */
    listNode *ln, *ln2;
    bool      ret = 1;
    listIter *li  = listGetIterator(flist, AL_START_HEAD);
    while ((ln = listNext(li)) != NULL) {
        f_t *flt  = ln->value;
        if (tmatch != flt->tmatch) continue;
        aobj a    = getCol(btr, rrow, flt->cmatch, akey, tmatch);
        if        (flt->inl) {
            listIter *li2 = listGetIterator(flt->inl, AL_START_HEAD);
            while((ln2 = listNext(li2)) != NULL) {
                aobj *a2  = ln2->value;
                ret       = (*OP_CMP[EQ])(a2, &a);        //DEBUG_PASS_FILT_INL
                if (ret) break;                   /* break INNER-LOOP on hit */
            }
            listReleaseIterator(li2);
            releaseAobj(&a);
            if (!ret) break;                      /* break OUTER-LOOP on miss */
        } else if (flt->alow.type != COL_TYPE_NONE) {
            ret = (*OP_CMP[GE])(&flt->alow, &a);          //DEBUG_PASS_FILT_LOW
            if (!ret) { releaseAobj(&a); break; } /* break OUTER-LOOP on miss */
            ret = (*OP_CMP[LE])(&flt->ahigh, &a);
            releaseAobj(&a);
            if (!ret) break;                      /* break OUTER-LOOP on miss */
        } else {
            ret = (*OP_CMP[flt->op])(&flt->akey, &a);     //DEBUG_PASS_FILT_KEY
            releaseAobj(&a);
            if (!ret) break;                      /* break OUTER-LOOP on miss */
        }
    }
    listReleaseIterator(li);
    return ret;
}

/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
static bool select_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    int  tmatch = g->co.w->wf.tmatch;
    if (!passFilters(g->co.btr, apk, rrow, g->co.w->flist, tmatch)) return 1;
    bool ret    = 1;
    if (!g->se.cstar) {
        robj *r = outputRow(g->co.btr, rrow, g->se.qcols, g->se.cmatchs,
                            apk, tmatch);
        if (q) {
            addRow2OBList(g->co.ll, g->co.wb, g->co.btr, r, g->co.ofree,
                          rrow, apk);
        } else {
            GET_LRUC
            if (!addReplyRow(g->co.c, r, tmatch, apk, lruc)) ret = 0;
        }
        if (!(EREDIS)) decrRefCount(r);
    }
    INCR(*card)
    CurrCard = *card;
    return ret;
}
bool opSelectSort(cli  *c,    list *ll,   wob_t *wb,
                  bool ofree, long *sent, int    tmatch) {
    bool     ret  = 1;
    obsl_t **v    = sortOB2Vector(ll);
    long     ofst = wb->ofst;
    for (int i = 0; i < (int)listLength(ll); i++) {
        if (wb->lim != -1 && *sent == wb->lim) break;
        if (ofst > 0) ofst--;
        else {
            *sent      = *sent + 1;
            obsl_t *ob = v[i];
            if (!addReplyRow(c, ob->row, tmatch, ob->apk, ob->lruc)) {
                ret = 0; break;
            }
        }
    }
    sortOBCleanup(v, listLength(ll), ofree);
    free(v); /* FREED 004 */
    return ret;
}
void iselectAction(cli *c,         cswc_t *w,     wob_t *wb,
                   int  cmatchs[], int     qcols, bool   cstar) {
    range_t g; qr_t    q;
    setQueued(w, wb, &q);
    list *ll     = initOBsort(q.qed, wb);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_ROBJ, NULL);
    g.se.cstar   = cstar;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    void *rlen   = cstar ? NULL : addDeferredMultiBulkLength(c);
    long card    = Op(&g, select_op);
    long sent    = 0;
    if (card) {
        if (q.qed) {
            if (!opSelectSort(c, ll, wb, g.co.ofree,
                              &sent, w->wf.tmatch)) goto isel_end;
        } else {
            sent = card;
        }
    }
    if (wb->lim != -1 && sent < card) card = sent;
    if      (cstar)  addReplyLongLong(c, card);
    else if (EREDIS) setDeferredMultiBulkLength(c, rlen, 0);
    else             setDeferredMultiBulkLength(c, rlen, card);

isel_end:
    if (wb->ovar) incrOffsetVar(c, wb, card);
    releaseOBsort(ll);
}

static bool dellist_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    if (!passFilters(g->co.btr, apk, rrow,
                     g->co.w->flist, g->co.w->wf.tmatch)) return 1;
    if (q) {
        addRow2OBList(g->co.ll, g->co.wb, g->co.btr, apk, g->co.ofree,
                      rrow, apk);
    } else {
        aobj *cln  = cloneAobj(apk);
        listAddNodeTail(g->co.ll, cln); /* UGLY: build list of PKs to delete */
    }
    INCR(*card) CurrCard = *card;
    return 1;
}
static void opDeleteSort(list *ll,    cswc_t *w,      wob_t *wb,   bool  ofree,
                         long  *sent, int     matches, int   inds[]) {
    obsl_t **v    = sortOB2Vector(ll);
    long     ofst = wb->ofst;
    for (int i = 0; i < (int)listLength(ll); i++) {
        if (wb->lim != -1 && *sent == wb->lim) break;
        if (ofst > 0) {
            ofst--;
        } else {
            *sent       = *sent + 1;
            obsl_t *ob  = v[i];
            aobj   *apk = ob->row;
            deleteRow(w->wf.tmatch, apk, matches, inds);
        }
    }
    sortOBCleanup(v, listLength(ll), ofree);
    free(v); /* FREED 004 */
}
void ideleteAction(redisClient *c, cswc_t *w, wob_t *wb) {
    range_t g; qr_t    q;
    setQueued(w, wb, &q);
    list *ll   = initOBsort(1, wb);
    if (!q.qed) ll->free = destroyAobj;
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    long  card = Op(&g, dellist_op);

    MATCH_INDICES(w->wf.tmatch)

    long sent = 0;
    if (card) {
        if (q.qed) {
            opDeleteSort(ll, w, wb, g.co.ofree, &sent, matches, inds);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *apk = ln->value;
                deleteRow(w->wf.tmatch, apk, matches, inds);
                sent++;
            }
            listReleaseIterator(li);
        }
    }
    if (wb->lim != -1 && sent < card) card = sent;
    addReplyLongLong(c, (ull)card);

    if (wb->ovar) incrOffsetVar(c, wb, card);
    releaseOBsort(ll);
}

static bool update_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    if (!passFilters(g->co.btr, apk, rrow,
                     g->co.w->flist, g->co.w->wf.tmatch)) return 1;
    if (q) {
        addRow2OBList(g->co.ll, g->co.wb, g->co.btr, apk, g->co.ofree,
                      rrow, apk);
    } else {
        //TODO non-FK/PK updates can be done HERE inline (w/o Qing in the ll)
        aobj *cln  = cloneAobj(apk);
        listAddNodeTail(g->co.ll, cln); /* UGLY: build list of PKs to update */
    }
    INCR(*card) CurrCard = *card;
    return 1;
}
static bool opUpdateSort(cli   *c,  list *ll,    cswc_t  *w,
                         wob_t *wb, bool  ofree, long    *sent,
                         bt   *btr, int   ncols, range_t *g) {
    bool     ret  = 1; /* presume success */
    obsl_t **v    = sortOB2Vector(ll);
    long     ofst = wb->ofst;
    for (int i = 0; i < (int)listLength(ll); i++) {
        if (wb->lim != -1 && *sent == wb->lim) break;
        if (ofst > 0) {
            ofst--;
        } else {
            *sent        = *sent + 1;
            obsl_t *ob   = v[i];
            aobj   *apk  = ob->row;
            void   *rrow = btFind(btr, apk);
            if (updateRow(c, btr, apk, rrow, w->wf.tmatch, ncols,
                          g->up.matches, g->up.indices, g->up.vals,
                          g->up.vlens, g->up.cmiss, g->up.ue) == -1) {
                ret = 0; break; /* negate presumed success */
            }
        }
    }
    sortOBCleanup(v, listLength(ll), ofree);
    free(v); /* FREED 004 */
    return ret;
}
void iupdateAction(cli  *c,      cswc_t *w,       wob_t *wb,
                   int   ncols,  int     matches, int    inds[],
                   char *vals[], uint32  vlens[], uchar  cmiss[],
                   ue_t  ue[]) {
    range_t g; qr_t    q;
    setQueued(w, wb, &q);
    list *ll     = initOBsort(1, wb);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    bt   *btr    = getBtr(w->wf.tmatch);
    g.up.btr     = btr;
    g.up.ncols   = ncols;
    g.up.matches = matches;
    g.up.indices = inds;
    g.up.vals    = vals;
    g.up.vlens   = vlens;
    g.up.cmiss   = cmiss;
    g.up.ue      = ue;
    long card    = Op(&g, update_op);
    if (card == -1) return; /* e.g. update MCI UNIQ Violation */
    long sent    = 0;
    bool err     = 0;
    if (card) {
        if (q.qed) { if (!opUpdateSort(c, ll, w, wb, g.co.ofree, &sent,
                                       btr, ncols, &g)) return; } /* MCI VIOL */
        else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *apk  = ln->value;
                void *rrow = btFind(g.up.btr, apk);
                if (updateRow(g.co.c, g.up.btr, apk, rrow, g.co.w->wf.tmatch,
                              g.up.ncols, g.up.matches, g.up.indices, g.up.vals,
                              g.up.vlens, g.up.cmiss, g.up.ue) == -1) {
                    err = 1; break;
                }
                sent++;
            }
            listReleaseIterator(li);
        }
    }
    if (!err) {
        if (wb->lim != -1 && sent < card) card = sent;
        addReplyLongLong(c, (ull)card);

        if (wb->ovar) incrOffsetVar(c, wb, card);
    }
    releaseOBsort(ll);
}

void dumpQueued(printer *prn, cswc_t *w, wob_t *wb, qr_t *q, bool debug) {
    if (debug) {
        r_ind_t *ri     = (w->wf.imatch == -1) ? NULL :
                                                 &Index[w->wf.imatch];
        bool     virt   = (w->wf.imatch == -1) ? 0    : ri->virt;
        int      cmatch = (w->wf.imatch == -1) ? -1   : ri->column;
        if (virt) (*prn)("\t\tqpk: %d pklim: %d pklo: %d\n",
                         q->pk, q->pk_lim, q->pk_lo);
        else      (*prn)("\t\tqfk: %d fklim: %d fklo: %d\n",
                         q->fk, q->fk_lim, q->fk_lo);
        (*prn)("\t\tvirt: %d asc: %d obc: %d lim: %ld" \
                    " ofst: %ld indcl: %d -> qed: %d\n",
            virt, wb->asc[0], wb->obc[0], wb->lim, wb->ofst, cmatch, q->qed);
    } else {
        (*prn)("\t\tqed:\t%d\n", q->qed);
    }
}

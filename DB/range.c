/*
 * This file implements Range OPS (iselect, idelete, iupdate)
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
#include <assert.h>

#include "adlist.h"
#include "redis.h"

#include "debug.h"
#include "lru.h"
#include "lfu.h"
#include "bt.h"
#include "bt_iterator.h"
#include "filter.h"
#include "orderby.h"
#include "index.h"
#include "wc.h"
#include "qo.h"
#include "colparse.h"
#include "find.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "range.h"

extern aobj_cmp *OP_CMP[7];
extern r_tbl_t  *Tbl;
extern r_ind_t  *Index;
extern uchar     OutputMode;

ulong  CurrCard = 0; // TODO remove - after no update on MCI cols FIX


/* NOTE: this struct contains pointers, it is to be used ONLY for derefs */
typedef struct inner_bt_data {
    row_op  *p;    range_t *g;    qr_t    *q;
    bt      *btr;  bt      *nbtr;
    long    *ofst; long    *card; long    *loops; bool    *brkr;
    int      obc;
} ibtd_t;
typedef bool node_op(ibtd_t *d);

// PROTOTYPES
static bool nBT_RowOp(ibtd_t *d,    qr_t *q,    wob_t *wb,  bool missed,
                      aobj   *bkey, void *brow,
                      bool    iss,  bool *ret,  bool   noop);
static bool dellist_op(range_t *g, aobj *apk, void *rrow, bool q, long *card);

#define DEBUG_SINGLE_PK                                       \
  printf("singleOpPK: tmatch: %d\n", g->co.w->wf.tmatch);
#define DEBUG_RANGE_PK                                        \
  printf("rangeOpPK: imatch: %d\n", g->co.w->wf.imatch);
#define DEBUG_UBT_OP                                                       \
  printf("uBT_Op: btr: %p UniqueIndexVal: ", d->g->co.btr);                \
  dumpAobj(printf, &UniqueIndexVal);
#define DEBUG_NODE_BT                                                      \
  printf("nodeBT_Op: nbtr->numkeys: %d\n", d->nbtr->numkeys);
#define DEBUG_NODE_BT_OBC_1                                                \
  printf("nodeBT_Op OBYI_1: key: ");                                       \
  printf("nbe_key: "); dumpAobj(printf, nbe->key);                         \
  DEBUG_BT_TYPE(printf, nbtr)
#define DEBUG_NODE_BT_OBC_2                                                \
  printf("nodeBT_Op OBYI_2: nbtr: %p ctype: %d obc: %d val: %p key: ",     \
         nbtr, ctype, d->obc, nbe->val); dumpAobj(printf, &akey);          \
  printf("nbe_key: "); dumpAobj(printf, nbe->key);
#define DEBUG_MCI_FIND                                                     \
  printf("in btMCIFindVal: trgr: %d\n", trgr);
#define DEBUG_MCI_FIND_MID                                                 \
  dumpFilter(printf, flt, "\t");
#define DEBUG_RUN_ON_NODE                                                  \
  printf("in runOnNode: ibtr: %p still: %u nop: %p\n", ibtr, still, nop);  \
  bt_dumptree(printf, ibtr, 0, 0);
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

//TODO inline
void init_range(range_t *g, redisClient *c,  cswc_t *w,     wob_t *wb,
                qr_t    *q, list        *ll, uchar   ofree, jb_t  *jb) {
    bzero(g, sizeof(range_t));
    g->co.c     = c;     g->co.w     = w;  g->co.wb = wb; g->co.ll    = ll;
    g->co.ofree = ofree; g->q        = q;  g->jb    = jb;
}
//TODO inline
static void init_ibtd(ibtd_t *d,    row_op *p,    range_t *g,     qr_t *q,
                      bt     *nbtr, long   *ofst, long    *card,  long *loops,
                      bool   *brkr, int     obc) {
    d->p    = p;    d->g    = g;    d->q     = q;     d->nbtr = nbtr;
    d->ofst = ofst; d->card = card; d->loops = loops; d->brkr = brkr;
    d->obc  = obc;
}

// QUEUE_FOR_ORDER_BY_SORT QUEUE_FOR_ORDER_BY_SORT QUEUE_FOR_ORDER_BY_SORT
static bool mci_fk_queued(wob_t *wb, r_ind_t *ri) { //printf("mci_fk_queued\n");
    uint32_t i;
    for (i = 0; i < (uint32_t)ri->nclist && i < wb->nob; i++) {
        if (wb->obc[i] != ri->bclist[i]) break;
    }
    if (i < wb->nob) {
        int obc = (ri->obc == -1) ? 0 : ri->obc;
        if (wb->obc[i] != obc) return 1; i++;
    }
    return (i != wb->nob);
}
static void setRangeQueued(cswc_t *w, wob_t *wb, qr_t *q) {
    bzero(q, sizeof(qr_t));
    r_ind_t *ri     = (w->wf.imatch == -1) ? NULL: &Index[w->wf.imatch];
    int      obc    = (!ri || ri->obc == -1) ? 0 : ri->obc;
    bool     virt   = ri ?  ri->virt : 0;
    int      cmatch = ri ? ri->column ? ri->column : -1 : -1;
    if (virt) { // NOTE: there is no inner_desc possible (no inner loop)
        q->pk_desc  = (wb->nob >= 1 && (!wb->asc[0] && wb->obc[0] == obc));
        q->pk       = (wb->nob > 1) || (wb->nob == 1 && wb->obc[0] != obc);
        q->pk_lim   = (!q->pk    && (wb->lim  != -1));
        q->pk_lo    = (q->pk_lim && (wb->ofst != -1));
        q->qed      = q->pk;
    } else {
        q->fk_desc  = (wb->nob >= 1 && (!wb->asc[0] && wb->obc[0] == cmatch));
        q->inr_desc = (wb->nob == 1 && (!wb->asc[0] && wb->obc[0] == obc)) ||
                      (wb->nob > 1 && (wb->obc[0] == cmatch) &&
                       (!wb->asc[1] && wb->obc[1] == obc)); // FK,PK DESC
        if (w->wtype & SQL_SINGLE_FK_LKP) {
            if (ri->nclist) q->fk = mci_fk_queued(wb, ri);
            else { // NO-Q: [OBY FK|OBY PK|OBY FK,PK|OBY PK,FK]
                q->fk = (wb->nob > 2) ||
                        (wb->nob == 1 &&  // [FK}&[PK]
                         (wb->obc[0] != cmatch) && (wb->obc[0] != obc)) ||
                        (wb->nob == 2 &&  // [FK,PK]&[PK,FK]
                         (!((wb->obc[0] == cmatch) && (wb->obc[1] == obc)) &&
                          !((wb->obc[1] == cmatch) && (wb->obc[0] == obc))));
            }
        } else { // FK RANGE QUERY (clist[MCI] not yet supported)
            q->fk   = (wb->nob > 2) || // NoQ: [OBY FK| OBY FK,PK]
                      (wb->nob == 1 && (wb->obc[0] != cmatch)) ||
                      (wb->nob == 2 && 
                       !((wb->obc[0] == cmatch) && (wb->obc[1] == obc)));
        }
        q->fk_lim   = (!q->fk    && (wb->lim  != -1));
        q->fk_lo    = (q->fk_lim && (wb->ofst != -1));
        q->qed      = q->fk;
    } //dumpQueued(printf, w, wb, q, 1);
}
static void setInQueued(cswc_t *w, wob_t *wb, qr_t *q) {
    setRangeQueued(w, wb, q); q->pk_lo = q->fk_lo = 0;// LIM OFST -> ALWAYS SORT
}
void setQueued(cswc_t *w, wob_t *wb, qr_t *q) { /* NOTE: NOT for JOINS */
    if (w->wf.inl) setInQueued   (w, wb, q);
    else           setRangeQueued(w, wb, q);
}

// OPERATIONS OPERATIONS OPERATIONS OPERATIONS OPERATIONS OPERATIONS OPERATIONS
//TODO inline
static bool pk_row_op(aobj  *apk, void *rrow, range_t *g,    row_op *p,
                      qr_t *q,    long    *card) {
    if (rrow && !(*p)(g, apk, rrow, q->qed, card)) return 0;
    else                                           return 1;
}
static bool pk_op_l(aobj *apk, void *rrow, range_t *g,     row_op *p, wob_t *wb,
                    qr_t *q,   long *card, long    *loops, bool   *brkr) {
printf("pk_op_l\n");
    *brkr = 0; INCR(*loops)
    long pkl = q->pk_lim;
    if (pkl && !q->pk_lo && wb->ofst != -1 && *loops < wb->ofst) return 1;
    bool ret = pk_row_op(apk, rrow, g, p, q, card);
    if (pkl && wb->lim == *card) { *brkr = 1;                    return 1; }
    return ret;
}
#define VOID_1 (void *)1
#define CBRK   { card = -1;   break; }
#define NBRK   { nbtr = NULL; break; }

//NOTE singleOpPK() used only in JOINs
static long singleOpPK(range_t *g, row_op *p) {               //DEBUG_SINGLE_PK
    cswc_t  *w     = g->co.w; qr_t *q = g->q;
    aobj    *apk   = &w->wf.akey;
    g->co.btr      = getBtr(w->wf.tmatch);
    dwm_t    dwm   = btFindD(g->co.btr, apk);         if (dwm.miss) return -1;
    void    *rrow  = dwm.k;                           if (!rrow)    return  0;
    bool     gost  = !UU(g->co.btr) && !(*(uchar *)rrow); if (gost) return -1;
    long     card  =  0;
    if (!pk_row_op(apk, rrow, g, p, q, &card))                      return -1;
    else                                                            return card;
}

// RANGE_DEL_SIMULATE_DR RANGE_DEL_SIMULATE_DR RANGE_DEL_SIMULATE_DR
#define DEBUG_RDSD                                                   \
  if (pk) {                                                          \
      printf("rDelSimDrPK: dr: %u dlt: %u key: ", dr, dlt);          \
      dumpAobj(printf, akey);                                        \
      printf("rDelSimDrPK: low:  "); dumpAobj(printf, &w->wf.alow);  \
      printf("rDelSimDrPK: high: "); dumpAobj(printf, &w->wf.ahigh); \
  } else {                                                           \
      printf("rDelSimDrFK: dr: %u asc: %d key: ", dr, t->asc);       \
      dumpAobj(printf, akey);                                        \
      printf("rDelSimDrFK: low:  "); dumpAobj(printf, &w->wf.alow);  \
      printf("rDelSimDrFK: high: "); dumpAobj(printf, &w->wf.ahigh); }

typedef struct range_del_sim_dr_t {
    long   *card; long *loops;  bool *brkr; // PK_OP_L
    ibtd_t *d;    bool  missed; void *brow; bool *ret; btSIter *bi; bool asc;
} rdsd_t;

//NOTE: RangeDelete/Update need to simulate DRs to run to completion
#define RDSR_ERR  0
#define RDSR_OK   1
#define RDSR_FULL 2

#define TBRK          { *t->brkr = 1; break; }
#define ASC_INCRAKEY  if (t->asc)  incrbyAobj(akey, 1);
#define DESC_DECRAKEY if (!t->asc) decrbyAobj(akey, 1);
#define DESC_CONT     {DESC_DECRAKEY continue; }
static int rDelSimDrGen(bool  pk,   row_op *p, range_t *g,    uint32 dr,
                        aobj *akey, rdsd_t *t, uint32   dlt) {
    cswc_t *w = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;       DEBUG_RDSD
    int i   = t->asc ? 0       : (int)dr - 1;
    int fin = t->asc ? (int)dr : -1;          uint32 j = 0;
    if (!t->asc)       incrbyAobj(akey, dlt ? dlt : dr); // REV PKs OK -> NOOP
    if (t->asc && dlt) incrbyAobj(akey, dr - dlt);
printf("i: %d fin: %d asc: %d dr: %d dlt: %d key: ", i, fin, t->asc, dr, dlt); dumpAobj(printf, akey);
    while (i != fin) {
        ASC_INCRAKEY
printf("%d: key: ", i); dumpAobj(printf, akey);
        if (aobjLT(akey, &w->wf.alow))  { if (t->asc) continue; else TBRK }
        if (aobjGT(akey, &w->wf.ahigh)) { if (t->asc) TBRK      else DESC_CONT }
        if (p) {
            if (!pk_op_l(akey, VOID_1, g, p, wb, q,
                         t->card, t->loops, t->brkr)) return RDSR_ERR;      j++;
            if (*t->brkr) break;
        } else {
            nBT_RowOp(t->d, q, wb, t->missed, akey, t->brow, 0, t->ret, 1); j++;
            if (*t->d->brkr) break;// PREV line (nBT_RowOp) can NOT fail -> NOOP
        }
        i = t->asc ? i + 1 : i - 1; // loop increment
        DESC_DECRAKEY
    }
    return j;
}
static int rDelSimDrPK(range_t *g, row_op *p, uint32 dr, aobj *akey, rdsd_t *t,
                       uint32   dlt) {
    uint32 j = rDelSimDrGen(1, p, g, dr, akey, t, dlt);
printf("END rDelSimDrPK: ret: %d\n", (j == dr) ? RDSR_FULL : RDSR_OK);
    return (j == dr) ? RDSR_FULL : RDSR_OK;
}
static int rDelSimDrFK(range_t *g, uint32 dr, aobj *akey, rdsd_t *t) {
    uint32  pdr   = btGetDR(t->d->g->co.btr, akey);
    aobj   *opkey = pdr ? cloneAobj(akey) : //NEXT_LINE: NextPK GHOST_KEY w/ DR
                          cloneAobj(btGetNext(t->d->g->co.btr, akey));//FREE 113
printf("opkey: "); dumpAobj(printf, opkey);
    uint32  j     = rDelSimDrGen(0, NULL, g, dr, akey, t, 0);
             btDecrDR_PK(t->d->nbtr,      akey,  j); // INODE_BTREE
    bool b = btDecrDR_PK(t->d->g->co.btr, opkey, j); // DATA__BTREE
    if (b) { // Housekeeping: HARD_DELETE Empty GHOST KEY -- ECASE:5
printf("EMPTY GHOST KEY DELETIION: key: "); dumpAobj(printf, opkey);
        dellist_op(t->d->g, opkey, t->brow, g->q->qed, t->d->card);
        DECR(*t->d->card) CurrCard = *t->d->card;// does not count towards total
    }
    destroyAobj(opkey);                                  // FREED 113
    return (j == dr) ? RDSR_FULL : RDSR_OK;
}

// RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK
static long rangeOpPK(range_t *g, row_op *p) {                 //DEBUG_RANGE_PK
    btEntry *be; btSIter *bi;
    cswc_t  *w     = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool     iss   = g->se.qcols ? 1 : 0; bool isu = g->up.ncols ? 1 : 0;
    bool     isd   = !iss && !isu;
printf("rangeOpPK: iss: %d isu: %d isd: %d\n", iss, isu, isd);
    bt      *btr   = getBtr(w->wf.tmatch); g->co.btr = btr;
    g->asc         = !q->pk_desc;
    bool     brkr  = 0; long loops = -1; long card =  0;
    //TODO XthIter ONLY @ ZERO miss_delete state
    bi = (q->pk_lo) ? 
              btGetXthIter  (btr, &w->wf.alow, &w->wf.ahigh, wb->ofst, g->asc) :
              btGetRangeIter(btr, &w->wf.alow, &w->wf.ahigh, g->asc);
    if (!bi) return card;
printf("rangeOpPK: bi: %p missed: %d\n", bi, bi->missed);
    if (iss && bi->missed) card = -1; //NOTE: MISSED only relevant for SELECT
    else {
printf("isd: %d dr: %d missed: %d\n", isd, bi->be.dr, bi->missed);
        bool ok = 1; bool ignore_first = 0;
        rdsd_t t; bzero(&t, sizeof(rdsd_t));
        t.card = &card; t.loops = &loops; t.brkr = &brkr; t.asc = g->asc;
        if (isd && bi->be.dr && bi->missed) {// 1st ROW w/in 0th DR
            int b = rDelSimDrPK(g, p, bi->be.dr, bi->be.key, &t, bi->mdelta);
            if      (b == RDSR_ERR)  ok = 0; // NEXT_LINE: iter8ted ENTIRE DR
            else if (b == RDSR_FULL) {
                if (g->asc) btRangeNext(bi, g->asc); else ignore_first = 1;
            }
printf("post rDelSimDrPK: card: %ld brkr: %d\n", card, brkr);
        }
        if (ok && !brkr) while ((be = btRangeNext(bi, g->asc))) {
printf("rangeOpPK: LOOP: missed: %d dr: %u\n", bi->missed, be->dr);
            if (iss && bi->missed) { card = -1; break; }
//TODO this skips the DESC rDelSimDrPK below, push into "if ! {pk_op_l}"
            if (isu && bi->missed) continue; // RANGE UPDATE skips GHOSTs
            if (!ignore_first && !g->asc && isd && be->dr) {
                if (rDelSimDrPK(g, p, be->dr, be->key, &t, 0) == RDSR_ERR) CBRK
                if (*t.brkr) break;
            }
            if (!(isd && isGhostRow(btr, be->x, be->i))) {// RangeDel SKIP GHOST
                if (!pk_op_l(be->key, be->val, g, p, wb, q,
                             &card, &loops, &brkr))                        CBRK
            }
            if (brkr) break;
            if (g->asc && isd && be->dr) {
                if (rDelSimDrPK(g, p, be->dr, be->key, &t, 0) == RDSR_ERR) CBRK
                if (*t.brkr) break;
            }
        }
        if (!g->asc) bi->missed = bi->nim;
printf("rangeOpPK: END: missed: %d card: %ld\n\n\n", bi->missed, card);
        if (iss && bi->missed) card = -1;
    }
    btReleaseRangeIterator(bi);
    return card;
}

// OTHER_BT_ITERATOR OTHER_BT_ITERATOR OTHER_BT_ITERATOR OTHER_BT_ITERATOR
static aobj  UniqueIndexVal;
void initX_DB_Range() { initAobj(&UniqueIndexVal); } //called on server startup 
static bool uBT_Op(ibtd_t *d) { /* OTHER_BTs (no evictions)*/    //DEBUG_UBT_OP
    INCR(*d->loops)
    if (d->g->se.cstar) { INCR(*d->card) return 1; }
    qr_t *q  = d->g->q; wob_t *wb = d->g->co.wb; /* code compaction */
    *d->brkr = 0;
    if (q->fk_lim) {
        if      (q->fk_lo && *d->loops < *d->ofst)    return 1;
        else if (wb->lim == *d->card) { *d->brkr = 1; return 1; }
    }
    void *rrow = btFind(d->g->co.btr, &UniqueIndexVal); /* FK lkp - must work */
    if (!(*d->p)(d->g, &UniqueIndexVal, rrow, d->g->q->qed, d->card)) return 0;
    return 1;
}

#define OBT_NODE_BT_OP(vcast, aobjpart, vtype)                              \
  { vcast *vvar = brow; akey.aobjpart = vvar->val; akey.type = vtype; }

// INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR
//NOTE: noop used for FK DELETEs of EVICTED rows (respect LIMIT OFFSET)
static bool nBT_RowOp(ibtd_t *d,    qr_t *q,    wob_t *wb,  bool missed,
                      aobj   *bkey, void *brow,
                      bool    iss,  bool *ret,  bool noop) {
printf("nBT_RowOp: noop: %d\n", noop);
    bt *nbtr = d->nbtr;
    INCR(*d->loops)
    if (q->fk_lim && !q->fk_lo && *d->loops < *d->ofst)         return 1;
    if (iss && missed) { *ret = 0;                              return 0; }
    if (noop) INCR(*d->card) // noop respects "OFFSET LIMIT" Used by rDelSimDr()
    else {
        void *key; aobj akey; initAobj(&akey);
        if (d->obc == -1) key = bkey;
        else {  /* ORDER BY INDEX query */                //DEBUG_NODE_BT_OBC_1
            uchar ctype = Tbl[d->g->co.btr->s.num].col[0].type; // PK_CTYPE
            if      C_IS_I(ctype) {
                if      UU(nbtr) initAobjInt(&akey, (uint32)(ulong)brow);
                else if LU(nbtr) OBT_NODE_BT_OP(luk, i, COL_TYPE_INT)
                else /* XU */    OBT_NODE_BT_OP(xuk, i, COL_TYPE_INT)
            } else if C_IS_L(ctype) {
                if      UL(nbtr) OBT_NODE_BT_OP(ulk, l, COL_TYPE_LONG)
                else if LL(nbtr) OBT_NODE_BT_OP(llk, l, COL_TYPE_LONG)
                else /* XL */    OBT_NODE_BT_OP(xlk, l, COL_TYPE_LONG)
            } else { // C_IS_X
                if      UX(nbtr) OBT_NODE_BT_OP(uxk, x, COL_TYPE_U128)
                else if LX(nbtr) OBT_NODE_BT_OP(lxk, x, COL_TYPE_U128)
                else /* XX */    OBT_NODE_BT_OP(xxk, x, COL_TYPE_U128)
            }
            key = &akey;                                //DEBUG_NODE_BT_OBC_2
        }
        void *rrow = btFind(d->g->co.btr, key); releaseAobj(&akey);
        if (!(*d->p)(d->g, key, rrow, q->qed, d->card)) { *ret = 0; return 0; }
    }
printf("nBT_RowOp: fklim: %d lim: %d card: %d\n", q->fk_lim, wb->lim, *d->card);
    if (q->fk_lim && wb->lim == *d->card) { *d->brkr = 1;           return 1; }
    return 1;
}
static bool nBT_Op(ibtd_t *d) {                              //DEBUG_NODE_BT
    cswc_t *w = d->g->co.w; wob_t *wb = d->g->co.wb; qr_t *q = d->g->q;
    if (d->g->se.cstar && !w->flist) { /* FK cstar w/o filters */
        if (d->nbtr->dirty)                return 0;
        INCRBY(*d->card, d->nbtr->numkeys) return 1;
    }
    if (q->fk_lo && FK_RQ(w->wtype) && d->nbtr->numkeys <= *d->ofst) {
        DECRBY(*d->ofst, d->nbtr->numkeys) return 1; // skip IndexNode
    }
    btEntry *nbe;
    bool     ret  = 1;                      /* presume success */
    bool     iss  = d->g->se.qcols ? 1 : 0; bool isu = d->g->up.ncols ? 1 : 0;
    bool     isd  = !iss && !isu;
    *d->brkr      = 0;
    bool     x    = (q->fk_lo && *d->ofst > 0);
    bool     nasc  = d->g->asc = !q->inr_desc;
    //TODO XthIter ONLY @ ZERO miss_delete state
    btSIter *nbi  = x ?
                     btGetFullXthIter  (d->nbtr, *d->ofst, nasc, w, wb->lim) :
                     btGetFullRangeIter(d->nbtr,           nasc, w);
if (nbi) printf("nBT_Op: nbi: %p isd: %d empty: %d\n", nbi, isd, nbi->empty);
else     printf("nBT_Op: NO nbi: x: %d\n", x);
    if      (!nbi)               return ret;
    else if (iss && nbi->missed) ret = 0; // MISSED only relevant for SELECT
    else if (!nbi->empty){
printf("GETITER: nbi: %p nbi.missed: %d isd: %d\n", nbi, nbi->missed, isd);
        rdsd_t t; bzero(&t, sizeof(rdsd_t)); bool ignore_first = 0;
        t.d = d; t.ret = &ret; t.bi = nbi; t.asc = nasc;
printf("dr: %d\n", nbi->be.dr); printf("key: "); dumpAobj(printf, nbi->be.key); printf("low: "); dumpAobj(printf, &w->wf.alow);
        if (isd && nbi->be.dr && nbi->missed) {
            t.missed = nbi->missed; t.brow = nbi->be.val; // Start w/in 0th DR
            int b    = rDelSimDrFK(d->g, nbi->be.dr, nbi->be.key, &t);
            if      (b == RDSR_ERR)  goto nbte; // NEXT_LINE iter8ted ENTIRE DR
            else if (b == RDSR_FULL) {
                if (nasc) btRangeNext(nbi, nasc); else ignore_first = 1;
            }
        }
        if (!*d->brkr) while ((nbe = btRangeNext(nbi, nasc))) {
printf("LOOP: nbi.missed: %d dr: %u nasc: %d\n", nbi->missed, nbe->dr, nasc);
            if (!ignore_first && !nasc && isd && nbe->dr) {
                t.missed = nbi->missed; t.brow = nbe->val;
                if (rDelSimDrFK(d->g, nbe->dr, nbe->key, &t) == RDSR_ERR) break;
            }
            if (!nBT_RowOp(d, q, wb, nbi->missed, nbe->key, nbe->val,
                           iss, &ret, 0) || *d->brkr)                     break;
            if (nasc && isd && nbe->dr) {
                t.missed = nbi->missed; t.brow = nbe->val;
                if (rDelSimDrFK(d->g, nbe->dr, nbe->key, &t) == RDSR_ERR) break;
            }
        }
        if (iss && nbi->missed) ret = 0; // FULL Itr8r -> no DR after end
    }

nbte:
    btReleaseRangeIterator(nbi);
    if (q->fk_lo)                         *d->ofst = 0; /* OFFSET fulfilled */
    if (q->fk_lim && wb->lim == *d->card) *d->brkr = 1; /* ORDERBY FK LIM*/
    return ret;
}

#define SETUNIQIVAL(vt, vcast, aobjpart)                     \
  { uv->enc      = uv->type = vt;                            \
    vcast *vvar  = btFind(nbtr, akey); if (!vvar)  return 0; \
    uv->aobjpart = vvar->val; }

static bool setUniqIndexVal(bt *nbtr, aobj *akey) {
    //printf("setUniqIndexVal: akey: "); dumpAobj(printf, akey);
    aobj *uv       = &UniqueIndexVal;
    if        UU(nbtr) {
        uv->enc = uv->type = COL_TYPE_INT;
        if (!(uv->i = (long)btFind(nbtr, akey))) return 0;
    } else if UL(nbtr) SETUNIQIVAL(COL_TYPE_LONG, ulk, l)
      else if LU(nbtr) SETUNIQIVAL(COL_TYPE_INT,  luk, i)
      else if LL(nbtr) SETUNIQIVAL(COL_TYPE_LONG, llk, l)
      else if UX(nbtr) SETUNIQIVAL(COL_TYPE_U128, uxk, x)
      else if XU(nbtr) SETUNIQIVAL(COL_TYPE_INT,  xuk, i)
      else if LX(nbtr) SETUNIQIVAL(COL_TYPE_U128, lxk, x)
      else if XL(nbtr) SETUNIQIVAL(COL_TYPE_LONG, xlk, l)
      else if XX(nbtr) SETUNIQIVAL(COL_TYPE_U128, xxk, x)
      else assert(!"setUniqIndexVal ERROR");
    return 1;
}

bt *btMCIFindVal(cswc_t *w, bt *nbtr, uint32 *nmatch, r_ind_t *ri) {
    if (nbtr && w->wf.klist) {
        listNode *ln;
        int       trgr = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
        int       i    = 0;                                     //DEBUG_MCI_FIND
        listIter *li   = listGetIterator(w->wf.klist, AL_START_HEAD);
        while ((ln = listNext(li))) {
            f_t *flt  = ln->value;                          //DEBUG_MCI_FIND_MID
            if (flt->op == NONE) break; /* MCI Joins can have empty flt's */
#if 0
            if (flt->op != EQ) { // MCI Indexes only support EQ ops
                do { // transfer rest of KLIST to FLIST
                    f_t *flt = ln->value;
                    addFltKey(&w->flist, cloneFilter(flt));
                } while ((ln = listNext(li))); break;
            }
#endif
            if (i == trgr) {
                ln = listNext(li); //TODO needed ???
                if (!setUniqIndexVal(nbtr, &flt->akey)) NBRK
                INCR(*nmatch) break;
            }
            bt  *xbtr = btIndFind(nbtr, &flt->akey);
            if (!xbtr)                                  NBRK /* MISS*/
            INCR(*nmatch) nbtr = xbtr; i++;
        } listReleaseIterator(li);
    }
    return nbtr;
}

#define OBT_RUNONNODE(vt, vcast, aobjpart) \
  {  uv->enc      = uv->type = vt;         \
     vcast *vvar  = nbe->val;              \
     uv->aobjpart = vvar->val; }

static bool runOnNode(bt      *ibtr, uint32  still,
                      node_op *nop,  ibtd_t *d,     r_ind_t *ri) {
    btEntry *nbe;                                           //DEBUG_RUN_ON_NODE
    still--;
    bool     ret  = 1; /* presume success */
    qr_t    *q    = d->g->q;     /* code compaction */
    bool     nasc = !q->inr_desc;
    btSIter *nbi  = btGetFullRangeIter(ibtr, nasc, NULL);
    if (!nbi) return ret;
    while ((nbe = btRangeNext(nbi, nasc))) {
        d->nbtr        = nbe->val;
        if (still) { /* Recurse until node*/
            if (!runOnNode(d->nbtr, still, nop, d, ri)) { ret = 0; break; }
        } else {
            if UNIQ(ri->cnstr) {
                //printf("runOnNode: UNIQ: "); DEBUG_BT_TYPE(printf, ibtr)
                d->nbtr        = ibtr; // Unique 1 step shorter
                aobj *uv       = &UniqueIndexVal;
                if        UU(d->nbtr) {
                    uv->enc = uv->type = COL_TYPE_INT;
                    uv->i   = (uint32)(ulong)nbe->val;
                } else if UL(d->nbtr) OBT_RUNONNODE(COL_TYPE_LONG, ulk, l)
                  else if LU(d->nbtr) OBT_RUNONNODE(COL_TYPE_INT,  luk, i)
                  else if LL(d->nbtr) OBT_RUNONNODE(COL_TYPE_LONG, llk, l)
                  else if UX(d->nbtr) OBT_RUNONNODE(COL_TYPE_U128, uxk, x)
                  else if XU(d->nbtr) OBT_RUNONNODE(COL_TYPE_INT,  xuk, i)
                  else if LX(d->nbtr) OBT_RUNONNODE(COL_TYPE_U128, lxk, x)
                  else if XL(d->nbtr) OBT_RUNONNODE(COL_TYPE_LONG, xlk, l)
                  else if XX(d->nbtr) OBT_RUNONNODE(COL_TYPE_U128, xxk, x)
                  else assert(!"runOnNode ERROR");
                  //printf("runOnNode: uv: "); dumpAobj(printf, uv);
            }  // NEXT LINE: When we get to NODE -> run [uBT_Op|nodeBT_Op]
            if (!(*nop)(d)) { ret = 0; break; }
        }
        if (*d->brkr) break;
    } btReleaseRangeIterator(nbi);
    return ret;
}
// RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK
static long rangeOpFK(range_t *g, row_op *p) {                 //DEBUG_RANGE_FK
    ibtd_t   d; btEntry *be; btSIter *bi;
    cswc_t  *w     = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool     iss   = g->se.qcols ? 1 : 0;
    g->co.btr      = getBtr (w->wf.tmatch);
    bt      *ibtr  = getIBtr(w->wf.imatch);
    r_ind_t *ri    = &Index [w->wf.imatch];
    node_op *nop   = UNIQ(ri->cnstr) ? uBT_Op : nBT_Op;
    uint32   nexpc = ri->clist ? (ri->clist->len - 1) : 0;
    bool     singu = (!ri->clist && UNIQ(ri->cnstr)); // SINGLE COL UNIQ
    long     ofst  = wb->ofst;
    g->asc         = !q->fk_desc;
    long     loops = -1; long card =  0; bool brkr =  0;

    bool     smplo = SIMP_UNIQ(ibtr) && q->fk_lo;
    if (!smplo) bi  = btGetRangeIter(ibtr, &w->wf.alow, &w->wf.ahigh, g->asc);
    else { // SIMPLE UNIQUE + OFFSET -> use SCION iter8trs
        bi   = btGetXthIter(ibtr, &w->wf.alow, &w->wf.ahigh, wb->ofst, g->asc);
        ofst = wb->ofst = -1; // OFFSET handled by btGetXthIter()
    }
    if (!bi) return card;
    init_ibtd(&d, p, g, q, NULL, &ofst, &card, &loops, &brkr, ri->obc);
    while ((be = btRangeNext(bi, g->asc))) {
printf("rangeOpFK: LOOP: bi->miss: %d be->val: %p\n", bi->missed, be->val);
        if (iss && !be->val) { card = -1; break; }
        uint32  nmatch = 0;
        d.nbtr         = singu ? ibtr : btMCIFindVal(w, be->val, &nmatch, ri);
        if (d.nbtr) {
            uint32 diff = nexpc - nmatch;
            if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) CBRK }
            else {
                if (singu && !setUniqIndexVal(d.nbtr, be->key))         CBRK
                if (!(*nop)(&d))                                        CBRK
            }
        }
        if (brkr) break;
    } btReleaseRangeIterator(bi);
    return card;
}
static long singleOpFK(range_t *g, row_op *p) {               //DEBUG_SINGLE_FK
    ibtd_t    d;
    cswc_t   *w      = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool      iss    = g->se.qcols ? 1 : 0;
    g->co.btr        = getBtr (w->wf.tmatch);
    bt       *ibtr   = getIBtr(w->wf.imatch);
    aobj     *afk    = &w->wf.akey;
    uint32    nmatch = 0;
    r_ind_t  *ri     = &Index[w->wf.imatch];
    node_op  *nop    = UNIQ(ri->cnstr) ? uBT_Op : nBT_Op;
    uint32    nexpc  = ri->clist ? (ri->clist->len - 1) : 0;
    bool      singu  = SIMP_UNIQ(ibtr);
    bool      exists = btIndExist(ibtr, afk);
    bt       *fibtr  = singu ? NULL : btIndFind(ibtr, afk);
printf("singleOpFK: fibtr: %p exists: %d\n", fibtr, exists);
    if (!singu && iss && exists && !fibtr) return -1;
    bt       *nbtr   = singu ? ibtr : btMCIFindVal(w, fibtr, &nmatch, ri);
printf("singleOpFK: nbtr: %p\n", nbtr);
    long      ofst   = wb->ofst;
    long      loops  = -1; long card =  0; bool brkr =  0;
    init_ibtd(&d, p, g, q, nbtr, &ofst, &card, &loops, &brkr, ri->obc);
    if (d.nbtr) {
        uint32 diff = nexpc - nmatch;
        if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) return -1; }
        else {
            if (singu && !setUniqIndexVal(nbtr, afk)) return -1;
            if (!(*nop)(&d))                                       return -1;
        }
    }
    return card;
}
static long rangeOp(range_t *g, row_op *p) {
    return (g->co.w->wtype & SQL_SINGLE_FK_LKP) ? singleOpFK(g, p) :
           (Index[g->co.w->wf.imatch].virt)     ? rangeOpPK (g, p) :
                                                  rangeOpFK (g, p);
}
long keyOp(range_t *g, row_op *p) {
    return (g->co.w->wtype & SQL_SINGLE_LKP)    ? singleOpPK(g, p) :
                                                  rangeOp   (g, p);
}

// TODO inOPs should use a BT not a LL -> do a bt_find()
static long inOpPK(range_t *g, row_op *p) {                //printf("inOpPK\n");
    listNode  *ln;
    cswc_t    *w      = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool       brkr   =  0;
    g->co.btr         = getBtr(w->wf.tmatch);
    long      loops   = -1;
    long      card    =  0;
    listIter *li      = listGetIterator(w->wf.inl, AL_START_HEAD);
    while((ln = listNext(li))) {
        aobj  *apk  = ln->value;
        dwm_t  dwm  = btFindD(g->co.btr, apk); if (dwm.miss) return -1;
        void  *rrow = dwm.k;
        bool   gost = !UU(g->co.btr) && !(*(uchar *)rrow); if (gost) continue;
        if (rrow && !pk_op_l(apk, rrow, g, p, wb, q, &card, &loops, &brkr)) CBRK
        if (brkr) break;
    } listReleaseIterator(li);
    return card;
}
static long inOpFK(range_t *g, row_op *p) {                //printf("inOpFK\n");
    ibtd_t    d; listNode *ln;
    cswc_t   *w       = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool      iss     = g->se.qcols ? 1 : 0;
    g->co.btr         = getBtr (w->wf.tmatch);
    bt       *ibtr    = getIBtr(w->wf.imatch);
    r_ind_t  *ri      = &Index [w->wf.imatch];
    node_op  *nop     = UNIQ(ri->cnstr) ? uBT_Op : nBT_Op;
    uint32    nexpc   = ri->clist ? (ri->clist->len - 1) : 0;
    long      ofst    = wb->ofst;
    long      loops   = -1; long card =  0; bool brkr =  0;
    init_ibtd(&d, p, g, q, NULL, &ofst, &card, &loops, &brkr, ri->obc);
    listIter *li      = listGetIterator(w->wf.inl, AL_START_HEAD);
    while((ln = listNext(li))) {
        uint32  nmatch = 0;
        aobj   *afk    = ln->value;
        bool    exists = btIndExist(ibtr, afk);
        bt     *beval  = btIndFind (ibtr, afk);
printf("inOpFK: beval: %p exists: %d\n", beval, exists);
        if (iss && exists && !beval) return -1;
        d.nbtr         = btMCIFindVal(w, beval, &nmatch, ri);
        if (d.nbtr) {
            uint32 diff = nexpc - nmatch;
            if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) CBRK }
            else if (!(*nop)(&d))                                       CBRK
        }
        if (brkr) break;
    } listReleaseIterator(li);
    return card;
}
static long inOp(range_t *g, row_op *p) {
    return Index[g->co.w->wf.imatch].virt ? inOpPK(g, p) : inOpFK(g, p);
}
long Op(range_t *g, row_op *p) {
    if (g->co.w->wtype == SQL_IN_LKP) return inOp( g, p);
    else                              return keyOp(g, p);
}

// FILTERS FILTERS FILTERS FILTERS FILTERS FILTERS FILTERS FILTERS FILTERS
bool passFilters(bt *btr, aobj *akey, void *rrow, list *flist, int tmatch) {
    if (!flist) return 1; /* no filters always passes */
    listNode *ln, *ln2;
    bool      ret = 1;
    listIter *li  = listGetIterator(flist, AL_START_HEAD);
    while ((ln = listNext(li))) {
        f_t *flt  = ln->value;
        if (tmatch != flt->tmatch) continue;
        aobj a    = getCol(btr, rrow, flt->cmatch, akey, tmatch);
        if        (flt->inl) {
            listIter *li2 = listGetIterator(flt->inl, AL_START_HEAD);
            while((ln2 = listNext(li2))) {
                aobj *a2  = ln2->value;
                ret       = (*OP_CMP[EQ])(a2, &a);        //DEBUG_PASS_FILT_INL
                if (ret) break;                   /* break INNER-LOOP on hit */
            } listReleaseIterator(li2);
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
    } listReleaseIterator(li);
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
            GET_LRUC GET_LFUC
            if (!addReplyRow(g->co.c, r, tmatch, apk, lruc, lrud, lfuc, lfu)) {
                ret = 0;
            }
        }
        if (!(EREDIS)) decrRefCount(r); //TODO MEMLEAK??? for EREDIS
    }
    INCR(*card) CurrCard = *card; return ret;
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
            if (!addReplyRow(c, ob->row, tmatch, ob->apk, ob->lruc, ob->lrud,
                                                          ob->lfuc, ob->lfu)) {
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
    list *ll     = initOBsort(q.qed, wb, 0);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_ROBJ, NULL);
    g.se.cstar   = cstar;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    void *rlen   = cstar || EREDIS ? NULL : addDeferredMultiBulkLength(c);
    long  card   = Op(&g, select_op);
printf("iselectAction: card: %ld\n", card);
    if (card == -1) { replaceDMB_WithDirtyMissErr(c, rlen); goto isele; }
    long sent    = 0;
    if (card) {
        if (q.qed) {
            if (!opSelectSort(c, ll, wb, g.co.ofree,
                              &sent, w->wf.tmatch))          goto isele;
        } else sent = card;
    }
    if (wb->lim != -1 && sent < card) card = sent;
    if      (cstar)   addReplyLongLong(c, card);
    else if (!EREDIS) setDeferredMultiBulkLength(c, rlen, card);

isele:
    if (wb->ovar) incrOffsetVar(c, wb, card);
    releaseOBsort(ll);
}

typedef list *list_adder(list *list, void *value);
static bool dellist_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
printf("START: dellist_op: asc: %d\n", g->asc);
    if (!passFilters(g->co.btr, apk, rrow,
                     g->co.w->flist, g->co.w->wf.tmatch)) return 1;
    if (q) {
        addRow2OBList(g->co.ll, g->co.wb, g->co.btr, apk, g->co.ofree,
                      rrow, apk);
    } else {
printf("dellist_op: adding: key: "); dumpAobj(printf, apk);
        aobj *cln  = cloneAobj(apk); //NOTE: next line builds BACKWARDS list
        list_adder *la = g->asc ? listAddNodeHead : listAddNodeTail;
        (*la)(g->co.ll, cln); /* UGLY: build list of PKs to delete */
    }
    INCR(*card) CurrCard = *card; return 1;
}
static void opDeleteSort(list *ll,    cswc_t *w,      wob_t *wb,   bool  ofree,
                         long  *sent, int     matches, int   inds[]) {
    obsl_t **v    = sortOB2Vector(ll);
    long     ofst = wb->ofst;
    for (int i = (int)listLength(ll) - 1; i >= 0; i--) { // REVERSE iteration
        if (wb->lim != -1 && *sent == wb->lim) break;
        if (ofst > 0) { ofst--; continue; }
        obsl_t *ob  = v[i];
        aobj   *apk = ob->row;
        if (deleteRow(w->wf.tmatch, apk, matches, inds, 0)) INCR(*sent)
    }
    sortOBCleanup(v, listLength(ll), ofree);
    free(v); /* FREED 004 */
}
void ideleteAction(redisClient *c, cswc_t *w, wob_t *wb) {
    range_t g; qr_t    q;
    setQueued(w, wb, &q);
    list *ll   = initOBsort(q.qed, wb, 1);
    if (!q.qed) ll->free = destroyAobj;
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    long  sent = 0;
    long  card = Op(&g, dellist_op);
    if (!card) addReply(c, shared.czero);
    if (card <= 0) { releaseOBsort(ll); return; }
    MATCH_INDICES(w->wf.tmatch)
    if (q.qed) {
        opDeleteSort(ll, w, wb, g.co.ofree, &sent, matches, inds);
    } else {
        listNode  *ln;
        listIter  *li = listGetIterator(ll, AL_START_HEAD);
        while((ln = listNext(li))) {
            aobj *apk = ln->value;
printf("ideleteAction: key: "); dumpAobj(printf, apk);
            if (deleteRow(w->wf.tmatch, apk, matches, inds, 0)) sent++;
        } listReleaseIterator(li);
    }
    if (sent < card) card = sent;
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
printf("update_op: adding: key: "); dumpAobj(printf, apk);
        //TODO non-FK/PK updates can be done HERE inline (w/o Qing in the ll)
        aobj *cln  = cloneAobj(apk);
        listAddNodeTail(g->co.ll, cln); /* UGLY: build list of PKs to update */
    }
    INCR(*card) CurrCard = *card; return 1;
}
static bool opUpdateSort(cli   *c,   list *ll,    cswc_t  *w,
                         wob_t *wb,  bool  ofree, long    *sent,
                         bt    *btr, int   ncols, range_t *g) {
    uc_t     uc;
    bool     ret  = 1; /* presume success */
    obsl_t **v    = sortOB2Vector(ll);
    long     ofst = wb->ofst;
    init_uc(&uc, btr, w->wf.tmatch, ncols, g->up.matches, g->up.indices,
            g->up.vals, g->up.vlens, g->up.cmiss, g->up.ue, g->up.le);
    for (int i = 0; i < (int)listLength(ll); i++) {
        if (wb->lim != -1 && *sent == wb->lim) break;
        if (ofst > 0) ofst--;
        else {
            *sent        = *sent + 1;
            obsl_t *ob   = v[i];
            aobj   *apk  = ob->row;
            void   *rrow = btFind(btr, apk);
            if (updateRow(c, &uc, apk, rrow) == -1) {
                ret = 0; break; /* negate presumed success */
            } //NOTE: rrow is no longer valid, updateRow() can change it
        }
    }
    release_uc(&uc); sortOBCleanup(v, listLength(ll), ofree); free(v);//FREED004
    return ret;
}
void iupdateAction(cli  *c,      cswc_t *w,       wob_t *wb,
                   int   ncols,  int     matches, int    inds[],
                   char *vals[], uint32  vlens[], uchar  cmiss[],
                   ue_t  ue[],   lue_t  *le) {
    range_t g; qr_t    q;
    setQueued(w, wb, &q);
    list *ll     = initOBsort(q.qed, wb, 1);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    bt   *btr    = getBtr(w->wf.tmatch); g.up.btr = btr;
    g.up.ncols   = ncols;
    g.up.matches = matches; g.up.indices = inds;
    g.up.vals    = vals;    g.up.vlens   = vlens;
    g.up.cmiss   = cmiss;
    g.up.ue      = ue;      g.up.le      = le;
    long card    = Op(&g, update_op);
    if (card == -1) goto iup_end; // MCI UNIQ Violation || DirtyMiss */
    long sent    = 0;
    bool err     = 0;
    if (card) {
        if (q.qed) {
            if (!opUpdateSort(c, ll, w, wb, g.co.ofree,
                              &sent, btr, ncols, &g))  goto iup_end; // MCI VIOL
        } else {
            listNode  *ln;
            uc_t  uc;
            init_uc(&uc, g.up.btr,   g.co.w->wf.tmatch,
                         g.up.ncols, g.up.matches, g.up.indices, g.up.vals,
                         g.up.vlens, g.up.cmiss,   g.up.ue,      g.up.le);
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li))) {
                aobj *apk  = ln->value;
                void *rrow = btFind(g.up.btr, apk);
                if (updateRow(g.co.c, &uc, apk, rrow) == -1) { err = 1; break; }
                //NOTE: rrow is no longer valid, updateRow() can change it
                sent++;
            } listReleaseIterator(li);
            release_uc(&uc);
        }
    }
    if (!err) {
        if (wb->lim != -1 && sent < card) card = sent;
        addReplyLongLong(c, (ull)card);
        if (wb->ovar) incrOffsetVar(c, wb, card);
    }

iup_end:
    releaseOBsort(ll);
}

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpQueued(printer *prn, cswc_t *w, wob_t *wb, qr_t *q, bool debug) {
    if (debug) {
        r_ind_t *ri     = (w->wf.imatch == -1) ? NULL :
                                                 &Index[w->wf.imatch];
        bool     virt   = (w->wf.imatch == -1) ? 0    : ri->virt;
        int      cmatch = (w->wf.imatch == -1) ? -1   : ri->column;
        if (virt) (*prn)("\t\tqpk: %d pklim: %d pklo: %d desc: %d\n",
                         q->pk, q->pk_lim, q->pk_lo, q->pk_desc);
        else      (*prn)("\t\tqfk: %d fklim: %d fklo: %d desc: %d\n",
                         q->fk, q->fk_lim, q->fk_lo, q->fk_desc);
        (*prn)("\t\tvirt: %d asc: %d obc: %d lim: %ld" \
                    " ofst: %ld indcol: %d inner_desc: %d -> qed: %d\n",
            virt, wb->asc[0], wb->obc[0], wb->lim, wb->ofst,
            cmatch, q->inr_desc, q->qed);
    } else {
        (*prn)("\t\tqed:\t%d\n", q->qed);
    }
}

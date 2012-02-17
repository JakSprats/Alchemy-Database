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

#include "xdb_hooks.h"

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
#include "rangedebug.h"
#include "range.h"

/* RANGE TODO LIST
    1.) SETUNIQIVAL should be btFindD() ... but this all needs to be TESTed
    2.) btMCIFindVal logic for !EQ flts
    3.) inOPs should use a BT not a LL -> do a bt_find()
    4.) select_op() MEMLEAK??? for EREDIS
    5.) non-FK/PK updates can be in update_op() (w/o Qing in the ll)
*/

// GLOBALS
extern aobj_cmp *OP_CMP[7];
extern r_tbl_t  *Tbl;
extern r_ind_t  *Index;

/* NOTE: this struct contains pointers, it is to be used ONLY for derefs */
typedef struct inner_bt_data {
    row_op  *p;    range_t *g;    qr_t    *q;
    bt      *btr;  bt      *nbtr;
    long    *ofst; long    *card; long    *loops; bool    *brkr;
    icol_t   obc;
} ibtd_t;
typedef bool node_op(ibtd_t *d);

// PROTOTYPES
static bool nBT_ROp(ibtd_t *d,    qr_t *q,    wob_t *wb,
                    aobj   *bkey, void *brow, bool *ret);
static bool dellist_op(range_t *g, aobj *apk, void *rrow, bool q, long *card);

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
                      bool   *brkr, icol_t  obc) {
    d->p    = p;    d->g    = g;    d->q     = q;     d->nbtr = nbtr;
    d->ofst = ofst; d->card = card; d->loops = loops; d->brkr = brkr;
    d->obc  = obc;
}

// QUEUE_FOR_ORDER_BY_SORT QUEUE_FOR_ORDER_BY_SORT QUEUE_FOR_ORDER_BY_SORT
bool icol_cmp(icol_t *ic1, icol_t *ic2) {
    bool ret =  ic1->cmatch == ic2->cmatch ? 0 :
                ic1->cmatch >  ic2->cmatch ? 1 : -1;
    if      (!ic1->nlo && !ic1->nlo) return ret;
    else if (ic1->nlo  && ic1->nlo) { // Compare "lo"
        if (ic1->nlo != ic2->nlo)    return ic1->nlo > ic2->nlo ? 1 : -1;
        else {
            for (uint32 i = 0; i < ic1->nlo; i++) {
                bool r = strcmp(ic1->lo[i], ic2->lo[i]); if (r) return r;
            }
            return 0;
        }
    } else                           return ic1->nlo ? 1 : -1;
}
static bool mci_fk_queued(wob_t *wb, r_ind_t *ri) { //printf("mci_fk_queued\n");
    uint32_t i;
    for (i = 0; i < (uint32_t)ri->nclist && i < wb->nob; i++) {
        if (icol_cmp(&wb->obc[i], &ri->bclist[i])) break;
    }
    if (i == wb->nob - 1) {
        if (icol_cmp(&wb->obc[i], &ri->obc)) return 1; i++;
    }
    return (i != wb->nob);
}
static void setRangeQueued(cswc_t *w, wob_t *wb, qr_t *q) {
    bzero(q, sizeof(qr_t));
    r_ind_t *ri     = (w->wf.imatch == -1) ? NULL: &Index[w->wf.imatch];
    bool     virt   = ri ? ri->virt : 0;
    DECLARE_ICOL(ic,  -1)  if (ri && ri->icol.cmatch)      ic  = ri->icol;
    DECLARE_ICOL(obc,  0); if (ri && ri->obc.cmatch != -1) obc = ri->obc;
    if (virt) { // NOTE: there is no inner_desc possible (no inner loop)
        q->pk_desc  = ((wb->nob >= 1) && !wb->asc[0] && 
                       !icol_cmp(&wb->obc[0], &obc));
        q->pk       = ((wb->nob > 1) || 
                       (wb->nob == 1 && icol_cmp(&wb->obc[0], &obc)));
        q->pk_lim   = (!q->pk    && (wb->lim  != -1));
        q->pk_lo    = (q->pk_lim && (wb->ofst != -1));
        q->xth      = q->pk_lo && !(w->flist && (wb->ofst != -1));
        q->qed      = q->pk;
    } else {
        q->fk_desc  = ((wb->nob >= 1) && (!wb->asc[0] && 
                       !icol_cmp(&wb->obc[0], &ic)));
        q->inr_desc = ((wb->nob == 1) && (!wb->asc[0] && 
                       !icol_cmp(&wb->obc[0], &obc))) ||
                      (wb->nob > 1  && !icol_cmp(&wb->obc[0], &ic) && // FK +
                       (!wb->asc[1] && !icol_cmp(&wb->obc[1], &obc)));// PK DESC
        if (w->wtype & SQL_SINGLE_FK_LKP) {
            if (ri->nclist) q->fk = mci_fk_queued(wb, ri);
            else { // NO-Q: [OBY FK|OBY PK|OBY FK,PK|OBY PK,FK]
                q->fk = (wb->nob > 2) ||
                        (wb->nob == 1 &&  // [FK|OBY]
                         icol_cmp(&wb->obc[0], &ic) && 
                         icol_cmp(&wb->obc[0], &obc)) ||
                        (wb->nob == 2 &&  // [FK,OBY]&[PK,OBY]
                         (!(!icol_cmp(&wb->obc[0], &ic)   && 
                            !icol_cmp(&wb->obc[1], &obc)) &&
                          !(!icol_cmp(&wb->obc[1], &ic)   && 
                            !icol_cmp(&wb->obc[0], &obc))));
            }
        } else { // FK RANGE QUERY (clist[MCI] not yet supported)
            q->fk   = (wb->nob > 2) || // NoQ: [OBY FK| OBY FK,PK]
                      (wb->nob == 1 && icol_cmp(&wb->obc[0], &ic)) ||
                      (wb->nob == 2 && // [FK&OBY]
                       !(!icol_cmp(&wb->obc[0], &ic) && 
                         !icol_cmp(&wb->obc[1], &obc)));
        }
        q->fk_lim   = (!q->fk    && (wb->lim  != -1));
        q->fk_lo    = (q->fk_lim && (wb->ofst != -1));
        q->xth      = q->fk_lo && !(w->flist && (wb->ofst != -1));
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
static bool pk_row_op(aobj *apk, void *rrow, range_t *g,    row_op *p,
                      qr_t *q,   long *card) {
    return (*p)(g, apk, rrow, q->qed, card);
}
static bool pk_op_l(aobj *apk, void *rrow, range_t *g,     row_op *p, wob_t *wb,
                    qr_t *q,   long *card, long    *loops, bool   *brkr) {
    *brkr    = 0; INCR(*loops)
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
    dwm_t    dwm   = btFindD(g->co.btr, apk);   if (dwm.miss) return -1;
    void    *rrow  = dwm.k;                     if (!rrow)    return  0;
    bool     gost  = IS_GHOST(g->co.btr, rrow); if (gost)     return -1;
    long     card  =  0;
    if (!pk_row_op(apk, rrow, g, p, q, &card))                return -1;
    else                                                      return card;
}

#define DELETE_MISS(c) addReply(c, shared.deletemiss)
#define UPDATE_MISS(c) addReply(c, shared.updatemiss)

// RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK RANGE_PK
static long rangeOpPK(range_t *g, row_op *p) {                 //DEBUG_RANGE_PK
    btEntry *be; btSIter *bi;
    cswc_t  *w     = g->co.w; wob_t *wb = g->co.wb; qr_t *q = g->q;
    bool     iss   = g->se.qcols ? 1 : 0;  bool isu  = g->up.ncols ? 1 : 0;
    bool     isd   = !iss && !isu;         bool upx  = g->up.upx;
printf("rangeOpPK: iss: %d isu: %d isd: %d upx: %d\n", iss, isu, isd, upx);
    bt      *btr   = getBtr(w->wf.tmatch); g->co.btr = btr;
    g->asc         = !q->pk_desc;
    bool     brkr  = 0; long loops = -1; long card =  0;
    bi = (q->xth) ? 
              btGetXthIter  (btr, &w->wf.alow, &w->wf.ahigh, wb->ofst, g->asc) :
              btGetRangeIter(btr, &w->wf.alow, &w->wf.ahigh, g->asc);
    if (!bi) return card;                                DEBUG_RANGEPK_PRE_LOOP
    if (!bi->empty) {
        if (bi->missed && !upx) {     card = -1; // iss error in iselectAction()
            if      (isd) DELETE_MISS(g->co.c);
            else if (isu) UPDATE_MISS(g->co.c);
        } else while ((be = btRangeNext(bi, g->asc))) {      DEBUG_RANGEPK_LOOP
            if (bi->missed && !upx) { card = -1;
                if (isu) UPDATE_MISS(g->co.c);
                if (isd) DELETE_MISS(g->co.c);
                break;
            }
            if (!pk_op_l(be->key, be->val, g, p, wb, q, &card, &loops, &brkr)) {
                card = -1; break;
            }
            if (brkr) break;
        }
        if ((card != -1) && !upx) {// FULL Iter8r, (last row dr > 0)
            DEBUG_RANGEPK_POST_LOOP
            if (q->pk_lim) { if (wb->lim > card && bi->missed) card = -1; }
            else if                               (bi->missed) card = -1;
        }
    } btReleaseRangeIterator(bi);
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
    // FK lookup must succeed, evictions not possible
    void *rrow = btFind(d->g->co.btr, &UniqueIndexVal);
    if (!(*d->p)(d->g, &UniqueIndexVal, rrow, d->g->q->qed, d->card)) return 0;
    return 1;
}

#define OBT_NODE_BT_OP(vcast, aobjpart, vtype)                              \
  { vcast *vvar = brow; akey.aobjpart = vvar->val; akey.type = vtype; }

// INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR INODE_ITERATOR
static bool nBT_ROp(ibtd_t *d,    qr_t *q,    wob_t *wb,
                    aobj   *bkey, void *brow, bool *ret) {
    bt *nbtr = d->nbtr;
    INCR(*d->loops)
    if (q->fk_lim && !q->fk_lo && *d->loops < *d->ofst)         return 1;
    void *key; aobj akey; initAobj(&akey);
    if (d->obc.cmatch == -1) key = bkey;
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
    // pk comes from Index, so it has not been evicted
    void *rrow = btFind(d->g->co.btr, key); releaseAobj(&akey);
    if (!(*d->p)(d->g, key, rrow, q->qed, d->card)) { *ret = 0; return 0; }
    DEBUG_NBT_ROP
    if (q->fk_lim && wb->lim == *d->card) { *d->brkr = 1;       return 1; }
    return 1;
}
static bool nBT_Op(ibtd_t *d) {                              //DEBUG_NODE_BT
    cswc_t *w = d->g->co.w; wob_t *wb = d->g->co.wb; qr_t *q = d->g->q;
    if (d->nbtr->root) {
        if (d->g->se.cstar && !w->flist) { /* FK cstar w/o filters */
            INCRBY(*d->card, d->nbtr->root->scion) return 1;
        }
        if (q->fk_lo && FK_RQ(w->wtype) && d->nbtr->root->scion <= *d->ofst) {
            DECRBY(*d->ofst, d->nbtr->root->scion) return 1; // skip INODE
        }
    }
    btEntry *nbe;
    bool     ret  = 1;                      /* presume success */
    bool     iss  = d->g->se.qcols ? 1 : 0; bool isu = d->g->up.ncols ? 1 : 0;
    bool     isd  = !iss && !isu;           bool upx = d->g->up.upx;
    *d->brkr      = 0;
    bool     x    = (q->xth && *d->ofst > 0);
    bool     nasc = d->g->asc = !q->inr_desc;
    btSIter *nbi  = x ?
                     btGetFullXthIter  (d->nbtr, *d->ofst, nasc, w, wb->lim) :
                     btGetFullRangeIter(d->nbtr,           nasc, w);
    if (!nbi) return ret;                                          DEBUG_NBT_OP
    if (!nbi->empty){                                     DEBUG_NBT_OP_GOT_ITER
        if (nbi->missed && !upx) {     ret = 0; // iss error in iselectAction()
            if      (isd) DELETE_MISS(d->g->co.c);
            else if (isu) UPDATE_MISS(d->g->co.c);
        } else while ((nbe = btRangeNext(nbi, nasc))) {       DEBUG_NBT_OP_LOOP
            if (nbi->missed && !upx) { ret = 0;
                if (isu) UPDATE_MISS(d->g->co.c); 
                if (isd) DELETE_MISS(d->g->co.c);
                break;
            }
            if (!nBT_ROp(d, q, wb, nbe->key, nbe->val, &ret) || *d->brkr) break;
        }
        if (ret && !upx) {// FULL Iter8r, LIMIT not used up & (last row dr > 0)
            if (q->fk_lim) { if (wb->lim > *d->card && nbi->be.dr) ret = 0; }
            else if                                   (nbi->be.dr) ret = 0;
        }                                                DEBUG_NBT_OP_POST_LOOP
    } btReleaseRangeIterator(nbi);
    if (q->fk_lo)                         *d->ofst = 0; /* OFFSET fulfilled */
    if (q->fk_lim && wb->lim == *d->card) *d->brkr = 1; /* ORDERBY FK LIM*/
    return ret;
}

//TODO SETUNIQIVAL should be btFindD() ... but this all needs to be TESTed
#define SETUNIQIVAL(vt, vcast, aobjpart)                     \
  { uv->enc      = uv->type = vt;                            \
    vcast *vvar  = btFind(nbtr, akey); if (!vvar)  return 0; \
    uv->aobjpart = vvar->val; }

static bool setUniqIndexVal(bt *nbtr, aobj *akey) {
    //printf("setUniqIndexVal: akey: "); dumpAobj(printf, akey);
    aobj *uv       = &UniqueIndexVal; uv->empty = 0;
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
#if 0 //TODO this should probably be activated -> TEST - may complicate QO logic
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
            if UNIQ(ri->cnstr) {                       //DEBUG_RUN_ON_NODE_UNIQ
                d->nbtr        = ibtr; // Unique 1 step shorter
                aobj *uv       = &UniqueIndexVal; uv->empty = 0;
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
                  else assert(!"runOnNode ERROR"); //DEBUG_RUN_ON_NODE_UNIQ_END
            }  // NEXT LINE: When we get to NODE -> run [uBT_Op|nBT_Op]
            if (!(*nop)(d)) { ret = 0; break; }
        }
        if (*d->brkr) break;
    } btReleaseRangeIterator(nbi);
    return ret;
}
// RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK RANGE_FK
static long rangeOpFK(range_t *g, row_op *p) {                 DEBUG_RANGE_FK
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
    bool     smplo = SIMP_UNIQ(ibtr) && q->xth;
    if (smplo) { // SIMPLE UNIQUE + OFFSET -> use SCION iter8trs
        bi   = btGetXthIter(ibtr, &w->wf.alow, &w->wf.ahigh, wb->ofst, g->asc);
        ofst = wb->ofst = -1; // OFFSET handled by btGetXthIter()
    }
    else bi = btGetRangeIter(ibtr, &w->wf.alow, &w->wf.ahigh, g->asc);
    if (!bi) return card;
    init_ibtd(&d, p, g, q, NULL, &ofst, &card, &loops, &brkr, ri->obc);
    while ((be = btRangeNext(bi, g->asc))) {                DEBUG_RANGE_FK_LOOP
        if (iss && !be->val) { card = -1; break; }
        uint32  nmatch  = 0;
        d.nbtr          = singu ? ibtr : btMCIFindVal(w, be->val, &nmatch, ri);
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
    bt       *fibtr  = singu ? NULL : btIndFind(ibtr, afk); DEBUG_SINGFK_INFO
    // CHECK for a 100% Evicted Index
    if (!singu && iss && !fibtr) { if (btIndExist(ibtr, afk)) return -1; }
    bt       *nbtr   = singu ? ibtr : btMCIFindVal(w, fibtr, &nmatch, ri);
    long      ofst   = wb->ofst;
    long      loops  = -1; long card =  0; bool brkr =  0;
    init_ibtd(&d, p, g, q, nbtr, &ofst, &card, &loops, &brkr, ri->obc);
    if (d.nbtr) {
        uint32 diff = nexpc - nmatch;
        if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) return -1; }
        else {
            if (singu && !setUniqIndexVal(nbtr, afk))              return -1;
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
        dwm_t  dwm  = btFindD(g->co.btr, apk);   if (dwm.miss) return -1;
        void  *rrow = dwm.k;
        bool   gost = IS_GHOST(g->co.btr, rrow); if (gost)     continue;
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
        bt     *beval  = btIndFind (ibtr, afk); //DEBUG_IN_OP_FK_LOOP
        if (iss && !beval) { if (btIndExist(ibtr, afk)) return -1; }
        d.nbtr         = btMCIFindVal(w, beval, &nmatch, ri);
        if (d.nbtr) {
            uint32 diff = nexpc - nmatch;
            if      (diff) { if (!runOnNode(d.nbtr, diff, nop, &d, ri)) CBRK }
            else if (!(*nop)(&d))                                       CBRK
            if (brkr) break;
        }
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
static bool runLuaFilter(lue_t *le, bt *btr, aobj *apk, void *rrow, int tmatch,
                         bool  *hf) {
    printf("runLuaFilter: fname: %s ncols: %d\n", le->fname, le->ncols);
    lua_getglobal(server.lua, le->fname);
    for (int i = 0; i < le->ncols; i++) {
        pushColumnLua(btr, rrow, tmatch, le->as[i], apk);
    }
    bool ret = 1;
    int  r   = DXDB_lua_pcall(server.lua, le->ncols, 1, 0);
    if (r) { *hf = 1; ret = 0;
        CURR_ERR_CREATE_OBJ "-ERR: running LUA FILTER (%s): %s [CARD: %ld]\r\n",
                 le->fname, lua_tostring(server.lua, -1), server.alc.CurrCard));
        lua_pop(server.lua, 1);
    } else {
        int t = lua_type(server.lua, -1);
        if        (t == LUA_TNUMBER)  {
            ret = lua_tonumber(server.lua, -1) ? 1: 0;
        } else if (t == LUA_TBOOLEAN) {
            ret = lua_toboolean(server.lua, -1);
        } else { *hf = 1; ret = 0;
            CURR_ERR_CREATE_OBJ
            "-ERR: LUA FILTER (%s): %s [CARD: %ld]\r\n",
             le->fname, "use NUMBER or BOOLEAN return types",
             server.alc.CurrCard));
        }
        lua_pop(server.lua, 1);
    }
    return ret;
}
bool passFilts(bt   *btr, aobj *apk, void *rrow, list *flist, int tmatch, 
               bool *hf) {
    if (!flist) return 1; /* no filters always passes */
    listNode *ln, *ln2;
    bool      ret = 1;       //printf("passFilts: nfliters: %d\n", flist->len);
    listIter *li  = listGetIterator(flist, AL_START_HEAD);
    while ((ln = listNext(li))) {
        f_t *flt  = ln->value;
        if (tmatch != flt->tmatch) continue;
        aobj a    = getCol(btr, rrow, flt->ic, apk, tmatch, NULL);
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
        } else if (flt->akey.type != COL_TYPE_NONE) {
            ret = (*OP_CMP[flt->op])(&flt->akey, &a);     //DEBUG_PASS_FILT_KEY
            releaseAobj(&a);
            if (!ret) break;                      /* break OUTER-LOOP on miss */
        } else if (flt->op == LFUNC) {
            ret = runLuaFilter(&flt->le, btr, apk, rrow, tmatch, hf);
        } else assert(!"passFilts ERROR");
    } listReleaseIterator(li);
    return ret;
}

#define OP_FILTER_CHECK \
  int  tmatch = g->co.w->wf.tmatch; bool hf = 0; \
  bool ret    = passFilts(g->co.btr, apk, rrow, g->co.w->flist, tmatch, &hf); \
  if (hf) return 0; if (!ret) return 1;

/* SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS SQL_CALLS */
static bool select_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    OP_FILTER_CHECK
    if (!g->se.cstar) {
        uchar ost = OR_NONE;
        robj *r = outputRow(g->co.btr, rrow,   g->se.qcols, g->se.ics,
                            apk,       tmatch, g->se.lfca,  &ost);
        if (ost == OR_ALLB_OK) { server.alc.CurrUpdated++; return 1; }
        if (ost == OR_ALLB_NO)                             return 1;
        if (ost == OR_LUA_FAIL)                            return 0;
        if (q) {
            if (!addRow2OBList(g->co.ll, g->co.wb, g->co.btr, r, g->co.ofree,
                               rrow,     apk)) return 0;
        } else {
            GET_LRUC GET_LFUC
            if (!addReplyRow(g->co.c, r, tmatch, apk, lruc, lrud, lfuc, lfu)) {
                ret = 0;
            }
        }
        if (!(EREDIS)) decrRefCount(r); //TODO MEMLEAK??? for EREDIS
    }
    INCR(*card) server.alc.CurrCard++; return ret;
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
void iselectAction(cli *c,      cswc_t *w,     wob_t *wb,
                   icol_t *ics, int     qcols, bool   cstar, lfca_t *lfca) {
    printf("\n\niselectAction\n");
    range_t g; qr_t q; setQueued(w, wb, &q);
    list *ll     = initOBsort(q.qed, wb, 0);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_ROBJ, NULL);
    g.se.cstar   = cstar; g.se.qcols   = qcols;
    g.se.ics     = ics;   g.se.lfca    = lfca;
    void *rlen   = (cstar || EREDIS) ? NULL : addDeferredMultiBulkLength(c);
    long  card   = Op(&g, select_op);

printf("iselectAction: card: %ld CurrCard: %ld CurrUpdated: %ld\n", card, server.alc.CurrCard, server.alc.CurrUpdated);

    if (card == -1) { replaceDMB(c, rlen, server.alc.CurrError); goto isele; }
    long sent    = 0;
    if (card) {
        if (q.qed) {
            if (!opSelectSort(c, ll, wb, g.co.ofree,
                              &sent, w->wf.tmatch))   goto isele;
        } else sent = card;
    }
    if (!card && server.alc.CurrUpdated) {
        setDeferredMultiBulkLong(c, rlen, server.alc.CurrUpdated);
    } else {
        if (wb->lim != -1 && sent < card) card = sent;
        if      (cstar)   addReplyLongLong(c, card);
        else if (!EREDIS) setDMBcard_cnames(c, w, ics, qcols, card, rlen, lfca);
    }

isele:
    if (wb->ovar) incrOffsetVar(c, wb, card);
    releaseOBsort(ll);
}

typedef list *list_adder(list *list, void *value);
static bool dellist_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
printf("START: dellist_op: asc: %d\n", g->asc);
    OP_FILTER_CHECK
    if (q) {
        if (!addRow2OBList(g->co.ll, g->co.wb, g->co.btr, apk, g->co.ofree,
                            rrow,     apk)) return 0;
    } else {
printf("dellist_op: adding: key: "); dumpAobj(printf, apk);
        aobj *cln  = cloneAobj(apk); //NOTE: next line builds BACKWARDS list
        list_adder *la = g->asc ? listAddNodeHead : listAddNodeTail;
        (*la)(g->co.ll, cln); /* UGLY: build list of PKs to delete */
    }
    INCR(*card) server.alc.CurrCard++; return ret;
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
        if (deleteRow(w->wf.tmatch, apk, matches, inds) == 1) INCR(*sent)
    }
    sortOBCleanup(v, listLength(ll), ofree);
    free(v); /* FREED 004 */
}
void ideleteAction(redisClient *c, cswc_t *w, wob_t *wb) {
    range_t g; qr_t q; setQueued(w, wb, &q);
    MATCH_INDICES(w->wf.tmatch)
    list *ll   = initOBsort(q.qed, wb, 1);
    if (!q.qed) ll->free = destroyAobj;
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    long  sent = 0;
    long  card = Op(&g, dellist_op);
    if (!card) addReply(c, shared.czero);
    if (card <= 0)                               goto idel_end;
    if (q.qed) {
        opDeleteSort(ll, w, wb, g.co.ofree, &sent, matches, inds);
    } else {
        listNode  *ln;
        listIter  *li = listGetIterator(ll, AL_START_HEAD);
        while((ln = listNext(li))) {
            aobj *apk = ln->value;
printf("ideleteAction: key: "); dumpAobj(printf, apk);
            if (deleteRow(w->wf.tmatch, apk, matches, inds) == 1) sent++;
        } listReleaseIterator(li);
    }
    if (sent < card) card = sent;
    addReplyLongLong(c, (ull)card);
    if (wb->ovar) incrOffsetVar(c, wb, card);

idel_end:
    releaseOBsort(ll);
}

static bool update_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    OP_FILTER_CHECK
    if (q) {
        if (!addRow2OBList(g->co.ll, g->co.wb, g->co.btr, apk, g->co.ofree,
                           rrow, apk)) return 0;
    } else {
printf("update_op: adding: key: "); dumpAobj(printf, apk);
        //TODO non-FK/PK updates can be done HERE inline (w/o Qing in the ll)
        aobj *cln  = cloneAobj(apk);
        listAddNodeTail(g->co.ll, cln); /* UGLY: build list of PKs to update */
    }
    INCR(*card) server.alc.CurrCard++; return ret;
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
            void   *rrow = btFind(btr, apk); // pk comes from LL
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
                   ue_t  ue[],   lue_t  *le,      bool   upi) {
    range_t g; qr_t q; setQueued(w, wb, &q);
    list *ll     = initOBsort(q.qed, wb, 1);
    init_range(&g, c, w, wb, &q, ll, OBY_FREE_AOBJ, NULL);
    bt   *btr    = getBtr(w->wf.tmatch); g.up.btr = btr;
    g.up.ncols   = ncols;
    g.up.matches = matches; g.up.indices = inds;
    g.up.vals    = vals;    g.up.vlens   = vlens; g.up.cmiss = cmiss;
    g.up.ue      = ue;      g.up.le      = le;    
    g.up.upx     = !upi && (wb->lim == -1) && !w->flist;
    printf("\n\niupdateAction: upx: %d upi: %d lim:% ld flist: %p\n", g.up.upx, upi, wb->lim, (void *)w->flist);
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
                void *rrow = btFind(g.up.btr, apk); // pk comes from LL
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
        int      cmatch = (w->wf.imatch == -1) ? -1   : ri->icol.cmatch;
        if (virt) (*prn)("\t\tqpk: %d pklim: %d pklo: %d desc: %d xth: %d\n",
                         q->pk, q->pk_lim, q->pk_lo, q->pk_desc, q->xth);
        else      (*prn)("\t\tqfk: %d fklim: %d fklo: %d desc: %d xth: %d\n",
                         q->fk, q->fk_lim, q->fk_lo, q->fk_desc, q->xth);
        (*prn)("\t\tvirt: %d asc: %d obc: %d lim: %ld" \
                    " ofst: %ld indcol: %d inner_desc: %d -> qed: %d\n",
            virt, wb->asc[0], wb->obc[0].cmatch, wb->lim, wb->ofst,
            cmatch, q->inr_desc, q->qed);
        //TODO dump ri->icol.lo
    } else {
        (*prn)("\t\tqed:\t%d xth: %d\n", q->qed, q->xth);
    }
}

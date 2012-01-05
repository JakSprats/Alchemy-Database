/*
 * This file implements the indexing logic of Alsosql
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
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "adlist.h"
#include "redis.h"

#include "bt.h"
#include "btreepriv.h"
#include "bt_iterator.h"
#include "row.h"
#include "orderby.h"
#include "luatrigger.h"
#include "colparse.h"
#include "stream.h"
#include "find.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "index.h"

/* INDEX TODO LIST:
     1.) refactor iEvictMCI() into iRemMCI
     2.) refactor iEvict() into iRem()
*/

extern r_tbl_t *Tbl;
extern uint32   Ind_HW; extern dict *IndD; extern list *DropI;

int      Num_indx;
r_ind_t *Index = NULL;

// PROTOTYPES
static void destroy_mci(bt *ibtr, bt_n *n, int imatch, int lvl);

/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
#define DEBUG_IADDMCI_UNIQ                                               \
  printf("fcol: "); dumpAobj(printf, &fcol);                             \
  printf("bt_dumptree(nbtr): %p\n", nbtr); bt_dumptree(printf, nbtr, 0);

typedef struct delete_pair_t { /* used in iRemMCI() */
    bt   *ibtr;
    bt   *nbtr;
    aobj  acol;
} dp_t;
static dp_t init_dp(bt *ibtr, aobj *acol, bt *nbtr) {
    dp_t dp;
    dp.ibtr = ibtr;
    dp.acol = *acol; /* NOTE original acol should not be released/destroyed */
    dp.nbtr = nbtr;
    return dp;
}

#define DEBUG_IADD                                \
  printf("iAdd: acol: "); dumpAobj(printf, acol); \
  printf("iAdd: apk:  "); dumpAobj(printf, apk);

static lxk LX_iAdd; static xlk XL_iAdd; static uxk UX_iAdd; static xuk XU_iAdd;
static xxk XX_iAdd; static ulk UL_iAdd; static luk LU_iAdd; static llk LL_iAdd;
#define OBT_IADD_UNIQ(sptr, aobjpart) \
  { sptr.val = apk->aobjpart; btAdd(ibtr, acol, &sptr); }

void iAddUniq(bt *ibtr, uchar pktyp, aobj *apk, aobj *acol) { // OBYI uses also
    if        C_IS_I(pktyp) {
        if      UU(ibtr) btAdd(ibtr, acol, VOIDINT apk->i); 
        else if LU(ibtr) OBT_IADD_UNIQ(LU_iAdd, i)
        else /* XU */    OBT_IADD_UNIQ(XU_iAdd, i)
    } else if C_IS_L(pktyp) {
        if      UL(ibtr) OBT_IADD_UNIQ(UL_iAdd, l)
        else if LL(ibtr) OBT_IADD_UNIQ(LL_iAdd, l)
        else /* XL */    OBT_IADD_UNIQ(XL_iAdd, l)
    } else {//C_IS_X
        if      UX(ibtr) OBT_IADD_UNIQ(UX_iAdd, x)
        else if LX(ibtr) OBT_IADD_UNIQ(LX_iAdd, x)
        else /* XX */    OBT_IADD_UNIQ(XX_iAdd, x)
    }
}
static bool iAdd(cli  *c,   bt    *ibtr,  aobj *acol,
                 aobj *apk, uchar  pktyp, aobj *ocol, int imatch) {//DEBUG_IADD
    r_ind_t *ri   = &Index[imatch];
    if (UNIQ(ri->cnstr)) { // SINGLE COLUMN UNIQUE INDEX
        if (btFind(ibtr, acol)) { if (c) addReply(c, shared.uviol); return 0; }
        iAddUniq(ibtr, pktyp, apk, acol);
    } else {               // SINGLE COLUMN NORMAL INDEX
        bt *nbtr = btIndFind(ibtr, acol);
        if (!nbtr) {
            uchar otype  = ocol ? ocol->type : COL_TYPE_NONE;
            nbtr         = createIndexNode(pktyp, otype);
            btIndAdd(ibtr, acol, nbtr);
            ibtr->msize += nbtr->msize;       // ibtr inherits nbtr
        }
        ulong size1  = nbtr->msize;
        if (!btIndNodeAdd(c, nbtr, apk, ocol)) return 0;
        ibtr->msize += (nbtr->msize - size1); // ibtr inherits nbtr
    }
    return 1;
}
static void destroy_index(bt *ibtr, bt_n *n, int imatch) {
    r_ind_t *ri = &Index[imatch];
    if (ri->clist)       { destroy_mci(ibtr, ibtr->root, imatch, 0); return; }
    if (UNIQ(ri->cnstr)) { bt_destroy (ibtr);                        return; }
    for (int i = 0; i < n->n; i++) {
        void *be   = KEYS(ibtr, n, i);
        bt   *nbtr = (bt *)parseStream(be, ibtr); if (nbtr) bt_destroy(nbtr);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            destroy_index(ibtr, NODES(ibtr, n)[i], imatch);
        }
    }
}
static void iRem(bt   *ibtr, aobj *acol, aobj *apk, aobj *ocol, int imatch,
                 bool  gost) {
    r_ind_t *ri   = &Index[imatch];
    if (UNIQ(ri->cnstr)) btDelete(ibtr, acol);
    else {
        bt  *nbtr    = btIndFind(ibtr, acol);
        ulong  size1 = nbtr->msize;
        int  nkeys   = btIndNodeDelete(nbtr, apk, ocol);
        ibtr->msize -= (size1 - nbtr->msize);
        if (!nkeys) {
            if (gost) btIndNull  (ibtr, acol);
            else      btIndDelete(ibtr, acol);
            ibtr->msize -= nbtr->msize; bt_destroy(nbtr);
        }
    }
}
//TODO test iEvict w/ SIMP_UNIQ()
//TODO refactor iEvict() into iRem()
static void iEvict(bt *ibtr, aobj *acol, aobj *apk, aobj *ocol) {
    printf("iEvict apk: "); dumpAobj(printf, apk);
    bt  *nbtr    = btIndFind     (ibtr, acol);
    ulong  size1 = nbtr->msize;
    int  nkeys   = btIndNodeEvict(nbtr, apk, ocol);
    ibtr->msize -= (size1 - nbtr->msize);
    if (!nkeys) {
        btIndNull(ibtr, acol); ibtr->msize -= nbtr->msize; bt_destroy(nbtr);
    }
}
static bool _iAddMCI(cli  *c,      bt   *btr,  aobj *apk,     uchar  pktyp,
                     int   imatch, void *rrow, bool  destroy, int    rec_ret,
                     aobj *ocol) {
    bt      *nbtr  = NULL;                 /* compiler warning */
    r_ind_t *ri    = &Index[imatch];
    dp_t     dpl[ri->nclist];
    bt      *ibl[ri->nclist];
    int      trgr  = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
    int      final = ri->nclist - 1;
    int      depth = UNIQ(ri->cnstr) ? ri->nclist - 1 : ri->nclist;
    bt      *ibtr  = getIBtr(imatch);      /* get MCI */
    int      ndstr = 0;
    bool     ret   = 1; /* assume success */
    for (int i = 0; i < depth; i++) {
        ibl[i]     = ibtr;
        aobj acol  = getCol(btr, rrow, ri->bclist[i], apk, ri->table);
        if (acol.empty)                                   goto iaddmci_err;
        nbtr       = btIndFind(ibtr, &acol);
        if (!nbtr) {
            if (i == final) {                     /* final  MID -> NODE*/
                uchar otype  = ocol ? ocol->type : COL_TYPE_NONE;
                nbtr = createIndexNode(pktyp, otype);
            } else {                              /* middle MID -> MID */
                uchar ntype = Tbl[ri->table].col[ri->bclist[i + 1]].type;
                if (i == trgr) nbtr = createU_MCI_IBT(ntype, imatch, pktyp);
                else           nbtr = createMCI_MIDBT(ntype, imatch);
            }
            ulong isize1 = ibtr->msize;
            btIndAdd(ibtr, &acol, nbtr);  /* add MID to ibtr */
            ulong idiff  = nbtr->msize + (ibtr->msize - isize1);
            for (int j = i; j >= 0; j--) ibl[j]->msize += idiff; /*mem-bookeep*/
        }
        if (destroy) dpl[ndstr] = init_dp(ibtr, &acol, nbtr);
        ndstr++; ibtr = nbtr; releaseAobj(&acol);
    }
    if (destroy)                                          goto iaddmci_err;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {                          //printf("_iAddMCI: UNIQ\n");
        aobj fcol = getCol(btr, rrow, ri->bclist[final], apk, ri->table);
        if (fcol.empty)                                   goto iaddmci_err;
        if (btFind(nbtr, &fcol)) {
            if (c) addReply(c, shared.uviol); { ret = 0;  goto iaddmci_err; }
        } /* Next ADD (FinFK|PK) 2 UUBT */
        iAddUniq(nbtr, pktyp, apk, &fcol);                 //DEBUG_IADDMCI_UNIQ
        releaseAobj(&fcol);
    } else {
        if (!btIndNodeAdd(c, nbtr, apk, ocol)) { ret = 0; goto iaddmci_err; }
    }
    ulong diff  = (nbtr->msize - size1);     /* memory bookeeping trickles up */
    if (diff) for (int i = 0; i < depth; i++) ibl[i]->msize += diff;
    return 1;

iaddmci_err: /* NOTE: a destroy pass is done to UNDO what was done */
    if (!ndstr)   return 1; /* first MCI COL was empty - nothing happened */
    if (!destroy) return _iAddMCI(c, btr, apk, pktyp, imatch,
                                 rrow, 1, ret, ocol);
    else { /* destroy information was collected, if nkeys ==1, its invalid */
        for (int j = ndstr - 1; j>= 0; j--) {
            if (dpl[j].ibtr->numkeys == 1) {
                btIndDelete(dpl[j].ibtr, &dpl[j].acol); bt_destroy(dpl[j].nbtr);
            }
        }
        return rec_ret;
    }
}
static inline bool iAddMCI(cli   *c,     bt  *btr,    aobj *apk, 
                           uchar  pktyp, int  imatch, void *rrow, aobj *ocol) {
    return _iAddMCI(c, btr, apk, pktyp, imatch, rrow, 0, 0, ocol);
}
static void destroy_mci(bt *ibtr, bt_n *n, int imatch, int lvl) {
    r_ind_t *ri     = &Index[imatch];
    int      trgr   = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
    int      final  = ri->nclist - 1;
    if (lvl == final) return;
    for (int i = 0; i < n->n; i++) {
        void *be    = KEYS(ibtr, n, i);
        bt   *nbtr  = (bt *)parseStream(be, ibtr);
        if (lvl == final || lvl == trgr) bt_destroy(nbtr);
        else { /* INNER RECURSE -> LEVEL INCREASE */
            destroy_mci(nbtr, nbtr->root, imatch, lvl + 1);
        }
    }
    if (!n->leaf) { /* NORMAL RECURSE -> SAME LEVEL */
        for (int i = 0; i <= n->n; i++) {
            destroy_mci(ibtr, NODES(ibtr, n)[i], imatch, lvl);
        }
    }
    bt_destroy(ibtr);
}
static void iRemMCI(bt   *btr, aobj *apk, int imatch, void *rrow, aobj *ocol,
                    bool  gost) {
    bt      *nbtr  = NULL; /* compiler warning */
    r_ind_t *ri    = &Index[imatch];
    dp_t     dpl[ri->nclist];
    int      final = ri->nclist - 1;
    int      depth = UNIQ(ri->cnstr) ? ri->nclist - 1 : ri->nclist;
    bt      *ibtr  = getIBtr(imatch);
    for (int i = 0; i < depth; i++) { /* find NODEBT, build DEL list */
        aobj acol = getCol(btr, rrow, ri->bclist[i], apk, ri->table);
        if (acol.empty) return; /* NOTE: no rollback, iAddMCI does rollback */
        nbtr      = btIndFind(ibtr, &acol);
        dpl[i]    = init_dp(ibtr, &acol, nbtr);
        ibtr      = nbtr; /* NOTE: DO NOT release acol - it is used later */
    }                     /* NOTE: DO NOT reuse nbtr   - it is used later */
    int nkeys;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {
        aobj dcol = getCol(btr, rrow, ri->bclist[final], apk, ri->table);
        nkeys     = btIndNodeDelete(nbtr, &dcol, NULL); // DEL FinalCol from UBT
        releaseAobj(&dcol); /* NOTE: I or L so not really needed */
    } else {
        nkeys     = btIndNodeDelete(nbtr, apk,   ocol);// del PK from NODEBT
    }
    ulong diff  = (size1 - nbtr->msize);      /* mem-bookeeping trickles up */
    if (diff) for (int i = 0; i < depth; i++) dpl[i].ibtr->msize -= diff;
    int i = depth - 1;         /* start at end */
    while (!nkeys && i >= 0) { /*previous DEL emptied BT->destroyBT,trickle-up*/
        ibtr         = dpl[i].ibtr;
        ulong isize1 = ibtr->msize;
        nkeys        = gost ? btIndNull  (ibtr, &dpl[i].acol) :
                              btIndDelete(ibtr, &dpl[i].acol);
        ulong idiff  = nbtr->msize + (isize1 - ibtr->msize); bt_destroy(nbtr);
        for (int j = i; j >= 0; j--) dpl[j].ibtr->msize -= idiff;/*trickle-up*/
        { i--; nbtr = ibtr; } /* go one step HIGHER in dpl[] - trickle-up */
    }
}
//TODO refactor iEvictMCI() into iRemMCI
static void iEvictMCI(bt *btr, aobj *apk, int imatch, void *rrow, aobj *ocol) {
    printf("iEvictMCI\n");
    bt      *nbtr  = NULL; /* compiler warning */
    r_ind_t *ri    = &Index[imatch];
    dp_t     dpl[ri->nclist];
    int      final = ri->nclist - 1;
    int      depth = UNIQ(ri->cnstr) ? ri->nclist - 1 : ri->nclist;
    bt      *ibtr  = getIBtr(imatch);
    for (int i = 0; i < depth; i++) { /* find NODEBT, build DEL list */
        aobj acol = getCol(btr, rrow, ri->bclist[i], apk, ri->table);
        if (acol.empty) return; /* NOTE: no rollback, iAddMCI does rollback */
        nbtr      = btIndFind(ibtr, &acol);
        dpl[i]    = init_dp(ibtr, &acol, nbtr);
        ibtr      = nbtr; /* NOTE: DO NOT release acol - it is used later */
    }                     /* NOTE: DO NOT reuse nbtr   - it is used later */
    int   nkeys ;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {
        aobj dcol = getCol(btr, rrow, ri->bclist[final], apk, ri->table);
        nkeys     = btIndNodeEvict(nbtr, &dcol, NULL); releaseAobj(&dcol);
    } else nkeys  = btIndNodeEvict(nbtr, apk,   ocol);
    ulong diff  = (size1 - nbtr->msize);
    if (diff) for (int i = 0; i < depth; i++) dpl[i].ibtr->msize -= diff;
    int i = depth - 1;         /* start at end */
    while (!nkeys && i >= 0) { /*previous DEL emptied BT->destroyBT,trickle-up*/
        ibtr         = dpl[i].ibtr;
        ulong isize1 = ibtr->msize;
        btIndNull(ibtr, &dpl[i].acol);
        ulong idiff  = nbtr->msize + (isize1 - ibtr->msize); bt_destroy(nbtr);
        for (int j = i; j >= 0; j--) dpl[j].ibtr->msize -= idiff;/*trickle-up*/
        { i--; nbtr = ibtr; } /* go one step HIGHER in dpl[] - trickle-up */
    }
}
static bool iAddStream(cli *c, bt *btr, uchar *stream, int imatch) {
    aobj apk;    convertStream2Key(stream, &apk, btr);
    void *rrow = parseStream(stream, btr);
    bool  ret  = addToIndex(c, btr, &apk, rrow, imatch); releaseAobj(&apk);
    return ret;
}
bool addToIndex(cli *c, bt *btr, aobj *apk, void *rrow, int imatch) {
    r_ind_t *ri    = &Index[imatch];
    if (ri->virt)                                                    return 1;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->luat) { luatAdd(btr, (luat_t *)ibtr, apk, imatch, rrow); return 1; }
    int      pktyp = Tbl[ri->table].col[0].type;
    if (ri->clist) {
        if (ri->obc == -1) {
            if (!iAddMCI(c, btr, apk, pktyp, imatch, rrow, NULL))    return 0;
        } else {
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);
            if (!iAddMCI(c, btr, apk, pktyp, imatch, rrow, &ocol))   return 0;
            releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->column, apk, ri->table);
        if (!acol.empty) {
            if (ri->obc == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                if (!iAdd(c, ibtr, &acol, apk, pktyp, NULL, imatch)) return 0;
            } else {             // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);
                bool ret  = iAdd(c, ibtr, &acol, apk, pktyp, &ocol, imatch);
                releaseAobj(&ocol); if (!ret)                        return 0;
            }
        }
        releaseAobj(&acol);
    }
    return 1;
}
void delFromIndex(bt *btr, aobj *apk, void *rrow, int imatch, bool gost) {
    r_ind_t *ri   = &Index[imatch];
    if (ri->virt)                                                     return;
    bt      *ibtr = getIBtr(imatch);
    if (ri->luat) { luatDel(btr,  (luat_t *)ibtr, apk, imatch, rrow); return; }
    if (ri->clist) {
        if (ri->obc == -1) iRemMCI(btr, apk, imatch, rrow, NULL, gost);
        else {                                                    // MCI OBY
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);
            iRemMCI(btr, apk, imatch, rrow, &ocol, gost); releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->column, apk, ri->table);
        if (!acol.empty) {
            if (ri->obc == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                iRem(ibtr, &acol, apk, NULL, imatch, gost);
            } else {             // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);
                iRem(ibtr, &acol, apk, &ocol, imatch, gost); releaseAobj(&ocol);
            }
        } releaseAobj(&acol);
    }
}
void evictFromIndex(bt *btr, aobj *apk, void *rrow, int imatch) {
    printf("Evict: imatch: %d apk: ", imatch); dumpAobj(printf, apk);
    r_ind_t *ri   = &Index[imatch];
    if (ri->virt)                                                     return;
    if (ri->luat) { printf("TODO: EVICT call its own LuatTrigger\n"); return; }
    bt      *ibtr = getIBtr(imatch);
    if (ri->clist) {
        if (ri->obc == -1) iEvictMCI(btr, apk, imatch, rrow, NULL);//MCI NORMAL
        else {
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);//MCI OBY
            iEvictMCI(btr, apk, imatch, rrow, &ocol); releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->column, apk, ri->table);
        if (!acol.empty) {
            if (ri->obc == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                iEvict(ibtr, &acol, apk, NULL);
            } else {             // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->table);
                iEvict(ibtr, &acol, apk, &ocol); releaseAobj(&ocol);
            }
        } releaseAobj(&acol);
    }
}
bool upIndex(cli *c, bt *ibtr, aobj *aopk,  aobj *ocol,
                               aobj *anpk,  aobj *ncol,  int pktyp,
                               aobj *oocol, aobj *nocol, int imatch) {
    r_ind_t *ri = &Index[imatch];
    if (ri->lfu) { // modify LFU here
        ocol->l = (ulong)(floor(log2((dbl)ocol->l))) + 1;
        ncol->l = (ulong)(floor(log2((dbl)ncol->l))) + 1;
    }
    if (aobjEQ(aopk, anpk) && aobjEQ(ncol, ocol))         return 1;// EQ -> NOOP
    if (!iAdd(c, ibtr, ncol, anpk, pktyp, nocol, imatch)) return 0;// ADD 1st
    if (!ocol->empty) iRem(ibtr, ocol, aopk, oocol, imatch, 0);
    return 1;
}
bool updateIndex(cli *c, bt *btr, aobj *aopk, void *orow, 
                                  aobj *anpk, void *nrow, int imatch) {
    r_ind_t *ri    = &Index[imatch];
    if (ri->virt)                                         return 1;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->luat) {
        luatAdd(btr, (luat_t *)ibtr, anpk, imatch, nrow); return 1;
        luatDel(btr, (luat_t *)ibtr, aopk, imatch, orow); return 1;
    }
    int      pktyp = Tbl[ri->table].col[0].type;
    bool     ok    = 1;
    if (ri->clist) {                                      /*ADD 1st can FAIL*/
        if (ri->obc == -1) {
            ok = iAddMCI(c, btr, anpk, pktyp, imatch, nrow, NULL);
            if (ok) iRemMCI(btr, aopk, imatch, orow, NULL, 0);
        } else {
            aobj oocol = getCol(btr, orow, ri->obc, aopk, ri->table);
            aobj nocol = getCol(btr, nrow, ri->obc, anpk, ri->table);
            ok = iAddMCI(c, btr, anpk, pktyp, imatch, nrow, &nocol);
            if (ok) iRemMCI(btr, aopk, imatch, orow, &oocol, 0);
            releaseAobj(&oocol); releaseAobj(&nocol);
        }
    } else {
        aobj  ocol = getCol(btr, orow, ri->column, aopk, ri->table);
        aobj  ncol = getCol(btr, nrow, ri->column, anpk, ri->table);
        if (ri->obc == -1) { // NORMAL
            ok = upIndex(c, ibtr, aopk, &ocol, anpk, &ncol, pktyp,
                         NULL, NULL, imatch);
        } else {             // OBC
            aobj oocol = getCol(btr, orow, ri->obc, aopk, ri->table);
            aobj nocol = getCol(btr, nrow, ri->obc, anpk, ri->table);
            ok = upIndex(c, ibtr, aopk, &ocol, anpk, &ncol, pktyp,
                         &oocol, &nocol, imatch);
            releaseAobj(&oocol); releaseAobj(&nocol);
        }
        releaseAobj(&ncol); releaseAobj(&ocol);
    }
    return ok;
}

/* CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX */
long buildIndex(cli *c, bt *btr, int imatch, long limit) {
    btEntry *be; btSIter *bi; long final = 0;
    r_ind_t *ri   = &Index[imatch];
    bool     prtl = (limit != -1);
    if (prtl) {
       uchar pktyp = Tbl[ri->table].col[0].type;
       final       = ri->ofst + limit;
       aobj alow, ahigh;
       if       (C_IS_I(pktyp)) {
           initAobjInt(&alow,  0); initAobjInt(&ahigh, final);
       } else if C_IS_L(pktyp) {
           initAobjLong(&alow, 0); initAobjLong(&ahigh, final);
       } else /* C_IS_X */      {
           initAobjU128(&alow, 0); initAobjU128(&ahigh, final);
       }
       bi = btGetXthIter(btr, &alow, &ahigh, ri->ofst, 1);
    } else bi = btGetFullRangeIter(btr, 1, NULL);

    long card = 0;
    while ((be = btRangeNext(bi, 1)) != NULL) {
        if (!iAddStream(c, btr, be->stream, imatch)) return -1;
        card++;
    } btReleaseRangeIterator(bi);

    if (prtl) {
        ri->ofst = final;
        if (final >= btr->numkeys) { ri->done = 1; ri->ofst = -1; }
    }
    return card;
}
static long buildNewIndex(cli *c, int  tmatch, int imatch, long limit) {
    bt   *btr  = getBtr(tmatch); if (!btr->numkeys) return 0;
    long  card = buildIndex(c, btr, imatch, limit);
    if (card == -1) emptyIndex(imatch);
    return card;
}
static void addIndex() { //printf("addIndex: Ind_HW: %d\n", Ind_HW);
    Ind_HW++;
    r_ind_t *indxs = malloc(sizeof(r_ind_t) * Ind_HW);
    bzero(indxs, sizeof(r_ind_t) * Ind_HW);
    for (int i = 0; i < Num_indx; i++) {
        memcpy(&indxs[i], &Index[i], sizeof(r_ind_t)); // copy index metadata
    }
    free(Index); Index = indxs;
}

int newIndex(cli    *c,     sds   iname, int  tmatch, int   cmatch,
             list   *clist, uchar cnstr, bool virt,   bool  lru,
             luat_t *luat,  int   obc,   bool prtl,   bool  lfu) {
    if (!DropI && Num_indx >= (int)Ind_HW) addIndex();
    int      imatch;
    if (DropI) {
        listNode *ln = (DropI)->head;
        imatch       = (int)(long)ln->value;
        DEL_NODE_ON_EMPTY_RELEASE_LIST(DropI, ln);
    } else {
        imatch       = Num_indx; Num_indx++;
    }
    r_ind_t *ri      = &Index[imatch]; bzero(ri, sizeof(r_ind_t));
    ri->name         = sdsdup(iname);                     /* DESTROY ME 055 */
    ri->table        = tmatch;       ri->column = cmatch; ri->clist = clist;
    ri->virt         = virt;         ri->cnstr  = cnstr;  ri->lru   = lru;
    ri->luat         = luat ? 1 : 0; ri->obc    = obc;    ri->lfu   = lfu;
    ri->done         = prtl ? 0 : 1; 
    ri->ofst         = prtl ? 1: -1; // NOTE: PKs start at 1 (not 0)
    r_tbl_t *rt      = &Tbl[tmatch];
    if (ri->column != -1) rt->col[ri->column].imatch = imatch;
    if (!rt->ilist) rt->ilist  = listCreate();           // DESTROY 088
    listAddNodeTail(rt->ilist, VOIDINT imatch);
    if (ri->luat) rt->nltrgr++;   // this table now has LUA TRIGGERS
    if (ri->clist) {
        listNode *ln; rt->nmci++; // this table now has MCI
        ri->nclist    = listLength(ri->clist);
        ri->bclist    = malloc(ri->nclist * sizeof(int)); /* FREE ME 053 */
        int       i   = 0;
        listIter *li  = listGetIterator(ri->clist, AL_START_HEAD);
        while((ln = listNext(li))) { /* convert clist to bclist */
            ri->bclist[i]                = (int)(long)ln->value;
            rt->col[ri->bclist[i]].indxd = 1; /* used in updateRow OVRWR */
            i++;
        } listReleaseIterator(li);
    } else if (!ri->luat) rt->col[ri->column].indxd = 1;
    if      (virt)                 rt->vimatch = imatch;
    else if (ri->luat) { //TODO btr should not point to luat, have ri->luat
        ri->btr      = (bt *)luat; luat->num   = imatch;
    } else {
        uchar pktyp  = rt->col[0].type;
        uchar ktype  = rt->col[cmatch].type;
        ri->btr = ri->clist       ? createMCIndexBT(ri->clist, imatch) :
                  UNIQ(ri->cnstr) ? createU_S_IBT  (ktype,     imatch, pktyp) :
        /* normal & lru/lfu */      createIndexBT  (ktype,     imatch);
    }
    ASSERT_OK(dictAdd(IndD, sdsdup(ri->name), VOIDINT(imatch + 1)));
    if (!virt && !lru && !lfu && !luat && !prtl) { //NOTE: failure -> emptyIndex
        if (buildNewIndex(c, tmatch, imatch, -1) == -1) return -1;
    }
    return imatch;
}

bool addC2MCI(cli *c, int cmatch, list *clist) {
    if (cmatch <= -1) {
        listRelease(clist);
        if (c) addReply(c, shared.indextargetinvalid); return 0;
    }
    if (!cmatch) {
        listRelease(clist); if (c) addReply(c, shared.mci_on_pk); return 0;
    }
    listAddNodeTail(clist, VOIDINT cmatch);
    return 1;
}
static bool ICommit(cli *c,      sds   iname,   sds   tname, char *cname,
                    uchar cnstr, sds   obcname, long  limit) {
    int      cmatch  = -1;
    list    *clist   = NULL;
    bool     prtl    = (limit != -1);
    int      tmatch  = find_table(tname);
    if (tmatch == -1) { addReply(c, shared.nonexistenttable);       return 0; }
    r_tbl_t *rt      = &Tbl[tmatch];
    if (prtl && !C_IS_NUM(rt->col[0].type)) { // INDEX CURSOR PK -> NUM
        addReply(c, shared.indexcursorerr);                         return 0;
    }
    SKIP_SPACES(cname);
    char *nextc = strchr(cname, ',');
    bool  new   = 1; // Used in Index Cursors
    if (nextc) {    /* Multiple Column Index */
        if UNIQ(cnstr) {
            if (rt->nmci) { addReply(c, shared.two_uniq_mci);       return 0;
            } else if (!C_IS_NUM(rt->col[0].type)) { /* INT & LONG */
                addReply(c, shared.uniq_mci_pk_notint);             return 0;
            }
        }
        //TODO use parseCommaSpaceList() ???
        int ocmatch = -1; /* first column can be used as normal index */
        clist       = listCreate();                  /* DESTROY ME 054 */
        while (1) {
            char *end = nextc - 1;
            REV_SKIP_SPACES(end)
            cmatch    = find_column_n(tmatch, cname, (end + 1 - cname));
            if (!addC2MCI(c, cmatch, clist))                        return 0;
            if (ocmatch == -1) ocmatch = cmatch;
            nextc++;
            SKIP_SPACES(nextc);
            cname     = nextc;
            nextc     = strchr(nextc, ',');
            if (!nextc) {
                cmatch = find_column(tmatch, cname);
                if (!addC2MCI(c, cmatch, clist))                    return 0;
                break;
            }
        }
        if UNIQ(cnstr) {/*NOTE: RESTRICTION: UNIQUE MCI final col must be NUM */
            listNode *ln = listLast(clist);
            int       fcmatch = (int)(long)ln->value;
            if (!C_IS_NUM(rt->col[fcmatch].type)) {
                addReply(c, shared.uniq_mci_notint);                return 0;
            }
        }
        for (int i = 0; i < Num_indx; i++) { /* already indxd? */
            r_ind_t *ri = &Index[i];
            if (ri->name && ri->clist && ri->table == tmatch) {
                if (clist->len == ri->clist->len) {
                    listNode *oln, *nln;
                    listIter *nli = listGetIterator(clist,     AL_START_HEAD);
                    listIter *oli = listGetIterator(ri->clist, AL_START_HEAD);
                    bool match = 1;
                    while ((nln = listNext(nli)) && (oln = listNext(oli))) {
                        if ((int)(long)nln->value != (int)(long)oln->value) {
                            match = 0; break;
                        }
                    } listReleaseIterator(nli); listReleaseIterator(oli);
                    if (match) {
                        if (prtl && !ri->done) {
                            new = 0;
                            if (strcmp(ri->name, iname)) {
                                addReply(c, shared.indexcursorerr);  return 0;
                            }
                        } else { addReply(c, shared.indexedalready); return 0; }
                    }
                }
            }
        }
        cmatch = ocmatch;
    } else {
        cmatch = find_column(tmatch, cname);
        if (cmatch <= -1) { addReply(c, shared.indextargetinvalid); return 0; }
        if UNIQ(cnstr) {/*NOTE: RESTRICTION: UNIQUE MCI both cols -> NUM */
            if (!C_IS_NUM(rt->col[cmatch].type) || !C_IS_NUM(rt->col[0].type)) {
                addReply(c, shared.uniq_simp_index_nums); return 0;
            }
         }
        for (int i = 0; i < Num_indx; i++) { /* already indxd? */
            r_ind_t *ri = &Index[i];
            if (ri->name && ri->table == tmatch && ri->column == cmatch) {
                if (prtl && !ri->done) {
                    new = 0;
                    if (strcmp(ri->name, iname)) {
                        addReply(c, shared.indexcursorerr);         return 0;
                    }
                } else { addReply(c, shared.indexedalready);        return 0; }
            }
        }
    }
    int obc = -1;
    if (obcname) {
        obc = find_column(tmatch, obcname);
        if (obc == -1) { addReply(c, shared.indexobcerr);           return 0; }
        if (obc ==  0) { addReply(c, shared.indexobcrpt);           return 0; }
        if (UNIQ(cnstr) || (obc == cmatch) ||
            !C_IS_NUM(rt->col[obc].type) || !C_IS_NUM(rt->col[0].type)) {
             addReply(c, shared.indexobcill);                       return 0;
        }

    }
    if (new) {
        if ((newIndex(c, iname, tmatch, cmatch, clist, cnstr, 0,
                      0, NULL,  obc,    prtl,   0)) == -1)          return 0;
    }
    if (prtl) {
        int imatch = find_partial_index(tmatch, cmatch);
        if (imatch == -1) { addReply(c, shared.indexcursorerr);     return 0; }
        long card = buildNewIndex(c, tmatch, imatch, limit);
        if (card == -1)                                             return 0;
        else { addReplyLongLong(c, (lolo)card);                     return 1; }
    }
    addReply(c, shared.ok);
    return 1;
}
void createIndex(redisClient *c) {
    if (c->argc < 6) { addReply(c, shared.index_wrong_nargs);        return; }
    int targ, coln; bool cnstr;
    if (!strcasecmp(c->argv[1]->ptr, "UNIQUE")) {
        cnstr = CONSTRAINT_UNIQUE; coln = 6; targ = 5;
        if (c->argc < 7) { addReply(c, shared.index_wrong_nargs);    return; }
    } else {
        cnstr = CONSTRAINT_NONE;   coln = 5; targ = 4;
    }
    if (strcasecmp(c->argv[coln - 2]->ptr, "ON")) {
        addReply(c, shared.createsyntax);                            return;
    }
    sds iname = c->argv[coln - 3]->ptr;
    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames);                     return;
    }
    char *token = c->argv[coln]->ptr;
    char *end   = strchr(token, ')');
    if (!end || (*token != '(')) { addReply(c, shared.createsyntax); return; }
    STACK_STRDUP(cname, (token + 1), (end - token - 1))

    sds  obcname = NULL;
    long limit   = -1;
    if (c->argc > (coln + 1)) { // CREATE INDEX i_t ON t (fk) ORDER BY ts
        if (c->argc == (coln + 4)) {
            if (strcasecmp(c->argv[coln + 1]->ptr, "ORDER") || 
                strcasecmp(c->argv[coln + 2]->ptr, "BY")) {
                addReply(c, shared.createsyntax);                    return;
            } else {
                obcname = c->argv[coln + 3]->ptr; coln += 3;
            }
        }
        if (c->argc > (coln + 1)) {
            bool ok = 0;
            if (c->argc == (coln + 3)) {
                if (!strcasecmp(c->argv[coln + 1]->ptr, "LIMIT")) {
                    limit = strtoul(c->argv[coln + 2]->ptr, NULL, 10);
                    if (limit > 0) ok = 1;
                }
            }
            if (!ok) { addReply(c, shared.createsyntax);             return; }
        }
    }
    ICommit(c, iname, c->argv[targ]->ptr, cname, cnstr, obcname, limit);
}
void emptyIndex(int imatch) { //printf("emptyIndex: imatch: %d\n", imatch);
    r_ind_t  *ri = &Index[imatch];
    if (!ri->name) return; /* previously deleted */
    dictDelete(IndD, ri->name); sdsfree(ri->name);      /* DESTROYED 055 */
    r_tbl_t  *rt = &Tbl[ri->table];
    rt->col[ri->column].imatch = -1;
    if (ri->column != -1) {
        listNode *ln = listSearchKey(rt->ilist, VOIDINT imatch);
        listDelNode(rt->ilist, ln);
    }
    if (ri->luat && rt->nltrgr) rt->nltrgr--;
    if (ri->clist) {
        if (rt->nmci) rt->nmci--;
        listRelease(ri->clist);                          /* DESTROYED 054 */
        free(ri->bclist);                                /* FREED 053 */
    }
    // ri->lru & ri->lfu can NOT be dropped, so no need to change rt
    if (!ri->luat && ri->btr) destroy_index(ri->btr, ri->btr->root, imatch);
    bzero(ri, sizeof(r_ind_t));
    ri->table   = ri->column = ri->obc = ri->ofst = -1;
    ri->cnstr   =  CONSTRAINT_NONE;
    if (imatch == (Num_indx - 1)) Num_indx--; // if last -> reuse
    else {                                    // else put on DropI for reuse
        if (!DropI) DropI = listCreate();
        listAddNodeTail(DropI, VOIDINT imatch);
    }
    server.dirty++;
}
void dropIndex(redisClient *c) {
    sds  iname  = c->argv[2]->ptr;
    int  imatch = match_partial_index_name(iname);
    if (imatch == -1)         { addReply(c, shared.nullbulk);           return;}
    r_ind_t *ri = &Index[imatch];
    if (ri->virt)             { addReply(c, shared.drop_virtual_index); return;}
    if (ri->lru)              { addReply(c, shared.drop_lru);           return;}
    if (ri->lfu)              { addReply(c, shared.drop_lfu);           return;}
    r_tbl_t *rt = &Tbl[ri->table];
    if (rt->sk == ri->column) { addReply(c, shared.drop_ind_on_sk);     return;}
    emptyIndex(imatch);
    addReply(c, shared.cone);
}

// DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
sds getMCIlist(list *clist, int tmatch) { // NOTE: used in DESC & AOF
    listNode *ln;
    sds       mci = sdsnewlen("(", 1);                   /* DESTORY ME 051 */
    int       i   = 0;
    listIter *li  = listGetIterator(clist, AL_START_HEAD);
    while((ln = listNext(li))) {
       if (i) mci = sdscatlen(mci, ", ", 2);
       int cmatch = (int)(long)ln->value;
       mci        = sdscatprintf(mci, "%s", Tbl[tmatch].col[cmatch].name);
       i++;
    } listReleaseIterator(li);
    mci = sdscatlen(mci, ")", 1);
    return mci;
}

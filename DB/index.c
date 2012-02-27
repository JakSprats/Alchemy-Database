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

#include "xdb_hooks.h"

#include "adlist.h"
#include "redis.h"

#include "bt.h"
#include "btreepriv.h"
#include "bt_iterator.h"
#include "range.h"
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

extern r_tbl_t  *Tbl;
extern uint32    Ind_HW; extern dict *IndD; extern list *DropI;
extern char     *Col_type_defs[];
extern dictType  sdsDictType;

// GLOBALS
int      Num_indx;
r_ind_t *Index = NULL;

// PROTOTYPES
static void destroy_mci(bt *ibtr, bt_n *n, int imatch, int lvl);
void luaPushError(lua_State *lua, char *error);

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
        aobj acol  = getCol(btr, rrow, ri->bclist[i], apk, ri->tmatch, NULL);
        if (acol.empty)                                   goto iaddmci_err;
        nbtr       = btIndFind(ibtr, &acol);
        if (!nbtr) {
            if (i == final) {                     /* final  MID -> NODE*/
                uchar otype  = ocol ? ocol->type : COL_TYPE_NONE;
                nbtr = createIndexNode(pktyp, otype);
            } else {                              /* middle MID -> MID */
                uchar ntype = 
                             Tbl[ri->tmatch].col[ri->bclist[i + 1].cmatch].type;
                if C_IS_O(ntype) ntype = ri->dtype; // DNI override
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
        aobj fcol = getCol(btr, rrow, ri->bclist[final], apk, ri->tmatch, NULL);
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
        aobj acol = getCol(btr, rrow, ri->bclist[i], apk, ri->tmatch, NULL);
        if (acol.empty) return; /* NOTE: no rollback, iAddMCI does rollback */
        nbtr      = btIndFind(ibtr, &acol);
        dpl[i]    = init_dp(ibtr, &acol, nbtr);
        ibtr      = nbtr; /* NOTE: DO NOT release acol - it is used later */
    }                     /* NOTE: DO NOT reuse nbtr   - it is used later */
    int nkeys;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {
        aobj dcol = getCol(btr, rrow, ri->bclist[final], apk, ri->tmatch, NULL);
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
        aobj acol = getCol(btr, rrow, ri->bclist[i], apk, ri->tmatch, NULL);
        if (acol.empty) return; /* NOTE: no rollback, iAddMCI does rollback */
        nbtr      = btIndFind(ibtr, &acol);
        dpl[i]    = init_dp(ibtr, &acol, nbtr);
        ibtr      = nbtr; /* NOTE: DO NOT release acol - it is used later */
    }                     /* NOTE: DO NOT reuse nbtr   - it is used later */
    int   nkeys ;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {
        aobj dcol = getCol(btr, rrow, ri->bclist[final], apk, ri->tmatch, NULL);
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
    if (ri->virt || ri->fname)                                       return 1;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->hlt) { luatAdd(btr, ri->luat, apk, imatch, rrow);        return 1; }
    int      pktyp = Tbl[ri->tmatch].col[0].type;
    if (ri->clist) {
        if (ri->obc.cmatch == -1) {
            if (!iAddMCI(c, btr, apk, pktyp, imatch, rrow, NULL))    return 0;
        } else {
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
            if (!iAddMCI(c, btr, apk, pktyp, imatch, rrow, &ocol))   return 0;
            releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->icol, apk, ri->tmatch, NULL);
        if (!acol.empty) {
            if (ri->obc.cmatch == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                if (!iAdd(c, ibtr, &acol, apk, pktyp, NULL, imatch)) return 0;
            } else {             // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
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
    if (ri->virt || ri->fname)                                        return;
    bt      *ibtr = getIBtr(imatch);
    if (ri->hlt) { luatDel(btr, ri->luat, apk, imatch, rrow);         return; }
    if (ri->clist) {
        if (ri->obc.cmatch == -1) iRemMCI(btr, apk, imatch, rrow, NULL, gost);
        else {                                                    // MCI OBY
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
            iRemMCI(btr, apk, imatch, rrow, &ocol, gost); releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->icol, apk, ri->tmatch, NULL);
        if (!acol.empty) {
            if (ri->obc.cmatch == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                iRem(ibtr, &acol, apk, NULL, imatch, gost);
            } else {                    // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
                iRem(ibtr, &acol, apk, &ocol, imatch, gost); releaseAobj(&ocol);
            }
        } releaseAobj(&acol);
    }
}
void evictFromIndex(bt *btr, aobj *apk, void *rrow, int imatch) {
    printf("Evict: imatch: %d apk: ", imatch); dumpAobj(printf, apk);
    r_ind_t *ri   = &Index[imatch];
    if (ri->virt || ri->fname)                                       return;
    if (ri->hlt) { printf("TODO: EVICT call its own LuatTrigger\n"); return; }
    bt      *ibtr = getIBtr(imatch);
    if (ri->clist) { // MCI
        if (ri->obc.cmatch == -1) iEvictMCI(btr, apk, imatch, rrow, NULL);//NORM
        else { // MCI OBY
            aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
            iEvictMCI(btr, apk, imatch, rrow, &ocol); releaseAobj(&ocol);
        }
    } else {
        aobj acol = getCol(btr, rrow, ri->icol, apk, ri->tmatch, NULL);
        if (!acol.empty) {
            if (ri->obc.cmatch == -1) { // NORMAL
                if (ri->lfu) acol.l = (ulong)(floor(log2((dbl)acol.l))) + 1;
                iEvict(ibtr, &acol, apk, NULL);
            } else {             // OBY
                aobj ocol = getCol(btr, rrow, ri->obc, apk, ri->tmatch, NULL);
                iEvict(ibtr, &acol, apk, &ocol); releaseAobj(&ocol);
            }
        } releaseAobj(&acol);
    }
}
//NOTE: upIndex() called from LRU/LFU
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

/* CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX */
long buildIndex(cli *c, bt *btr, int imatch, long limit) {
    btEntry *be; btSIter *bi; long final = 0;
    r_ind_t *ri   = &Index[imatch];
    bool     prtl = (limit != -1);
    if (prtl) {
       uchar pktyp = Tbl[ri->tmatch].col[0].type;
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

static bool createLuaElementIndex(cli *c, int tmatch, icol_t ic, int imatch) {
    r_tbl_t *rt  = &Tbl  [tmatch];
    r_ind_t *ri  = &Index[imatch];
    bool     ret = 1;
    CLEAR_LUA_STACK
    lua_getfield  (server.lua, LUA_GLOBALSINDEX, "createIndLuaEl");
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[ic.cmatch].name);
    int argc = 2;
    for (uint32 i = 0; i < ri->icol.nlo; i++) {
        lua_pushstring(server.lua, ri->icol.lo[i]); argc++;
    }
    if (DXDB_lua_pcall(server.lua, argc, 0, 0)) { ret = 0;
        ADD_REPLY_FAILED_LUA_STRING_CMD("createIndLuaEl")
    }
    CLEAR_LUA_STACK return ret;
}
bool runLuaFunctionIndexFunc(cli *c, sds iconstrct, sds tname, sds  iname) {
    bool ret = 1;
    CLEAR_LUA_STACK
    lua_getfield   (server.lua, LUA_GLOBALSINDEX, iconstrct);
    lua_pushlstring(server.lua, tname, sdslen(tname));
    lua_pushlstring(server.lua, iname, sdslen(iname));
    if (DXDB_lua_pcall(server.lua, 2, 0, 0)) { ret = 0;
        if (c) { ADD_REPLY_FAILED_LUA_STRING_CMD(iconstrct); }
    }
    CLEAR_LUA_STACK return ret;
}
int newIndex(cli    *c,     sds    iname, int  tmatch,    icol_t ic,
             list   *clist, uchar  cnstr, bool virt,      bool   lru,
             luat_t *luat,  icol_t obc,   bool prtl,      bool   lfu,
             uchar   dtype, sds    fname, sds  iconstrct, sds idestrct) {
    if (ic.nlo > 1) {
        addReply(c, shared.nested_dni); return -1;
    }
    int imatch;
    if (!DropI && Num_indx >= (int)Ind_HW) addIndex();
    if (DropI) {
        listNode *ln = (DropI)->head;
        imatch       = (int)(long)ln->value;
        DEL_NODE_ON_EMPTY_RELEASE_LIST(DropI, ln);
    } else {
        imatch       = Num_indx; Num_indx++;
    }
    r_tbl_t *rt = &Tbl  [tmatch];
    r_ind_t *ri = &Index[imatch]; bzero(ri, sizeof(r_ind_t));
    ri->name    = sdsdup(iname);                     // FREE 055
    ri->tmatch  = tmatch; ri->icol  = ic;    ri->clist = clist;
    ri->virt    = virt;   ri->cnstr = cnstr; ri->lru   = lru;
    ri->obc     = obc;    ri->lfu   = lfu;
    if (fname)     ri->fname     = sdsdup(fname);          // FREE 162
    if (iconstrct) ri->iconstrct = sdsdup(iconstrct);      // FREE 167
    if (idestrct)  ri->idestrct  = sdsdup(idestrct);       // FREE 166
    ri->hlt     = luat ? 1 :  0;
    ri->done    = prtl ? 0 :  1; 
    ri->dtype   = (ic.cmatch == -1 || dtype) ? dtype : rt->col[ic.cmatch].type;
    ri->ofst    = prtl ? 1 : -1;// NOTE: PKs start at 1 (not 0)
    if (!rt->ilist) rt->ilist  = listCreate();           // DESTROY 088
    listAddNodeTail(rt->ilist, VOIDINT imatch);
    if (ri->icol.cmatch != -1) {
        rt->col[ri->icol.cmatch].imatch = imatch;
        ci_t *ci = dictFetchValue(rt->cdict, rt->col[ri->icol.cmatch].name);
        if (!ci->ilist) ci->ilist = listCreate();        // FREE 148
        listAddNodeTail(ci->ilist, VOIDINT imatch);
    }
    if (fname) {
        if (!rt->fdict) rt->fdict = dictCreate(&sdsDictType, NULL);
        int fimatch = INTVOID dictFetchValue(rt->fdict, fname);
        if (fimatch) {
             addReply(c, shared.luafuncindex_rpt);         goto newind_err;
        }
        ASSERT_OK(dictAdd(rt->fdict, sdsdup(fname), VOIDINT(imatch + 1)));// >0
    }
    if (ri->hlt) rt->nltrgr++;   // this table now has LUA TRIGGERS
    if (ri->clist) {
        listNode *ln; rt->nmci++; // this table now has MCI
        ri->nclist   = listLength(ri->clist);
        ri->bclist   = malloc(ri->nclist * sizeof(icol_t)); /* FREE ME 053 */
        bzero(ri->bclist, ri->nclist * sizeof(icol_t));
        int       i  = 0;
        listIter *li = listGetIterator(ri->clist, AL_START_HEAD);
        while((ln = listNext(li))) { /* convert clist to bclist */
            icol_t *ic = ln->value;
            cloneIC(&ri->bclist[i], ic);
            rt->col[ic->cmatch].indxd = 1; /* used in updateRow OVRWR */
            i++;
        } listReleaseIterator(li);
    } else if (ri->icol.cmatch != -1) rt->col[ri->icol.cmatch].indxd = 1;
    if      (virt) rt->vimatch = imatch;
    else if (ri->hlt) {
        ri->luat = luat; luat->num = imatch;
    } else {
        if (iconstrct &&
            !runLuaFunctionIndexFunc(c, iconstrct, rt->name, iname)) {
                                                           goto newind_err;
        }
        if (ri->icol.nlo &&
            !createLuaElementIndex(c, tmatch, ic, imatch)) goto newind_err;
        uchar pktyp = rt->col[0].type;
        if        (ri->clist) {
            ri->btr = createMCI_IBT(ri->clist, imatch, ri->dtype);
        } else if UNIQ(ri->cnstr) {
            ri->btr = createU_S_IBT(ri->dtype, imatch, pktyp);
        } else { // Normal & LRU/LFU
            ri->btr = createIndexBT(ri->dtype, imatch);
        }

    }
    ASSERT_OK(dictAdd(IndD, sdsdup(ri->name), VOIDINT(imatch + 1)));
    if (!virt && !lru && !lfu && !luat && !prtl && !fname) {
        //NOTE: failure -> emptyIndex
        if (buildNewIndex(c, tmatch, imatch, -1) == -1)    goto newind_err;
    }
    return imatch;

newind_err:
    emptyIndex(c, imatch); return -1;
}

bool addC2MCI(cli *c, icol_t ic, list *clist) {
    if (ic.cmatch <= -1) {
        if (c) { addReply(c, shared.indextargetinvalid); }
        listRelease(clist); return 0;
    }
    if (!ic.cmatch) {
        listRelease(clist); if (c) { addReply(c, shared.mci_on_pk); } return 0;
    }
    icol_t *mic = malloc(sizeof(icol_t)); cloneIC(mic, &ic);
    listAddNodeTail(clist, VOIDINT mic);
    return 1;
}
static bool ICommit(cli   *c,    sds iname,     sds  tname, sds   cname,
                    uchar cnstr, sds obcname,   long limit, uchar dtype,
                    sds   fname, sds iconstrct, sds idestrct) {
    DECLARE_ICOL(ic, -1)
    list    *clist   = NULL;
    bool     prtl    = (limit != -1);
    int      tmatch  = find_table(tname);
    if (tmatch == -1) { addReply(c, shared.nonexistenttable);       return 0; }
    r_tbl_t *rt      = &Tbl[tmatch];
    if (rt->dirty) { addReply(c, shared.buildindexdirty);           return 0; }
    if (prtl && !C_IS_NUM(rt->col[0].type)) { // INDEX CURSOR PK -> NUM
        addReply(c, shared.indexcursorerr);                         return 0;
    }
    bool  new   = 1; // Used in Index Cursors
    char *nextc = fname ? NULL : strchr(cname, ',');
    if (fname) { // NOOP
    } else if (nextc) {    /* Multiple Column Index */
        char *cn = cname;
        if UNIQ(cnstr) {
            //TODO why cant we have 2 UNIQ MCI's on a table?
            if (rt->nmci) { addReply(c, shared.two_uniq_mci);       return 0;
            } else if (!C_IS_NUM(rt->col[0].type)) { /* INT & LONG */
                addReply(c, shared.uniq_mci_pk_notint);             return 0;
            }
        }
        DECLARE_ICOL(oic, -1)
        clist       = listCreate();                  /* DESTROY ME 054 */
        while (1) {
            char *end = nextc - 1;
            REV_SKIP_SPACES(end)
            oic       = find_column_n(tmatch, cn, (end + 1 - cn));
            if (!addC2MCI(c, oic, clist))                           return 0;
            //TODO releaseIC(oic);
            if (ic.cmatch == -1) cloneIC(&ic, &oic); // 1st col can be index
            nextc++;
            SKIP_SPACES(nextc);
            cn        = nextc;
            nextc     = strchr(nextc, ',');
            if (!nextc) {
                oic = find_column(tmatch, cn);
                if (!addC2MCI(c, oic, clist))                       return 0;
                break;
            }
        }
        if UNIQ(cnstr) {/*NOTE: RESTRICTION: UNIQUE MCI final col must be NUM */
            listNode *ln      = listLast(clist);
            icol_t   *fic     = ln->value;
            int       fcmatch = fic->cmatch;
            if (!C_IS_NUM(rt->col[fcmatch].type)) {
                addReply(c, shared.uniq_mci_notint);                return 0;
            }
        }
        for (int i = 0; i < Num_indx; i++) { /* already indxd? */
            r_ind_t *ri = &Index[i];
            if (ri->name && ri->clist && ri->tmatch == tmatch) {
                if (clist->len == ri->clist->len) {
                    listNode *oln, *nln;
                    listIter *nli = listGetIterator(clist,     AL_START_HEAD);
                    listIter *oli = listGetIterator(ri->clist, AL_START_HEAD);
                    bool match = 1;
                    while ((nln = listNext(nli)) && (oln = listNext(oli))) {
                        icol_t *nic = nln->value; icol_t *oic = oln->value;
                        if (!icol_cmp(nic, oic)) { match = 0; break; }
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
    } else {
        ic = find_column_sds(tmatch, cname);
        if (ic.cmatch <= -1) {
            addReply(c, shared.indextargetinvalid); return 0;
        }
        if UNIQ(cnstr) {/*NOTE: RESTRICTION: UNIQUE MCI both cols -> NUM */
            if ((!C_IS_NUM(rt->col[ic.cmatch].type) && !C_IS_NUM(dtype)) ||
                !C_IS_NUM(rt->col[0].type)) {
                addReply(c, shared.uniq_simp_index_nums); return 0;
            }
         }
        for (int i = 0; i < Num_indx; i++) { /* already indxd? */
            r_ind_t *ri = &Index[i];
            if (ri->name && ri->tmatch == tmatch && !icol_cmp(&ri->icol, &ic)) {
                if (prtl && !ri->done) {
                    new = 0;
                    if (strcmp(ri->name, iname)) {
                        addReply(c, shared.indexcursorerr);         return 0;
                    }
                } else { addReply(c, shared.indexedalready);        return 0; }
            }
        }
    }
    DECLARE_ICOL(obc, -1)
    if (obcname) {
        obc = find_column(tmatch, obcname);
        if (obc.cmatch == -1) { addReply(c, shared.indexobcerr);    return 0; }
        if (obc.cmatch ==  0) { addReply(c, shared.indexobcrpt);    return 0; }
        if (UNIQ(cnstr) || (!icol_cmp(&obc, &ic)) ||
            !C_IS_NUM(rt->col[obc.cmatch].type)   ||
            !C_IS_NUM(rt->col[0].type)) {
             addReply(c, shared.indexobcill);                       return 0;
        }
    }
    if (new) {
        if ((newIndex(c,   iname, tmatch, ic,    clist, cnstr, 0, 0, NULL,
                      obc, prtl,  0,      dtype, fname,
                      iconstrct, idestrct)) == -1)                  return 0;
    }
    if (prtl) {
        int imatch = find_partial_index(tmatch, ic);
        if (imatch == -1) { addReply(c, shared.indexcursorerr);     return 0; }
        long card = buildNewIndex(c, tmatch, imatch, limit);
        if (card == -1)                                             return 0;
        else { addReplyLongLong(c, (lolo)card);                     return 1; }
    }
    addReply(c, shared.ok);
    return 1;
}
void createIndex(redisClient *c) {
    if (c->argc < 6) { addReply(c, shared.index_wrong_nargs);         return; }
    int targ, coln; bool cnstr;
    if (!strcasecmp(c->argv[1]->ptr, "UNIQUE")) {
        cnstr = CONSTRAINT_UNIQUE; coln = 6; targ = 5;
        if (c->argc < 7) { addReply(c, shared.index_wrong_nargs);     return; }
    } else {
        cnstr = CONSTRAINT_NONE;   coln = 5; targ = 4;
    }
    if (strcasecmp(c->argv[coln - 2]->ptr, "ON")) {
        addReply(c, shared.createsyntax);                             return;
    }
    sds iname = c->argv[coln - 3]->ptr;
    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames);                      return;
    }
    char *token = c->argv[coln]->ptr;
    char *end   = strrchr(token + sdslen(token) - 1, ')');
    if (!end || (*token != '(')) { addReply(c, shared.createsyntax);  return; }
    token++; SKIP_SPACES(token);
    sds  obcname = NULL, fname = NULL, iconstrct = NULL, idestrct = NULL;
    sds cname = sdsnewlen(token, (end - token));         // FREE 158

    uchar  dtype = COL_TYPE_NONE;
    char  *dn    = strchr(cname, '.');
    if (dn) {
        if (c->argc < (coln + 2)) {
            addReply(c, shared.createsyntax_dn);               goto cr8i_end;
        }
        coln++;
        if (!parseColType(c, c->argv[coln]->ptr, &dtype)) {
            addReply(c, shared.createsyntax_dn);               goto cr8i_end;
        }
    }
    bool findex = (cname[sdslen(cname) - 1] == ')' &&
                   cname[sdslen(cname) - 2] == '(');
    long limit  = -1;
    if (findex) {
        char *prn = strchr(cname, '(');
        fname     = sdsnewlen(cname, (prn - cname));     // FREE 159
        if (c->argc < (coln + 3)) {
            addReply(c, shared.create_findex);                 goto cr8i_end;
        }
        coln++;
        if (!parseColType(c, c->argv[coln]->ptr, &dtype)) {
            addReply(c, shared.create_findex);                 goto cr8i_end;
        }
        coln++;
        iconstrct  = sdsdup(c->argv[coln]->ptr);           // FREE 160;
        coln++;
        if (c->argc > coln) idestrct = sdsdup(c->argv[coln]->ptr); // FREE 165
    } else if (c->argc > (coln + 1)) {
        if (strcasecmp(c->argv[coln + 1]->ptr, "ORDER") &&
            (c->argc == (coln + 4))) { 
            if (strcasecmp(c->argv[coln + 1]->ptr, "ORDER") || 
                strcasecmp(c->argv[coln + 2]->ptr, "BY")) {
                addReply(c, shared.createsyntax);              goto cr8i_end;
            } else {
                coln += 3; obcname = sdsdup(c->argv[coln]->ptr); // FREE 161
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
            if (!ok) { addReply(c, shared.createsyntax);       goto cr8i_end; }
        }
    }
    if (limit != -1 && fname) {
        addReply(c, shared.create_findex);                     goto cr8i_end;
    }
    ICommit(c, iname, c->argv[targ]->ptr, cname, cnstr, obcname, limit, dtype,
            fname, iconstrct, idestrct);

cr8i_end:
    if (fname)     sdsfree(fname);                       // FREED 158
    if (iconstrct) sdsfree(iconstrct);                   // FREED 159
    if (idestrct)  sdsfree(idestrct);                    // FREED 165
    if (obcname)   sdsfree(obcname);                     // FREED 161
    sdsfree(cname);                                      // FREED 158
}

// RUN_INDEX RUN_INDEX RUN_INDEX RUN_INDEX RUN_INDEX RUN_INDEX RUN_INDEX
#define INDXRUN_NO_LT   1
#define INDXRUN_ONLY_LT 2
static bool __runInsertIndexes(cli  *c,      bt   *btr,
                               aobj *npk,    void *nrow, int matches,
                               int   inds[], int   flble) {
    if (!matches) return 1;
    for (int i = 0; i < matches; i++) {
        if (flble == INDXRUN_NO_LT   &&  Index[inds[i]].hlt) continue;
        if (flble == INDXRUN_ONLY_LT && !Index[inds[i]].hlt) continue;
        if (!addToIndex(c, btr, npk, nrow, inds[i])) {
            for (int j = 0; j < i; j++) { // ROLLBACK previous ADD-INDEXes
                delFromIndex(btr, npk, nrow, inds[j], 0);
            }
            return 0;
        }
    }
    return 1;
}
bool runFailableInsertIndexes(cli *c,       bt  *btr,  aobj *npk, void *nrow,
                              int  matches, int  inds[]) {
    //printf("runFailableInsertIndexes\n");
    return __runInsertIndexes(c, btr, npk, nrow, matches, inds, INDXRUN_NO_LT);
}
void runLuaTriggerInsertIndexes(cli *c,       bt  *btr,  aobj *npk, void *nrow,
                                int  matches, int  inds[]) {
    //printf("runLuaTriggerInsertIndexes\n");
    __runInsertIndexes(c, btr, npk, nrow, matches, inds, INDXRUN_ONLY_LT);
}

void runPreUpdateLuatriggers(bt  *btr,    aobj *opk, void *orow,
                             int  matches, int  inds[]) {
    //printf("runPreUpdateLuatriggers: orow: %p\n", orow);
    if (!matches) return;
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri = &Index[inds[i]];
        if(!ri->hlt) continue; // ONLY LUATRIGGERS
        luatPreUpdate(btr, ri->luat, opk, inds[i], orow);
    }
}
void runPostUpdateLuatriggers(bt  *btr,     aobj *npk, void *nrow,
                              int  matches, int  inds[]) {
    //printf("runPostUpdateLuatriggers: nrow: %p\n", nrow);
    if (!matches) return;
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri = &Index[inds[i]];
        if(!ri->hlt) continue; // ONLY LUATRIGGERS
        luatPostUpdate(btr, ri->luat, npk, inds[i], nrow);
    }
}
void runDeleteIndexes(bt   *btr, aobj *opk, void *orow, int matches, int inds[],
                      bool  wgost) {
    for (int i = 0; i < matches; i++) {
        delFromIndex(btr, opk, orow, inds[i], wgost);
    }
}

// DESTROY_INDEX DESTROY_INDEX DESTROY_INDEX DESTROY_INDEX DESTROY_INDEX
static void emptyLuaTableElementIndex(int imatch) {
    r_ind_t *ri = &Index[imatch];
    r_tbl_t *rt = &Tbl  [ri->tmatch];
    CLEAR_LUA_STACK lua_getfield(server.lua, LUA_GLOBALSINDEX, "dropIndLuaEl");
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[ri->icol.cmatch].name);
    int argc = 2;
    for (uint32 i = 0; i < ri->icol.nlo; i++) {
        lua_pushstring(server.lua, ri->icol.lo[i]); argc++;
    }
    DXDB_lua_pcall(server.lua, argc, 0, 0); CLEAR_LUA_STACK
}
void emptyIndex(cli *c, int imatch) {
    r_ind_t *ri = &Index[imatch];
    if (!ri->name) return; /* previously deleted */
    r_tbl_t *rt = &Tbl[ri->tmatch];
    if (ri->idestrct) {
        runLuaFunctionIndexFunc(c, ri->idestrct, rt->name, ri->name);
        sdsfree(ri->idestrct);                           // FREED 166
    }
    if (ri->fname)     sdsfree(ri->fname);               // FREED 162
    if (ri->iconstrct) sdsfree(ri->iconstrct);           // FREED 167
    dictDelete(IndD, ri->name); sdsfree(ri->name);       /* DESTROYED 055 */
    if (ri->icol.cmatch != -1) {
        rt->col[ri->icol.cmatch].imatch = -1;
        listNode *ln = listSearchKey(rt->ilist, VOIDINT imatch);
        listDelNode(rt->ilist, ln);
    }
    if (ri->hlt) { rt->nltrgr--; destroy_lua_trigger(ri->luat); }
    if (ri->clist) {
        if (rt->nmci) rt->nmci--;
        listRelease(ri->clist);                          /* DESTROYED 054 */
        free(ri->bclist);                                /* FREED 053 */
    }
    //NOTE:  ri->lru & ri->lfu can NOT be dropped, so no need to change rt
    if (ri->icol.cmatch != -1 && ri->btr && !ri->hlt) {
        destroy_index(ri->btr, ri->btr->root, imatch);
    }
    if (ri->icol.nlo) {
        for (uint32 i = 0; i < ri->icol.nlo; i++) sdsfree(ri->icol.lo[i]);
        emptyLuaTableElementIndex(imatch);
        //TODO free ri->icol.lo & set to NULL
    }
    bzero(ri, sizeof(r_ind_t));
    ri->tmatch = ri->icol.cmatch = ri->obc.cmatch = ri->ofst = -1;
    ri->cnstr  = CONSTRAINT_NONE;
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
    r_tbl_t *rt = &Tbl[ri->tmatch];
    if (rt->sk != -1 && 
        rt->sk == ri->icol.cmatch) {addReply(c, shared.drop_ind_on_sk); return;}
    emptyIndex(c, imatch);
    addReply(c, shared.cone);
}

// LUA_INDEX_CALLBACKS LUA_INDEX_CALLBACKS LUA_INDEX_CALLBACKS
#define DEBUG_LUA_INDEX_OP                                                \
  printf("tname: %s cname: %s ename: %s\n", tname, cname, ename);         \
  printf("apk:  "); dumpAobj(printf, &apk);                               \
  printf("tmatch: %d cmatch: %d imatch: %d\n", tmatch, ic.cmatch, imatch);
#define DEBUG_LUA_UPDATE_INDEX               \
  DEBUG_LUA_INDEX_OP                         \
  printf("ocol: "); dumpAobj(printf, &ocol); \
  printf("ncol: "); dumpAobj(printf, &ncol);

static bool validatePKandOldVal(lua_State *lua, bt *ibtr,
                                aobj *apk, aobj *ocol) {
    bt   *nbtr = btIndFind(ibtr, ocol);
    if (!nbtr) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyDeleteIndexByName() - OVAL does not exist");
        return 1;
    }
    if (!btIndNodeExist(nbtr, apk)) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyDeleteIndexByName() - PK does not exist");
        return 1;
    }
    return 0;
}
//TODO tname, cname, ename can be local variables
static void getTblColElmntFromLua(lua_State *lua, int stack_size,
                                  sds *tname,  sds    *cname, sds *ename,
                                  int *tmatch, icol_t *ic,    int *imatch) {
    size_t len;
    stack_size *= -1;
    char   *s = (char *)lua_tolstring(lua, stack_size, &len); stack_size++;
    *tname    = sdsnewlen(s, len);
    *tmatch   = find_table(*tname);
    s         = (char *)lua_tolstring(lua, stack_size, &len); stack_size++;
    *cname    = sdsnewlen(s, len);
    s         = (char *)lua_tolstring(lua, stack_size, &len); stack_size++;
    *ename    = sdsnewlen(s, len);
    *ic       = find_column_sds(*tmatch, *cname);
    ic->nlo   = 1;
    ic->lo    = malloc(sizeof(sds) * ic->nlo);   // FREE 146
    ic->lo[0] = sdsdup(*ename);
    *imatch   = find_index(*tmatch, *ic);
}

int luaAlchemySetIndex(lua_State *lua) {
    int  ssize = 5;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemySetIndex(tbl, col, elmnt, pk, val)");
        return 1;
    }
    sds tname, cname, ename; icol_t ic; int tmatch, imatch;
    getTblColElmntFromLua(lua, ssize, &tname,  &cname, &ename,
                                      &tmatch, &ic,    &imatch);
    uchar  pktyp = Tbl[tmatch].col[0].type;
    uchar  ctype = Index[imatch].dtype;
    aobj acol; initAobjFromLua(&acol, ctype); lua_pop(lua, 1);
    aobj apk;  initAobjFromLua(&apk,  pktyp); CLEAR_LUA_STACK
    sdsfree(tname); sdsfree(cname); sdsfree(ename);
    bt    *ibtr  = getIBtr(imatch);
    //TODO CurrClient? check error propogation
    bool   ret   = iAdd(server.alc.CurrClient, ibtr, &acol, &apk, pktyp,
                        NULL, imatch);
    lua_pushboolean(lua, ret); return 1;
}
int luaAlchemyUpdateIndex(lua_State *lua) {
    int  ssize = 6;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyUpdateIndex(tbl, col, elmnt, pk, old, new)");
        return 1;
    }
    sds tname, cname, ename; icol_t ic; int tmatch, imatch;
    getTblColElmntFromLua(lua, ssize, &tname,  &cname, &ename,
                                      &tmatch, &ic,    &imatch);
    uchar  pktyp = Tbl[tmatch].col[0].type;
    uchar  ctype = Index[imatch].dtype;
    aobj ncol; initAobjFromLua(&ncol, ctype); lua_pop(lua, 1);
    aobj ocol; initAobjFromLua(&ocol, ctype); lua_pop(lua, 1);
    aobj apk;  initAobjFromLua(&apk,  pktyp); CLEAR_LUA_STACK
    //DEBUG_LUA_UPDATE_INDEX
    sdsfree(tname); sdsfree(cname); sdsfree(ename);
    bt    *ibtr  = getIBtr(imatch);
    if (validatePKandOldVal(lua, ibtr, &apk, &ocol)) return 1;
    bool   ret   = upIndex(server.alc.CurrClient, ibtr, &apk, &ocol, &apk,
                           &ncol, pktyp,
                           NULL, NULL, imatch);
    lua_pushboolean(lua, ret); return 1;
}
int luaAlchemyDeleteIndex(lua_State *lua) { //printf("luaAlchemyDeleteIndex\n");
    int  ssize = 5;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyDeleteIndex(tbl, col, elmnt, pk, old)");
        return 1;
    }
    sds tname, cname, ename; icol_t ic; int tmatch, imatch;
    getTblColElmntFromLua(lua, ssize, &tname,  &cname, &ename,
                                      &tmatch, &ic,    &imatch);
    uchar  pktyp = Tbl[tmatch].col[0].type;
    uchar  ctype = Index[imatch].dtype;
    aobj ocol; initAobjFromLua(&ocol, ctype); lua_pop(lua, 1);
    aobj apk;  initAobjFromLua(&apk,  pktyp); CLEAR_LUA_STACK
    sdsfree(tname); sdsfree(cname); sdsfree(ename);
    bt    *ibtr  = getIBtr(imatch);
    if (validatePKandOldVal(lua, ibtr, &apk, &ocol)) return 1;
    bool   gost  = 0;
    iRem(ibtr, &ocol, &apk, NULL, imatch, gost);
    lua_pushboolean(lua, (int)1); return 1;
}

// LUA_INDEX_BY_NAME_CALLBACKS LUA_INDEX_BY_NAME_CALLBACKS
static int getIndexFromLua(lua_State *lua) {
    size_t len;
    char   *s  = (char *)lua_tolstring(lua, 1, &len);
    sds iname  = sdsnewlen(s, len);                       // FREE  164
    int imatch = match_index_name(iname); sdsfree(iname); // FREED 164
    return imatch;
}
int luaAlchemySetIndexByName(lua_State *lua) {
    int  ssize = 3;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemySetIndexByName(iname, pk, val)");
        return 1;
    }
    if (lua_type(lua, 1) != LUA_TSTRING || lua_type(lua, 2) != LUA_TNUMBER || 
        lua_type(lua, 3) != LUA_TNUMBER) {
        CLEAR_LUA_STACK
        luaPushError(lua,
                     "alchemySetIndexByName(iname[STRING], pk[NUM], val[NUM])");
        return 1;
    }
    int   imatch = getIndexFromLua(lua);    int   tmatch = Index[imatch].tmatch;
    uchar pktyp  = Tbl[tmatch].col[0].type; uchar ctype  = Index[imatch].dtype;
    long  pk     = lua_tointeger(server.lua, 2);
    long  val    = lua_tointeger(server.lua, 3);
    CLEAR_LUA_STACK
    aobj acol; initAobjFromLong(&acol, val, ctype);
    aobj apk;  initAobjFromLong(&apk,  pk,  pktyp);
    bt    *ibtr  = getIBtr(imatch);
    bool   ret   = iAdd(server.alc.CurrClient, ibtr, &acol, &apk, pktyp,
                        NULL, imatch);
    lua_pushboolean(lua, ret); return 1;
}
int luaAlchemyUpdateIndexByName(lua_State *lua) {
    int  ssize = 4;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyUpdateIndexByName(iname, pk, old, new)");
        return 1;
    }
    if (lua_type(lua, 1) != LUA_TSTRING || lua_type(lua, 2) != LUA_TNUMBER || 
        lua_type(lua, 3) != LUA_TNUMBER || lua_type(lua, 4) != LUA_TNUMBER)   {
        CLEAR_LUA_STACK
        luaPushError(lua,
                     "alchemyUpdateIndexByName(iname[STRING], pk[NUM]," \
                                             " oldval[NUM] newval[NUM])");
        return 1;
    }
    int   imatch = getIndexFromLua(lua);    int   tmatch = Index[imatch].tmatch;
    uchar pktyp  = Tbl[tmatch].col[0].type; uchar ctype  = Index[imatch].dtype;
    long  pk     = lua_tointeger(server.lua, 2);
    long  oval   = lua_tointeger(server.lua, 3);
    long  nval   = lua_tointeger(server.lua, 4);
    CLEAR_LUA_STACK
    aobj ocol; initAobjFromLong(&ocol, oval, ctype);
    aobj ncol; initAobjFromLong(&ncol, nval, ctype);
    aobj apk;  initAobjFromLong(&apk,  pk,   pktyp);
    bt    *ibtr  = getIBtr(imatch);
    if (validatePKandOldVal(lua, ibtr, &apk, &ocol)) return 1;
    bool   ret   = upIndex(server.alc.CurrClient, ibtr, &apk, &ocol, &apk,
                           &ncol, pktyp,
                           NULL, NULL, imatch);
    lua_pushboolean(lua, ret); return 1;
}
int luaAlchemyDeleteIndexByName(lua_State *lua) {
    int  ssize = 3;
    int  argc  = lua_gettop(lua);
    if (argc != ssize) {
        CLEAR_LUA_STACK
        luaPushError(lua, "alchemyDeleteIndexByName(iname, pk, old)");
        return 1;
    }
    if (lua_type(lua, 1) != LUA_TSTRING || lua_type(lua, 2) != LUA_TNUMBER || 
        lua_type(lua, 3) != LUA_TNUMBER) {
        CLEAR_LUA_STACK
        luaPushError(lua,
                     "alchemyDeleteIndexByName(iname[STRING], pk[NUM], "\
                                              "oval[NUM])");
        return 1;
    }
    int   imatch = getIndexFromLua(lua);    int   tmatch = Index[imatch].tmatch;
    uchar pktyp  = Tbl[tmatch].col[0].type; uchar ctype  = Index[imatch].dtype;
    long  pk     = lua_tointeger(server.lua, 2);
    long  oval   = lua_tointeger(server.lua, 3);
    CLEAR_LUA_STACK
    aobj ocol; initAobjFromLong(&ocol, oval, ctype);
    aobj apk;  initAobjFromLong(&apk,  pk,   pktyp);
    bt    *ibtr  = getIBtr(imatch);
    if (validatePKandOldVal(lua, ibtr, &apk, &ocol)) return 1;
    bool   gost  = 0;
    iRem(ibtr, &ocol, &apk, NULL, imatch, gost);
    lua_pushboolean(lua, (int)1); return 1;
}

// DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
sds getMCIlist(list *clist, int tmatch) { // NOTE: used in DESC & AOF
    listNode *ln;
    sds       mci = sdsnewlen("(", 1);                   /* DESTORY ME 051 */
    int       i   = 0;
    listIter *li  = listGetIterator(clist, AL_START_HEAD);
    while((ln = listNext(li))) {
       if (i) mci = sdscatlen(mci, ", ", 2);
       icol_t *ic = ln->value;
       int cmatch = ic->cmatch; 
       mci        = sdscatprintf(mci, "%s", Tbl[tmatch].col[cmatch].name);
       i++;
    } listReleaseIterator(li);
    mci = sdscatlen(mci, ")", 1);
    return mci;
}

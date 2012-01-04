/*
 *
 * Creation of different btree types and
 * Public Btree Operations w/ stream abstractions under the covers

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
#include <assert.h>
#include <strings.h>

#include "redis.h" /* defines REDIS_BTREE */

#include "btree.h"
#include "bt.h"
#include "bt_iterator.h"
#include "index.h"
#include "query.h"
#include "stream.h"
#include "common.h"

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

//TODO move to create_xbt.c
#define CREATE_OBT(kt, ks, bf, cmp)                                         \
    bts_t bts;           bts.ktype = kt;                 bts.btype = btype; \
    bts.ksize = ks;      bts.bflag = bf;                 bts.num   = num;   \
    return bt_create(cmp, TRANS_ONE, &bts, 0);

bt *createUUBT(int num, uchar btype) {                //printf("createUUBT\n");
    CREATE_OBT(COL_TYPE_INT, UU_SIZE, BTFLAG_UINT_UINT, uuCmp);
}
bt *createULBT(int num, uchar btype) {                //printf("createULBT\n");
    CREATE_OBT(COL_TYPE_INT, UL_SIZE, BTFLAG_UINT_ULONG, ulCmp);
}
bt *createLUBT(int num, uchar btype) {                //printf("createLUBT\n");
    CREATE_OBT(COL_TYPE_LONG, LU_SIZE, BTFLAG_ULONG_UINT, luCmp);
}
bt *createLLBT(int num, uchar btype) {                //printf("createLLBT\n");
    CREATE_OBT(COL_TYPE_LONG, LL_SIZE, BTFLAG_ULONG_ULONG, llCmp);
}
bt *createUXBT(int num, uchar btype) {                //printf("createUXBT\n");
    CREATE_OBT(COL_TYPE_INT, UX_SIZE, BTFLAG_UINT_U128, uxCmp);
}
bt *createXUBT(int num, uchar btype) {                //printf("createXUBT\n");
    CREATE_OBT(COL_TYPE_U128, XU_SIZE, BTFLAG_U128_UINT, xuCmp);
}
bt *createLXBT(int num, uchar btype) {                //printf("createLXBT\n");
    CREATE_OBT(COL_TYPE_LONG, LX_SIZE, BTFLAG_ULONG_U128, lxCmp);
}
bt *createXLBT(int num, uchar btype) {                //printf("createXLBT\n");
    CREATE_OBT(COL_TYPE_U128, XL_SIZE, BTFLAG_U128_ULONG, xlCmp);
}
bt *createXXBT(int num, uchar btype) {                //printf("createXXBT\n");
    CREATE_OBT(COL_TYPE_U128, XX_SIZE, BTFLAG_U128_U128, xxCmp);
}
static bt *createOBT(uchar ktype, uchar vtype, int tmatch, uchar btype) {
    if        (C_IS_I(ktype)) {
        if (C_IS_I(vtype)) return createUUBT(tmatch, btype);
        if (C_IS_L(vtype)) return createULBT(tmatch, btype);
        if (C_IS_X(vtype)) return createUXBT(tmatch, btype);
    } else if (C_IS_L(ktype)) {
        if (C_IS_I(vtype)) return createLUBT(tmatch, btype);
        if (C_IS_L(vtype)) return createLLBT(tmatch, btype);
        if (C_IS_X(vtype)) return createLXBT(tmatch, btype);
    } else if (C_IS_X(ktype)) {
        if (C_IS_I(vtype)) return createXUBT(tmatch, btype);
        if (C_IS_L(vtype)) return createXLBT(tmatch, btype);
        if (C_IS_X(vtype)) return createXXBT(tmatch, btype);
    }
    return NULL;
}
#define ASSIGN_CMP(ktype) C_IS_I(ktype) ? btIntCmp   : \
                          C_IS_L(ktype) ? btLongCmp  : \
                          C_IS_X(ktype) ? btU128Cmp  : \
                          C_IS_F(ktype) ? btFloatCmp : \
                          /* TEXT */      btTextCmp

bt *createDBT(uchar ktype, int tmatch) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->col_count == 2) {
        bt *obtr = createOBT(ktype, rt->col[1].type, tmatch, BTREE_TABLE);
        if (obtr) return obtr;
    }
    bts_t bts;
    bts.ktype = ktype;       bts.btype = BTREE_TABLE; bts.ksize = VOIDSIZE;
    bts.bflag = BTFLAG_NONE; bts.num   = tmatch; 
    return bt_create(ASSIGN_CMP(ktype), TRANS_ONE, &bts, rt->dirty);
}
bt *createIBT(uchar ktype, int imatch, uchar btype) {
    bt_cmp_t cmp; bts_t bts;
    bts.ktype = ktype; bts.btype = btype; bts.num = imatch;
    if        C_IS_I(ktype) { /* NOTE: under the covers: UL */
        bts.ksize = UL_SIZE; cmp = ulCmp;
        bts.bflag = BTFLAG_UINT_ULONG  + BTFLAG_UINT_INDEX;
    } else if C_IS_L(ktype) { /* NOTE: under the covers: LL */
        bts.ksize = LL_SIZE; cmp = llCmp;
        bts.bflag = BTFLAG_ULONG_ULONG + BTFLAG_ULONG_INDEX;
    } else if C_IS_X(ktype) { /* NOTE: under the covers: XL */
        bts.ksize = XL_SIZE; cmp = xlCmp;
        bts.bflag = BTFLAG_U128_ULONG  + BTFLAG_U128_INDEX;
    } else {                  /* STRING or FLOAT */
        bts.ksize = VOIDSIZE; cmp = ASSIGN_CMP(ktype);
        bts.bflag = BTFLAG_NONE;
    }
    return bt_create(cmp, TRANS_ONE, &bts, 0);
}
static bt *_createUIBT(uchar ktype, int imatch, uchar pktyp, uchar bflag) {
    if (C_IS_I(ktype)) {
        return  C_IS_I(pktyp) ? createUUBT(imatch, bflag) :
               (C_IS_L(pktyp) ? createULBT(imatch, bflag) :
             /* C_IS_X */       createUXBT(imatch, bflag));
    } else if (C_IS_L(ktype)) {
        return  C_IS_I(pktyp) ? createLUBT(imatch, bflag) :
               (C_IS_L(pktyp) ? createLLBT(imatch, bflag) :
             /* C_IS_X */       createLXBT(imatch, bflag));
    } else if (C_IS_X(ktype)) {
        return  C_IS_I(pktyp) ? createXUBT(imatch, bflag) :
               (C_IS_L(pktyp) ? createXLBT(imatch, bflag) :
             /* C_IS_X */       createXXBT(imatch, bflag));
    } else { assert(!"_createUIBT ERROR"); return NULL; }
}
bt *createU_S_IBT(uchar ktype, int imatch, uchar pktyp) { // UNIQ SIMPLE
    return _createUIBT(ktype, imatch, pktyp, BT_SIMP_UNIQ);
}
bt *createU_MCI_IBT(uchar ktype, int imatch, uchar pktyp) {  // UNIQ MCI
    return _createUIBT(ktype, imatch, pktyp, BT_MCI_UNIQ);
}

bt *createMCI_MIDBT(uchar ktype, int imatch) {
    return createIBT(ktype, imatch, BTREE_MCI_MID);
}
bt *createMCIndexBT(list *clist, int imatch) {
    listNode *ln    = listFirst(clist);
    r_tbl_t  *rt    = &Tbl[Index[imatch].table];
    uchar     ktype = rt->col[(int)(long)ln->value].type;
    return createIBT(ktype, imatch, BTREE_MCI);
}
bt *createIndexNode(uchar ktype, uchar obctype) {                /* INODE_BT */
    if (obctype != COL_TYPE_NONE) {
        bt *btr       =  createOBT(obctype, ktype, -1, BTREE_INODE);
        btr->s.bflag |= BTFLAG_OBC; //NOTE: will fail INODE(btr)
        return btr;
    }
    bt_cmp_t cmp; bts_t bts;
    bts.ktype = ktype; bts.btype = BTREE_INODE; bts.num   = -1;
    if        (C_IS_I(ktype)) {
        cmp = uintCmp;           bts.ksize = UINTSIZE;  bts.bflag = BTFLAG_NONE;
    } else if (C_IS_L(ktype)) {
        cmp = ulongCmp;          bts.ksize = ULONGSIZE; bts.bflag = BTFLAG_NONE;
    } else if (C_IS_X(ktype)) {
        cmp = u128Cmp;           bts.ksize = U128SIZE;  bts.bflag = BTFLAG_NONE;
    } else {
        cmp = ASSIGN_CMP(ktype); bts.ksize = VOIDSIZE;  bts.bflag = BTFLAG_NONE;
    }
    return bt_create(cmp, TRANS_ONE, &bts, 0);
}
bt *createIndexBT(uchar ktype, int imatch) {
    return createIBT(ktype, imatch, BTREE_INDEX);
}

//TODO move to abt.c
/* ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE */
#define DEBUG_REP_NSIZE         printf("osize: %d nsize: %d\n", osize, nsize);
#define DEBUG_REP_SAMESIZE      printf("abt_replace: SAME_SIZE\n");
#define DEBUG_REP_REALLOC       printf("o2: %p o: %p\n", o2stream, *ostream);
#define DEBUG_REP_REALLOC_OVRWR printf("abt_replace: REALLOC OVERWRITE\n");
#define DEBUG_REP_RELOC_NEW     printf("abt_replace: REALLOC -> new \n");

static int abt_replace(bt *btr, aobj *akey, void *val) {
    uint32 ssize; DECLARE_BT_KEY(akey, 0)
    uchar  *nstream = createStream(btr, val, btkey, ksize, &ssize);
    if NORM_BT(btr) {
        uchar  **ostream = (uchar **)bt_find_loc(btr, btkey);
        destroyBTKey(btkey, med);                        /* FREED 026 */
        uint32   osize   = getStreamMallocSize(btr, *ostream);
        uint32   nsize   = getStreamMallocSize(btr,  nstream); //DEBUG_REP_NSIZE
        if (osize == nsize) { /* if ROW size doesnt change, just overwrite */
            memcpy(*ostream, nstream, osize);             //DEBUG_REP_SAMESIZE
            destroyStream(btr, nstream);
        } else {              /* try to realloc (may re-use memory -> WIN) */
            bt_decr_dsize(btr, osize); bt_incr_dsize(btr, nsize);
            void *o2stream = realloc(*ostream, nsize);      //DEBUG_REP_REALLOC
            if (o2stream == *ostream) { /* realloc overwrite -> WIN */
                memcpy(*ostream, nstream, nsize);     //DEBUG_REP_REALLOC_OVRWR
                destroyStream(btr, nstream);
            } else {          /* new pointer, copy nstream, replace in BT */
                *ostream = nstream;/* REPLACE ptr in BT */ //DEBUG_REP_RELOC_NEW
            }
        }
    } else {
        uchar *dstream = bt_replace(btr, btkey, nstream);
        destroyBTKey(btkey, med);                        /* FREED 026 */
        destroyStream(btr, dstream);
    }
    return ssize;
}
static void *abt_find(bt *btr, aobj *akey) {
    DECLARE_BT_KEY(akey, 0)
    uchar *stream = bt_find(btr, btkey, akey);
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return parseStream(stream, btr);
}
static bool abt_exist(bt *btr, aobj *akey) { //NOTE: Evicted Indexes are NULL
    DECLARE_BT_KEY(akey, 0)
    bool ret = bt_exist(btr, btkey, akey);
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return ret;
}
static dwm_t abt_find_d(bt *btr, aobj *akey) { //NOTE: use for dirty tables
    dwm_t dwme; bzero(&dwme, sizeof(dwm_t));
    DECLARE_BT_KEY(akey, dwme)
    dwm_t  dwm = findnodekey(btr, btr->root, btkey, akey);
    destroyBTKey(btkey, med);                            // FREED 026
    dwm.k      = parseStream(dwm.k, btr);
    return dwm;
}
static bool abt_del(bt *btr, aobj *akey) { // DELETE the row
    DECLARE_BT_KEY(akey, 0)
    dwd_t  dwd    = bt_delete(btr, btkey);               /* FREED 028 */
    uchar *stream = dwd.k;
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return destroyStream(btr, stream);                   /* DESTROYED 027 */
}
static bool abt_evict(bt *btr, aobj *akey) {
    DECLARE_BT_KEY(akey, 0)
    uchar * stream = bt_evict(btr, btkey);
    destroyBTKey(btkey, med);                            // FREED 026
    return destroyStream(btr, stream);                   // DESTROYED 027
}
static uint32 abt_get_dr(bt *btr, aobj *akey) {
    DECLARE_BT_KEY(akey, 0)
    uint32 dr = bt_get_dr(btr, btkey, akey); destroyBTKey(btkey, med);
    return dr;
}
static uint32 abt_insert(bt *btr, aobj *akey, void *val) {
#ifndef TEST_WITH_TRANS_ONE_ONLY
    if (btr->numkeys == TRANS_ONE_MAX) btr = abt_resize(btr, TRANS_TWO);
#endif
    uint32 ssize; DECLARE_BT_KEY(akey, 0)
    char   *stream = createStream(btr, val, btkey, ksize, &ssize); /*DEST 027*/
    destroyBTKey(btkey, med);                            /* FREED 026 */
    bt_insert(btr, stream, 0);                           /* FREE ME 028 */
    return ssize;
}
bt *abt_resize(bt *obtr, uchar trans) {              // printf("abt_resize\n");
    bts_t bts;
    memcpy(&bts, &obtr->s, sizeof(bts_t)); /* copy flags */
    bt *nbtr    = bt_create(obtr->cmp, trans, &bts, obtr->dirty);
    nbtr->dsize = obtr->dsize;
    if (obtr->root) {
        bt_to_bt_insert(nbtr, obtr, obtr->root); /* 1.) copy from old to new */
        bt_release(obtr, obtr->root);            /* 2.) release old */
        memcpy(obtr, nbtr, sizeof(bt));          /* 3.) overwrite old w/ new */
        free(nbtr);                              /* 4.) free new */
    } //bt_dump_info(obtr, obtr->ktype);
    return obtr;
}

// PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API
/* DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA */
int   btAdd    (bt *btr, aobj *apk, void *val) {
                                      return abt_insert (btr, apk, val); }
int   btReplace(bt *btr, aobj *apk, void *val) {
                                      return abt_replace(btr, apk, val); }
void *btFind   (bt *btr, aobj *apk) { return abt_find   (btr, apk); }
int   btDelete (bt *btr, aobj *apk) { return abt_del    (btr, apk); }

// DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY
dwm_t btFindD  (bt *btr, aobj *apk) { return abt_find_d(btr, apk); }
//NOTE: btFindD() must precede btEvict()
bool  btEvict  (bt *btr, aobj *apk) { return abt_evict (btr, apk); }

/* INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX */
void  btIndAdd   (bt *ibtr, aobj *ikey, bt *nbtr) {
                                                 abt_insert (ibtr, ikey, nbtr);}
bt   *btIndFind  (bt *ibtr, aobj *ikey) { return abt_find   (ibtr, ikey);      }
bool  btIndExist (bt *ibtr, aobj *ikey) { return abt_exist  (ibtr, ikey);      }
int   btIndDelete(bt *ibtr, aobj *ikey) {        abt_del    (ibtr, ikey); 
                                          return ibtr->numkeys;                }
int   btIndNull  (bt *ibtr, aobj *ikey) {        abt_replace(ibtr, ikey, NULL);
                                          return ibtr->numkeys;                }

/* INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE */
#define DEBUG_INODE_ADD                                                   \
    printf("btIndNodeAdd: apk : "); dumpAobj(printf, apk);                \
    if (ocol) { printf("btIndNodeAdd: ocol: "); dumpAobj(printf, ocol); }
#define DEBUG_INODE_EVICT                                                 \
    printf("btIndNodeEvict: nkeys: %d apk: %p: ",                         \
            nbtr->numkeys, (void *)apk); dumpAobj(printf, apk);

void *btIndNodeFind(bt *nbtr, aobj *apk) { return abt_find(nbtr, apk); }
bool  btIndNodeAdd (cli *c, bt *nbtr, aobj *apk, aobj *ocol) { DEBUG_INODE_ADD
    if (ocol) {
        if (abt_find(nbtr, ocol)) {
            if (c) addReply(c, shared.obindexviol); return 0;
        }
        iAddUniq(nbtr, apk->type, apk, ocol);
    } else abt_insert(nbtr, apk, NULL);
    return 1;
}
int  btIndNodeDelete(bt *nbtr, aobj *apk, aobj *ocol) {
    abt_del  (nbtr, ocol ? ocol : apk); return nbtr->numkeys;
}
int  btIndNodeEvict(bt *nbtr, aobj *apk, aobj *ocol) {       DEBUG_INODE_EVICT
    abt_evict(nbtr, ocol ? ocol : apk); return nbtr->numkeys;
}

// HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER
uint32  btGetDR  (bt *btr, aobj *akey) { return abt_get_dr(btr, akey); }

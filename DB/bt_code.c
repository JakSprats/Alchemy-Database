/*
 * Copyright 1997-1999, 2001 John-Mark Gurney.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.

AGPL License

Copyright (c) 2010-2011 Russell Sullivan <jaksprats AT gmail DOT com>
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

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "zmalloc.h"
#include "adlist.h"

#include "btree.h"
#include "btreepriv.h"
#include "bt.h"
#include "bt_iterator.h"
#include "stream.h"
#include "query.h"
#include "redis.h"
#include "common.h"

// GLOBALS
extern r_ind_t *Index;

/* PROTOYPES */
static bt_data_t findminkey (bt *btr, bt_n *x);
static bt_data_t findmaxkey (bt *btr, bt_n *x);
static int       real_log2(unsigned int a, int nbits);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
//#define BT_MEM_PROFILE
#ifdef BT_MEM_PROFILE
static ulong tot_bt_data     = 0; static ulong tot_bt_data_mem = 0;
static ulong tot_num_bt_ns   = 0; static ulong tnbtnmem        = 0;
static ulong tot_num_bts     = 0; static ulong tot_num_bt_mem  = 0;
void dump_bt_mem_profile(bt *btr) {
    printf("BT: num: %d\n", btr->s.num);
    printf("tot_bt_data:     %lu\n", tot_bt_data);
    printf("tot_bt_data_mem: %lu\n", tot_bt_data_mem);
    printf("tot_num_bts:     %lu\n", tot_num_bts);
    printf("tot_bt_mem:      %lu\n", tot_num_bt_mem);
    printf("tot_num_btn:     %lu\n", tot_num_bt_ns);
    printf("tbtn_mem:        %lu\n", tnbtnmem);
    fflush(NULL);
}
  #define BT_MEM_PROFILE_BT   {tot_num_bts++; tot_num_bt_mem += size;}
  #define BT_MEM_PROFILE_MLC  {tot_bt_data++; tot_bt_data_mem += size;}
  #define BT_MEM_PROFILE_NODE {tot_num_bt_ns++; tnbtnmem += size;}
#else
void dump_bt_mem_profile(bt *btr) { btr = NULL; return; }
  #define BT_MEM_PROFILE_BT
  #define BT_MEM_PROFILE_MLC
  #define BT_MEM_PROFILE_NODE
#endif
#ifdef BTREE_DEBUG
  unsigned long BtreeNodeNum = 0;
  #define BT_ADD_NODE_NUM btn->num = BtreeNodeNum++;
#endif

// HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER HELPER
static ulong getNumKey(bt *btr, bt_n *x, int i) {
    if (i < 0 || i >= x->n) return 0;
    else {
        aobj  akey; void *be = KEYS(btr, x, i);
        convertStream2Key(be, &akey, btr);
        return  C_IS_I(btr->s.ktype) ? akey.i : akey.l;
    }
}
bool isGhostRow(bt *btr, bt_n *x, int i) {
    aobj akey;
    uchar *stream = KEYS(btr, x, i); convertStream2Key(stream, &akey, btr);
    void  *rrow   = parseStream(stream, btr);
    return rrow && !(*((uchar *)(rrow)));
}

// MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT
#define DEBUG_INCR_MEM \
    printf("INCR MEM: osize: %ld plus: %lu nsize: %ld\n", btr->msize, size, (btr->msize + size));
#define DEBUG_DECR_MEM \
    printf("DECR MEM: osize: %ld minus: %lu nsize: %ld\n", btr->msize, size, (btr->msize - size));
#define DEBUG_BT_MALLOC \
    printf("bt_MALLOC: %p size: %d\n", btr, size);
#define DEBUG_ALLOC_BTN \
    printf("allocbtreeNODE: %p leaf: %d size: %d\n", btr, leaf, size);
#define DEBUG_ALLOC_BTREE \
    printf("allocBTREE: %p size: %d\n", btr, size);
#define DEBUG_BT_FREE \
    printf("bt_FREE: %p size: %d\n", btr, size);
#define DEBUG_FREE_BTN \
    printf("bt_free_btreeNODE: %p leaf: %d size: %lu\n", btr, x->leaf, size);
#define DEBUG_ALLOC_DS \
  printf("alloc_ds: leaf: %d n: %d t: %d\n", x->leaf, btr->t * 2, btr->t); \
  printf("alloc_ds: x: %p dsp: %p ds: %p dssize: %u\n", x, dsp, ds, dssize);

/* NOTE used-memory bookkeeping maintained at the Btree level */
static void bt_increment_used_memory(bt *btr, size_t size) {  //DEBUG_INCR_MEM
    btr->msize += (ull)size;
    increment_used_memory(size);
}
void bt_incr_dsize(bt *btr, size_t size) {
    bt_increment_used_memory(btr, size);
    btr->dsize += size;
}
static void bt_decrement_used_memory(bt *btr, size_t size) {  //DEBUG_DECR_MEM
    btr->msize -= (ull)size;
    decrement_used_memory(size);
}
void bt_decr_dsize(bt *btr, size_t size) {
    bt_decrement_used_memory(btr, size);
    btr->dsize -= size;
}
void *bt_malloc(bt *btr, int size) {                         //DEBUG_BT_MALLOC
    BT_MEM_PROFILE_MLC
    bt_incr_dsize(btr, size);
    return malloc(size);
}
static void alloc_ds(bt *btr, bt_n *x, size_t size) {
    void   **dsp    = (void *)((char *)x + size); bzero(dsp, sizeof(bds_t));
    size_t   dssize = (x->leaf ? (btr->t * 2) : btr->t) * sizeof(uint32);
    void    *ds     = malloc(dssize); bzero(ds, dssize); // FREEME 108
    *dsp            = ds;
    DEBUG_ALLOC_DS
}
static bt_n *allocbtreenode(bt *btr, bool leaf, bool dirty) {
    btr->numnodes++;
    GET_BTN_SIZES(leaf, dirty)                                //DEBUG_ALLOC_BTN
    BT_MEM_PROFILE_NODE
    bt_n   *x     = malloc(msize);
    bt_increment_used_memory(btr, msize); bzero(x, msize);
    x->leaf       = 1;
    x->dirty      = dirty ? 1 : 0; //BT_ADD_NODE_NUM
    if (dirty) alloc_ds(btr, x, size);
    return x;
}
static bt *allocbtree() {
    int  size = sizeof(struct btree);
    BT_MEM_PROFILE_BT
    bt  *btr  = (bt *)malloc(size);                      /* FREE ME 035 */
    bzero(btr, size);                                      //DEBUG_ALLOC_BTREE
    bt_increment_used_memory(btr, size);
    return btr;
}
void bt_free(bt *btr, void *v, int size) {                     //DEBUG_BT_FREE
    bt_decr_dsize(btr, size); free(v);
}
void bt_free_btreenode(bt *btr, bt_n *x) {
    GET_BTN_SIZES(x->leaf, x->dirty)
    bt_decrement_used_memory(btr, msize);
    if (x->dirty) free(GET_DS(x, size));                 // FREED 108
    free(x);                                             /* FREED 035 */
}
void bt_free_btree(bt *btr) { free(btr); }

bt *bt_create(bt_cmp_t cmp, uchar trans, bts_t *s) {
    int    n        = (trans == TRANS_ONE) ? 7 : 255;
    uchar  t        = (uchar)((int)(n + 1) / 2);
    int    kbyte    = sizeof(bt_n) + n * s->ksize;
    int    nbyte    = kbyte + (n + 1) * VOIDSIZE;
    bt    *btr      = allocbtree();
    memcpy(&btr->s, s, sizeof(bts_t)); /* ktype, btype, ksize, bflag, num */
    btr->cmp        = cmp;
    btr->keyofst    = sizeof(bt_n);
    uint32 nodeofst = btr->keyofst + n * s->ksize;
    btr->nodeofst   = (ushort16)nodeofst;
    btr->t          = t;
    int nbits       = real_log2(n, sizeof(int) * 8) + 1;
    nbits           = 1 << (real_log2(nbits, sizeof(int) * 8) + 1);
    btr->nbits      = (uchar)nbits;
    btr->nbyte      = nbyte;
    btr->kbyte      = kbyte;
    btr->root       = allocbtreenode(btr, 1, 0);
    btr->numnodes   = 1;
    //printf("bt_create\n"); bt_dump_info(printf, btr);
    return btr;
}

// BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH BINARY_SEARCH
/* This is the real log2 function.  It is only called when we don't have
 * a value in the table. -> which is basically never */
static inline int real_log2(unsigned int a, int nbits) {
    uint32 i = 0;
    uint32 b = (nbits + 1) / 2; /* divide in half rounding up */
    while (b) {
        i = (i << 1);
        if (a >= (unsigned int)(1 << b)) { // select top half and mark this bit
            a /= (1 << b);
            i = i | 1;
        } else {                           // select bottom half & dont set bit 
            a &= (1 << b) - 1;
        }
        b /= 2;
    }
    return i;
}

/* Implement a lookup table for the log values.  This will only allocate
 * memory that we need.  This is much faster than calling the log2 routine
 * every time.  Doing 1 million insert, searches, and deletes will generate
 * ~58 million calls to log2.  Using a lookup table IS NECESSARY!
 -> memory usage of this is trivial, like less than 1KB */
static inline int _log2(unsigned int a, int nbits) {
    static char   *table   = NULL;
    static uint32  alloced = 0;
    uint32 i;
    if (a >= alloced) {
        table = realloc(table, (a + 1) * sizeof *table);
        for (i = alloced; i < a + 1; i++) table[i] = -1;
        alloced = a + 1;
    }
    if (table[a] == -1) table[a] = real_log2(a, nbits);
    return table[a];
}

#define DEBUG_ADD_TO_CIPOS \
  if (x->leaf) printf("LEAF: i: %d CurrPos: %d\n", i, Index[btr->s.num].cipos);\
  else printf("NODE: i: %d CurrPos: %d\n", i, Index[btr->s.num].cipos);

static inline void add_to_cipos(bt *btr, bt_n *x, int i) {
    if (i == -1) return;
    if (!x->leaf) {
        for (int j = 0; j <= i; j++) {
            bt_n *xp = NODES(btr, x)[j]; Index[btr->s.num].cipos += xp->scion;
        }
    }
    Index[btr->s.num].cipos += i;                         //DEBUG_ADD_TO_CIPOS
}

static int findkindex(bt *btr, bt_n *x, bt_data_t k, int *r, btIterator *iter) {
    if (x->n == 0) return -1;
    int b, tr;
    int *rr = r ? r : &tr ; /* rr: key is greater than current entry */
    int  i  = 0;
    int  a  = x->n - 1;
    while (a > 0) {
        b            = _log2(a, (int)btr->nbits);
        int slot     = (1 << b) + i;
        bt_data_t k2 = KEYS(btr, x, slot);
        if ((*rr = btr->cmp(k, k2)) < 0) {
            a        = (1 << b) - 1;
        } else {
            a       -= (1 << b);
            i       |= (1 << b);
        }
    }
    if ((*rr = btr->cmp(k, KEYS(btr, x, i))) < 0)  i--;
    if (SIMP_UNIQ(btr) && Index[btr->s.num].iposon) add_to_cipos(btr, x, i);
    if (iter) {
        iter->bln->in = (i > 0) ? i : 0;
        iter->bln->ik = (i > 0) ? i : 0;
    }
    return i;
}

// KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING KEY_SHUFFLING
#define DEBUG_KEY_OTHER                                                        \
  if UU(btr) { uint32 key = (long)v / UINT_MAX;                                \
               uint32 val = (long)v % UINT_MAX;                                \
               printf("\t\tUU: v: %d KEY: %lu VAL: %lu\n", v, key, val); }     \
  if LU(btr) { luk *lu = (luk *)v; printf("\t\tLU: KEY: %lu VAL: %lu\n",       \
                                           lu->key, lu->val); }                \
  else if UL(btr) { ulk *ul = (ulk *)v; printf("\t\tUL: KEY: %u  VAL: %lu\n",  \
                                               ul->key, ul->val); }            \
  else if LL(btr) { llk *ll = (llk *)v; printf("\t\tLL: KEY: %lu VAL: %lu\n",  \
                                               ll->key, ll->val); }            \
  else if UX(btr) { uxk *ux = (uxk *)v; printf("\t\tUX: KEY: %u ", ux->key);   \
                                        printf(" VAL: ");                      \
                                   DEBUG_U128(printf, ux->val); printf("\n"); }\
  else if XU(btr) { xuk *xu = (xuk *)v; printf("\t\tXU: KEY: ");               \
                                   DEBUG_U128(printf, xu->key);                \
                                        printf(" VAL: %u\n", xu->val); }       \
  else if LX(btr) { lxk *lx = (lxk *)v; printf("\t\tLX: KEY: %llu ", lx->key); \
                                        printf(" VAL: ");                      \
                                   DEBUG_U128(printf, lx->val); printf("\n"); }\
  else if XL(btr) { xlk *xl = (xlk *)v; printf("\t\tXL: KEY: ");               \
                                   DEBUG_U128(printf, xl->key);                \
                                   printf(" VAL: %lu\n", xl->val); }           \
  else if XX(btr) { xxk *xx = (xxk *)v; printf("\t\tXX: KEY: ");               \
                                   DEBUG_U128(printf, xx->key);                \
                                   printf(" VAL: ");                           \
                                   DEBUG_U128(printf, xx->val); printf("\n"); }\
  if ISVOID(btr) printf("\t\tVOID: p: %p lu: %lu\n", v, v);                    \
  if INODE_X(btr) {                                                            \
      uint128 *pbu = v; printf("\t\tINODE_X: ");                               \
      DEBUG_U128(printf, *pbu); printf("\n"); }

#define DEBUG_AKEYS                                                            \
  printf("AKEYS: i: %d ofst: %d v: %p uint: %d uu: %d lu: %d ul: %d ll: %d\n", \
          i, ofst, v, ISUINT(btr), UU(btr), LU(btr), UL(btr), LL(btr));        \
  DEBUG_KEY_OTHER
#define DEBUG_KEYS                                                        \
  printf("KEYS: uint: %d void: %d i: %d\n", ISUINT(btr), ISVOID(btr), i);
#define DEBUG_SETBTKEY_OBT \
  if (p) printf("setBTKey: memcpy to v: %p\n", v);

// NOTE: KEYS can be [4,8,12,16 bytes], so logic is needed
#define ISVOID(btr)  (btr->s.ksize == VOIDSIZE)
#define ISUINT(btr)  (btr->s.ksize == UINTSIZE)

static inline void **AKEYS(bt *btr, bt_n *x, int i) {
    int   ofst = (i * btr->s.ksize);
    char *v    = (char *)x + btr->keyofst + ofst;                 //DEBUG_AKEYS
    return (void **)v;
}
#define OKEYS(btr, x) ((void **)((char *)x + btr->keyofst))
inline void *KEYS(bt *btr, bt_n *x, int i) {                       //DEBUG_KEYS
    if      ISVOID(btr) return                  OKEYS(btr, x)[i];
    else if ISUINT(btr) return VOIDINT (*(int *)AKEYS(btr, x, i));
    else /* OTHER_BT */ return (void *)         AKEYS(btr, x, i);
}

// DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY DIRTY
typedef struct btn_pos {
    bt_n *x;
    int   i;
} bp_t;
static inline void free_bp(void *v) { free(v); }
typedef struct two_bp_gens {
    bp_t p; // parent
    bp_t c; // child
} tbg_t;

#define DEBUG_INCR_PREV                                       \
  printf("tbg.p.x: %p tbg.p.i: %d tbg.c.x: %p tbg.c.i: %d\n", \
          tbg.p.x, tbg.p.i, tbg.c.x, tbg.c.i);
#define DEBUG_ADD_DS_TO_BTN \
  printf("MMMMMMMMMMMM: addDStoBTN: to x: %p returning y: %p - p: %p pi: %d\n",\
          x, y, p, pi);
#define DEBUG_GET_DR \
  printf("getDR: x: %p i: %d ds: %p -> dr: %u\n", x, i, ds, dr);
#define DEBUG_ZERO_DR \
  printf("zeroDR: dirty: %d x: %p i: %d p: %p, pi: %d key: ", \
         x->dirty, x, i, p, pi); printKey(btr, x, i);
#define DEBUG_SET_DR_1 \
  printf("============setDR: dirty: %d x: %p i: %d dr: %u p: %p, pi: %d key: ",\
         x->dirty, x, i, dr, p, pi); printKey(btr, x, i);
#define DEBUG_SET_DR_2 \
  printf("ds: %p i: %d dr: %d ds[i]: %d\n", ds, i, dr, ds[i]);
#define DEBUG_INCR_DR_1 \
  printf("++++++++++++incrDR: dirty: %d x: %p i: %d dr: %u p: %p, pi: %d key: ",          x->dirty, x, i, dr, p, pi); printKey(btr, x, i);
#define DEBUG_INCR_DR_2 \
  uint32 odr = ds[i];
#define DEBUG_INCR_DR_3 \
  printf("ds: %p i: %d dr: %d ds[i]: %d odr: %d\n", ds, i, dr, ds[i], odr);

//TODO inline
static bt_n *addDStoBTN(bt *btr, bt_n *x, bt_n *p, int pi) {
    bt_n *y = allocbtreenode(btr, x->leaf, 1);
    GET_BTN_SIZE(x->leaf) memcpy(y, x, size); y->dirty = btr->dirty = 1;
    if (x == btr->root) btr->root         = y;
    else                NODES(btr, p)[pi] = y; // update parent NODE bookkeeping
    bt_free_btreenode(btr, x);                              DEBUG_ADD_DS_TO_BTN
    return y;
}
uint32 getDR(bt *btr, bt_n *x, int i) {
    if (!x->dirty) return 0;
    GET_DS_FROM_BTN(x)
    uint32 dr = ds[i];                                    //DEBUG_GET_DR
    return dr;
}
static bt_n *zeroDR(bt *btr, bt_n *x, int i, bt_n *p, int pi) {
    if (!x->dirty) return x;                                DEBUG_ZERO_DR
    GET_DS_FROM_BTN(x)
    ds[i] = 0;
    return x;
}
static bt_n *setDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (!dr) return x;                                      DEBUG_SET_DR_1
    if (!x->dirty) x = addDStoBTN(btr, x, p, pi);
    GET_DS_FROM_BTN(x)
    ds[i] = dr;                                             DEBUG_SET_DR_2
    return x;
}
static bt_n *incrDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (!dr) return x;                                      DEBUG_INCR_DR_1
    if (!x->dirty) x = addDStoBTN(btr, x, p, pi);
    GET_DS_FROM_BTN(x)                                      DEBUG_INCR_DR_2
    ds[i] += dr;                                            DEBUG_INCR_DR_3
    return x;
}
static bt_n *overwriteDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi) {
    if (dr) return setDR (btr, x, i, dr, p, pi);
    else    return zeroDR(btr, x, i,     p, pi);
}

#define DEBUG_GET_C_REC_1 \
printf("get_prev_child_recurse: x: %p i: %d xp: %p xp->leaf: %d xp->n: %d\n", \
        x, i, xp, xp->leaf, xp->n);
#define DEBUG_GET_C_REC_2 \
  printf("get_prev_child_recurse: tbg.c.i: %d\n", tbg.c.i);
#define DEBUG_INCR_PREV_DR \
  printf("incrPrevDR: x: %p i: %d dr: %d key: ", x, i, dr); printKey(btr, x, i);

static tbg_t get_prev_child_recurse(bt *btr, bt_n *x, int i) {
    bt_n *xp = NODES(btr, x)[i];                            DEBUG_GET_C_REC_1
    if (!xp->leaf) return get_prev_child_recurse(btr, xp, xp->n);
    tbg_t tbg;
    tbg.p.x = x;  tbg.p.i = i;
    tbg.c.x = xp; tbg.c.i = xp->n - 1;                      DEBUG_GET_C_REC_2
    return tbg;
}
//TODO break into incrPrevDR_[1,2B,2C]
static bt_n *incrPrevDR(bt *btr, bt_n *x, int i, uint32 dr, bt_n *p, int pi,
                        list *plist, bool isb) {
    if (!dr) return x;                                      DEBUG_INCR_PREV_DR
    if (x->leaf) { //printf("incrPrevDR: leaf\n");
        if (i > 0) return incrDR(btr, x, i - 1, dr, p,  pi);  // prev sibling
        else   {
            if (x == findminnode(btr, btr->root)) { // MIN KEY
                btr->dirty_left += dr; btr->dirty = 1; return x;
            }
            listNode *ln; bt_n *rx = btr->root; int ri = 0;
            listIter *li = listGetIterator(plist, AL_START_HEAD);
            while((ln = listNext(li))) { // walk recursion backwards
                bp_t *bp = ln->value;
                //printf("LLLLLLLLL: x: %p i: %d\n", bp->x, bp->i);
                if (bp->i) { rx = bp->x; ri = bp->i - 1; break; }
            }
            bt_n *prx = btr->root; int pri = 0;
            if (rx != btr->root) { // get parent
                ln  = listNext(li); bp_t *bp = ln->value;
                prx = bp->x;        pri      = bp->i;
            } listReleaseIterator(li);
            //printf("rx: %p ri: %d prx: %p pri: %d\n", rx, ri, prx, pri);
            incrDR(btr, rx, ri, dr, prx, pri);
            return x; // x not modified, only rx
        }
    } else { //printf("incrPrevDR NODE i: %d isb: %d\n", i, isb);
        if (isb) { // NOTE: for CASE2_B (need recursive previous child)
            uint32 drc = getDR(btr, x, i);
            if (drc) { dr += drc; zeroDR(btr, x, i, p, pi); }
            tbg_t tbg  = get_prev_child_recurse(btr, x, i);
            DEBUG_INCR_PREV
            incrDR(btr, tbg.c.x, tbg.c.i, dr, tbg.p.x, tbg.p.i);
            return x; // x not modified, only tbg.c.x
        }
        return incrDR(btr, x, i, dr, p,  pi); //NOTE: for CASE2_A (i==prev)
    }
}

// SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY
#define DEBUG_SET_KEY \
  printf("setBTKey: ksize: %d btr: %p v: %p p: %p uint: %d void: %d uu: %d " \
         "lu: %d ul: %d ll: %d\n",                                           \
          btr->s.ksize, btr, v, p, ISUINT(btr), ISVOID(btr), UU(btr),        \
          LU(btr), UL(btr), LL(btr));                                        \
  DEBUG_KEY_OTHER
#define DEBUG_SET_BTKEY_2A \
  printf("22222AAAAAA: setBTKeyCase2_A: x: %p i: %d\n", x, i);
#define DEBUG_SET_BTKEY_2B \
  printf("22222BBBBBB: setBTKeyCase2_B: x: %p i: %d\n", x, i);
#define DEBUG_SET_BTKEY_2C \
  printf("22222CCCCCC: setBTKeyCase2_C: x: %p i: %d\n", x, i);
#define DEBUG_SET_BTKEY_INS \
  printf("IIIIIIIIIII: setBTKeyInsert\n");
#define DEBUG_SET_BTKEY \
  printf("KKKKKKKKKKK: setBTKey dx: %p di: %d sx: %p si: %d dr: %u skey: ", \
        dx, di, sx, si, dr); printKey(btr, sx, si);
#define DEBUG_MV_X_KEYS_1                                                \
  printf("VVVVVVVVVVV: drt: mvXKeys: x2x: %d dx: %p dii: %d sx: %p sii: %d " \
         "drs: %d\n", x2x, *dx, dii, *sx, sii, drs);                         \
  printf("drd: %u dkey: ", drd); printKey(btr, *dx, dii);                    \
  printf("drs: %u skey: ", drs); printKey(btr, *sx, sii);
#define DEBUG_MV_X_KEYS_2 \
  printf("ZERO DEST: dii: %d\n", dii);
#define DEBUG_TRIM_BTN \
  printf("trimBTN: x: %p n: %d\n", x, x->n);

static void setBTKeyRaw(bt *btr, bt_n *x, int i, void *src) { //PRIVATE
    void **dest = AKEYS(btr, x, i);
    if      ISVOID(btr) *dest                  = src;   
    else if ISUINT(btr) *(int *)((long *)dest) = (int)(long)src;
    else { /* OTHER_BT */                              //DEBUG_SETBTKEY_OTHERBT
        if (src) memcpy(dest, src, btr->s.ksize);
        else     bzero (dest,      btr->s.ksize);
    }                                                           //DEBUG_SET_KEY
}
static bt_n *setBTKeyCase2_A(bt   *btr, bt_n *x, int i,  dwd_t dwd,
                             bool  drt, bt_n *p, int pi, list *plist) {
    DEBUG_SET_BTKEY_2A
    if (drt) x = incrPrevDR(btr, x, i, dwd.dr, p, pi, plist, 0);
    else     x = zeroDR    (btr, x, i, p, pi);
    setBTKeyRaw(btr, x, i, dwd.k);
    return x;
}
static bt_n *setBTKeyCase2_B(bt   *btr, bt_n *x, int i,  dwd_t dwd,
                             bool  drt, bt_n *p, int pi, list *plist) {
    DEBUG_SET_BTKEY_2B
    if (drt) {
        x = incrPrevDR(btr, x, i, 1,      p, pi, plist, 1);
        x = setDR     (btr, x, i, dwd.dr, p, pi);
    }
//TODO else x = zeroDR() ???
    setBTKeyRaw(btr, x, i, dwd.k);
    return x;
}


static bt_n *setBTKeyCase2_C(bt  *btr, bt_n *x, int i, dwd_t dwd,
                             bool  drt, bt_n *p, int pi) {   DEBUG_SET_BTKEY_2C
    if (drt) x = setDR(btr, x, i, dwd.dr, p, pi);
//TODO else x = zeroDR() ???
    setBTKeyRaw(btr, x, i, dwd.k);
    return x;
}
static bt_n *setBTKeyInsert(bt   *btr, bt_n *x, int i, void *src,
                            bt_n *p,   int   pi) { //DEBUG_SET_BTKEY_INS
    x = zeroDR(btr, x, i, p, pi); setBTKeyRaw(btr, x, i, src);
    return x;
}
static bt_n *setBTKey(bt *btr,  bt_n *dx, int di,  bt_n *sx, int si,
                      bool drt, bt_n *pd, int pdi, bt_n *ps, int psi) {
    if (drt) {
        uint32 dr = getDR      (btr, sx, si);               DEBUG_SET_BTKEY
        dx        = overwriteDR(btr, dx, di, dr, pd, pdi);
        sx        = zeroDR     (btr, sx, si, ps, psi);
    }
//TODO else x = zeroDR() ???
    setBTKeyRaw(btr, dx, di, KEYS(btr, sx, si));
    return dx;
}

static void mvXKeys(bt     *btr, bt_n   **dx, int di,
                                 bt_n   **sx, int si,  uint32 num, uint32 ks,
                    bool    drt, bt_n    *pd, int pdi,
                                 bt_n    *ps, int psi) {
    if (!num) return;
    bool x2x     = (*dx == *sx);
    bool forward = (di >= si);
    int i        = forward ? (int)num - 1:      0;
    int end      = forward ?      -1     : (int)num;
    while (i != end) { // DS remove destDR from dx @i, add srcDR to sx @i
        int    sii = si + i;  int dii = di + i;
        uint32 drs = getDR(btr, *sx, sii), drd = getDR(btr, *dx, dii);
        if (drs) {                                      DEBUG_MV_X_KEYS_1
            *dx = setDR (btr, *dx, dii, drs, pd, pdi);
            if (x2x && *dx != *sx) *sx = *dx;
            *sx = zeroDR(btr, *sx, sii, pd, pdi);
            if (x2x && *dx != *sx) *dx = *sx;
        } else if (drd) {                               DEBUG_MV_X_KEYS_2
            *dx = zeroDR(btr, *dx, dii, pd, pdi);
        }
        bt_data_t *dest = AKEYS(btr, *dx, di);
        bt_data_t *src  = AKEYS(btr, *sx, si);
        void      *dk   = (char *)dest + (i * ks);
        void      *sk   = (char *)src  + (i * ks);
        memcpy(dk, sk, ks);
        if (forward) i--; else i++;
    }
}

// NOTE: trimBTN*() do not ever dirty btn's [x2x & ]
static bt_n *trimBTN(bt *btr, bt_n *x, bool drt, bt_n *p, int pi) {
    DEBUG_TRIM_BTN
    if (drt) x = zeroDR(btr, x, x->n, p, pi);
    x->n--; return x;
}
static bt_n *trimBTN_n(bt *btr, bt_n *x, int n, bool drt, bt_n *p, int pi) {
    if (drt) {
        for (int i = x->n; i >= (x->n - n); i--) {
            x = zeroDR(btr, x, i, p, pi);
        }}
    x->n -= n; return x;
}

// SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION SCION
static inline void incr_scion(bt_n *x, int n) { x->scion += n; }
static inline void decr_scion(bt_n *x, int n) { x->scion -= n; }
static inline void move_scion(bt *btr, bt_n *z, bt_n *y, int n) {
    for (int i = 0; i < n; i++) { incr_scion(y, NODES(btr, z)[i]->scion); }
}

// INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
static bool btreesplitchild(bt *btr, bt_n *x, int i, bt_n *y, bt_n *p, int pi) {
    ushort16  t = btr->t;
    bt_n     *z = allocbtreenode(btr, y->leaf, y->dirty); //TODO dirtymath
    z->leaf     = y->leaf; /* duplicate leaf setting */
    for (int j = 0; j < t - 1; j++) {
        z = setBTKey(btr, z, j, y, j + t, 1, p, pi, p, pi);
    }
    y    = trimBTN_n(btr, y, t, 1, p, pi);
                                    decr_scion(y, t);    // half num_nodes in Y 
    z->n = t - 1;                   incr_scion(z, t - 1);// half num_nodes in Z 
    if (!y->leaf) { /* if it's an internal node, copy the ptr's too */
        for (int j = 0; j < t; j++) {
            uint32_t scion   = NODES(btr, y)[j + t]->scion;
            decr_scion(y, scion); incr_scion(z, scion);
            NODES(btr, z)[j] = NODES(btr, y)[j + t];
        }
    }
    for (int j = x->n; j > i; j--) { /* move nodes in parent down one */
        NODES(btr, x)[j + 1] = NODES(btr, x)[j];
    }
    NODES(btr, x)[i + 1] = z; /* store new node */

    /* adjust the keys from previous move, and store new key */
    for (int j = x->n - 1; j >= i; j--) {
        x = setBTKey(btr, x, j + 1, x, j, 1, p, pi, p, pi);
    }
    x = setBTKey(btr, x, i, y, y->n, 1, p, pi, p, pi);
    x->n++;
    return 1;
}

#define GETN(btr) ((2 * btr->t) - 1)
static void btreeinsertnonfull(bt *btr, bt_n *x, bt_data_t k, bt_n *p, int pi) {
    if (x->leaf) { /* we are a leaf, just add it in */
        int i = findkindex(btr, x, k, NULL, NULL);
        if (i != x->n - 1) {
            mvXKeys(btr, &x, i + 2, &x, i + 1, (x->n - i - 1), btr->s.ksize,
                    1, p, pi, p, pi);
        }
        x = setBTKeyInsert(btr, x, i + 1, k, p, pi);
        x->n++; incr_scion(x, 1);
    } else { /* not leaf */
        int i = findkindex(btr, x, k, NULL, NULL) + 1;
        /* make sure that the next node isn't full */
        if (NODES(btr, x)[i]->n == GETN(btr)) {
            if (btreesplitchild(btr, x, i, NODES(btr, x)[i], x, i)) {
                if (btr->cmp(k, KEYS(btr, x, i)) > 0) i++;
            }
        }
        btreeinsertnonfull(btr, NODES(btr, x)[i], k, x, i); incr_scion(x, 1);
    }
}
void bt_insert(bt *btr, bt_data_t k) {
    bt_n *r  = btr->root;
    bt_n *p  = r;
    int   pi = 0;
    if (r->n == GETN(btr)) { /* NOTE: tree increase height */
        bt_n *s          = allocbtreenode(btr, 0, r->dirty); //TODO dirtymath
        btr->root        = s;
        s->leaf          = 0;
        s->n             = 0;
        incr_scion(s, r->scion);
        NODES(btr, s)[0] = r;
        btreesplitchild(btr, s, 0, r, p, pi);
        p = r            = s;
        btr->numnodes++;
        
    }
    btreeinsertnonfull(btr, r, k, p, pi);    /* finally insert the new node */
    btr->numkeys++;
}

// DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
#define DEBUG_DEL_START \
  printf("START: ndk\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_POST_S \
  printf("POSTS: s: %d i: %d r: %d leaf: %d x.n: %d\n", \
          s, i, r, x->leaf, x->n);
#define DEBUG_DEL_POST_FINDKINDEX \
  printf("NDK: x: %p i: %d p: %p pi: %d key: ",  \
         x, i, p, pi); printKey(btr, x, i);
#define DEBUG_DEL_CASE_1 \
  printf("ndk CASE_1    s: %d i: %d x->n: %d\n", s, i, x->n); \
  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_1_DIRTY \
  printf("CASE1 drt: %d i: %d s: %d dr: %u ngost: %d key: ", \
          drt, i, s, dwd.dr, dwd.ngost); printKey(btr, x, i);
#define DEBUG_DEL_CASE_2 \
  printf("ndk CASE_2 x[i].n: %d x[i+1].n: %d t: %d\n", \
          NODES(btr, x)[i]->n, NODES(btr, x)[i + 1]->n, btr->t); \
  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2a \
  printf("ndk CASE_2a\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2b \
  printf("ndk CASE_2b\n");  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2c \
  printf("ndk CASE_2c\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3a1 \
  printf("ndk CASE_3a1\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3a2 \
  printf("ndk CASE_3a2\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3b1 \
  printf("ndk CASE_3b1\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3b2 \
  printf("ndk CASE_3b2\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_END \
  printf("END: ndk\n"); //bt_dumptree(printf, btr, 0);

/* NOTE: case_2c_ptr retains the deleted pointer to be passed to the caller */
void *case_2c_ptr = NULL;

#define ADD_BP(plist, p, pi)                                   \
  if (plist) {                                                 \
    bp_t *bp = (bp_t *)malloc(sizeof(bp_t)); /* FREE ME 109 */ \
    bp->x = p; bp->i = pi;                                     \
    listAddNodeHead(plist, bp);                                \
  }

/* remove an existing key from the tree. KEY MUST EXIST
   the s parameter:
     1.) for normal operation pass it as 0,
     2.) delete the max node, pass it as 1,
     3.) delete the min node, pass it as 2.
 */
#define MAX_KEY_SIZE 32 /* NOTE: ksize > 8 bytes needs buffer for CASE 1 */
static char BT_DelBuf[MAX_KEY_SIZE];

static dwd_t deletekey(bt   *btr, bt_n *x,  bt_data_t k,    int    s, bool drt,
                       bt_n *p,   int   pi, list     *plist) {
    bt_n *xp, *y, *z; bt_data_t kp;
    int       yn, zn, i = 0;
    int       ks = btr->s.ksize;
    int       r  = -1;
    if (s) { /* min or max node deletion */
        if (x->leaf) r = 0;
        else {
            switch (s) {
                case 1: r =  1; break;    /* max node */
                case 2: r = -1; break;    /* min node */
            }
        }
        switch (s) {
            case 1:  i = x->n - 1; break; /* max node/leaf */
            case 2:  i = -1;       break; /* min node/leaf */
        }
    } else i = findkindex(btr, x, k, &r, NULL);              //DEBUG_DEL_POST_S
    DEBUG_DEL_POST_FINDKINDEX

    if (!drt) decr_scion(x, 1); // every deletekey() results in one less scion

    /* Case 1
     * If the key k is in node x and x is a leaf, delete the key k from x. */
    if (x->leaf) {
        dwd_t dwd; bzero(&dwd, sizeof(dwd_t));
        if (s == 2) i++;                                     //DEBUG_DEL_CASE_1
        kp     = KEYS (btr, x, i);
        dwd.dr = getDR(btr, x, i);
        if (!drt && getDR(btr, x, i)) dwd.ngost = 1;
        if (drt && !s) dwd.dr++; // NOTE: only place DR grows
        DEBUG_DEL_CASE_1_DIRTY
        if (dwd.ngost && INODE(btr) && !s) { // INODE does ngost HERE
            dwd.ngost = 0; x = incrPrevDR(btr, x, i, dwd.dr, p, pi, plist, 0);
        }
        if (!s && drt) x = incrPrevDR(btr, x, i, dwd.dr, p, pi, plist, 0);
        if (!s)        x = zeroDR(btr, x, i, p, pi);
        if BIG_BT(btr) memcpy(BT_DelBuf, kp, btr->s.ksize);
        mvXKeys(btr, &x, i, &x, i + 1, (x->n - i - 1), ks, drt, p, pi, p, pi);
        printf("CASE1: post mvXKeys\n");
        x      = trimBTN(btr, x, drt, p, pi);
        if BIG_BT(btr) dwd.k = BT_DelBuf;
        else           dwd.k = kp;
        return dwd;
    }

    if (r == 0) {                                           //DEBUG_DEL_CASE_2
        /* Case 2
         * if the key k is in the node x, and x is an internal node */
        if ((yn = NODES(btr, x)[i]->n) >= btr->t) {         //DEBUG_DEL_CASE_2a
            /* Case 2a
             * if the child y that precedes k in node x has at
             * least t keys, then find the predecessor k' of
             * k in the subtree rooted at y.  Recursively delete
             * k', and replace k by k' in x.
             *   Currently the deletion isn't done in a single
             *   downward pass was that would require special
             *   unwrapping of the delete function. */
            kp        = KEYS(btr, x, i);
            xp        = NODES(btr, x)[i];
            ADD_BP(plist, x, i)
            printf("CASE2A recurse: key: "); printKey(btr, x, i);
            dwd_t dwd = deletekey(btr, xp, NULL, 1, drt, x, i, plist);
            dwd.dr++;
            printf("2A return: dr: %d\n", dwd.dr);
            x         = setBTKeyCase2_A(btr, x, i, dwd, drt, p, pi, plist);
            dwd.k     = kp; // swap back in K's original value
            return dwd;
        }
        if ((zn = NODES(btr, x)[i + 1]->n) >= btr->t) {     //DEBUG_DEL_CASE_2b
            /* Case 2b
             * if the child z that follows k in node x has at
             * least t keys, then find the successor k' of
             * k in the subtree rooted at z.  Recursively delete
             * k', and replace k by k' in x.
             *   See above for comment on single downward pass. */
            kp        = KEYS(btr, x, i);
            xp        = NODES(btr, x)[i + 1];
            ADD_BP(plist, x, i + 1)
            printf("CASE2B recurse: key: "); printKey(btr, x, i);
            dwd_t dwd = deletekey(btr, xp, NULL, 2, drt, x, i + 1, plist);
            printf("2B return\n");
            x         = setBTKeyCase2_B(btr, x, i, dwd, drt, p, pi, plist);
            dwd.k     = kp; // swap back in K's original value
            return dwd;
        }
        if (yn == btr->t - 1 && zn == btr->t - 1) {         //DEBUG_DEL_CASE_2c
            /* Case 2c
             * if both y and z have only t - 1 keys, merge k
             * and all of z into y, so that x loses both k and
             * the pointer to z, and y now contains 2t - 1
             * keys. */
            if (!case_2c_ptr) case_2c_ptr = KEYS(btr, x, i); // BUG in original
            y = NODES(btr, x)[i];
            z = NODES(btr, x)[i + 1];
            dwd_t dwd; dwd.k = k; dwd.dr = getDR(btr, x, i);
            y = setBTKeyCase2_C(btr, y, y->n++, dwd, drt, x, i);
            mvXKeys(btr, &y, y->n, &z, 0, z->n, ks, drt, x, i, x, i + 1);
            incr_scion(y, z->n);
            if (!y->leaf) {
                move_scion(btr, z, y, z->n + 1);
                //TODO abstract NODE memmove to have similar API to KEYS
                memmove(NODES(btr, y) + y->n, NODES(btr, z),
                                              (z->n + 1) * VOIDSIZE);
            }
            y->n += z->n;
            x = zeroDR(btr, x, i, p, pi);
            mvXKeys(btr, &x, i, &x, i + 1, (x->n - i - 1), ks,
                    drt, p, pi, p, pi);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                    (x->n - i - 1) * VOIDSIZE);
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, z);
            ADD_BP(plist, x, i)
            printf("CASE2C key: "); printKey(btr, x, i);
            return deletekey(btr, y, k, s, drt, x, i, plist);
        }
    }
    /* Case 3
     * if k is not present in internal node x, determine the root xp of
     * the appropriate subtree that must contain k, if k is in the tree
     * at all.  If xp has only t - 1 keys, execute step 3a or 3b as
     * necessary to guarantee that we descend to a node containing at
     * least t keys.  Finish by recursing on the appropriate child of x. */
    i++;
    if ((xp = NODES(btr, x)[i])->n == btr->t - 1) { /* case 3a-c are !x->leaf */
        /* Case 3a
         * If xp has only t - 1 keys but has a sibling with at least t keys,
           give xp an extra key by moving a key from x down into xp,
           moving a key from xp's immediate left or right sibling up into x,
           and moving the appropriate child from the sibling into xp. */
        if (i > 0 && (y = NODES(btr, x)[i - 1])->n >= btr->t) {
            printf("CASE3A1 key: "); printKey(btr, x, i);
            /* left sibling has t keys */                  //DEBUG_DEL_CASE_3a1
            mvXKeys(btr, &xp, 1, &xp, 0, xp->n, ks,
                    drt, x, i, x, i);
            if (!xp->leaf) memmove(NODES(btr, xp) + 1, NODES(btr, xp),
                                   (xp->n + 1) * VOIDSIZE);
            xp = setBTKey(btr, xp, 0,     x, i    - 1, drt, x,  i,  p, pi);
            x  = setBTKey(btr, x,  i - 1, y, y->n - 1, drt, p,  pi, x, i - 1);
            if (!xp->leaf) NODES(btr, xp)[0] = NODES(btr, y)[y->n];
            y  = trimBTN(btr, y, drt, x, i - 1); decr_scion(y,  1);
            xp->n++;    incr_scion(xp, 1);
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n >= btr->t) {
            printf("CASE3A2 key: "); printKey(btr, x, i);
            /* right sibling has t keys */                 //DEBUG_DEL_CASE_3a2
            xp = setBTKey(btr, xp, xp->n++, x, i, drt, x, i, p, pi);
            incr_scion(xp, 1);
            x  = setBTKey(btr, x,  i,       y, 0, drt, p, pi, x, i + 1);
            if (!xp->leaf) NODES(btr, xp)[xp->n] = NODES(btr, y)[0];
            mvXKeys(btr, &y, 0, &y, 1, y->n - 1, ks, drt, x, i + 1, x, i + 1);
            y  = trimBTN(btr, y, drt, x, i + 1); decr_scion(y,  1);
            if (!y->leaf) memmove(NODES(btr, y), NODES(btr, y) + 1,
                                                 (y->n + 1) * VOIDSIZE);
        }
        /* Case 3b
         * If xp and all of xp's siblings have t - 1 keys, merge xp with
           one sibling, which involves moving a key from x down into the
           new merged node to become the median key for that node.  */
        else if (i > 0 && (y = NODES(btr, x)[i - 1])->n == btr->t - 1) {
            printf("CASE3B1 key: "); printKey(btr, x, i);
            /* merge i with left sibling */                //DEBUG_DEL_CASE_3b1
            y = setBTKey(btr, y, y->n++, x, i - 1, drt, x, i - 1, p, pi);
            incr_scion(y, 1);
            mvXKeys(btr, &y, y->n, &xp, 0, xp->n, ks, drt, x, i - 1, x, i);
            incr_scion(y, xp->n);
            if (!xp->leaf) {
                move_scion(btr, xp, y, xp->n + 1);
                memmove(NODES(btr, y) + y->n, NODES(btr, xp),
                                              (xp->n + 1) * VOIDSIZE);
            }
            y->n += xp->n;
            mvXKeys(btr, &x, i - 1, &x, i, (x->n - i), ks, drt, p, pi, p, pi);
            memmove(NODES(btr, x) + i, NODES(btr, x) + i + 1,
                                       (x->n - i) * VOIDSIZE);
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, xp);
            xp = y; i--; // i decr for parent-arg in recursion (below)
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n == btr->t - 1) {
            printf("CASE3B2 key: "); printKey(btr, x, i);
            /* merge i with right sibling */               //DEBUG_DEL_CASE_3b2
            xp = setBTKey(btr, xp, xp->n++, x, i, drt, x, i, p, pi);
            incr_scion(xp, 1);
            mvXKeys(btr, &xp, xp->n, &y, 0, y->n, ks, drt, x, i, x, i + 1);
            incr_scion(xp, y->n);
            if (!xp->leaf) {
                move_scion(btr, y, xp, y->n + 1);
                memmove(NODES(btr, xp) + xp->n, NODES(btr, y),
                                                (y->n + 1) * VOIDSIZE);
            }
            xp->n += y->n;
            mvXKeys(btr, &x, i, &x, i + 1, (x->n - i - 1), ks,
                    drt, p, pi, p, pi);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                                           (x->n - i - 1) * VOIDSIZE);
            x = trimBTN(btr, x, drt, p, pi);
            bt_free_btreenode(btr, y);
        }
    } //printf("RECURSE CASE 3\n");
    ADD_BP(plist, x, i)
    printf("CASE3: xp: %p x: %p i: %d key: ", xp, x, i); printKey(btr, x, i);
    return deletekey(btr, xp, k, s, drt, x, i, plist);
}

static dwd_t remove_key(bt *btr, bt_data_t k, bool drt, aobj *akey) {
    dwd_t dwde; bzero(&dwde, sizeof(dwd_t));
    if (!btr->root) return dwde;
    ulong  odr  = 0;
    case_2c_ptr = NULL;     //DEBUG_DEL_START
    bt_n  *p    = btr->root; int pi = 0;
    list  *plist; // NOTE: plist stores ancestor line during recursive delete
    if (drt) {
        plist = listCreate(); plist->free = free_bp; ADD_BP(plist, p, pi)//FR110
    } else plist = NULL;
    if (akey && !drt && C_IS_NUM(btr->s.ktype)) { //DELETE a MISS, ADD a GHOST
        dwm_t dwm = findnodekey(btr, btr->root, k, akey);
        odr       = (ulong)getDR(btr, dwm.x, dwm.i);
        //TODO replace MISS w/ GHOST, abt abstraction needed (CODE HERE)
        //     currently, R is deleted and G is added (need to change R to G)
    }
    dwd_t dwd   = deletekey(btr, btr->root, k, 0, drt, p, pi, plist);
    btr->numkeys--;                                             //DEBUG_DEL_END
    /* remove empty non-leaf node from root, */
    if (!btr->root->n && !btr->root->leaf) { /* NOTE: tree decrease height */
        btr->numnodes--;
        bt_n *x   = btr->root;
        btr->root = NODES(btr, x)[0];
        bt_free_btreenode(btr, x);
    }
    if (akey && dwd.ngost) { // only for BTREE_TABLE (Inode done in deletekey())
        dwd.ngost = C_IS_I(btr->s.ktype) ? akey->i : akey->l;
        dwd.dr    = odr;
    }
    if (case_2c_ptr) dwd.k = case_2c_ptr;
    if (plist) listRelease(plist);                       // FREED 110
    return dwd;
}
dwd_t bt_delete(bt *btr, bt_data_t k, aobj *akey) {
    return remove_key(btr, k, 0, akey);
}
bt_data_t bt_evict(bt *btr, bt_data_t k) { printf("bt_evict k: %p\n", k);
    dwd_t dwd = remove_key(btr, k, 1, NULL);
    return dwd.k;
}
void bt_insert_ghost(bt *btr, bt_data_t k, aobj *akey,
                              bt_data_t stream, uint32 dr) {
    printf("bt_insert_ghost: dr: %u key: \n", dr); dumpAobj(printf, akey);
    bt_insert(btr, stream); // insert a "GHOST" row
    dwm_t dwm = findnodekey(btr, btr->root, k, akey); // GHOST DR -> diff
    setDR(btr, dwm.x, dwm.i, dr, dwm.p, dwm.pi);
} //TODO setDR() on bt_insert()

#define DEBUG_BT_DELETE_D \
    printf("bt_delete_d: qkey: %lu mkey: %lu dpdr: %lu ndr: %lu\n", \
            qkey, mkey, dpdr, ndr);

// bt_delete_d handles the deletion of an EVICTed Key (cases 2,3,4,7)
void bt_delete_d(bt *btr, bt_data_t k, aobj *akey, bt_data_t stream) {
    dwm_t  dwm  = findnodekey(btr, btr->root, k, akey);
    ulong  dr   = (ulong)getDR(btr, dwm.x, dwm.i); // get PREV's DR
    ulong  mkey = getNumKey(btr, dwm.x, dwm.i);
    ulong  qkey = C_IS_I(btr->s.ktype) ? akey->i : akey->l;
    ulong  dpdr = (qkey == mkey) ? 0 : qkey - mkey - 1; // Delta w/ PREV DR
    ulong  ndr  = mkey + dr - qkey; /*DR after Evicted Key*/  DEBUG_BT_DELETE_D
    if (!dpdr) { // KEY is final EVICTION in PREV's DR
        uchar *stream = KEYS(btr, dwm.x, dwm.i);
        void  *rrow   = parseStream(stream, btr);
        bool   gost   = !UU(btr) && rrow && !(*(uchar *)rrow);
printf("gost: %d\n", gost);
        if (gost) bt_delete(btr, KEYS(btr, dwm.x, dwm.i), NULL); // ECASE:7
        else      zeroDR(btr, dwm.x, dwm.i, dwm.p, dwm.pi);      // ECASE:2
    } else  setDR(btr, dwm.x, dwm.i, dpdr, dwm.p, dwm.pi);       // ECASE:3
    if (ndr) { // delete from middle of sequential evictions     -- ECASE:4
        bt_insert(btr, stream); // insert a "GHOST" row
        dwm         = findnodekey(btr, btr->root, k, akey); // GHOST DR -> diff
        setDR(btr, dwm.x, dwm.i, ndr, dwm.p, dwm.pi);
    }
} //TODO setDR() on bt_insert()

// ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS
#define DEBUG_FIND_NODE_KEY \
  if (x->leaf) printf("LEAF: findnodekey: i: %d r: %d x: %p\n", i ,r, x); \
  else         printf("NODE: findnodekey: i: %d r: %d x: %p\n", i ,r, x);
#define DEBUG_CURRKEY_MISS \
  printf("key_covers_miss: mkey: %lu qkey: %lu span: %lu -> hit: %d\n", \
          mkey, qkey, span, (qkey >= mkey && qkey <= span));
#define DEBUG_BT_II \
  printf("r: %d i: %d x->n: %d miss: %d\n", r, i, x->n, miss);

static inline bool key_covers_miss(bt *btr, bt_n *x, int i, aobj *akey) {
    if (!(C_IS_NUM(btr->s.ktype))) return 0;
    if (i < 0) i = 0;
    ulong mkey = getNumKey(btr, x, i);
    ulong dr   = (ulong)getDR(btr, x, i);
    if (mkey && dr) {
        ulong qkey = C_IS_I(btr->s.ktype) ? akey->i : akey->l;
        ulong span = mkey + dr;
        DEBUG_CURRKEY_MISS
        if (qkey >= mkey && qkey <= span) return 1;
    }
    return 0;
}
#define SET_DWM_XIP { dwm.x = x; dwm.i = i; dwm.p = p; dwm.pi = pi; }
dwm_t findnodekey(bt *btr, bt_n *x, bt_data_t k, aobj *akey) {
    int    r = -1,             i = 0;
    bt_n  *p = btr->root; int pi = 0;
    dwm_t  dwm; bzero(&dwm, sizeof(dwm_t)); SET_DWM_XIP
    if (SIMP_UNIQ(btr) && Index[btr->s.num].iposon) Index[btr->s.num].cipos = 0;
    while (x) {
        i = findkindex(btr, x, k, &r, NULL); 
        DEBUG_FIND_NODE_KEY
        if (i >= 0 && !r) { SET_DWM_XIP dwm.k = KEYS(btr, x, i); return dwm; }
        if (key_covers_miss(btr, x, i, akey)) { SET_DWM_XIP dwm.miss = 1; }
        if (x->leaf)       {            dwm.k = NULL;            return dwm; }
        p = x; pi = i + 1; x = NODES(btr, x)[i + 1];
    }
    return dwm;
}
bt_data_t bt_find(bt *btr, bt_data_t k, aobj *akey) { //Indexes still use this
    dwm_t dwm = findnodekey(btr, btr->root, k, akey);
    return dwm.k;
}

static bool check_min_miss(bt *btr, aobj *alow) {
    if (!btr->dirty_left) return 0;
    aobj amin; convertStream2Key(bt_min(btr), &amin, btr);
    return aobjEQ(alow, &amin);
}
int bt_init_iterator(bt *btr, bt_data_t k, btIterator *iter, aobj *alow) {
    if (!btr->root) return II_FAIL;
    int        r          = -1;
    bool       lmiss      = check_min_miss(btr, alow);
    bool       miss       =  0;
    uchar      only_right =  1;
    bt_n      *x          = btr->root;
    while (x) {
        int i = findkindex(btr, x, k, &r, iter);
        if (i >= 0 && r == 0) return lmiss ? II_L_MISS : II_OK;
printf("btr: %p x: %p i: %d alow: %p\n", btr, x, i, alow);
        if (key_covers_miss(btr, x, i, alow)) miss = 1; //DEBUG_BT_II
        if (miss)             return II_MISS;
        if (r < 0 || i != (x->n - 1)) only_right = 0;
        if (x->leaf) {
            if      (i != (x->n - 1)) only_right = 0;
            return only_right ? II_ONLY_RIGHT : II_LEAF_EXIT;
        }
        iter->bln->child = get_new_iter_child(iter);
        x                = NODES(btr, x)[i + 1];
        to_child(iter, x);
    }
    return II_FAIL;
}
static bt_data_t findnodekeyreplace(bt *btr, bt_n *x,
                                    bt_data_t k, bt_data_t val) {
    if (!btr->root) return NULL;
    int i, r = -1;
    while (x) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) {
            bt_data_t b = KEYS(btr, x, i);
            setBTKeyRaw(btr, x, i, val); // overwrite (no dirty IO)
            return b;
        }
        if (x->leaf) return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}
bt_data_t bt_replace(bt *btr, bt_data_t k, bt_data_t val) {
    return findnodekeyreplace(btr, btr->root, k, val);
}
bool bt_exist(bt *btr, bt_data_t k, aobj *akey) {
    int     r    = -1, i;
    bool    miss =  0;
    bt_n   *x    = btr->root;
    while (x) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return 1;
        if (key_covers_miss(btr, x, i, akey)) miss = 1;
        if (miss)             return 1;
        if (x->leaf)          return 0;
        x = NODES(btr, x)[i + 1];
    }
    return 0;
}
/* NOTE: bt_find_loc only for VOIDPTR BTs (e.g. BTREE_TABLE) */
bt_data_t *bt_find_loc(bt *btr, bt_data_t k) { /* pointer to BT ROW */
    int i, r = -1;
    bt_n *x  = btr->root;
    while (x) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return &OKEYS(btr, x)[i];
        if (x->leaf)          return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}

static bt_data_t findminkey(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, 0);
    else         return findminkey(btr, NODES(btr, x)[0]);
}
bt_n *findminnode(bt *btr, bt_n *x) {
    if (x->leaf) return x;
    else         return findminnode(btr, NODES(btr, x)[0]);
}
static bt_data_t findmaxkey(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, x->n - 1);
    else         return findmaxkey(btr, NODES(btr, x)[x->n]);
}
bt_data_t bt_min(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    else                             return findminkey(btr, btr->root);
}
bt_data_t bt_max(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    else                             return findmaxkey(btr, btr->root);
}
uint32 bt_get_dr(bt *btr, bt_data_t k, aobj *akey) {
    dwm_t  dwm  = findnodekey(btr, btr->root, k, akey);
    return getDR(btr, dwm.x, dwm.i);
}
bool bt_decr_dr_pk(bt *btr, bt_data_t k, aobj *akey, uint32 by) {
    dwm_t  dwm  = findnodekey(btr, btr->root, k, akey);
    uint32 dr   = getDR(btr, dwm.x, dwm.i);
    if (dr) dr -= by;
    if (dr) { setDR (btr, dwm.x, dwm.i, dr, dwm.p, dwm.pi); return 0; }
    else    { zeroDR(btr, dwm.x, dwm.i,     dwm.p, dwm.pi); return 1; }
    //TODO zeroDR leaves a dangling GHOST-KEY, it needs cleaning up
}
// DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR DESTRUCTOR
static void destroy_bt_node(bt *btr, bt_n *x) {
    for (int i = 0; i < x->n; i++) {
        void *be    = KEYS(btr, x, i);
        int   ssize = getStreamMallocSize(btr, be);
        if (!INODE(btr) && NORM_BT(btr)) bt_free(btr, be, ssize);
    }
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            destroy_bt_node(btr, NODES(btr, x)[i]);
        }}
    bt_free_btreenode(btr, x); /* memory management in btr */
}
void bt_destroy(bt *btr) {
    if (btr->root) {
        if (btr->numkeys) destroy_bt_node  (btr, btr->root);
        else              bt_free_btreenode(btr, btr->root); 
        btr->root  = NULL;
    }
    bt_free_btree(btr);
}
void bt_release(bt *btr, bt_n *x) { /* dont destroy data, just btree */
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            bt_release(btr, NODES(btr, x)[i]);
        }}
    bt_free_btreenode(btr, x); /* memory management in btr */
}

// CLONE CLONE CLONE CLONE CLONE CLONE CLONE CLONE CLONE CLONE CLONE CLONE
void bt_to_bt_insert(bt *nbtr, bt *obtr, bt_n *x) {
    for (int i = 0; i < x->n; i++) {
        void *be = KEYS(obtr, x, i);
        //TODO DS: n.DR = o.DR ... (cloneDR)
        bt_insert(nbtr, be);
    }
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            bt_to_bt_insert(nbtr, obtr, NODES(obtr, x)[i]);
        }}
}

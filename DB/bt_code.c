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

#include "zmalloc.h"

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
static bt_data_t findminnode(bt *btr, bt_n *x);
static bt_data_t findmaxnode(bt *btr, bt_n *x);

//#define BT_MEM_PROFILE
#ifdef BT_MEM_PROFILE
static ulong tot_bt_data     = 0;
static ulong tot_bt_data_mem = 0;
static ulong tot_num_bt_ns   = 0;
static ulong tnbtnmem        = 0;
static ulong tot_num_bts     = 0;
static ulong tot_num_bt_mem  = 0;
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

#define DEBUG_INCR_MEM \
    printf("INCR MEM: osize: %ld plus: %lu nsize: %ld\n", ibtr->msize, size, (ibtr->msize + size));
#define DEBUG_DECR_MEM \
    printf("DECR MEM: osize: %ld minus: %lu nsize: %ld\n", ibtr->msize, size, (ibtr->msize - size));
#define DEBUG_BT_MALLOC \
    printf("bt_MALLOC: %p size: %d\n", ibtr, size);
#define DEBUG_ALLOC_BTN \
    printf("allocbtreeNODE: %p leaf: %d size: %d\n", ibtr, leaf, size);
#define DEBUG_ALLOC_BTREE \
    printf("allocBTREE: %p size: %d\n", ibtr, size);
#define DEBUG_BT_FREE \
    printf("bt_FREE: %p size: %d\n", ibtr, size);
#define DEBUG_FREE_BTN \
    printf("bt_free_btreeNODE: %p leaf: %d size: %lu\n", ibtr, x->leaf, size);

/* This is the real log2 function.  It is only called when we don't have
 * a value in the table. -> which is basically never */
static inline int real_log2(unsigned int a, int nbits) {
    uint32 i = 0;
    uint32 b = (nbits + 1) / 2; /* divide in half rounding up */
    while (b) {
        i = (i << 1);
        if (a >= (unsigned int)(1 << b)) {
            /* select the top half and mark this bit */
            a /= (1 << b);
            i = i | 1;
        } else {
            /* select the bottom half and don't set the bit */
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

#define DEBUG_SET_KEY \
  printf("setBTKey: ksize: %d btr: %p v: %p p: %p uint: %d void: %d uu: %d " \
         "lu: %d ul: %d ll: %d ux: %d xu: %d lx: %d xl: %d xx: %d\n",        \
          btr->s.ksize, btr, v, p, ISUINT(btr), ISVOID(btr), UU(btr),        \
          LU(btr), UL(btr), LL(btr), UX(btr), XU(btr),                       \
          LX(btr), XL(btr), XX(btr));                                        \
  DEBUG_KEY_OTHER
#define DEBUG_AKEYS                                                            \
  printf("AKEYS: i: %d ofst: %d v: %p uint: %d uu: %d lu: %d ul: %d ll: %d\n", \
          i, ofst, v, ISUINT(btr), UU(btr), LU(btr), UL(btr), LL(btr));        \
  DEBUG_KEY_OTHER
#define DEBUG_KEYS \
  printf("KEYS: uint: %d void: %d i: %d\n", ISUINT(btr), ISVOID(btr), i);
#define DEBUG_SETBTKEY_OBT \
  if (p) printf("setBTKey: memcpy to v: %p\n", v);

/* NOTE KEYS can be (void *) or uint, [8 and 4 bytes], so logic is needed */
#define ISVOID(btr)  (btr->s.ksize == VOIDSIZE)
#define ISUINT(btr)  (btr->s.ksize == UINTSIZE)
static inline void setBTKey(bt *btr, void **v, void *p) {
    if      ISVOID(btr) *v                  = p;   
    else if ISUINT(btr) *(int *)((long *)v) = (int)(long)p;
    else { /* OTHER_BT */                                  //DEBUG_SETBTKEY_OBT
        if (p) memcpy(v, p, btr->s.ksize);
        else   bzero (v,    btr->s.ksize);
    }                                                           //DEBUG_SET_KEY
}
static inline void **AKEYS(bt *btr, bt_n *x, int i) {
    int   ofst = (i * btr->s.ksize);
    char *v    = (char *)x + btr->keyofst + ofst;                 //DEBUG_AKEYS
    return (void **)v;
}
#define OKEYS(btr, x) ((void **)((char *)x + btr->keyofst))
void *KEYS(bt *btr, bt_n *x, int i) {                              //DEBUG_KEYS
    if      ISVOID(btr) return                  OKEYS(btr, x)[i];
    else if ISUINT(btr) return VOIDINT (*(int *)AKEYS(btr, x, i));
    else /* OTHER_BT */ return (void *)         AKEYS(btr, x, i);
}

/* NOTE used-memory bookkeeping maintained at the Btree level */
static void bt_increment_used_memory(bt *ibtr, size_t size) {  //DEBUG_INCR_MEM
    ibtr->msize += (ull)size;
    increment_used_memory(size);
}
void bt_incr_dsize(bt *ibtr, size_t size) {
    bt_increment_used_memory(ibtr, size);
    ibtr->dsize += size;
}
static void bt_decrement_used_memory(bt *ibtr, size_t size) {  //DEBUG_DECR_MEM
    ibtr->msize -= (ull)size;
    decrement_used_memory(size);
}
void bt_decr_dsize(bt *ibtr, size_t size) {
    bt_decrement_used_memory(ibtr, size);
    ibtr->dsize -= size;
}
void *bt_malloc(bt *ibtr, int size) {                         //DEBUG_BT_MALLOC
    BT_MEM_PROFILE_MLC
    bt_incr_dsize(ibtr, size);
    return malloc(size);
}
#ifdef BTREE_DEBUG
unsigned long BtreeNodeNum = 0;
#endif
static bt_n *allocbtreenode(bt *ibtr, bool leaf) {
    ibtr->numnodes++;
    size_t  size = leaf ? ibtr->kbyte : ibtr->nbyte;          //DEBUG_ALLOC_BTN
    BT_MEM_PROFILE_NODE
    bt_n   *btn  = malloc(size);
    bt_increment_used_memory(ibtr, size);
    bzero(btn, size);
    btn->leaf    = 1;
#ifdef BTREE_DEBUG
    btn->num     = BtreeNodeNum++;
#endif
    return btn;
}
static bt *allocbtree() {
    int  size = sizeof(struct btree);
    BT_MEM_PROFILE_BT
    bt  *ibtr = (bt *)malloc(size);                      /* FREE ME 035 */
    bzero(ibtr, size);                                      //DEBUG_ALLOC_BTREE
    bt_increment_used_memory(ibtr, size);
    return ibtr;
}

void bt_free(bt *ibtr, void *v, int size) {                     //DEBUG_BT_FREE
    bt_decr_dsize(ibtr, size);
    free(v);
}
void bt_free_btreenode(bt *ibtr, bt_n *x) {
    size_t size = x->leaf ? ibtr->kbyte : ibtr->nbyte;         //DEBUG_FREE_BTN
    bt_decrement_used_memory(ibtr, size);
    free(x);                                             /* FREED 035 */
}
void bt_free_btree(bt *btr) {
    free(btr);
}

bt *bt_create(bt_cmp_t cmp, uchar trans, bts_t *s) {
    /* NOTE: the two BT node sizes are 128 and 4096 */
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
    btr->root       = allocbtreenode(btr, 1);
    btr->numnodes   = 1;
    //printf("bt_create\n"); bt_dump_info(printf, btr);
    return btr;
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
    Index[btr->s.num].cipos += i + 1;                     //DEBUG_ADD_TO_CIPOS
}

static int findkindex(bt *btr, bt_n *x, bt_data_t k, int *r, btIterator *iter) {
    if (x->n == 0) return -1;
    int b, tr;
    int *rr = (r == NULL) ? &tr : r; /* rr: key is greater than current entry */
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

#if 0 //TODO something is wrong w/ this, it corrupts the Btree (deletion)
static bool split_leaf2left(bt *btr, bt_n *x, bt_n *y, bt_n *xp, int i) {
    ushort16 t  = btr->t;
    int blocksize = (t - 1) * btr->s.ksize;
    /* 1.) move x to xp_mid */
    setBTKey(btr, AKEYS(btr, xp, t - 1), KEYS(btr, x, i - 1));
    /* 2.) move y_mid to x */
    setBTKey(btr, AKEYS(btr, x, i - 1), KEYS(btr, y, t - 1));
    /* 3.) move begin(y) to mid(xp) */
    memmove(AKEYS(btr, xp, t), AKEYS(btr, y, 0), blocksize);
    xp->n += t;
    /* 4.) move y back t keys */
    memmove(AKEYS(btr, y, 0), AKEYS(btr, y, t), blocksize);
    y->n = t - 1;
    return 0;
}
static bool btreesplitchild(bt *btr, bt_n *x, int i, bt_n *y) {
    ushort16 t = btr->t;
    if (i && y->leaf && (btr->s.bflag & BTFLAG_AUTO_INC)) {
        bt_n *xp = NODES(btr, x)[i - 1];
        if ((xp->n <= t - 1)) return split_leaf2left(btr, x, y, xp, i);
    }
#endif

static inline void incr_scion(bt_n *x, int n) {
    x->scion += n;
}
static inline void decr_scion(bt_n *x, int n) {
    x->scion -= n;
}

static bool btreesplitchild(bt *btr, bt_n *x, int i, bt_n *y) {
    ushort16  t = btr->t;
    bt_n     *z = allocbtreenode(btr, y->leaf);
    z->leaf     = y->leaf; /* duplicate leaf setting */
    for (int j = 0; j < t - 1; j++) { // TODO single memcpy()
        setBTKey(btr, AKEYS(btr, z, j), KEYS(btr, y, j + t));
    }
    y->n = t - 1; decr_scion(y, t);     /* half num_nodes in Y */
    z->n = t - 1; incr_scion(z, t - 1); /* half num_nodes in Z */
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
        setBTKey(btr, AKEYS(btr, x, j + 1), KEYS(btr, x, j));
    }
    setBTKey(btr, AKEYS(btr, x, i), KEYS(btr, y, y->n));
    x->n++;
    return 1;
}
#define GETN(btr) ((2 * btr->t) - 1)
static void btreeinsertnonfull(bt *btr, bt_n *x, bt_data_t k) {
    if (x->leaf) { /* we are a leaf, just add it in */
        int i = findkindex(btr, x, k, NULL, NULL);
        if (i != x->n - 1) {
            memmove(AKEYS(btr, x, i + 2), AKEYS(btr, x, i + 1),
                    (x->n - i - 1) * btr->s.ksize);
        }
        setBTKey(btr, AKEYS(btr, x, i + 1), k);
        x->n++; incr_scion(x, 1);
    } else { /* not leaf */
        int i = findkindex(btr, x, k, NULL, NULL) + 1;
        /* make sure that the next node isn't full */
        if (NODES(btr, x)[i]->n == GETN(btr)) {
            if (btreesplitchild(btr, x, i, NODES(btr, x)[i])) {
                if (btr->cmp(k, KEYS(btr, x, i)) > 0) i++;
            }
        }
        btreeinsertnonfull(btr, NODES(btr, x)[i], k); incr_scion(x, 1);
    }
}
void bt_insert(bt *btr, bt_data_t k) {
    bt_n *r = btr->root;
    if (r->n == GETN(btr)) { /* NOTE: tree increase height */
        bt_n *s          = allocbtreenode(btr, 0);
        btr->root        = s;
        s->leaf          = 0;
        s->n             = 0;
        incr_scion(s, r->scion);
        NODES(btr, s)[0] = r;
        btreesplitchild(btr, s, 0, r);
        r                = s;
        btr->numnodes++;
    }
    btreeinsertnonfull(btr, r, k);    /* finally insert the new node */
    btr->numkeys++;
}

#define DEBUG_DEL_START \
  printf("START: ndk\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_POST_S \
  printf("POSTS: s: %d i: %d r: %d leaf: %d x.n: %d\n", s, i, r, x->leaf, x->n);
#define DEBUG_DEL_CASE_1 \
  printf("ndk CASE_1    s: %d i: %d x->n: %d\n", s, i, x->n); \
  //bt_dumptree(printf, btr, 0);
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
#define DEBUG_DEL_CASE_3A1 \
  printf("ndk CASE_3a1\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3A2 \
  printf("ndk CASE_3a2\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3B \
  printf("ndk CASE_3b\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3B2 \
  printf("ndk CASE_3b2\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_END \
  printf("END: ndk\n"); //bt_dumptree(printf, btr, 0);

/* NOTE: case_2c_ptr retains the deleted pointer to be passed to the caller */
void *case_2c_ptr = NULL;
/*
 * remove an existing key from the tree. KEY MUST EXIST
 * the s parameter:
   1.) for normal operation pass it as 0,
   2.) delete the max node, pass it as 1,
   3.) delete the min node, pass it as 2.
 */
#define MAX_KEY_SIZE 32 /* NOTE: ksize > 8 bytes needs buffer for CASE 1 */
static char BT_DelBuf[MAX_KEY_SIZE];

static inline void move_scion(bt *btr, bt_n *z, bt_n *y, int n) {
    for (int i = 0; i < n; i++) { incr_scion(y, NODES(btr, z)[i]->scion); }
}
static bt_data_t nodedeletekey(bt *btr, bt_n *x, bt_data_t k, int s) {
    bt_n *xp, *y, *z; bt_data_t kp;
    int       i, yn, zn;
    int       ks = btr->s.ksize;
    int       r  = -1;
    if (x == NULL) return 0;
    if (s) { /* min or max node deletion */
        if (!x->leaf) {
            switch (s) {
                case 1: r =  1; break;
                case 2: r = -1; break;
            }
        } else r = 0;
        switch (s) {
            case 1:  i = x->n - 1; break; /* max node */
            case 2:  i = -1;       break; /* min node */
            default: i = 42;       break; /* should NOT happen */
        }
    } else i = findkindex(btr, x, k, &r, NULL);              //DEBUG_DEL_POST_S

    decr_scion(x, 1); // every nodedeletekey() will result in one less scion

    /* Case 1
     * If the key k is in node x and x is a leaf, delete the key k from x. */
    if (x->leaf) {
        if (s == 2) i++;                                     //DEBUG_DEL_CASE_1
        kp = KEYS(btr, x, i);
        if BIG_BT(btr) memcpy(BT_DelBuf, kp, btr->s.ksize);
        memmove(AKEYS(btr, x, i), AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
        x->n--;
        setBTKey(btr, AKEYS(btr, x, x->n), NULL); /* RUSS added for iterator */
        if BIG_BT(btr) return BT_DelBuf;
        else           return kp;
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
            xp = NODES(btr, x)[i];
            kp = KEYS(btr, x, i);
            setBTKey(btr, AKEYS(btr, x, i), nodedeletekey(btr, xp, NULL, 1));
            return kp;
        }
        if ((zn = NODES(btr, x)[i + 1]->n) >= btr->t) {     //DEBUG_DEL_CASE_2b
            /* Case 2b
             * if the child z that follows k in node x has at
             * least t keys, then find the successor k' of
             * k in the subtree rooted at z.  Recursively delete
             * k', and replace k by k' in x.
             *   See above for comment on single downward pass. */
            xp = NODES(btr, x)[i + 1];
            kp = KEYS(btr, x, i);
            setBTKey(btr, AKEYS(btr, x, i), nodedeletekey(btr, xp, NULL, 2));
            return kp;
        }
        if (yn == btr->t - 1 && zn == btr->t - 1) {         //DEBUG_DEL_CASE_2c
            /* Case 2c
             * if both y and z have only t - 1 keys, merge k
             * and all of z into y, so that x loses both k and
             * the pointer to z, and y now contains 2t - 1
             * keys.
             *   RUSS fixed a bug here, the return ptr was wrong */
            if (!case_2c_ptr) case_2c_ptr = KEYS(btr, x, i);
            y = NODES(btr, x)[i];
            z = NODES(btr, x)[i + 1];
            setBTKey(btr, AKEYS(btr, y, y->n++), k);
            memmove(AKEYS(btr, y, y->n), AKEYS(btr, z, 0), z->n * ks);
            incr_scion(y, z->n);
            if (!y->leaf) {
                move_scion(btr, z, y, z->n + 1);
                memmove(NODES(btr, y) + y->n, NODES(btr, z),
                                              (z->n + 1) * VOIDSIZE);
            }
            y->n += z->n;
            memmove(AKEYS(btr, x, i),AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                    (x->n - i - 1) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(btr, z);
            return nodedeletekey(btr, y, k, s);
        }
    }
    /* Case 3
     * if k is not present in internal node x, determine the root x' of
     * the appropriate subtree that must contain k, if k is in the tree
     * at all.  If x' has only t - 1 keys, execute step 3a or 3b as
     * necessary to guarantee that we descend to a node containing at
     * least t keys.  Finish by recursing on the appropriate child of x. */
    i++;
    if ((xp = NODES(btr, x)[i])->n == btr->t - 1) { /* case 3a-c are !x->leaf */
        /* Case 3a
         * If x' has only t - 1 keys but has a sibling with at
         * least t keys, give x' an extra key by moving a key
         * from x down into x', moving a key from x''s immediate
         * left or right sibling up into x, and moving the
         * appropriate child from the sibling into x'. */
        if (i > 0 && (y = NODES(btr, x)[i - 1])->n >= btr->t) {
            /* left sibling has t keys */                  //DEBUG_DEL_CASE_3A1
            memmove(AKEYS(btr, xp, 1), AKEYS(btr, xp, 0), xp->n * ks);
            if (!xp->leaf) memmove(NODES(btr, xp) + 1, NODES(btr, xp),
                                   (xp->n + 1) * VOIDSIZE);
            setBTKey(btr, AKEYS(btr, xp, 0), KEYS(btr, x, i - 1));
            setBTKey(btr, AKEYS(btr, x, i - 1), KEYS(btr, y, y->n - 1));
            if (!xp->leaf) NODES(btr, xp)[0] = NODES(btr, y)[y->n];
            y->n--;  decr_scion(y,  1);
            xp->n++; incr_scion(xp, 1);
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n >= btr->t) {
            /* right sibling has t keys */                 //DEBUG_DEL_CASE_3A2
            setBTKey(btr, AKEYS(btr, xp, xp->n++), KEYS(btr, x, i));
            incr_scion(xp, 1);
            setBTKey(btr, AKEYS(btr, x, i), KEYS(btr, y, 0));
            if (!xp->leaf) NODES(btr, xp)[xp->n] = NODES(btr, y)[0];
            y->n--;  decr_scion(y, 1);
            memmove(AKEYS(btr, y, 0), AKEYS(btr, y, 1), y->n * ks);
            if (!y->leaf) memmove(NODES(btr, y), NODES(btr, y) + 1,
                                  (y->n + 1) * VOIDSIZE);
        }
        /* Case 3b
         * If x' and all of x''s siblings have t - 1 keys, merge
         * x' with one sibling, which involves moving a key from x
         * down into the new merged node to become the median key
         * for that node.  */
        else if (i > 0 && (y = NODES(btr, x)[i - 1])->n == btr->t - 1) {
            /* merge i with left sibling */                 //DEBUG_DEL_CASE_3B
            setBTKey(btr, AKEYS(btr, y, y->n++), KEYS(btr, x, i - 1));
            incr_scion(y, 1);
            memmove(AKEYS(btr, y, y->n), AKEYS(btr, xp, 0), xp->n * ks);
            incr_scion(y, xp->n);
            if (!xp->leaf) {
                move_scion(btr, xp, y, xp->n + 1);
                memmove(NODES(btr, y) + y->n, NODES(btr, xp),
                                              (xp->n + 1) * VOIDSIZE);
            }
            y->n += xp->n;
            memmove(AKEYS(btr, x, i - 1), AKEYS(btr, x, i), (x->n - i) * ks);
            memmove(NODES(btr, x) + i, NODES(btr, x) + i + 1,
                    (x->n - i) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(btr, xp);
            xp = y;
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n == btr->t - 1) {
            /* merge i with right sibling */               //DEBUG_DEL_CASE_3B2
            setBTKey(btr, AKEYS(btr, xp, xp->n++), KEYS(btr, x, i));
            incr_scion(xp, 1);
            memmove(AKEYS(btr, xp, xp->n), AKEYS(btr, y, 0), y->n * ks);
            incr_scion(xp, y->n);
            if (!xp->leaf) {
                move_scion(btr, y, xp, y->n + 1);
                memmove(NODES(btr, xp) + xp->n, NODES(btr, y),
                                                (y->n + 1) * VOIDSIZE);
            }
            xp->n += y->n;
            memmove(AKEYS(btr, x, i),AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                    (x->n - i - 1) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(btr, y);
        }
    } //printf("RECURSE\n");
    return nodedeletekey(btr, xp, k, s);
}

bt_data_t bt_delete(bt *btr, bt_data_t k) {
    bt_n *x;
    case_2c_ptr = NULL;                                       //DEBUG_DEL_START
    bt_data_t r = nodedeletekey(btr, btr->root, k, 0);
    btr->numkeys--;                                             //DEBUG_DEL_END
    /* remove empty non-leaf node from root, */
    if (!btr->root->n && !btr->root->leaf) { /* NOTE: tree decrease height */
        btr->numnodes--;
        x         = btr->root;
        btr->root = NODES(btr, x)[0];
        bt_free_btreenode(btr, x);
    }
    return case_2c_ptr ? case_2c_ptr : r;
}

static bt_data_t findminnode(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, 0);
    else         return findminnode(btr, NODES(btr, x)[0]);
}
static bt_data_t findmaxnode(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, x->n - 1);
    else         return findmaxnode(btr, NODES(btr, x)[x->n]);
}
bt_data_t bt_min(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    return findminnode(btr, btr->root);
}
bt_data_t bt_max(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    return findmaxnode(btr, btr->root);
}

static bt_data_t findnodekey(bt *btr, bt_n *x, bt_data_t k) {
    int i, r; 
    if (SIMP_UNIQ(btr) && Index[btr->s.num].iposon) Index[btr->s.num].cipos = 0;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return KEYS(btr, x, i);
        if (x->leaf)          return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}
bt_data_t bt_find(bt *btr, bt_data_t k) {
    return findnodekey(btr, btr->root, k);
}

/* NOTE: bt_find_loc only for VOIDPTR BTs (e.g. BTREE_TABLE) */
bt_data_t *bt_find_loc(bt *btr, bt_data_t k) { /* pointer to BT ROW */
    int i, r;
    bt_n *x = btr->root;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return &OKEYS(btr, x)[i];
        if (x->leaf)          return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}

/* copy of findnodekey */
int bt_init_iterator(bt *btr, bt_data_t k, btIterator *iter) {
    if (!btr->root) return -1;
    int i, r;
    uchar  only_right = 1;
    bt_n  *x          = btr->root;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, iter);
        if (i >= 0 && r == 0) return 0;
        if (r < 0 || i != (x->n - 1)) only_right = 0;
        if (x->leaf) {
            if (i != (x->n - 1)) only_right = 0;
            return only_right ? RET_ONLY_RIGHT : RET_LEAF_EXIT;
        }
        iter->bln->child = get_new_iter_child(iter);
        x                = NODES(btr, x)[i + 1];
        to_child(iter, x);
    }
    return -1;
}

/* copy of findnodekey */
static bt_data_t findnodekeyreplace(bt *btr, bt_n *x,
                                    bt_data_t k, bt_data_t val) {
    if (!btr->root) return NULL;
    int i, r;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) {
            bt_data_t b     = KEYS(btr, x, i);
            setBTKey(btr, AKEYS(btr, x, i), val);
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

static void destroy_bt_node(bt *ibtr, bt_n *n) {
    for (int i = 0; i < n->n; i++) {
        void *be    = KEYS(ibtr, n, i);
        int   ssize = getStreamMallocSize(ibtr, be);
        if (!INODE(ibtr) && NORM_BT(ibtr)) bt_free(ibtr, be, ssize);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            destroy_bt_node(ibtr, NODES(ibtr, n)[i]);
        }
    }
    bt_free_btreenode(ibtr, n); /* memory management in ibtr */
}
void bt_destroy(bt *btr) {
    if (btr->root) {
        if (btr->numkeys) destroy_bt_node  (btr, btr->root);
        else              bt_free_btreenode(btr, btr->root); 
        btr->root  = NULL;
    }
    bt_free_btree(btr);
}
void bt_release(bt *ibtr, bt_n *n) { /* dont destroy data, just btree */
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            bt_release(ibtr, NODES(ibtr, n)[i]);
        }
    }
    bt_free_btreenode(ibtr, n); /* memory management in btr */
}
void bt_to_bt_insert(bt *nbtr, bt *obtr, bt_n *n) {
    for (int i = 0; i < n->n; i++) {
        void *be = KEYS(obtr, n, i);
        bt_insert(nbtr, be);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            bt_to_bt_insert(nbtr, obtr, NODES(obtr, n)[i]);
        }
    }
}

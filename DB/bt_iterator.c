/* B-tree Implementation.
 *
 * This file implements in memory b-tree tables with insert/del/replace/find/
 * operations.

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
#include <float.h>
#include <assert.h>

#include "bt.h"
#include "stream.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "bt_iterator.h"

#define FFLUSH fflush(NULL);
#define DUMP_CURR_KEY \
  { void *curr = KEYS(iter->btr, iter->bln->self, iter->bln->ik); \
    aobj  key; convertStream2Key(curr, &key, iter->btr);          \
    printf("ik: %d\n", iter->bln->ik); dumpAobj(printf, &key); } FFLUSH

bt_ll_n *get_new_iter_child(btIterator *iter) {//printf("get_new_iter_child\n");
    assert(iter->num_nodes < MAX_BTREE_DEPTH);
    bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
    bzero(nn, sizeof(bt_ll_n));
    iter->num_nodes++;
    return nn;
}

// ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR
void become_child(btIterator *iter, bt_n* self) { // printf("become_child\n");
    iter->depth++;
    iter->bln->child->parent = iter->bln;
    iter->bln->child->ik     = 0;
    iter->bln->child->in     = 0;
    iter->bln->child->self   = self;
    iter->bln                = iter->bln->child;
}
static bool advance_node(btIterator *iter, bool recursed) {
    if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
    bool good = 0;
    if (recursed) { if((iter->bln->in + 1) <  iter->bln->self->n) good = 1; }
    else          { if((iter->bln->in + 1) <= iter->bln->self->n) good = 1; }
    if (good)     { iter->bln->in++; return 1; }
    return 0;
}
static void iter_to_parent_recurse(btIterator *iter) { 
    if (!iter->bln->parent) { iter->finished = 1; return; } /* finished */
    iter->depth--;
    bt   *btr    = iter->btr;
    void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
    iter->bln    = iter->bln->parent;                      /* -> parent */
    void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
    int   x      = btr->cmp(child, parent);
    if (x > 0) {
        if (!advance_node(iter, 1)) iter_to_parent_recurse(iter);/*right-leaf*/
    }
}
static void iter_leaf(btIterator *iter) { // LEAF (n means numkeys)
    if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
    else                                          iter_to_parent_recurse(iter);
}
static void become_child_recurse(btIterator *iter, bt_n* self) {
    become_child(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        if (!iter->bln->child) iter->bln->child = get_new_iter_child(iter);
        bt *btr = iter->btr;
        become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
    }
}
static void iter_node(btIterator *iter) {
    advance_node(iter, 0);
    bt *btr = iter->btr;
    if (!iter->bln->child) { iter->bln->child = get_new_iter_child(iter); }
    become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
}

void *btNext(btIterator *iter) { //printf("btNext "); DUMP_CURR_KEY
    if (iter->finished) return NULL;
    void *curr = KEYS(iter->btr, iter->bln->self, iter->bln->ik);
    if (iter->bln->self->leaf) (*iter->iLeaf)(iter);
    else                       (*iter->iNode)(iter);
    return curr;
}

// DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR
void become_child_rev(btIterator *iter, bt_n* self) {
    iter->depth++;
    iter->bln->child->parent = iter->bln;
    iter->bln->child->ik     = self->n - 1;
    iter->bln->child->in     = self->n;
    iter->bln->child->self   = self;
    iter->bln                = iter->bln->child;
}
static void become_child_recurse_rev(btIterator *iter, bt_n* self) {
    become_child_rev(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        if (!iter->bln->child) iter->bln->child = get_new_iter_child(iter);
        bt *btr = iter->btr;
        become_child_recurse_rev(iter,
                                 NODES(btr, iter->bln->self)[iter->bln->in]);
    }
}
static bool retreat_node(btIterator *iter, bool recursed) {
    if (recursed) {
        if (iter->bln->ik) iter->bln->ik--;
        if (iter->bln->in) { iter->bln->in--; return 1;}
        else                                  return 0;
    } else {
        if (iter->bln->in == iter->bln->self->n) iter->bln->in--;
        return 1;
    }
}
static void iter_to_parent_recurse_rev(btIterator *iter) { 
    if (!iter->bln->parent) { iter->finished = 1; return; } /* finished */
    iter->depth--;
    bt   *btr    = iter->btr;
    void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
    iter->bln    = iter->bln->parent;                      /* -> parent */
    void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
    int   x      = btr->cmp(child, parent);
    if (x < 0) {
        if (!retreat_node(iter, 1)) iter_to_parent_recurse_rev(iter);
    }
}
static void iter_leaf_rev(btIterator *iter) {
    if (iter->bln->ik) iter->bln->ik--;
    else               iter_to_parent_recurse_rev(iter);
}
static void iter_node_rev(btIterator *iter) {
    retreat_node(iter, 0);
    bt *btr = iter->btr;
    if (!iter->bln->child) iter->bln->child = get_new_iter_child(iter);
    become_child_recurse_rev(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
}

// INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR
static int btIterInit(bt *btr, bt_data_t bkey, struct btIterator *iter) {
    int ret = bt_init_iterator(btr, bkey, iter);
    if (ret) { /* range queries, find nearest match */
        int x = btr->cmp(bkey, KEYS(btr, iter->bln->self, iter->bln->ik));
        if (x > 0) {
            if      (ret == RET_LEAF_EXIT)  btNext(iter); /* find next */
            else if (ret == RET_ONLY_RIGHT) return 0;     /*off end of B-tree */
        }
    }
    return 1;
}

static void init_iter(btIterator  *iter, bt          *btr,
                      iter_single *itl, iter_single *itn) {
    iter->btr         = btr;
    iter->highs       = NULL;
    iter->high        = 0;
    iter->highf       = FLT_MIN;
    iter->iLeaf       = itl;
    iter->iNode       = itn;
    iter->finished    = 0;
    iter->num_nodes   = 0;
    iter->bln         = &(iter->nodes[0]);
    iter->num_nodes++;
    iter->bln->parent = NULL;
    iter->bln->self   = btr->root;
    iter->bln->child  = NULL;
    iter->depth       = 0;
}

static int     WhichIter = 0;
static btSIter BT_Iterators[MAX_NUM_INDICES]; /* avoid malloc()s */

static btSIter *createIterator(bt *btr, iter_single *itl, iter_single *itn) {
    assert(WhichIter >= 0 && WhichIter < MAX_NUM_INDICES);
    btSIter *siter = &BT_Iterators[WhichIter];
    siter->ktype   = btr->s.ktype;
    siter->which   = WhichIter;
    WhichIter++;
    initAobj(&siter->key);
    siter->be.key  = &(siter->key);
    siter->be.val  = NULL;
    init_iter(&siter->x, btr, itl, itn);
    return siter;
}
void btReleaseRangeIterator(btSIter *siter) {
    if (siter) {
        if (siter->x.highs) free(siter->x.highs);        /* FREED 058 */
        siter->x.highs = NULL;
        WhichIter--;
    }
}
static void setHigh(btSIter *siter, aobj *high, uchar ktype) {
    if        (C_IS_S(ktype)) {
        siter->x.highs = malloc(high->len + 1);          /* FREE ME 058 */
        memcpy(siter->x.highs, high->s, high->len);
        siter->x.highs[high->len] = '\0';
    } else if (C_IS_I(ktype)) { siter->x.high  = high->i; }
      else if (C_IS_L(ktype)) { siter->x.high  = high->l; }
      else if (C_IS_F(ktype)) { siter->x.highf = high->f; }
}

btSIter *btGetRangeIter(bt *btr, aobj *alow, aobj *ahigh, bool asc) {
    if (!btr->root || !btr->numkeys) return NULL;
    bool med; uint32 ksize;                 //bt_dumptree(btr, btr->ktype);
    btSIter *siter = createIterator(btr, asc ? iter_leaf : iter_leaf_rev, 
                                         asc ? iter_node : iter_node_rev);

    setHigh(siter, asc ? ahigh : alow, btr->s.ktype);
    char    *bkey  = createBTKey(asc ? alow : ahigh, &med, &ksize, btr); //D032
    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroyBTKey(bkey, med);                             /* DESTROYED 032 */
    return siter;
}
btEntry *btRangeNext(btSIter *siter, bool asc) {
    if (!siter) return NULL;
    void *be = btNext(&(siter->x));
    if (!be)    return NULL;
    convertStream2Key(be, siter->be.key, siter->x.btr);
    siter->be.val = parseStream(be, siter->x.btr);
    if        (C_IS_I(siter->ktype) || C_IS_L(siter->ktype)) {
        ulong l = C_IS_I(siter->ktype) ? (ulong)(siter->key.i) : siter->key.l;
        if (l == siter->x.high)  siter->x.finished = 1;       /* exact match */
        return asc ? ((l <= siter->x.high) ?  &(siter->be) : NULL) :
                     ((l >= siter->x.high) ?  &(siter->be) : NULL);
    } else if (C_IS_F(siter->ktype)) {
        float f = siter->key.f;
        if (f == siter->x.highf) siter->x.finished = 1;       /* exact match */
        return asc ? ((f <= siter->x.highf) ? &(siter->be) : NULL) :
                     ((f >= siter->x.highf) ? &(siter->be) : NULL);
    } else { /* COL_TYPE_STRING */
        int r = strncmp(siter->key.s, siter->x.highs, siter->key.len);
        if (r == 0)              siter->x.finished = 1;       /* exact match */
        return asc ? ((r <= 0) ?              &(siter->be) : NULL) : 
                     ((r >= 0) ?              &(siter->be) : NULL);
    }
}

// FULL_BTREE_ITERATOR FULL_BTREE_ITERATOR FULL_BTREE_ITERATOR
bool assignMinKey(bt *btr, aobj *key) { //TODO combine w/ btIterInit()
    void *e = bt_min(btr);              //      iter can be initialised
    if (!e) return 0;                   //      w/ this lookup
    convertStream2Key(e, key, btr);
    return 1;
}
bool assignMaxKey(bt *btr, aobj *key) {
    void *e = bt_max(btr);
    if (!e) return 0;
    convertStream2Key(e, key, btr);
    return 1;
}

btSIter *btGetFullRangeIter(bt *btr, bool asc) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL) || !assignMaxKey(btr, &aH)) return NULL;
    bool med; uint32 ksize;
    btSIter *siter = createIterator(btr, asc ? iter_leaf : iter_leaf_rev,
                                         asc ? iter_node : iter_node_rev);
    setHigh(siter, asc ? &aH : &aL, btr->s.ktype);
    char    *bkey  = createBTKey(asc ? &aL : &aH, &med, &ksize, btr); //DEST 030
    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroyBTKey(bkey, med);                             /* DESTROYED 030 */
    releaseAobj(&aL); releaseAobj(&aH);
    return siter;
}

/* FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER */
typedef struct three_longs {
    long l1; long l2; long l3;
} tl_t;
static void iter_leaf_cnt(btIterator *iter) {
    long cnt  = (long)(iter->bln->self->n - iter->bln->ik);
    tl_t *tl  = (tl_t *)iter->data;
    tl->l3    = tl->l2 - tl->l1;
    //printf("LEAF: l1: %ld l2: %ld l3: %ld cn: %ld\n",
    //          tl->l1, tl->l2, tl->l3, cnt);
    tl->l1   += cnt; /* "count += n */
    if (tl->l1 >= tl->l2) return;

    /* move to end of block */
    iter->bln->ik = iter->bln->self->n ? iter->bln->self->n - 1 : 0;
    iter_to_parent_recurse(iter);
}
static void iter_node_cnt(btIterator *iter) {
    tl_t *tl = (tl_t *)iter->data;
    tl->l1++;/* "count++" */
    tl->l3   = tl->l2 - tl->l1;
    //printf("NODE: l1: %ld l2: %ld l3: %ld\n", tl->l1, tl->l2, tl->l3);
    if (tl->l1 > tl->l2) return;
    advance_node(iter, 0);
    bt *btr  = iter->btr;
    if (!iter->bln->child) { iter->bln->child = get_new_iter_child(iter); }
    become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
}
static btSIter *XthIteratorFind(btSIter *siter, aobj *alow, long x, bt *btr) {
    bool med; uint32 ksize;
    char *bkey = createBTKey(alow, &med, &ksize, btr);   /* DESTROY ME 031 */
    if (!bkey) return NULL;
    bt_init_iterator(btr, bkey, &(siter->x));
    destroyBTKey(bkey, med);                             /* DESTROYED 031 */

    tl_t tl;
    tl.l1         = 0; /* count */
    tl.l2         = x; /* offset */
    tl.l3         = 0; /* final difference */
    siter->x.data = &tl;
    while (1) {
        void *be = btNext(&(siter->x));
        if (!be) break;
        //printf("cnt: %d x: %ld\n", tl.l1, tl.l2);
        if (tl.l1 >= tl.l2) {
            //printf("leftover: %ld ik: %d\n", tl.l3, siter->x.bln->ik);
            siter->x.bln->ik += tl.l3;
            //printf("ik: %d n1: %d\n",siter->x.bln->ik, siter->x.bln->self->n);
            if (siter->x.bln->ik == siter->x.bln->self->n) {
                //printf("XthIteratorFind iter_to_parent_recurse\n");
                iter_to_parent_recurse(&(siter->x));
            }
            break;
        }
    }
    /* reset to normal iterators */
    siter->x.iNode = iter_node;
    siter->x.iLeaf = iter_leaf;
    return siter;
}
btSIter *btGetXthIter(bt *btr, aobj *alow, aobj *ahigh, long x) {
    if (!btr->root || !btr->numkeys) return NULL;
    btSIter *siter = createIterator(btr, iter_leaf_cnt, iter_node_cnt);
    setHigh(siter, ahigh, btr->s.ktype);
    return XthIteratorFind(siter, alow, x, btr);
}
btSIter *btGetFullXthIter(bt *btr, long x) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL) || !assignMaxKey(btr, &aH)) return NULL;
    btSIter *siter = createIterator(btr, iter_leaf_cnt, iter_node_cnt);
    setHigh(siter, &aH, btr->s.ktype);
    siter          = XthIteratorFind(siter, &aL, x, btr);
    releaseAobj(&aL); releaseAobj(&aH);
    return siter;
}

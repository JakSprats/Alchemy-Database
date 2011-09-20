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
    printf("ik: %d key: ", iter->bln->ik); dumpAobj(printf, &key); } FFLUSH
#define DEBUG_ITER_NODE \
  printf("iter_node: ik: %d in: %d\n", iter->bln->ik, iter->bln->in);

#define GET_NEW_CHILD(iter) \
 if (!iter->bln->child) { iter->bln->child = get_new_iter_child(iter); }

bt_ll_n *get_new_iter_child(btIterator *iter) { //printf("get_newiterchild\n");
    assert(iter->num_nodes < MAX_BTREE_DEPTH);
    bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
    bzero(nn, sizeof(bt_ll_n));
    iter->num_nodes++;
    return nn;
}

// ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR
void become_child(btIterator *iter, bt_n* self) {  //printf("become_child\n");
    iter->depth++;
    iter->bln->child->parent = iter->bln;
    iter->bln->child->ik     = 0;
    iter->bln->child->in     = 0;
    iter->bln->child->self   = self;
    iter->bln                = iter->bln->child;
}
static void iter_to_parent_recurse(btIterator *iter) {  //printf("to_parent\n");
    if (!iter->bln->parent) { iter->finished = 1; return; } /* finished */
    iter->depth--;
    bt   *btr    = iter->btr;
    void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
    iter->bln    = iter->bln->parent;                      /* -> parent */
    void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
    int   x      = btr->cmp(child, parent);
    if (x > 0) {
        if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
        if ((iter->bln->in + 1) < iter->bln->self->n) iter->bln->in++;
        else iter_to_parent_recurse(iter);/*right-leaf*/
    }
}
static void iter_leaf(btIterator *iter) { //printf("iter_leaf\n");
    if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
    else                                          iter_to_parent_recurse(iter);
}
static void become_child_recurse(btIterator *iter, bt_n* self) {
    become_child(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        GET_NEW_CHILD(iter)
        bt *btr = iter->btr;
        become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
    }
}
static void iter_node(btIterator *iter) { //DEBUG_ITER_NODE
    if ((iter->bln->ik + 1) <  iter->bln->self->n) iter->bln->ik++;
    if ((iter->bln->in + 1) <= iter->bln->self->n) iter->bln->in++;
    GET_NEW_CHILD(iter)
    become_child_recurse(iter,
                         NODES(iter->btr, iter->bln->self)[iter->bln->in]);
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
        GET_NEW_CHILD(iter)
        bt *btr = iter->btr;
        become_child_recurse_rev(iter,
                                 NODES(btr, iter->bln->self)[iter->bln->in]);
    }
}
//TODO break this function out wherever it is called
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
    GET_NEW_CHILD(iter)
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

// SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR
//TODO Reverse Xth Iterator
#define DEBUG_ITER_LEAF_CNT                                          \
  printf("iter_leaf_cnt: key: "); DUMP_CURR_KEY                      \
  printf("LEAF: ik: %d n: %d flcnt: %d cnt: %d ofst: %d\n",          \
         iter->bln->ik, iter->bln->self->n, fl->cnt, cnt, fl->ofst);
#define DEBUG_ITER_NODE_CNT                                          \
  printf("iter_node_cnt\n");                                         \
  printf("kid_in: %d kid: %p in: %d\n", kid_in, kid, iter->bln->in); \
  printf("scioned: %d scion: %d diff: %d cnt: %d ofst: %d\n",        \
          scioned, kid->scion, fl->diff, fl->cnt, fl->ofst);
#define DEBUG_SCION_FIND \
  printf("btScionFind: PRE _LOOP: ofst: %d key: ", ofst); \
  btIterator *iter = &siter->x; DUMP_CURR_KEY

typedef struct four_longs {
    long cnt; long ofst; long diff; long over;
} fol_t;
static void iter_leaf_cnt(btIterator *iter) {
    long   cnt     = (long)(iter->bln->self->n - iter->bln->ik);
    fol_t *fl      = (fol_t *)iter->data; //DEBUG_ITER_LEAF_CNT
    if (fl->cnt + cnt >= fl->ofst) { fl->over = fl->ofst - fl->cnt; return; }
    fl->cnt       += cnt;
    fl->diff       = fl->ofst - fl->cnt;
    iter->bln->ik  = iter->bln->self->n - 1; /* move to end of block */
    iter->bln->in  = iter->bln->self->n;
    iter_to_parent_recurse(iter);
}
static void next_node_or_parent(btIterator *iter) {
    if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
    if ((iter->bln->in + 1) < iter->bln->self->n) iter->bln->in++;
    else                                          iter_to_parent_recurse(iter);
}
static void iter_node_cnt(btIterator *iter) {
    fol_t *fl      = (fol_t *)iter->data;
    uchar  kid_in  = (iter->bln->in == iter->bln->self->n) ? iter->bln->in :
                                                             iter->bln->in + 1;
    bt_n  *kid     = NODES(iter->btr, iter->bln->self)[kid_in];
    bool   scioned = (fl->diff > kid->scion); //DEBUG_ITER_NODE_CNT
    if (scioned) {
        fl->cnt  += kid->scion + 1; // +1 for NODE itself
        fl->diff  = fl->ofst - fl->cnt;
        next_node_or_parent(iter);
        if (!fl->diff) { fl->over = 0; return; }
    } else {
        fl->cnt++;
        fl->diff  = fl->ofst - fl->cnt;
        if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
        iter->bln->in++;
        GET_NEW_CHILD(iter)
        become_child_recurse(iter,
                             NODES(iter->btr, iter->bln->self)[iter->bln->in]);
        if (!fl->diff) { fl->over = 0; return; }
    }
}
static bool XthIteratorFind(btSIter *siter, aobj *alow, long ofst, bt *btr) {
    bool med; uint32 ksize;
    char *bkey = createBTKey(alow, &med, &ksize, btr);   /* DESTROY ME 031 */
    if (!bkey) return 0;
    bt_init_iterator(btr, bkey, &(siter->x));
    destroyBTKey(bkey, med);                             /* DESTROYED 031 */
    siter->x.iNode = iter_node_cnt; siter->x.iLeaf = iter_leaf_cnt;//SCION ItR8r
    fol_t fl; bzero(&fl, sizeof(fol_t)); fl.ofst = ofst; fl.over = -1;
    siter->x.data = &fl;
    while (1) {
        void *be = btNext(&(siter->x));
        if (!be) break;
        if (fl.over != -1) {
            //printf("ik: %d over: %d\n", siter->x.bln->ik, fl.over);
            siter->x.bln->ik += fl.over;
            if (siter->x.bln->ik == siter->x.bln->self->n) {
                siter->x.bln->ik--; iter_to_parent_recurse(&(siter->x));
            }
            break;
        }
    }
    siter->x.iNode = iter_node; siter->x.iLeaf = iter_leaf; // normal ItR8r
    return 1;
}
btSIter *btGetXthIter(bt *btr, aobj *alow, aobj *ahigh, long ofst) {
    if (!btr->root || !btr->numkeys) return NULL;
    btSIter *siter = createIterator(btr, iter_leaf_cnt, iter_node_cnt);
    setHigh(siter, ahigh, btr->s.ktype);
    if (!XthIteratorFind(siter, alow, ofst, btr)) return NULL;
    return siter;
}

static long btScionFind(btSIter *siter, bt_n *x, long ofst, bt *btr) {
    // DEBUG_SCION_FIND
    for (int i = 0; i <= x->n; i++) {
        uint32_t scion = NODES(btr, x)[i]->scion;
        //printf("%d: ofst: %ld scion: %d\n", i, ofst, scion);
        if (scion == ofst) {
            siter->x.bln->in = i;
            siter->x.bln->ik = (i == siter->x.bln->self->n) ? i - 1 : i;
            return 0;
        } else if (scion > ofst) { //printf("MAKE CHILD: i: %d\n", i);
            siter->x.bln->in = i;
            siter->x.bln->ik = (i == siter->x.bln->self->n) ? i - 1 : i;
            siter->x.bln->child = get_new_iter_child(&siter->x);
            become_child(&siter->x, NODES(btr, x)[i]);
            bt_n *kid = NODES(btr, x)[i];
            if (!kid->leaf) {
                return btScionFind(siter, kid, ofst, btr);
            }
            break;
        } else ofst -= (scion + 1); // +1 for NODE itself
    } //printf("btScionFind: POST_LOOP: ofst: %d\n", ofst);
    siter->x.bln->ik = ofst;
    return 0;
}
btSIter *btGetFullXthIter(bt *btr, long ofst) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL) || !assignMaxKey(btr, &aH)) return NULL;
    bool asc = 1;
    btSIter *siter = createIterator(btr, asc ? iter_leaf : iter_leaf_rev, 
                                         asc ? iter_node : iter_node_rev);
    setHigh(siter, &aH, btr->s.ktype);
    btScionFind(siter, btr->root, ofst, btr);
    releaseAobj(&aL); releaseAobj(&aH);
    //btIterator *iter = &siter->x; DUMP_CURR_KEY
    return siter;
}

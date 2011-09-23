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

#define DUMP_CURR_KEY                                                \
  { void *curr = KEYS(iter->btr, iter->bln->self, iter->bln->ik);    \
    aobj  key; convertStream2Key(curr, &key, iter->btr);             \
    printf("ik: %d key: ", iter->bln->ik); dumpAobj(printf, &key); } \
    fflush(NULL);

// HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES HELPER_DEFINES
#define GET_NEW_CHILD(iter) \
 if (!iter->bln->child) { iter->bln->child = get_new_iter_child(iter); }

#define CR8ITER8R(btr, asc, l, lrev, n, nrev) \
  btSIter *siter = createIterator(btr, asc ? l : lrev, asc ? n : nrev);

bt_ll_n *get_new_iter_child(btIterator *iter) { //printf("get_newiterchild\n");
    assert(iter->num_nodes < MAX_BTREE_DEPTH);
    bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
    bzero(nn, sizeof(bt_ll_n));
    iter->num_nodes++;
    return nn;
}

// ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR ASC_ITERATOR
#define DEBUG_ITER_NODE \
  printf("iter_node: ik: %d in: %d\n", iter->bln->ik, iter->bln->in);

void to_child(btIterator *iter, bt_n* self) {  //printf("to_child\n");
    iter->depth++;
    iter->bln->child->parent = iter->bln;
    iter->bln->child->ik     = 0;
    iter->bln->child->in     = 0;
    iter->bln->child->self   = self;
    iter->bln                = iter->bln->child;
}
static void toparentrecurse(btIterator *iter) {  //printf("to_parent\n");
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
        else                                          toparentrecurse(iter);
    }
}
static void iter_leaf(btIterator *iter) { //printf("iter_leaf\n");
    if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
    else                                          toparentrecurse(iter);
}
static void tochildrecurse(btIterator *iter, bt_n* self) {
    to_child(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        GET_NEW_CHILD(iter)
        tochildrecurse(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
    }
}
static void iter_node(btIterator *iter) { //DEBUG_ITER_NODE
    if ((iter->bln->ik + 1) <  iter->bln->self->n) iter->bln->ik++;
    if ((iter->bln->in + 1) <= iter->bln->self->n) iter->bln->in++;
    GET_NEW_CHILD(iter)
    tochildrecurse(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
}

void *btNext(btIterator *iter) { //printf("btNext "); DUMP_CURR_KEY
    if (iter->finished) return NULL;
    void *curr = KEYS(iter->btr, iter->bln->self, iter->bln->ik);
    if (iter->bln->self->leaf) (*iter->iLeaf)(iter);
    else                       (*iter->iNode)(iter);
    return curr;
}

// DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR DESC_ITERATOR
#define DEBUG_ITER_NODE_REV                                   \
    printf("iter_node_rev: ik: %d in: %d self.n: %d\n",       \
           iter->bln->ik, iter->bln->in, iter->bln->self->n);
#define DEBUG_TOPARENTRECURSEREV \
  printf("toparentrecurserev: ik: %d in: %d\n", iter->bln->ik, iter->bln->in);

void to_child_rev(btIterator *iter, bt_n* self) {
    iter->depth++;
    iter->bln->child->parent = iter->bln;
    iter->bln->child->ik     = self->n - 1;
    iter->bln->child->in     = self->n;
    iter->bln->child->self   = self;
    iter->bln                = iter->bln->child;
}
static void tochildrecurserev(btIterator *iter, bt_n* self) {
    to_child_rev(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        GET_NEW_CHILD(iter)
        tochildrecurserev(iter,
                          NODES(iter->btr, iter->bln->self)[iter->bln->in]);
    }
}
static void toparentrecurserev(btIterator *iter) {  //DEBUG_TOPARENTRECURSEREV
    if (!iter->bln->parent) { iter->finished = 1; return; } /* finished */
    iter->depth--;
    bt   *btr    = iter->btr;
    void *child  = KEYS(btr, iter->bln->self, iter->bln->ik);
    iter->bln    = iter->bln->parent;                      /* -> parent */
    void *parent = KEYS(btr, iter->bln->self, iter->bln->ik);
    int   x      = btr->cmp(child, parent);
    if (x < 0) {
        if (iter->bln->ik) iter->bln->ik--;
        if (iter->bln->in) iter->bln->in--;
        else               toparentrecurserev(iter);
    }
    if (iter->bln->in == iter->bln->self->n) iter->bln->in--;
}
static void iter_leaf_rev(btIterator *iter) { //printf("iter_leaf_rev\n");
    if (iter->bln->ik) iter->bln->ik--;
    else               toparentrecurserev(iter);
}
static void iter_node_rev(btIterator *iter) { //DEBUG_ITER_NODE_REV
    GET_NEW_CHILD(iter)
    tochildrecurserev(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
}

// INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR INIT_ITERATOR
static int btIterInit(bt *btr, bt_data_t bkey, struct btIterator *iter) {
    int ret = bt_init_iterator(btr, bkey, iter);
    if (ret) { /* range queries, find nearest match */
        int x = btr->cmp(bkey, KEYS(btr, iter->bln->self, iter->bln->ik));
        if (x > 0) {
            if      (ret == RET_LEAF_EXIT)  btNext(iter); // find next
            else if (ret == RET_ONLY_RIGHT) return 0;     // off end of B-tree
        }
    }
    return 1;
}
static void init_iter(btIterator  *iter, bt          *btr,
                      iter_single *itl, iter_single *itn) {
    iter->btr         = btr;
    iter->highs       = NULL; iter->high  = 0; iter->highf = FLT_MIN;
    iter->iLeaf       = itl;  iter->iNode = itn;
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
    siter->which   = WhichIter; WhichIter++;
    initAobj(&siter->key);
    siter->be.key  = &(siter->key); siter->be.val = NULL;
    init_iter(&siter->x, btr, itl, itn);
    return siter;
}
void btReleaseRangeIterator(btSIter *siter) {
    if (siter) {
        if (siter->x.highs) free(siter->x.highs);        /* FREED 058 */
        siter->x.highs = NULL; WhichIter--;
    }
}

// COMMON_ASC_DESC_ITERATOR COMMON_ASC_DESC_ITERATOR COMMON_ASC_DESC_ITERATOR
static void setHigh(btSIter *siter, aobj *high, uchar ktype) {
    if        (C_IS_S(ktype)) {
        siter->x.highs            = malloc(high->len + 1);    /* FREE ME 058 */
        memcpy(siter->x.highs, high->s, high->len);
        siter->x.highs[high->len] = '\0';
    } else if (C_IS_I(ktype)) { siter->x.high  = high->i; }
      else if (C_IS_L(ktype)) { siter->x.high  = high->l; }
      else if (C_IS_F(ktype)) { siter->x.highf = high->f; }
}

btSIter *btGetRangeIter(bt *btr, aobj *alow, aobj *ahigh, bool asc) {
    if (!btr->root || !btr->numkeys) return NULL;
    bool med; uint32 ksize;                 //bt_dumptree(btr, btr->ktype);
    CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
    setHigh(siter, asc ? ahigh : alow, btr->s.ktype);
    char    *bkey  = createBTKey(asc ? alow : ahigh, &med, &ksize, btr); //D032
    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter); siter = NULL;
    }
    destroyBTKey(bkey, med);                                /* DESTROYED 032 */
    return siter;
}
btEntry *btRangeNext(btSIter *siter, bool asc) {
    if (!siter) return NULL;
    void *be = btNext(&(siter->x)); if (!be) return NULL;
    convertStream2Key(be, siter->be.key, siter->x.btr);
    siter->be.val    = parseStream(be, siter->x.btr);
    siter->be.stream = be;
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
bool assignMinKey(bt *btr, aobj *key) {       //TODO combine w/ btIterInit()
    void *e = bt_min(btr); if (!e) return 0;  //      iter can be initialised
    convertStream2Key(e, key, btr); return 1; //      w/ this lookup
}
bool assignMaxKey(bt *btr, aobj *key) {
    void *e = bt_max(btr); if (!e) return 0;
    convertStream2Key(e, key, btr); return 1;
}
btSIter *btGetFullRangeIter(bt *btr, bool asc) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL) || !assignMaxKey(btr, &aH)) return NULL;
    bool med; uint32 ksize;
    CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
    setHigh(siter, asc ? &aH : &aL, btr->s.ktype);
    char *bkey  = createBTKey(asc ? &aL : &aH, &med, &ksize, btr); //DEST 030
    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter); siter = NULL;
    }
    destroyBTKey(bkey, med);                             /* DESTROYED 030 */
    releaseAobj(&aL); releaseAobj(&aH);
    return siter;
}

// SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR SCION_ITERATOR
#define DEBUG_ITER_LEAF_SCION                                          \
  printf("iter_leaf_scion: key: "); DUMP_CURR_KEY                      \
  printf("LEAF: ik: %d n: %d flcnt: %d cnt: %d ofst: %d\n",          \
         iter->bln->ik, iter->bln->self->n, fl->cnt, cnt, fl->ofst);
#define DEBUG_ITER_NODE_SCION                                          \
  printf("iter_node_scion\n");                                         \
  printf("kid_in: %d kid: %p in: %d\n", kid_in, kid, iter->bln->in); \
  printf("scioned: %d scion: %d diff: %d cnt: %d ofst: %d\n",        \
          scioned, kid->scion, fl->diff, fl->cnt, fl->ofst);

typedef struct four_longs {
    long cnt; long ofst; long diff; long over;
} fol_t;

static void iter_leaf_scion(btIterator *iter) {
    fol_t *fl      = (fol_t *)iter->data; //DEBUG_ITER_LEAF_SCION
    long   cnt     = (long)(iter->bln->self->n - iter->bln->ik);
    if (fl->cnt + cnt >= fl->ofst) { fl->over = fl->ofst - fl->cnt; return; }
    fl->cnt       += cnt;
    fl->diff       = fl->ofst - fl->cnt;
    iter->bln->ik  = iter->bln->self->n - 1; /* move to end of block */
    iter->bln->in  = iter->bln->self->n;
    toparentrecurse(iter);
}
static void iter_node_scion(btIterator *iter) {
    fol_t *fl      = (fol_t *)iter->data;
    uchar  kid_in  = (iter->bln->in == iter->bln->self->n) ? iter->bln->in :
                                                             iter->bln->in + 1;
    bt_n  *kid     = NODES(iter->btr, iter->bln->self)[kid_in];
    bool   scioned = (fl->diff > kid->scion); //DEBUG_ITER_NODE_SCION
    if (scioned) {
        fl->cnt  += kid->scion + 1; // +1 for NODE itself
        fl->diff  = fl->ofst - fl->cnt;
        if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
        if ((iter->bln->in + 1) < iter->bln->self->n) iter->bln->in++;
        else                                          toparentrecurse(iter);
        if (!fl->diff) { fl->over = 0; return; }
    } else {
        fl->cnt++;
        fl->diff  = fl->ofst - fl->cnt;
        if ((iter->bln->ik + 1) < iter->bln->self->n) iter->bln->ik++;
        iter->bln->in++;
        GET_NEW_CHILD(iter)
        tochildrecurse(iter, NODES(iter->btr, iter->bln->self)[iter->bln->in]);
        if (!fl->diff) { fl->over = 0; return; }
    }
}

// REV_SCION_ITERATOR REV_SCION_ITERATOR REV_SCION_ITERATOR REV_SCION_ITERATOR
#define DEBUG_ITER_LEAF_SCION_REV                                    \
  printf("iter_leaf_scion_rev: key: "); DUMP_CURR_KEY                \
  printf("LEAF: ik: %d n: %d flcnt: %d cnt: %d ofst: %d\n",          \
         iter->bln->ik, iter->bln->self->n, fl->cnt, cnt, fl->ofst);
#define DEBUG_ITER_NODE_SCION_REV                                    \
  printf("iter_node_scion_rev: key: "); DUMP_CURR_KEY                \
  printf("ik: %d in: %d selfn: %d scioned: %d scion: %d"             \
         " diff: %d cnt: %d ofst: %d\n",                             \
          iter->bln->ik, iter->bln->in, iter->bln->self->n, scioned, \
          kid->scion, fl->diff, fl->cnt, fl->ofst);

static void iter_leaf_scion_rev(btIterator *iter) {
    fol_t *fl      = (fol_t *)iter->data; //DEBUG_ITER_LEAF_SCION_REV
    long   cnt     = (long)(iter->bln->ik + 1); // +1 for KEY itself
    if (fl->cnt + cnt >= fl->ofst) { fl->over = fl->ofst - fl->cnt; return; }
    fl->cnt       += cnt;
    fl->diff       = fl->ofst - fl->cnt;
    iter->bln->ik  = iter->bln->in = 0; /* move to start of block */
    toparentrecurserev(iter);
}
static void iter_node_scion_rev(btIterator *iter) {
    fol_t *fl      = (fol_t *)iter->data;
    bt_n  *kid     = NODES(iter->btr, iter->bln->self)[iter->bln->in];
    bool   scioned = (fl->diff > kid->scion); //DEBUG_ITER_NODE_SCION_REV
    if (scioned) {
        fl->cnt  += kid->scion + 1; // +1 for NODE itself
        fl->diff  = fl->ofst - fl->cnt;
        if (iter->bln->ik) iter->bln->ik--;
        if (iter->bln->in) iter->bln->in--;
        else               toparentrecurserev(iter);
        if (!fl->diff) { fl->over = 0; return; }
    } else {
        fl->cnt++;
        fl->diff  = fl->ofst - fl->cnt;
        GET_NEW_CHILD(iter)
        tochildrecurserev(iter,
                          NODES(iter->btr, iter->bln->self)[iter->bln->in]);
        if (!fl->diff) { fl->over = 0; return; }
    } //printf("END iter_node_scion_rev: key: "); DUMP_CURR_KEY
}

// COMMON_SCION_ITERATOR COMMON_SCION_ITERATOR COMMON_SCION_ITERATOR
static bool XthIterFind(btSIter *siter, aobj *astart, long ofst, bool asc) {
    bool med; uint32 ksize;
    btIterator *iter = &siter->x;
    char       *bkey = createBTKey(astart, &med, &ksize, iter->btr); // DEST 031
    if (!bkey) return 0;
    bt_init_iterator(iter->btr, bkey, iter); destroyBTKey(bkey, med);//DESTD 031
    fol_t fl; bzero(&fl, sizeof(fol_t)); fl.ofst = ofst; fl.over = -1;
    iter->data = &fl;
    while (1) {
        void *be = btNext(iter); if (!be) break;
        if (fl.over != -1) {//printf("ik: %d ovr: %d\n", iter->bln->ik,fl.over);
            if (asc) {
                iter->bln->ik += fl.over;
                if (iter->bln->ik == iter->bln->self->n) {
                    iter->bln->ik--; toparentrecurse(iter);
                }
            } else {
                iter->bln->ik -= fl.over;
                if (iter->bln->ik < 0) {
                    iter->bln->in = iter->bln->ik = 0; toparentrecurserev(iter);
                }
            }
            break;
        }
    } //printf("END XthIterfind: in: %d key: ", iter->bln->in); DUMP_CURR_KEY
    iter->iLeaf = asc ? iter_leaf : iter_leaf_rev; // NORMAL ITERATORS
    iter->iNode = asc ? iter_node : iter_node_rev;
    return 1;
}
btSIter *btGetXthIter(bt *btr, aobj *alow, aobj *ahigh, long ofst, bool asc) {
    if (!btr->root || !btr->numkeys) return NULL;
    CR8ITER8R(btr, asc, iter_leaf_scion, iter_leaf_scion_rev,
                        iter_node_scion, iter_node_scion_rev);
    setHigh(siter, asc ? ahigh : alow, btr->s.ktype);
    if (!XthIterFind(siter, asc ? alow : ahigh, ofst, asc)) return NULL;
    return siter;
}

#define DEBUG_SCION_FIND \
  printf("btScionFind: PRE _LOOP: asc: %d ofst: %d key: ", asc, ofst); \
  btIterator *iter = &siter->x; DUMP_CURR_KEY
#define DEBUG_SCION_FIND_END \
  printf("btScionFind: POST_LOOP: ofst: %d ik: %d\n", ofst, siter->x.bln->ik);
static void btScionFind(btSIter *siter, bt_n *x, long ofst, bt *btr, bool asc) {
    int beg = asc ? 0        : x->n;                         //DEBUG_SCION_FIND
    int fin = asc ? x->n + 1 : -1;   // LOOPS: (i=0,i<=x-n) & (i=x->n,i>= 0)
    int i   = beg;
    while (i != fin) { //printf("%d: ofst: %ld scion: %d\n", i, ofst, scion);
        uint32_t scion = NODES(btr, x)[i]->scion;
        if (scion >= ofst) { //printf("MAKE CHILD: i: %d\n", i);
            bool i_end_n = (i == siter->x.bln->self->n);
            siter->x.bln->in = i;
            siter->x.bln->ik = (i_end_n) ? i - 1 : i;
            if (scion == ofst) {
                if (!asc) { siter->x.bln->in = siter->x.bln->ik = i - 1; }
                return;
            }
            siter->x.bln->child = get_new_iter_child(&siter->x);
            to_child(&siter->x, NODES(btr, x)[i]);
            bt_n *kid = NODES(btr, x)[i];
            if (!kid->leaf) { btScionFind(siter, kid, ofst, btr, asc); return; }
            break;
        } else ofst -= (scion + 1); // +1 for NODE itself
        i = asc ? i + 1 : i - 1; // loop increment
    }
    if (asc) siter->x.bln->ik = ofst;
    else     siter->x.bln->ik = siter->x.bln->self->n - 1 - ofst;
    return;                                              //DEBUG_SCION_FIND_END
}
btSIter *btGetFullXthIter(bt *btr, long ofst, bool asc) {
    if (!btr->root || !btr->numkeys || ofst > btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL) || !assignMaxKey(btr, &aH)) return NULL;
    CR8ITER8R(btr, asc, iter_leaf, iter_leaf_rev, iter_node, iter_node_rev);
    setHigh(siter, asc ? &aH : &aL, btr->s.ktype);
    btScionFind(siter, btr->root, ofst, btr, asc);
    releaseAobj(&aL); releaseAobj(&aH);
    return siter;
}

/* B-tree Implementation.
 *
 * This file implements in memory b-tree tables with insert/del/replace/find/
 * operations.

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.

 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "redis.h"
#include "bt.h"
#include "bt_iterator.h"
#include "parser.h"
#include "common.h"

/* GLOBALS */
#define RL4 redisLog(4,
#define RL7 if (iter->which == 0) redisLog(4,

bt_ll_n *get_new_iter_child(btIterator *iter) {
    assert(iter->num_nodes < MAX_BTREE_DEPTH);
    bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
    iter->num_nodes++;
    nn->child   = NULL;
    return nn;
}

void become_child(btIterator *iter, bt_n* self) {
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
    if (recursed) { if((iter->bln->in + 1) < iter->bln->self->n)  good = 1; }
    else          { if((iter->bln->in + 1) <= iter->bln->self->n) good = 1; }
    if (good) {
        iter->bln->in++;
        return 1;
    }
    return 0;
}

static void iter_to_parent_recurse(btIterator *iter) {
    if (!iter->bln->parent) {
        iter->finished = 1;                                        // -> exit
        return;
    }
    iter->depth--;
    struct btree *btr = iter->btr;
    void *child       = KEYS(btr, iter->bln->self)[iter->bln->ik];
    iter->bln         = iter->bln->parent;                        // -> parent
    void *parent      = KEYS(btr, iter->bln->self)[iter->bln->ik];
    int   x           = btr->cmp(child, parent);
    if (x > 0) {
        if (!advance_node(iter, 1)) {
            iter_to_parent_recurse(iter);                     // right-most-leaf
        }
    }
}

static void iter_leaf(btIterator *iter) {
    if ((iter->bln->ik + 1) < iter->bln->self->n) { // LEAF (n means numkeys)
        iter->bln->ik++;
    } else {
        iter_to_parent_recurse(iter);
    }
}

static void become_child_recurse(btIterator *iter, bt_n* self) {
    become_child(iter, self);
    if (!iter->bln->self->leaf) { // depth-first
        if (!iter->bln->child) iter->bln->child = get_new_iter_child(iter);
        struct btree *btr = iter->btr;
        become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
    }
}

static void iter_node(btIterator *iter) {
    advance_node(iter, 0);
    struct btree *btr = iter->btr;

    if (!iter->bln->child) {
        iter->bln->child = get_new_iter_child(iter);
    }
    become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
}

void *btNext(btIterator *iter) {
    if (iter->finished) return NULL;

    //RL4 "btNext: leaf: %d", iter->bln->self->leaf);
    struct btree *btr  = iter->btr;
    void         *curr = KEYS(btr, iter->bln->self)[iter->bln->ik];
    if (iter->bln->self->leaf) (*iter->iLeaf)(iter);
    else                       (*iter->iNode)(iter);
    return curr;
}

static int btIterInit(bt *btr, bt_data_t simkey, struct btIterator *iter) {
    int ret = bt_init_iterator(btr, simkey, iter);
    if (ret) { /* range queries, find nearest match */
        int x = btr->cmp(simkey, KEYS(btr, iter->bln->self)[iter->bln->ik]);
        if (x > 0) {
            if (ret == RET_LEAF_EXIT)  { /* find next */
                btNext(iter);
            } else if (ret == RET_ONLY_RIGHT) { /* off the end of the B-tree */
                return 0;
            }
        }
    }
    return 1;
}

/* Currently: BT_Iterators[2] would work UNTIL parallel joining is done, then MAX_NUM_INDICES is needed */
static btSIter BT_Iterators[MAX_NUM_INDICES]; /* avoid malloc()s */

static void init_iter(btIterator  *iter,
                      bt          *btr,
                      iter_single *itl,
                      iter_single *itn) {
    iter->btr         = btr;
    iter->highc       = NULL;
    iter->iLeaf       = itl;
    iter->iNode       = itn;
    iter->finished    = 0;
    iter->num_nodes   = 0;
    iter->bln         = &(iter->nodes[iter->num_nodes]);
    iter->num_nodes++;
    iter->bln->parent = NULL;
    iter->bln->self   = btr->root;
    iter->bln->child  = NULL;
    iter->depth       = 0;
}

static btSIter *createIterator(bt          *btr,
                               bool         virt,
                               int          which,
                               iter_single *itl,
                               iter_single *itn) {
    assert(which >= 0 || which < MAX_NUM_INDICES);
    btSIter *siter = &BT_Iterators[which];

    siter->ktype   = (unsigned char)btr->ktype;
    siter->vtype   = (unsigned char)(virt ? REDIS_ROW : REDIS_BTREE);
    siter->which   = which;
    siter->key.ptr = NULL;
    siter->be.key  = &(siter->key);
    siter->val.ptr = NULL;
    siter->be.val  = &(siter->val);

    init_iter(&siter->x, btr, itl, itn);
    return siter;
}

static void setHigh(btSIter *siter, robj *high, uchar ktype, bool robjed) {
    char *s = (char *)(high->ptr);
    if      (ktype == COL_TYPE_STRING) siter->x.highc = _strdup(s);
    else if (ktype == COL_TYPE_INT)    siter->x.high  = robjed ? (long)s :
                                                                 atoi(s);
    else if (ktype == COL_TYPE_FLOAT)  siter->x.highf = atof(s);
}

//NOTE: can not NEST this function
btSIter *btGetRangeIterator(robj *o, robj *low, robj *high, bool virt) {
    struct btree     *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    btSIter *siter = createIterator(btr, virt, 0, iter_leaf, iter_node);
    setHigh(siter, high, btr->ktype, 0);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKey(low, btr->ktype, &med, &sflag, &ksize); /*FREE*/
    if (!simkey) return NULL;
    if (!btIterInit(btr, simkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroySimKey(simkey, med);                                      /* freeD */

    return siter;
}

btEntry *btRangeNext(btSIter *siter) {
    if (!siter) return NULL;
    destroyAssignKeyRobj(&(siter->key)); /* previous assignKeyRobj */

    void *be = btNext(&(siter->x));
    if (!be) return NULL;
    assignKeyRobj(be,               siter->be.key);
    assignValRobj(be, siter->vtype, siter->be.val, siter->x.btr->is_index);

    char *k  = (char *)(((robj *)siter->be.key)->ptr);

    if (       siter->ktype == COL_TYPE_INT) {
        unsigned long l = (unsigned long)(k);
        //if (l <= siter->high) RL4 "I: btRangeNext: %p key:%ld", be, l);
        if (l == siter->x.high) siter->x.finished = 1; /* exact match of high */
        return ((l <= siter->x.high) ? &(siter->be) : NULL);
    } else if (siter->ktype == COL_TYPE_FLOAT) {
        float f = atof(k);
        //if (f <= siter->x.highf) RL4 "F: btRangeNext: %p key:%f", be, f);
        if (f == siter->x.highf) siter->x.finished = 1; /* exact match highf */
        return ((f <= siter->x.highf) ? &(siter->be) : NULL);
    } else if (siter->ktype == COL_TYPE_STRING) {
        int r = strcmp(k, siter->x.highc);
        //if (r <= 0) RL4 "S: btRangeNext: %p key: %s", be, k);
        if (r == 0) siter->x.finished = 1; /* exact match of the high */
        return ((r <= 0) ? &(siter->be) : NULL);
    }
    return NULL; /* never happens */
}

static robj BtL, BtH;

bool assignMinKey(bt *btr, robj *key) {
    void *e  = bt_min(btr);
    if (!e) return 0;
    assignKeyRobj(e, key);
    return 1;
}
bool assignMaxKey(bt *btr, robj *key) {
    void *e  = bt_max(btr);
    if (!e) return 0;
    assignKeyRobj(e, key);
    return 1;
}

//TODO this function and its global vars are hideous - clean this up
//NOTE: can not NEST this function
btSIter *btGetFullRangeIterator(robj *o, bool virt) {
    struct btree *btr  = (struct btree *)(o->ptr);
    if (!btr->root) return NULL;
    if (!assignMinKey(btr, &BtL)) return NULL;
    if (!assignMaxKey(btr, &BtH)) return NULL;

    btSIter *siter = createIterator(btr, virt, 1, iter_leaf, iter_node);
    setHigh(siter, &BtH, btr->ktype, 1);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = /* FREE me*/
               createSimKeyFromRaw(BtL.ptr, btr->ktype, &med, &sflag, &ksize);
    destroyAssignKeyRobj(&BtH);
    destroyAssignKeyRobj(&BtL);

    if (!simkey) return NULL;
    if (!btIterInit(btr, simkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroySimKey(simkey, med);                                    /* freeD */

    return siter;
}

void btReleaseRangeIterator(btSIter *siter) {
    if (siter) {
        destroyAssignKeyRobj(&(siter->key));
        if (siter->x.highc) free(siter->x.highc);
        siter->x.highc = NULL;
    }
}

/* FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER */
/* FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER */
typedef struct three_longs {
    long l1;
    long l2;
    long l3;
} tl_t;

static void iter_leaf_count(btIterator *iter) {
    long cnt  = (long)(iter->bln->self->n - iter->bln->ik);
    tl_t *tl  = (tl_t *)iter->data;
    tl->l3    = tl->l2 - tl->l1;
    //RL4 "LEAF: l1: %ld l2: %ld l3: %ld cn: %ld", tl->l1, tl->l2, tl->l3, cnt);
    tl->l1   += cnt; /* "count += n */
    if (tl->l1 >= tl->l2) return;

    iter->bln->ik = iter->bln->self->n - 1; /* move to end of block */
    iter_to_parent_recurse(iter);
}

static void iter_node_count(btIterator *iter) {
    tl_t *tl = (tl_t *)iter->data;
    tl->l1++;/* "count++" */
    tl->l3    = tl->l2 - tl->l1;
    //RL4 "NODE: l1: %ld l2: %ld l3: %ld", tl->l1, tl->l2, tl->l3);
    if (tl->l1 > tl->l2) return;

    advance_node(iter, 0);
    struct btree *btr = iter->btr;

    if (!iter->bln->child) {
        iter->bln->child = get_new_iter_child(iter);
    }
    become_child_recurse(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
}

static btSIter *XthIteratorFind(btSIter *siter, robj *low, long x, bt *btr) {
    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKey(low, btr->ktype, &med, &sflag, &ksize); /*FREE*/
    if (!simkey) return NULL;
    bt_init_iterator(btr, simkey, &(siter->x));
    destroySimKey(simkey, med);                                /* freeD */

    tl_t tl;
    tl.l1         = 0; /* count */
    tl.l2         = x; /* offset */
    tl.l3         = 0; /* final difference */
    siter->x.data = &tl;
    while (1) {
        void *be = btNext(&(siter->x));
        if (!be) break;
        //RL4 "cnt: %d x: %ld", tl.l1, tl.l2);
        if (tl.l1 >= tl.l2) {
            //RL4 "leftover: %ld ik: %d", tl.l3, siter->x.bln->ik);
            siter->x.bln->ik += tl.l3;
            //RL4 "ik: %d n1: %d", siter->x.bln->ik, siter->x.bln->self->n);
            if (siter->x.bln->ik == siter->x.bln->self->n) {
                //RL4 "XthIteratorFind iter_to_parent_recurse");
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

btSIter *btGetIteratorXth(robj *o, robj *low, robj *high, long x, bool virt) {
    bt *btr  = (struct btree *)(o->ptr);
    if (!btr->root) return NULL;
    btSIter *siter = createIterator(btr, virt, 1,
                                    iter_leaf_count, iter_node_count);
    setHigh(siter, high, btr->ktype, 0);
    return XthIteratorFind(siter, low, x, btr);
}

//NOTE: can not NEST this function
btSIter *btGetFullIteratorXth(robj *o, long x, bool virt) {
    bt *btr  = (struct btree *)(o->ptr);
    if (!btr->root) return NULL;
    if (!assignMinKey(btr, &BtL)) return NULL;
    if (!assignMaxKey(btr, &BtH)) return NULL;

    btSIter *siter = createIterator(btr, virt, 1,
                                    iter_leaf_count, iter_node_count);
    setHigh(siter, &BtH, btr->ktype, 1);
    siter = XthIteratorFind(siter, &BtL, x, btr);

    destroyAssignKeyRobj(&BtH);
    destroyAssignKeyRobj(&BtL);

    return siter;
}

// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
static btIterator JoinIterator; /* avoid malloc()s */

static btIterator *createJoinIterator(bt *btr) {
    btIterator *iter  = &JoinIterator;
    init_iter(iter, btr, iter_leaf, iter_node);
    return iter;
}

static btIterator *btGetJoinRangeIterator(bt           *btr,
                                          joinRowEntry *low, 
                                          joinRowEntry *high, 
                                          int           ktype) {
    btIterator *iter = createJoinIterator(btr);
    if (!btIterInit(btr, low, iter)) {
        btReleaseJoinRangeIterator(iter);
        return NULL;
    }
    //TODO setHigh()
    robj *hkey = high->key;
    if      (ktype == COL_TYPE_STRING) iter->highc = _strdup(hkey->ptr);
    else if (ktype == COL_TYPE_INT)    iter->high  = (int)(long)(hkey->ptr);
    else if (ktype == COL_TYPE_FLOAT)  iter->highf = atof(hkey->ptr);
    return iter;
}

btIterator *btGetJoinFullRangeIterator(bt *btr, int ktype) {
    joinRowEntry *low  = bt_min(btr);
    joinRowEntry *high = bt_max(btr);
    return btGetJoinRangeIterator(btr, low, high, ktype);
}

joinRowEntry *btJoinRangeNext(btIterator *iter, int ktype) {
    if (!iter) return NULL;
    joinRowEntry *jre = btNext(iter);
    if (!jre) return NULL;

    char *k = (char *)(jre->key->ptr);
    if (       ktype == COL_TYPE_INT) {
        unsigned long l = (unsigned long)(k);
        //if (l <= iter->high) RL4 "I: btJoinRangeNext: %p key:%ld", k, l);
        if (l == iter->high) iter->finished = 1; /* exact match of high */
        return ((l <= iter->high) ? jre : NULL);
    } else if (ktype == COL_TYPE_STRING) {
        int r = strcmp(k, iter->highc);
        //if (r <= 0) RL4 "S: btJoinRangeNext: %p key: %s", k, k);
        if (r == 0) iter->finished = 1; /* exact match of the high */
        return ((r <= 0) ? jre : NULL);
    } else if (ktype == COL_TYPE_FLOAT) {
        float f = atof(k);
        if (f == iter->highf) iter->finished = 1; /* exact match of high */
        return ((f <= iter->highf) ? jre : NULL);
    }
    return NULL; /* never happens */
}

void btReleaseJoinRangeIterator(btIterator *iter) {
    if (iter) {
        if (iter->highc) free(iter->highc);
        iter->highc = NULL;
    }
}


#if 0
/* used for DEBUGGING the Btrees innards */
void btreeCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    robj *o    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr  = (bt *)o->ptr;
    bool  virt = 1;
    bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    addReply(c, shared.ok);
}
#endif

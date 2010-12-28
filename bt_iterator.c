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
#include <float.h>
#include <assert.h>

#include "redis.h"

#include "bt.h"
#include "parser.h"
#include "stream.h"
#include "aobj.h"
#include "common.h"
#include "bt_iterator.h"

/* GLOBALS */
#define RL4 redisLog(4,
#define RL7 if (iter->which == 0) redisLog(4,

bt_ll_n *get_new_iter_child(btIterator *iter) {
    assert(iter->num_nodes < MAX_BTREE_DEPTH);
    bt_ll_n *nn = &(iter->nodes[iter->num_nodes]);
    bzero(nn, sizeof(bt_ll_n));
    iter->num_nodes++;
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
        if (!advance_node(iter, 1)) {                        // right-most-leaf
            iter_to_parent_recurse(iter);
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

static int btIterInit(bt *btr, bt_data_t bkey, struct btIterator *iter) {
    int ret = bt_init_iterator(btr, bkey, iter);
    if (ret) { /* range queries, find nearest match */
        int x = btr->cmp(bkey, KEYS(btr, iter->bln->self)[iter->bln->ik]);
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

static void init_iter(btIterator  *iter,
                      bt          *btr,
                      iter_single *itl,
                      iter_single *itn) {
    iter->btr         = btr;
    iter->highs       = NULL;
    iter->high        = 0;
    iter->highf       = FLT_MIN;
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

/* Currently: BT_Iterators[2] would work UNTIL parallel joining is done,
     then MAX_NUM_INDICES is needed */
static btSIter BT_Iterators[MAX_NUM_INDICES]; /* avoid malloc()s */
static btSIter *createIterator(bt          *btr,
                               int          which,
                               iter_single *itl,
                               iter_single *itn) {
    assert(which >= 0 || which < MAX_NUM_INDICES);
    btSIter *siter = &BT_Iterators[which];
    siter->ktype   = (unsigned char)btr->ktype;
    siter->which   = which;
    initAobj(&siter->key);
    siter->be.key  = &(siter->key);
    siter->be.val  = NULL;
    init_iter(&siter->x, btr, itl, itn);
    return siter;
}

static void setHigh(btSIter *siter, aobj *high, uchar ktype) {
    if      (ktype == COL_TYPE_STRING) siter->x.highs = _strdup(high->s);
    else if (ktype == COL_TYPE_INT)    siter->x.high  = high->i;
    else if (ktype == COL_TYPE_FLOAT)  siter->x.highf = high->f;
}

btSIter *btGetRangeIterator(bt *btr, robj *low, robj *high) {
    if (!btr->root || !btr->numkeys) return NULL;
    bool med; uchar sflag; uint32 ksize;
    //bt_dumptree(btr, btr->ktype);
    btSIter *siter = createIterator(btr, 0, iter_leaf, iter_node);
    aobj    *alow  = copyRobjToAobj(low, btr->ktype);  //TODO LAME
    aobj    *ahigh = copyRobjToAobj(high, btr->ktype); //TODO LAME
    setHigh(siter, ahigh, btr->ktype);
    char *bkey = createBTKey(alow, btr->ktype, &med, &sflag, &ksize);/*FREE*/
    destroyAobj(ahigh);                                //TODO LAME
    destroyAobj(alow);                                 //TODO LAME

    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroyBTKey(bkey, med);                                      /* freeD */

    return siter;
}

btEntry *btRangeNext(btSIter *siter) {
    if (!siter) return NULL;
    void *be = btNext(&(siter->x));
    if (!be)    return NULL;
    convertStream2Key(be, siter->be.key);
    siter->be.val = parseStream(be, siter->x.btr->btype);
    if (       siter->ktype == COL_TYPE_INT) {
        ulong l = (ulong)(siter->key.i);
        if (l == siter->x.high)  siter->x.finished = 1;       /* exact match */
        return ((l <= siter->x.high) ?  &(siter->be) : NULL);
    } else if (siter->ktype == COL_TYPE_FLOAT) {
        float f = siter->key.f;
        if (f == siter->x.highf) siter->x.finished = 1;       /* exact match */
        return ((f <= siter->x.highf) ? &(siter->be) : NULL);
    } else {                /* COL_TYPE_STRING */
        int r = strncmp(siter->key.s, siter->x.highs, siter->key.len);
        if (r == 0)              siter->x.finished = 1;       /* exact match */
        return ((r <= 0) ?              &(siter->be) : NULL);
    }
}

bool assignMinKey(bt *btr, aobj *key) {
    void *e = bt_min(btr);
    if (!e) return 0;
    convertStream2Key(e, key);
    return 1;
}
bool assignMaxKey(bt *btr, aobj *key) {
    void *e = bt_max(btr);
    if (!e) return 0;
    convertStream2Key(e, key);
    return 1;
}
btSIter *btGetFullRangeIterator(bt *btr) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL)) return NULL;
    if (!assignMaxKey(btr, &aH)) return NULL;
    bool med; uchar sflag; uint32 ksize;

    btSIter *siter = createIterator(btr, 1, iter_leaf, iter_node);
    setHigh(siter, &aH, btr->ktype);
    char    *bkey  = createBTKey(&aL, btr->ktype, &med, &sflag, &ksize);/*FREE*/
    if (!bkey) return NULL;
    if (!btIterInit(btr, bkey, &(siter->x))) {
        btReleaseRangeIterator(siter);
        siter = NULL;
    }
    destroyBTKey(bkey, med);                                       /* FREED */

    return siter;
}

void btReleaseRangeIterator(btSIter *siter) {
    if (siter) {
        if (siter->x.highs) free(siter->x.highs);
        siter->x.highs = NULL;
    }
}

/* FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER */
/* FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER FIND_N_ITER */
typedef struct three_longs {
    long l1;
    long l2;
    long l3;
} tl_t;

static void iter_leaf_cnt(btIterator *iter) {
    long cnt  = (long)(iter->bln->self->n - iter->bln->ik);
    tl_t *tl  = (tl_t *)iter->data;
    tl->l3    = tl->l2 - tl->l1;
    //RL4 "LEAF: l1: %ld l2: %ld l3: %ld cn: %ld", tl->l1, tl->l2, tl->l3, cnt);
    tl->l1   += cnt; /* "count += n */
    if (tl->l1 >= tl->l2) return;

    /* move to end of block */
    iter->bln->ik = iter->bln->self->n ? iter->bln->self->n - 1 : 0;
    iter_to_parent_recurse(iter);
}

static void iter_node_cnt(btIterator *iter) {
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

static btSIter *XthIteratorFind(btSIter *siter, aobj *alow, long x, bt *btr) {
    bool med; uchar sflag; unsigned int ksize;
    char *bkey = createBTKey(alow, btr->ktype, &med, &sflag, &ksize);/*FREE*/
    if (!bkey) return NULL;
    bt_init_iterator(btr, bkey, &(siter->x));
    destroyBTKey(bkey, med);                                /* freeD */

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

btSIter *btGetIteratorXth(bt *btr, robj *low, robj *high, long x) {
    if (!btr->root || !btr->numkeys) return NULL;
    btSIter *siter = createIterator(btr, 1, iter_leaf_cnt, iter_node_cnt);
    aobj    *alow  = copyRobjToAobj(low,  btr->ktype); //TODO LAME
    aobj    *ahigh = copyRobjToAobj(high, btr->ktype); //TODO LAME
    setHigh(siter, ahigh, btr->ktype);
    destroyAobj(ahigh);                                //TODO LAME
    siter          = XthIteratorFind(siter, alow, x, btr);
    destroyAobj(alow);                                 //TODO LAME
    return siter;
}

btSIter *btGetFullIteratorXth(bt *btr, long x) {
    if (!btr->root || !btr->numkeys) return NULL;
    aobj aL, aH;
    if (!assignMinKey(btr, &aL)) return NULL;
    if (!assignMaxKey(btr, &aH)) return NULL;

    btSIter *siter = createIterator(btr, 1, iter_leaf_cnt, iter_node_cnt);
    setHigh(siter, &aH, btr->ktype);
    return XthIteratorFind(siter, &aL, x, btr);
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
    if      (ktype == COL_TYPE_STRING) iter->highs = _strdup(hkey->ptr);
    else if (ktype == COL_TYPE_INT)    iter->high  = (int)(long)(hkey->ptr);
    else if (ktype == COL_TYPE_FLOAT)  iter->highf = atof(hkey->ptr);
    return iter;
}

btIterator *btGetJoinFullRangeIterator(bt *btr, int ktype) {
    if (!btr->root || !btr->numkeys) return NULL;
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
        int r = strcmp(k, iter->highs);
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
        if (iter->highs) free(iter->highs);
        iter->highs = NULL;
    }
}

#if 0
/* used for DEBUGGING the Btrees innards */
void btreeCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    robj *btt  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr  = (bt *)btt->ptr;
    bt_dumptree(btr, btr->ktype);
    addReply(c, shared.ok);
}
#endif

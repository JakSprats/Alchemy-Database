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
    if (good ) {
        iter->bln->in++;
        return 1;
    }
    return 0;
}

static void iter_to_parent(btIterator *iter) {
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
            iter_to_parent(iter);                            // right-most-leaf
        }
    }
}

static void iter_leaf(btIterator *iter) {
    if ((iter->bln->ik + 1) < iter->bln->self->n) { // LEAF (n means numkeys)
        iter->bln->ik++;
    } else {
        iter_to_parent(iter);
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

    struct btree *btr  = iter->btr;
    void         *curr = KEYS(btr, iter->bln->self)[iter->bln->ik];
    if (iter->bln->self->leaf) iter_leaf(iter);
    else                       iter_node(iter);
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

static void init_iter(btIterator *iter, bt *btr) {
    iter->btr         = btr;
    iter->highc       = NULL;
    iter->finished    = 0;
    iter->num_nodes   = 0;
    iter->bln         = &(iter->nodes[iter->num_nodes]);
    iter->num_nodes++;
    iter->bln->parent = NULL;
    iter->bln->self   = btr->root;
    iter->bln->child  = NULL;
    iter->depth       = 0;
}

static btSIter *createIterator(bt *btr, bool virt, int which) {
    assert(which >= 0 || which < MAX_NUM_INDICES);
    btSIter *iter = &BT_Iterators[which];

    iter->ktype   = (unsigned char)btr->ktype;
    iter->vtype   = (unsigned char)(virt ? REDIS_ROW : REDIS_BTREE);
    iter->which   = which;
    iter->key.ptr = NULL;
    iter->be.key  = &(iter->key);
    iter->val.ptr = NULL;
    iter->be.val  = &(iter->val);

    init_iter(&iter->x, btr);
    return iter;
}

//NOTE: can not NEST this function
btSIter *btGetRangeIterator(robj *o, void *low, void *high, bool virt) {
    int which = 0;
    struct btree     *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    btSIter *iter = createIterator(btr, virt, which);

    char *s = (char *)(((robj *)high)->ptr);
    if      (btr->ktype == COL_TYPE_STRING) iter->x.highc = _strdup(s);
    else if (btr->ktype == COL_TYPE_INT)    iter->x.high  = atoi(s);
    else if (btr->ktype == COL_TYPE_FLOAT)  iter->x.highf = atof(s);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKey(low, btr->ktype, &med, &sflag, &ksize); /*FREE*/
    if (!simkey) return NULL;
    if (!btIterInit(btr, simkey, &(iter->x))) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                      /* freeD */

    return iter;
}

btEntry *btRangeNext(btSIter *iter, bool asc) {
    asc = 0; /* compiler warning */
    if (!iter) return NULL;
    if (iter->key.ptr && iter->ktype == COL_TYPE_STRING) {
        sdsfree(iter->key.ptr); /* free previous assignKeyRobj sflag[1,4] */
        iter->key.ptr = NULL;
    }

    void *be = btNext(&(iter->x));
    if (!be) return NULL;
    assignKeyRobj(be,              iter->be.key);
    assignValRobj(be, iter->vtype, iter->be.val, iter->x.btr->is_index);

    char *k  = (char *)(((robj *)iter->be.key)->ptr);

    if (       iter->ktype == COL_TYPE_INT) {
        unsigned long l = (unsigned long)(k);
        //if (l <= iter->high) RL4 "I: btRangeNext: %p key:%ld", be, l);
        if (l == iter->x.high) iter->x.finished = 1; /* exact match of high */
        return ((l <= iter->x.high) ? &(iter->be) : NULL);
    } else if (iter->ktype == COL_TYPE_FLOAT) {
        float f = atof(k);
        //if (f <= iter->x.highf) RL4 "F: btRangeNext: %p key:%f", be, f);
        if (f == iter->x.highf) iter->x.finished = 1; /* exact match of highf */
        return ((f <= iter->x.highf) ? &(iter->be) : NULL);

    } else if (iter->ktype == COL_TYPE_STRING) {
        int r = strcmp(k, iter->x.highc);
        //if (r <= 0) RL4 "S: btRangeNext: %p key: %s", be, k);
        if (r == 0) iter->x.finished = 1; /* exact match of the high */
        return ((r <= 0) ? &(iter->be) : NULL);
    }
    return NULL; /* never happens */
}

static robj BtLow, BtHigh;

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
btSIter *btGetFullRangeIterator(robj *o, bool asc, bool virt) {
    asc = 0; /* compiler warning */
    struct btree *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    if (!assignMinKey(btr, &BtLow))  return NULL;
    if (!assignMaxKey(btr, &BtHigh)) return NULL;

    btSIter *iter = createIterator(btr, virt, 1);
    if      (btr->ktype == COL_TYPE_STRING) iter->x.highc = _strdup(BtHigh.ptr);
    else if (btr->ktype == COL_TYPE_INT)    iter->x.high  = (long)BtHigh.ptr;
    else if (btr->ktype == COL_TYPE_FLOAT)  iter->x.highf = atof(BtHigh.ptr);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = /* FREE me*/
               createSimKeyFromRaw(BtLow.ptr, btr->ktype, &med, &sflag, &ksize);
    if (btr->ktype == COL_TYPE_STRING) {
        sdsfree(BtHigh.ptr); /* free assignKeyRobj sflag[1,4] */
        BtHigh.ptr = NULL;
        sdsfree(BtLow.ptr); /* free assignKeyRobj sflag[1,4] */
        BtLow.ptr  = NULL;
    }

    if (!simkey) return NULL;
    if (!btIterInit(btr, simkey, &(iter->x))) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                    /* freeD */

    return iter;
}

void btReleaseRangeIterator(btSIter *iter) {
    if (iter) {
        if (iter->key.ptr && iter->ktype == COL_TYPE_STRING) {
            sdsfree(iter->key.ptr); /* free previous assignKeyRobj sflag[1,4] */
            iter->key.ptr = NULL;
        }
        if (iter->x.highc) free(iter->x.highc);
        iter->x.highc = NULL;
    }
}

// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
static btIterator JoinIterator; /* avoid malloc()s */

static btIterator *createJoinIterator(bt *btr) {
    btIterator *iter  = &JoinIterator;
    init_iter(iter, btr);
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

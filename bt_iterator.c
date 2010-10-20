/* B-tree Implementation.
 *
 * This file implements in memory b-tree tables with insert/del/replace/find/
 * operations.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "redis.h"
#include "bt.h"
#include "bt_iterator.h"
#include "common.h"

/* GLOBALS */
#define RL4 redisLog(4,
#define RL7 if (iter->which == 0) redisLog(4,

/* Currently: BT_Iterators[2] would work UNTIL parallel joining is done, then MAX_NUM_INDICES is needed */
static btStreamIterator BT_Iterators[MAX_NUM_INDICES]; /* avoid malloc()s */

/* copy of strdup - compiles w/o warnings */
static inline char *_strdup(char *s) {
    int len = strlen(s);
    char *x = malloc(len + 1);
    memcpy(x, s, len);
    x[len]  = '\0';
    return x;
}

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
    struct btree *btr  = iter->btr;
    void *child        = KEYS(btr, iter->bln->self)[iter->bln->ik];
    iter->bln          = iter->bln->parent;                        // -> parent
    void *parent       = KEYS(btr, iter->bln->self)[iter->bln->ik];
    int   x            = btr->cmp(child, parent);
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
        become_child(iter, NODES(btr, iter->bln->self)[iter->bln->in]);
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

int init_iterator(bt *btr, bt_data_t simkey, struct btIterator *iter) {
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

static btStreamIterator *createIterator(bt *btr, bool virt, int which) {
    assert(which >= 0 || which < MAX_NUM_INDICES);
    btStreamIterator *iter = &BT_Iterators[which];

    iter->ktype            = (unsigned char)btr->ktype;
    iter->vtype            = (unsigned char)(virt ? REDIS_ROW : REDIS_BTREE);
    iter->which            = which;
    iter->key.ptr          = NULL;
    iter->be.key           = &(iter->key);
    iter->val.ptr          = NULL;
    iter->be.val           = &(iter->val);

    iter->x.btr            = btr;
    iter->x.highc          = NULL;
    iter->x.finished       = 0;
    iter->x.num_nodes      = 0;
    iter->x.bln            = &(iter->x.nodes[iter->x.num_nodes]);
    iter->x.num_nodes++;
    iter->x.bln->parent    = NULL;
    iter->x.bln->self      = btr->root;
    iter->x.bln->child     = NULL;
    iter->x.depth          = 0;
    return iter;
}

//NOTE: can not NEST this function
btStreamIterator *btGetRangeIterator(robj *o,
                                     void *low,
                                     void *high,
                                     bool  virt) {
    int which = 0;
    struct btree     *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    btStreamIterator *iter = createIterator(btr, virt, which);

    char *s = (char *)(((robj *)high)->ptr);
    if      (btr->ktype == COL_TYPE_STRING) iter->x.highc = _strdup(s);
    else if (btr->ktype == COL_TYPE_INT)    iter->x.high  = atoi(s);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKey(low, btr->ktype, &med, &sflag, &ksize); /*FREE*/
    if (!simkey) return NULL;
    if (!init_iterator(btr, simkey, &(iter->x))) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                      /* freeD */

    return iter;
}

btEntry *btRangeNext(btStreamIterator *iter, bool asc) {
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
btStreamIterator *btGetFullRangeIterator(robj *o, bool asc, bool virt) {
    asc = 0; /* compiler warning */
    struct btree *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    if (!assignMinKey(btr, &BtLow))  return NULL;
    if (!assignMaxKey(btr, &BtHigh)) return NULL;

    btStreamIterator *iter = createIterator(btr, virt, 1);
    if (       btr->ktype == COL_TYPE_STRING) {
        iter->x.highc = _strdup(BtHigh.ptr);
    } else if (btr->ktype == COL_TYPE_INT) {
        //RL4 "GetFullRangeIterator: low: %u high: %u", BtLow.ptr, BtHigh.ptr);
        iter->x.high  = (long)BtHigh.ptr;
    }

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKeyFromRaw(BtLow.ptr,
                                       btr->ktype,
                                       &med,
                                       &sflag,
                                       &ksize); /* FREE me*/
    if (!simkey) return NULL;
    if (!init_iterator(btr, simkey, &(iter->x))) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                    /* freeD */

    return iter;
}

void btReleaseRangeIterator(btStreamIterator *iter) {
    if (iter) {
        if (iter->x.highc) free(iter->x.highc);
        iter->x.highc = NULL;
    }
}

// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
// JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT
static btIterator JoinIterator; /* avoid malloc()s */

static btIterator *createJoinIterator(bt *btr) {
    btIterator *iter  = &JoinIterator;
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
    return iter;
}

btIterator *btGetJoinRangeIterator(bt           *btr,
                                   joinRowEntry *low, 
                                   joinRowEntry *high, 
                                   int           ktype) {
    btIterator *iter = createJoinIterator(btr);
    if (!init_iterator(btr, low, iter)) {
        btReleaseJoinRangeIterator(iter);
        return NULL;
    }
    robj *hkey = high->key;
    //RL4 "hkey->ptr: %p", hkey->ptr);
    if      (ktype == COL_TYPE_STRING) iter->highc = _strdup(hkey->ptr);
    else if (ktype == COL_TYPE_INT)    iter->high  = (int)(long)(hkey->ptr);
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
    }
    return NULL; /* never happens */
}

void btReleaseJoinRangeIterator(btIterator *iter) {
    if (iter) {
        if (iter->highc) free(iter->highc);
        iter->highc = NULL;
    }
}

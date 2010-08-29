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

/* GLOBALS */
#define RL4 redisLog(4,
#define RL7 if (iter->which == 0) redisLog(4,
char *strdup(char *);

/* Currently: BT_Iterators[2] would work UNTIL parallel joining is done, then MAX_NUM_INDICES is needed */
static btIterator BT_Iterators[MAX_NUM_INDICES]; /* avoid malloc()s */

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
        struct btree *btr  = iter->btr;
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

static void *btNext(btIterator *iter) {
    if (iter->finished) return NULL;

    struct btree *btr  = iter->btr;
    void         *curr = KEYS(btr, iter->bln->self)[iter->bln->ik];
    if (iter->bln->self->leaf) iter_leaf(iter);
    else                       iter_node(iter);
    return curr;
}

static int init_iterator(bt *btr, bt_data_t simkey, struct btIterator *iter) {
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

static btIterator *createIterator(bt *btr, bool virt, int which) {
    assert(which >= 0 || which < MAX_NUM_INDICES);
    btIterator *iter = &BT_Iterators[which];
    iter->btr        = btr;
    iter->ktype      = (unsigned char)btr->ktype;
    iter->vtype      = (unsigned char)(virt ? REDIS_ROW : REDIS_BTREE);

    iter->highc      = NULL;
    iter->key.ptr    = NULL;
    iter->be.key     = &(iter->key);
    iter->val.ptr    = NULL;
    iter->be.val     = &(iter->val);

    iter->finished   = 0;
    iter->which      = which;
    iter->num_nodes  = 0;
    iter->bln        = &(iter->nodes[iter->num_nodes]);
    iter->num_nodes++;
    iter->bln->parent = NULL;
    iter->bln->self   = btr->root;
    iter->bln->child  = NULL;
    iter->depth      = 0;
    return iter;
}

//NOTE: can not NEST this function
btIterator *btGetGenericRangeIterator(robj *o,
                                      void *low,
                                      void *high,
                                      bool  virt,
                                      int   which) {
    struct btree *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    btIterator   *iter = createIterator(btr, virt, which);

    char *s = (char *)(((robj *)high)->ptr);
    if (     btr->ktype == COL_TYPE_STRING) iter->highc = strdup(s);
    else if (btr->ktype == COL_TYPE_INT)    iter->high  = atoi(s);

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKey(low, btr->ktype, &med, &sflag, &ksize); /*FREE*/
    if (!simkey) return NULL;
    if (!init_iterator(btr, simkey, iter)) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                      /* freeD */

    return iter;
}
btIterator *btGetRangeIterator(robj *o, void *low, void *high, bool virt) {
    return btGetGenericRangeIterator(o, low, high, virt, 0);
}

btEntry *btRangeNext(btIterator *iter, bool asc) {
    asc = 0; /* compiler warning */
    if (!iter) return NULL;
    if (iter->key.ptr && iter->ktype == COL_TYPE_STRING) {
        sdsfree(iter->key.ptr); /* free previous assignKeyRobj sflag[1,4] */
        iter->key.ptr = NULL;
    }

    void *be = btNext(iter);
    if (!be) return NULL;
    assignKeyRobj(be,              iter->be.key);
    assignValRobj(be, iter->vtype, iter->be.val, iter->btr->is_index);

    char *k  = (char *)(((robj *)iter->be.key)->ptr);

    if (       iter->ktype == COL_TYPE_INT) {
        unsigned long l = (unsigned long)(k);
        //if (l <= iter->high) RL4 "I: btRangeNext: %p key:%ld", be, l);
        if (l == iter->high) iter->finished = 1; /* exact match of the high */
        return ((l <= iter->high) ? &(iter->be) : NULL);
    } else if (iter->ktype == COL_TYPE_STRING) {
        int r = strcmp(k, iter->highc);
        //if (r <= 0) RL4 "S: btRangeNext: %p key: %s", be, k);
        if (r == 0) iter->finished = 1; /* exact match of the high */
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
btIterator *btGetFullRangeIterator(robj *o, bool asc, bool virt) {
    asc = 0; /* compiler warning */
    struct btree *btr  = (struct btree *)(o->ptr);
    //bt_dumptree(btr, btr->ktype, (virt ? REDIS_ROW : REDIS_BTREE));
    if (!assignMinKey(btr, &BtLow))  return NULL;
    if (!assignMaxKey(btr, &BtHigh)) return NULL;

    btIterator   *iter = createIterator(btr, virt, 1);
    if (       btr->ktype == COL_TYPE_STRING) {
        iter->highc = strdup(BtHigh.ptr);
    } else if (btr->ktype == COL_TYPE_INT) {
        //RL4 "GetFullRangeIterator: low: %u high: %u", BtLow.ptr, BtHigh.ptr);
        iter->high  = (long)BtHigh.ptr;
    }

    bool med; uchar sflag; unsigned int ksize;
    char *simkey = createSimKeyFromRaw(BtLow.ptr,
                                       btr->ktype,
                                       &med,
                                       &sflag,
                                       &ksize); /* FREE me*/
    if (!simkey) return NULL;
    if (!init_iterator(btr, simkey, iter)) {
        btReleaseRangeIterator(iter);
        iter = NULL;
    }
    destroySimKey(simkey, med);                                    /* freeD */

    return iter;
}

void btReleaseRangeIterator(btIterator *iter) {
    if (iter) {
        if (iter->highc) free(iter->highc);
        iter->highc = NULL;
    }
}

#if 0
//TODO Update w/ streams (btEntry has been abstracted out)
int btNumRecsRange(bt *btr, void *low, void *high) {
    btEntry e, f;
    e.key = low;
    f.key = high;
    int i = bt_find_closest_slot(btr, btr->root, &e);
    int j = bt_find_closest_slot(btr, btr->root, &f);
    //redisLog(REDIS_NOTICE,"slots %d ... %d\n\n", i, j);
    return (j - i);
}
#endif

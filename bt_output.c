/*-
 * Copyright 1997-1998, 2001 John-Mark Gurney.
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
 *
 *
 */

#include "btreepriv.h"
#include "redis.h"
#include "bt.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#define RL4 redisLog(4,

static void dumpnode(struct btree *btr, struct btreenode *,
                     int ktype, int vtype, int depth);
int treeheight(struct btree *btr);


void bt_dumptree(struct btree *btr, int ktype, int vtype) {
    RL4 "bt_dumptree: %p ktype: %d vtype: %d", btr, ktype, vtype);
    RL4 "numkeys: %d numnodes: %d", btr->numkeys, btr->numnodes);
    RL4 "keyoff: %d  nodeptroff: %d t: %d textra: %d height: %d",
         btr->keyoff, btr->nodeptroff, btr->t, btr->textra, treeheight(btr));

    //bt_treestats(btr);
    if (btr->root && btr->numkeys > 0) dumpnode(btr, btr->root, ktype, vtype, 0);
}

static void dumpnode(struct btree     *btr,
                     struct btreenode *x,
                     int               ktype,
                     int               vtype,
                     int               depth) {
    int i;

    //RL4 "type: %d ptr: %p: leaf: %d, n: %d", ktype, (void *)x, x->leaf, x->n);
    if (!x->leaf) {
        RL4 "%d: NODE: %d", depth, x->n);
    } else {
        RL4 "%d: LEAF: %d", depth, x->n);
    }

    for (i = 0; i < x->n; i++) {
        //RL4 "key_n: %d: %p", i, KEYS(btr, x)[i]);
        void *be = KEYS(btr, x)[i];
        robj key, val;
        robj      *rk = (robj *)(&key);
        robj      *rv = (robj *)(&val);
        assignKeyRobj(be, rk);
        assignValRobj(be, vtype, rv, btr->is_index);

        char      *c  = (char *)(rk->ptr);
        char      *s  = (char *)(rv->ptr);
        if (ktype == COL_TYPE_STRING) {
            RL4 "  S: key: %s: val: %p slot: %d - %p", c, s, i, be);
        } else if (ktype == COL_TYPE_INT) {
            RL4 "  I: key: %u: val: %u slot: %d - %p", c, s, i, be);
        } else {
            RL4 "  F: key: %s: val: %f slot: %d - %p", c, s, i, be);
        }
    }

    if (!x->leaf) {
        depth++;
        for (i = 0; i <= x->n; i++) {
            dumpnode(btr, NODES(btr, x)[i], ktype, vtype, depth);
        }
    }
}

void bt_treestats(struct btree *btr) {
    RL4 "root: %p, keyoff: %d, nodeptroff: %d, " \
        " t: %d, nbits: %d, textra: %d, height: %d, numkeys: %d, numnodes: %d",
        (void *)btr->root, btr->keyoff,
         btr->nodeptroff, btr->t, btr->nbits, btr->textra, treeheight(btr),
         btr->numkeys, btr->numnodes);
}


static int checkbtreenode(struct btree     *btr,
                          struct btreenode *x,
                          void             *kmin,
                          void             *kmax,
                          int               isroot) {
    int i;

    if (x == NULL)
        /* check that the two keys are in order */
        if (btr->cmp(kmin, kmax) >= 0)
            return 0;
        else
            return 1;
    else {
        if (!isroot && (x->n < btr->t - 1 || x->n > 2 * btr->t - 1)) {
            redisLog(REDIS_NOTICE,"node, to few or to many: %d\n", x->n);
            bt_dumptree(btr, 0, 0);
            exit(1);
        }
        /* check subnodes */
        if (x->n == 0 && !x->leaf)
            if (!checkbtreenode(btr, NODES(btr, x)[0], kmin, kmax,
                0))
                return 0;
            else
                return 1;
        else if (x->n == 0 && x->leaf && !isroot) {
            redisLog(REDIS_NOTICE,"leaf with no keys!!\n");
            bt_dumptree(btr, 0, 0);
            if (!checkbtreenode(btr, NULL, kmin, kmax, 0))
                return 0;
            else
                return 1;
        }
        if (!checkbtreenode(btr, NODES(btr, x)[0], kmin,
            KEYS(btr, x)[0], 0))
            return 0;
        for (i = 1; i < x->n; i++)
            if (!checkbtreenode(btr, NODES(btr, x)[i],
                KEYS(btr, x)[i - 1], KEYS(btr, x)[i], 0))
                return 0;
        if (!checkbtreenode(btr, NODES(btr, x)[i], KEYS(btr, x)[i - 1],
            kmax, 0))
            return 0;
    }
    return 1;
}

int bt_checktree(struct btree *btr, void *kmin, void *kmax) {
    return checkbtreenode(btr, btr->root, kmin, kmax, 1);
}

int treeheight(struct btree *btr) {
    struct btreenode *x;
    int ret;

    x = btr->root;
    ret = 0;

    while (!x->leaf) {
        x = NODES(btr, x)[0];
        ret++;
    }

    return ++ret;
}

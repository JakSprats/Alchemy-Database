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


#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "redis.h"

#include "btreepriv.h"
#include "bt.h"
#include "stream.h"

static void dumpnode(bt *btr, bt_n *n, int depth);
int treeheight(bt *btr);

void bt_dump_info(bt *btr) {
    printf("bt_dumptree: %p ktype: %d\n", (void *)btr, btr->ktype);
    printf("numkeys: %d numnodes: %d\n",  btr->numkeys, btr->numnodes);
    printf("keyoff: %d  nodeptroff: %d t: %d textra: %d height: %d\n",
         btr->keyoff, btr->nodeptroff, btr->t, btr->textra, treeheight(btr));
}

void bt_dumptree(bt *btr) {
    bt_dump_info(btr);
    //bt_treestats(btr);
    if (btr->root && btr->numkeys > 0) dumpnode(btr, btr->root, 0);
}

static void dumpnode(bt *btr, bt_n *x, int depth) {
    int i;

    if (!x->leaf) printf("%d: NODE: %d\n", depth, x->n);
    else          printf("%d: LEAF: %d\n", depth, x->n);

    for (i = 0; i < x->n; i++) {
        void *be = KEYS(btr, x, i);
        aobj  key;
        void *rrow;
        convertStream2Key(be, &key, btr);
        rrow = parseStream(be, btr);
        printf("  key: "); dumpAobj(&key);
        printf("  row: %p\n", rrow);
    }

    if (!x->leaf) {
        depth++;
        for (i = 0; i <= x->n; i++) {
            dumpnode(btr, NODES(btr, x)[i], depth);
        }
    }
}

void bt_treestats(bt *btr) {
    printf("root: %p, keyoff: %d, nodeptroff: %d, " \
      " t: %d, nbits: %d, textra: %d, height: %d, numkeys: %d, numnodes: %d\n",
        (void *)btr->root, btr->keyoff,
         btr->nodeptroff, btr->t, btr->nbits, btr->textra, treeheight(btr),
         btr->numkeys, btr->numnodes);
}


static int checkbtreenode(bt *btr, bt_n *x, void *kmin, void *kmax, int isrut) {
    int i;

    if (x == NULL)
        /* check that the two keys are in order */
        if (btr->cmp(kmin, kmax) >= 0) return 0;
        else                           return 1;
    else {
        if (!isrut && (x->n < btr->t - 1 || x->n > 2 * btr->t - 1)) {
            printf("node, to few or to many: %d\n", x->n);
            bt_dumptree(btr);
            exit(1);
        }
        /* check subnodes */
        if (x->n == 0 && !x->leaf)
            if (!checkbtreenode(btr, NODES(btr, x)[0], kmin, kmax, 0)) return 0;
            else                                                       return 1;
        else if (x->n == 0 && x->leaf && !isrut) {
            printf("leaf with no keys!!\n");
            bt_dumptree(btr);
            if (!checkbtreenode(btr, NULL, kmin, kmax, 0)) return 0;
            else                                           return 1;
        }
        if (!checkbtreenode(btr, NODES(btr, x)[0], kmin, KEYS(btr, x, 0), 0))
            return 0;
        for (i = 1; i < x->n; i++)
            if (!checkbtreenode(btr, NODES(btr, x)[i],
                                KEYS(btr, x, i - 1), KEYS(btr, x, i), 0))
                                    return 0;
        if (!checkbtreenode(btr, NODES(btr, x)[i], KEYS(btr, x, i - 1),
                            kmax, 0)) return 0;
    }
    return 1;
}

int bt_checktree(bt *btr, void *kmin, void *kmax) {
    return checkbtreenode(btr, btr->root, kmin, kmax, 1);
}

int treeheight(bt *btr) {
    bt_n *x   = btr->root;
    int   ret = 0;
    while (!x->leaf) {
        x = NODES(btr, x)[0];
        ret++;
    }
    return ++ret;
}


/* used for DEBUGGING the Btrees innards */
#include "alsosql.h"
extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern r_tbl_t Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
void btreeCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    robj *btt  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr  = (bt *)btt->ptr;
    bt_dumptree(btr);
    addReply(c, shared.ok);
}


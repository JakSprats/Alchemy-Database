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

#include "btreepriv.h"
#include "stream.h"
#include "colparse.h"
#include "query.h"
#include "bt.h"

static void dumpnode(printer *prn, bt *btr, bt_n *n, int depth, bool is_index);
int treeheight(bt *btr);

void bt_dump_info(printer *prn, bt *btr) {
    (*prn)("BT t: %d nbits: %d nbyte: %d kbyte: %d "               \
           "ksize: %d koff: %d noff: %d numkeys: %d numnodes: %d " \
           "height: %d btr: %p btype: %d ktype: %d bflag: %d "     \
           "num: %d root: %p\n",
            btr->t, btr->nbits, btr->nbyte, btr->kbyte, btr->s.ksize,
            btr->keyofst, btr->nodeofst, btr->numkeys, btr->numnodes,
            treeheight(btr), (void *)btr,
            btr->s.btype, btr->s.ktype, btr->s.bflag, btr->s.num, btr->root);
    DEBUG_BT_TYPE((*prn), btr);
}

void bt_dumptree(printer *prn, bt *btr, bool is_index) {
    bt_dump_info(prn, btr);
    if (btr->root && btr->numkeys > 0) {
        dumpnode(prn, btr, btr->root, 0, is_index);
    }
    (*prn)("\n");
}

static void dumpnode(printer *prn, bt *btr, bt_n *x, int depth, bool is_index) {
    int i;

    if (!x->leaf) (*prn)("%d: NODE: %d -> (%p)\n", depth, x->n, (void *)x);
    else          (*prn)("%d: LEAF: %d -> (%p)\n", depth, x->n, (void *)x);

    for (i = 0; i < x->n; i++) {
        void *be = KEYS(btr, x, i);
        aobj  key;
        convertStream2Key(be, &key, btr);
        void *rrow = parseStream(be, btr);
        if (is_index) {
            (*prn)("\tINDEX-KEY: "); dumpAobj(prn, &key);
            bt_dumptree(prn, (bt *)rrow, 0);
        } else {
            if        UL(btr) { 
                if UP(btr) (*prn)("\t\tUL: PTR: %p\t", rrow);
                else {
                    ulk *ul = (ulk *)rrow;
                    (*prn)("\t\tUL: KEY: %u  VAL: %lu\t", ul->key, ul->val); 
                }
            } else if LU(btr) { 
                luk *lu = (luk *)rrow;
                (*prn)("\t\tLU: KEY: %lu VAL: %lu\t", lu->key, lu->val);
            } else if LL(btr) { 
                if LP(btr) (*prn)("\t\tLL: PTR: %p\t", rrow);
                else {
                    llk *ll = (llk *)rrow;
                    (*prn)("\t\tLL: KEY: %lu VAL: %lu\t", ll->key, ll->val);
                }
            } else {
                (*prn)("\t\tROW: %p\t", rrow);
            }
            (*prn)("KEY: "); dumpAobj(prn, &key);
        }
    }

    if (!x->leaf) {
        depth++;
        for (i = 0; i <= x->n; i++) {
            dumpnode(prn, btr, NODES(btr, x)[i], depth, is_index);
        }
    }
}

static int checkbtreenode(bt *btr, bt_n *x, void *kmin, void *kmax, int isrut) {
    int i;

    if (x == NULL){ /* check that the two keys are in order */
        if (btr->cmp(kmin, kmax) >= 0) return 0;
        else                           return 1;
    } else {
        if (!isrut && (x->n < btr->t - 1 || x->n > 2 * btr->t - 1)) {
            printf("node, to few or to many: %d\n", x->n);
            bt_dumptree(printf, btr, 0);
            exit(1);
        }
        /* check subnodes */
        if (x->n == 0 && !x->leaf) {
            if (!checkbtreenode(btr, NODES(btr, x)[0], kmin, kmax, 0)) return 0;
            else                                                       return 1;
        } else if (x->n == 0 && x->leaf && !isrut) {
            printf("leaf with no keys!!\n");
            bt_dumptree(printf, btr, 0);
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
    if (!x) return 0;
    int   ret = 0;
    while (!x->leaf) {
        x = NODES(btr, x)[0];
        ret++;
    }
    return ++ret;
}


/* used for DEBUGGING the Btrees innards */
#include "alsosql.h"
#include "index.h"
extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern r_tbl_t Tbl[MAX_NUM_TABLES];
extern r_ind_t Index[MAX_NUM_INDICES];
void btreeCommand(redisClient *c) {
    initQueueOutput();
    printer *prn = queueOutput;
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    MATCH_INDICES(tmatch)
    bt *btr = getBtr(tmatch);
    bt_dumptree(prn, btr, 0); //TODO put in sdsfunc
    if (matches) {
        for (int i = 0; i < matches; i++) {
            int j = inds[i];
            if (!Index[j].virt && !Index[j].luat) {
                bt   *ibtr = getIBtr(j);
                (*prn)("INDEX: %d (%p)\n", inds[i], (void *)ibtr);
                bt_dumptree(prn, ibtr, 1);
            }
        }
    }
    dumpQueueOutput(c);
}

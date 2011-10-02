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

#include "debug.h"
#include "btreepriv.h"
#include "stream.h"
#include "find.h"
#include "query.h"
#include "bt.h"

extern sds DumpOutput;

// PROTOTYPES
static void dumpnode(printer *prn, bt *btr, bt_n *n, int depth,
                     bool is_index, int slot);
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
        dumpnode(prn, btr, btr->root, 0, is_index, 0);
    }
    (*prn)("\n");
}
void dump_node(bt *btr, bt_n *x) {
    printf(" NODE: n: %d scion: %d -> (%p)\n", x->n, x->scion, (void *)x);
    for (int i = 0; i < x->n; i++) {
        void *be = KEYS(btr, x, i);
        aobj  key; convertStream2Key(be, &key, btr);
        printf("KEY: "); dumpAobj(printf, &key);
    }   
}

uint32_t Scion = 0;
static void sum_scion(bt *btr, bt_n *x, int depth, int treeheight) {
    if (!x->leaf) {
        depth++;
        for (int i = 0; i <= x->n; i++) {
            if (!i && depth == treeheight) Scion += x->scion;
            sum_scion(btr, NODES(btr, x)[i], depth, treeheight);
        }
    }
}
void validate_root_scion(bt *btr) {
    printf("validate_root_scion\n"); bt_dump_info(printf, btr);
    if (btr->root && btr->numkeys > 0) {
        Scion = 0;
        uint32_t root_scion = btr->root->scion;
        sum_scion(btr, btr->root, 0, treeheight(btr) - 2);
        printf("root_scion: %d rest_scion: %d\n", root_scion, Scion);
    }
}

static void dumpnode(printer *prn, bt *btr, bt_n *x,
                     int depth, bool is_index, int slot) {
    int i;

#ifdef BTREE_DEBUG
    if (!x->leaf) (*prn)("%d: NODE[%d]: n: %d scion: %d -> (%p) slot: %d\n",
                          depth, x->num, x->n, x->scion, (void *)x, slot);
#else
    slot = 0; // compiler warning
    if (!x->leaf) (*prn)("%d: NODE: n: %d scion: %d -> (%p)\n",
                          depth, x->n, x->scion, (void *)x);
#endif
    else          (*prn)("%d: LEAF: n: %d -> (%p)\n", depth, x->n, (void *)x);

    for (i = 0; i < x->n; i++) {
        void *be = KEYS(btr, x, i);
        aobj  key;
        convertStream2Key(be, &key, btr);
        void *rrow = parseStream(be, btr);
        if (is_index) {
            (*prn)("\tINDEX-KEY: "); dumpAobj(prn, &key);
            bt_dumptree(prn, (bt *)rrow, 0);
        } else {
            //TODO UU & LUP
            if        UL(btr) { 
                if UP(btr) (*prn)("\t\tUL: PTR: %p\t", rrow);
                else {
                    ulk *ul = (ulk *)rrow;
                    (*prn)("\t\tUL[%d]: KEY: %u VAL: %lu\t",
                                    i, ul->key, ul->val); 
                }
            } else if LU(btr) { 
                luk *lu = (luk *)rrow;
                (*prn)("\t\tLU[%d]: KEY: %lu VAL: %lu\t",
                                i, lu->key, lu->val);
            } else if LL(btr) { 
                if LP(btr) (*prn)("\t\tLL: PTR: %p\t", rrow);
                else {
                    llk *ll = (llk *)rrow;
                    (*prn)("\t\tLL[%d]: KEY: %lu VAL: %lu\t",
                                    i, ll->key, ll->val);
                }
            } else {
                (*prn)("\t\tROW[%d]: %p\t", i, rrow);
            }
            (*prn)("KEY: "); dumpAobj(prn, &key);
        }
    }

    if (!x->leaf) {
        depth++;
        for (i = 0; i <= x->n; i++) {
            dumpnode(prn, btr, NODES(btr, x)[i], depth, is_index, i);
        }
    }
}

static int checkbtreenode(bt *btr, bt_n *x, void *kmin, void *kmax, int isrut) {
    if (x == NULL){ /* check that the two keys are in order */
        if (btr->cmp(kmin, kmax) >= 0) return 0;
        else                           return 1;
    } else {
        if (!isrut && (x->n < btr->t - 1 || x->n > 2 * btr->t - 1)) {
            printf("node, to few or to many: %d\n", x->n);
            bt_dumptree(printf, btr, 0); exit(1);
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
        int i;
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
extern r_tbl_t *Tbl;
extern r_ind_t *Index;
void btreeCommand(redisClient *c) {
    initQueueOutput();
    printer *prn = queueOutput;
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    MATCH_INDICES(tmatch)
    bt   *btr   = getBtr(tmatch);
    char *fname = NULL;
   if (c->argc > 4 && (!strcasecmp(c->argv[2]->ptr, "TO") &&
                       !strcasecmp(c->argv[3]->ptr, "FILE"))) {
       fname = c->argv[4]->ptr;
    }

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
    if (fname) {
        FILE *fp    = NULL;
        if((fp = fopen(fname, "w")) == NULL) return;
        fwrite(DumpOutput, sdslen(DumpOutput), 1, fp);
        fclose(fp);
        addReply(c, shared.ok);
    }
    else dumpQueueOutput(c);
}

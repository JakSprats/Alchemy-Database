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

AGPL License

Copyright (c) 2010-2011 Russell Sullivan <jaksprats AT gmail DOT com>
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

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "debug.h"
#include "aobj.h"
#include "btreepriv.h"
#include "stream.h"
#include "find.h"
#include "query.h"
#include "bt.h"

extern sds DumpOutput;

// PROTOTYPES
static void dump_tree_node(printer *prn, bt *btr, bt_n *x, int depth,
                           bool is_index, int slot, bool is_inode);
static int treeheight(bt *btr);

#define PRINT_EVICTED_KEYS

void printKey(bt *btr, bt_n *x, int i) {
    if (i < 0 || i >= x->n) printf(" NO KEY\n");
    else {
        aobj akey; void *be = KEYS(btr, x, i);
        //printf("btr: %p x: %p i: %d be: %p\n", btr, x, i, be);
        convertStream2Key(be, &akey, btr);
        dumpAobj(printf, &akey);
    }
}

void bt_dump_info(printer *prn, bt *btr) {
    (*prn)("BT t: %d nbits: %d nbyte: %d kbyte: %d "               \
           "ksize: %d koff: %d noff: %d numkeys: %d numnodes: %d " \
           "height: %d btr: %p btype: %d ktype: %d bflag: %d "     \
           "num: %d root: %p dirty_left: %u\n",
            btr->t, btr->nbits, btr->nbyte, btr->kbyte, btr->s.ksize,
            btr->keyofst, btr->nodeofst, btr->numkeys, btr->numnodes,
            treeheight(btr), (void *)btr,
            btr->s.btype, btr->s.ktype, btr->s.bflag, btr->s.num, btr->root,
            btr->dirty_left);
    DEBUG_BT_TYPE((*prn), btr);
}

void bt_dumptree(printer *prn, bt *btr, bool is_index, bool is_inode) {
    bt_dump_info(prn, btr);
    if (btr->root && btr->numkeys > 0) {
        dump_tree_node(prn, btr, btr->root, 0, is_index, 0, is_inode);
    }
    (*prn)("\n");
}
void dump_single_node(bt *btr, bt_n *x, bool is_inode) {
    printf(" NODE: n: %d scion: %d -> (%p)\n", x->n, x->scion, (void *)x);
    bool is_node = INODE(btr);
    for (int i = 0; i < x->n; i++) {
        void *be = KEYS(btr, x, i);
        aobj  akey; convertStream2Key(be, &akey, btr);
        printf("KEY: "); dumpAobj(printf, &akey);
#ifdef PRINT_EVICTED_KEYS
        uint32 dr = getDR(btr, x, i);
        if (is_inode && dr) printf("\t\t\t\tDR: %d\n", dr);
        else {
            ulong beg = C_IS_I(btr->s.ktype) ? akey.i : akey.l;
            for (ulong j = 1; j <= (ulong)dr; j++) {
                printf("\t\t\t\t\tEVICTED KEY:\t\t\t%lu\n", beg + j);
            }
        }
#else
        printf("\t\t\t\tDR: %d\n", getDR(btr, x, i));
#endif
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
static void validate_root_scion(bt *btr) {
    printf("validate_root_scion\n"); bt_dump_info(printf, btr);
    if (btr->root && btr->numkeys > 0) {
        Scion = 0;
        uint32_t root_scion = btr->root->scion;
        sum_scion(btr, btr->root, 0, treeheight(btr) - 2);
        printf("root_scion: %d rest_scion: %d\n", root_scion, Scion);
    }
}

static void dump_tree_node(printer *prn, bt *btr, bt_n *x,
                           int depth, bool is_index, int slot, bool is_inode) {
    if (!x->leaf) {
#ifdef BTREE_DEBUG
        (*prn)("%d: NODE[%d]: ", depth, x->num);
#else
        (*prn)("%d: NODE: ",     depth);
#endif
        if (x->dirty) {
            GET_DS_FROM_BTN(x)
            (*prn)("slot: %d n: %d scion: %d -> (%p) ds: %p\n",
                    slot, x->n, x->scion, (void *)x, ds);
        } else {
            (*prn)("slot: %d n: %d scion: %d -> (%p)\n",
                    slot, x->n, x->scion, (void *)x);
        }
    } else {
        if (x->dirty) {
            GET_DS_FROM_BTN(x)
            (*prn)("%d: LEAF: slot: %d n: %d scion: %d -> (%p) ds: %p\n",
                    depth, slot, x->n, x->scion, (void *)x, ds);
        } else {
            (*prn)("%d: LEAF: slot: %d n: %d scion: %d -> (%p)\n",
                    depth, slot, x->n, x->scion, (void *)x);
        }
        if (btr->dirty_left) {
            if (findminnode(btr, btr->root) == x) {
#ifdef PRINT_EVICTED_KEYS
                if (is_inode) (*prn)("\t\tDL: %u\n", btr->dirty_left);
                else {
                    for (uint32 i = 1; i <= btr->dirty_left; i++) {
                        (*prn)("\t\t\t\t\tEVICTED KEY:\t\t\t%u\n", i);
                    }
                }
#else
                (*prn)("\t\tDL: %u\n", btr->dirty_left);
#endif
            }
        }
    }

    for (int i = 0; i < x->n; i++) {
        void *be  = KEYS(btr, x, i);
        aobj  akey; convertStream2Key(be, &akey, btr);
        void *rrow = parseStream(be, btr);
        if (is_index && rrow) {
            (*prn)("\tINDEX-KEY: "); dumpAobj(prn, &akey);
            if (!SIMP_UNIQ(btr)) bt_dumptree(prn, (bt *)rrow, 1, 0);
        } else {
            bool key_printed = 0;
            if        UU(btr) {  key_printed = 1;
                ulong uu = (ulong)rrow;
                (*prn)("\t\tUU[%d]: KEY: %u VAL: %u\n",
                       i, (uu / UINT_MAX), (uu % UINT_MAX));
            } else if UL(btr) { 
                if UP(btr) (*prn)("\t\tUL: PTR: %p\t", rrow);
                else { key_printed = 1;
                    ulk *ul = (ulk *)rrow;
                    (*prn)("\t\tUL[%d]: KEY: %u VAL: %lu\n",
                           i, ul->key, ul->val); 
                }
            } else if LU(btr) { 
                if LUP(btr) (*prn)("\t\tLU: PTR: %p\t", rrow);
                else { key_printed = 1;
                    luk *lu = (luk *)rrow;
                    (*prn)("\t\tLU[%d]: KEY: %lu VAL: %lu\n",
                           i, lu->key, lu->val);
                }
            } else if LL(btr) { 
                if LLP(btr) (*prn)("\t\tLL: PTR: %p\t", rrow);
                else { key_printed = 1;
                    llk *ll = (llk *)rrow;
                    (*prn)("\t\tLL[%d]: KEY: %lu VAL: %lu\n",
                           i, ll->key, ll->val);
                }
            } else if UX(btr) { key_printed = 1;
                uxk *ux = (uxk *)rrow;
                (*prn)("\t\tUX[%d]: KEY: %u ", i, ux->key);
                (*prn)(" VAL: "); DEBUG_U128(prn, ux->val); (*prn)("\n");
            } else if XU(btr) { key_printed = 1;
                xuk *xu = (xuk *)rrow;
                (*prn)("\t\tXU[%d]: KEY: ", i); DEBUG_U128(prn, xu->key);
                (*prn)(" VAL: %u\n", xu->val);
            } else if LX(btr) { key_printed = 1;
                lxk *lx = (lxk *)rrow;
                (*prn)("\t\tLX[%d]: KEY: %lu ", i, lx->key);
                (*prn)(" VAL: "); DEBUG_U128(prn, lx->val); (*prn)("\n");
            } else if XL(btr) { 
                if XLP(btr) (*prn)("\t\tXL: PTR: %p\t", rrow);
                else { key_printed = 1;
                    xlk *xl = (xlk *)rrow;
                    (*prn)("\t\tXL[%d]: KEY: ", i); DEBUG_U128(prn, xl->key);
                    (*prn)(" VAL: %lu\n", xl->val);
                }
            } else if XX(btr) { key_printed = 1;
                xxk *xx = (xxk *)rrow;
                (*prn)("\t\tXX[%d]: KEY: ", i); DEBUG_U128(prn, xx->key);
                (*prn)(" VAL: "); DEBUG_U128(prn, xx->val); (*prn)("\n");
            } else {
                bool gost = !UU(btr) && rrow && !*((uchar *)rrow);
                if (gost) (*prn)("\t\tROW [%d]: %p \tGHOST-", i, rrow);
                else      (*prn)("\t\tROW [%d]: %p\t",        i, rrow);
            }
            if (!key_printed) { (*prn)("KEY: "); dumpAobj(prn, &akey); }
            if (x->dirty) {
#ifdef PRINT_EVICTED_KEYS
                uint32 dr = getDR(btr, x, i);
                if (is_inode && dr) (*prn)("\t\t\t\tDR: %d\n", dr);
                else {
                    ulong beg = C_IS_I(btr->s.ktype) ? akey.i : akey.l;
                    for (ulong j = 1; j <= (ulong)dr; j++) {
                        (*prn)("\t\t\t\t\tEVICTED KEY:\t\t\t%lu\n", beg + j);
                    }
                }
#else
                (*prn)("\t\t\t\tDR: %d\n", getDR(btr, x, i));
#endif
            }
        }
    }

    if (!x->leaf) {
        depth++;
        for (int i = 0; i <= x->n; i++) {
            (*prn)("\t\tNPTR[%d]: %p\n", i, NODES(btr, x)[i]);
        }
        for (int i = 0; i <= x->n; i++) {
            dump_tree_node(prn, btr, NODES(btr, x)[i], depth,
                           is_index, i, is_inode);
        }
    }
}

static int checkbtreenode(bt *btr, bt_n *x, void *kmin, void *kmax, 
                          int isrut, printer *prn) {
    if (x == NULL){ /* check that the two keys are in order */
        if (btr->cmp(kmin, kmax) >= 0) return 0;
        else                           return 1;
    } else {
        if (!isrut && (x->n < btr->t - 1 || x->n > 2 * btr->t - 1)) {
            (*prn)("node, too few or to many: %d\n", x->n);
            bt_dumptree(prn, btr, 0, 0); exit(-1);
        }
        /* check subnodes */
        if (x->n == 0 && !x->leaf) {
            int ret = checkbtreenode(btr, NODES(btr, x)[0], kmin, kmax, 0, prn);
            return ret;
        } else if (x->n == 0 && x->leaf && !isrut) {
            (*prn)("leaf with no keys!!\n");
            bt_dumptree(prn, btr, 0, 0);
            int ret = checkbtreenode(btr, NULL, kmin, kmax, 0, prn);
            return ret;
        }
        if (!x->leaf) {
            if (!checkbtreenode(btr, NODES(btr, x)[0],
                                kmin, KEYS(btr, x, 0), 0, prn)) return 0;
            int i;
            for (i = 1; i < x->n; i++) {
                int ret = checkbtreenode(btr, NODES(btr, x)[i],
                                         KEYS(btr, x, i - 1),
                                         KEYS(btr, x, i),     0, prn);
                if (!ret) return 0;
            }
            int ret = checkbtreenode(btr, NODES(btr, x)[i], 
                                    KEYS(btr, x, i - 1), kmax, 0, prn);
            if (!ret) return 0;
        }
    }
    return 1;
}

static int bt_checktree(bt *btr, void *kmin, void *kmax, printer *prn) {
    return checkbtreenode(btr, btr->root, kmin, kmax, 1, prn);
}

static int treeheight(bt *btr) {
    bt_n *x   = btr->root; if (!x) return 0;
    int   ret = 0;
    while (!x->leaf) { x = NODES(btr, x)[0]; ret++; }
    return ++ret;
}


#ifdef CLIENT_BTREE_DEBUG
/* used for DEBUGGING the Btrees innards */
#include "alsosql.h"
#include "index.h"
extern struct redisServer server;
extern struct sharedObjectsStruct shared;
extern r_tbl_t *Tbl;
extern r_ind_t *Index;
void btreeCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    initQueueOutput();
    printer *prn = queueOutput;
    MATCH_INDICES(tmatch)
    bt   *btr   = getBtr(tmatch);
    char *fname = NULL;
    if (c->argc > 4 && (!strcasecmp(c->argv[2]->ptr, "TO") &&
                        !strcasecmp(c->argv[3]->ptr, "FILE"))) {
       fname = c->argv[4]->ptr;
    }

    bt_dumptree(prn, btr, 0, 0); //TODO put in sdsfunc
    if (matches) {
        for (int i = 0; i < matches; i++) {
            int j = inds[i];
            if (!Index[j].virt && !Index[j].luat) {
                bt   *ibtr = getIBtr(j);
                (*prn)("INDEX: %d (%p)\n", inds[i], (void *)ibtr);
                if (ibtr) bt_dumptree(prn, ibtr, 1, 0);// NULL 4 empty INodes
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

#include "aobj.h"
#include "bt_iterator.h"
static int bt_validate_dirty(bt *btr, printer *prn) {
    if (!btr->root || !btr->numkeys) return 1; // EMPTY BTREE
    btEntry *be; 
    int      ret = 1;
    uint32   beg = btr->dirty_left + 1;
    btSIter *bi  = btGetFullRangeIter(btr, 1, NULL);
    while ((be = btRangeNext(bi, 1))) {
        aobj   *apk = be->key;
        //printf("pk: %d be->x: %p be->i: %d\n", apk->i, be->x, be->i);
        uint32  dr  = getDR(btr, be->x, be->i);
        if (beg != apk->i) {
            (*prn)("ERROR: bt_validate_dirty: beg: %u pk: %u\n", beg, apk->i);
            ret = 0; goto bt_val_end;
        }
        beg += (dr + 1);
    }

bt_val_end:
    btReleaseRangeIterator(bi);
    return ret;
}

void validateBTommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    initQueueOutput();
    printer *prn = queueOutput;
    bt   *btr  = getBtr(tmatch);
    void *kmin = bt_min(btr);
    void *kmax = bt_max(btr);
    (*prn)("validateBTommand: bt_checktree: %d\n",
           bt_checktree(btr, kmin, kmax, prn));
    (*prn)("validateBTommand: bt_validate_dirty: %d\n", 
           bt_validate_dirty(btr, prn));
    dumpQueueOutput(c);
}
#endif

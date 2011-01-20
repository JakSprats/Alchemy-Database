/*
 * Copyright 1997-1999, 2001 John-Mark Gurney.
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

GPL License

Copyright (c) 2010-2011 Russell Sullivan <jaksprats AT gmail DOT com>
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

#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "btree.h"
#include "btreepriv.h"
#include "bt.h"
#include "bt_iterator.h"
#include "redis.h"
#include "common.h"

/* FROM zmalloc.c */
extern size_t          used_memory;
extern int             zmalloc_thread_safe;
extern pthread_mutex_t used_memory_mutex;

#define BT_MEM_PROFILE
#ifdef BT_MEM_PROFILE
static ulong tot_bt_data     = 0;
static ulong tot_bt_data_mem = 0;
static ulong tot_num_bt_ns   = 0;
static ulong tnbtnmem        = 0;
static ulong tot_num_bts     = 0;
static ulong tot_num_bt_mem  = 0;
void dump_bt_mem_profile(bt *btr) {
    printf("BT: %d\n", btr->num);
    printf("tot_bt_data:     %lu\n", tot_bt_data);
    printf("tot_bt_data_mem: %lu\n", tot_bt_data_mem);
    printf("tot_num_bts:     %lu\n", tot_num_bts);
    printf("tot_bt_mem:      %lu\n", tot_num_bt_mem);
    printf("tot_num_btn:     %lu\n", tot_num_bt_ns);
    printf("tbtn_mem:        %lu\n", tnbtnmem);
    fflush(NULL);
}
  #define BT_MEM_PROFILE_BT   {tot_num_bts++; tot_num_bt_mem += size;}
  #define BT_MEM_PROFILE_MLC  {tot_bt_data++; tot_bt_data_mem += size;}
  #define BT_MEM_PROFILE_NODE {tot_num_bt_ns++; tnbtnmem += size;}
#else
  #define BT_MEM_PROFILE_BT
  #define BT_MEM_PROFILE
  #define BT_MEM_PROFILE_NODE
#endif
/*
 * This is the real log2 function.  It is only called when we don't have
 * a value in the table. -> which is basically never
 */
static inline int real_log2(unsigned int a, int nbits) {
    uint32 i = 0;
    uint32 b = (nbits + 1) / 2; /* divide in half rounding up */
    while (b) {
        i = (i << 1);
        if (a >= (unsigned int)(1 << b)) {
            /* select the top half and mark this bit */
            a /= (1 << b);
            i = i | 1;
        } else {
            /* select the bottom half and don't set the bit */
            a &= (1 << b) - 1;
        }
        b /= 2;
    }
    return i;
}

/*
 * Implement a lookup table for the log values.  This will only allocate
 * memory that we need.  This is much faster than calling the log2 routine
 * every time.  Doing 1 million insert, searches, and deletes will generate
 * ~58 million calls to log2.  Using a lookup table IS NECESSARY!
 -> memory usage of this is trivial, like less than 1KB
 */
static inline int _log2(unsigned int a, int nbits) {
    static char   *table   = NULL;
    static uint32  alloced = 0;
    uint32 i;
    if (a >= alloced) {
        table = realloc(table, (a + 1) * sizeof *table);
        for (i = alloced; i < a + 1; i++) table[i] = -1;
        alloced = a + 1;
    }
    if (table[a] == -1) table[a] = real_log2(a, nbits);
    return table[a];
}

/* NOTE KEYS can be (void *) or uint, [8 and 4 bytes], so logic is needed */
static inline void setKey(bt *btr, void **v, void *p) {
    if (btr->ksize == UINTSIZE) *(int *)((long *)v) = (int)(long)p;
    else                        *v                  = p;
}
void **AKEYS(bt *btr, bt_n *x, int i) {
    int   ofst = (btr->ksize == UINTSIZE) ? (i * UINTSIZE) : (i * VOIDSIZE);
    char *v    = (char *)x + btr->keyofst + ofst;
    return (void **)v;
}
#define OKEYS(btr, x) ((void **)((char *)x + btr->keyofst))
void *KEYS(bt *btr, bt_n *x, int i) {
    if (btr->ksize == UINTSIZE) return (void *)(long)(*(int *)AKEYS(btr, x, i));
    else                        return OKEYS(btr, x)[i];
}

/* NOTE used-memory bookkeeping maintained at the Btree level */
static void bt_increment_used_memory(bt *btr, size_t size) {
    btr->malloc_size += (ull)size;
    increment_used_memory(size);
}
static void bt_decrement_used_memory(bt *btr, size_t size) {
    btr->malloc_size -= (ull)size;
    decrement_used_memory(size);
}

void *bt_malloc(int size, bt *btr) {
BT_MEM_PROFILE_MLC
    void *v         = malloc(size);
    bt_increment_used_memory(btr, size);
    btr->data_size += size;
    return v;
}
static bt_n *allocbtreenode(bt *btr, bool leaf) {
    btr->numnodes++;
    size_t  size = leaf ? btr->kbyte : btr->nbyte;
BT_MEM_PROFILE_NODE
    bt_n   *btn  = malloc(size);
    bt_increment_used_memory(btr, size);
    bzero(btn, size);
    btn->leaf    = 1;
    return btn;
}
static bt *allocbtree(void) {
    int  size = sizeof(struct btree);
BT_MEM_PROFILE_BT
    bt  *btr  = (bt *)malloc(size);                      /* FREE ME 035 */
    bzero(btr, size);
    bt_increment_used_memory(btr, size);
    return btr;
}

void bt_free(void *v, bt *btr, int size) {
    bt_decrement_used_memory(btr, size);
    btr->data_size -= size;
    free(v);
}
void bt_free_btreenode(bt_n *x, bt *btr) {
    size_t size = x->leaf ? btr->kbyte : btr->nbyte;
    bt_decrement_used_memory(btr, size);
    free(x);                                             /* FREED 035 */
}
void bt_free_btree(void *v, bt *btr) {
    int  size = sizeof(struct btree);
    if (btr) bt_decrement_used_memory(btr, size);
    free(v);
}

bt *bt_create(bt_cmp_t cmp, uchar trans, int ksize, uchar iainc) {
    /* the two BT node sizes are 128 and 4096 */
    int    n        = (trans < TRANSITION_TWO_BTREE) ? 7 : 255;
    uchar  t        = (uchar)((int)(n + 1) /2);
    int    kbyte    = sizeof(bt_n) + n * ksize;
    int    nbyte    = kbyte + (n + 1) * VOIDSIZE;
    bt    *btr      = allocbtree();
    btr->cmp        = cmp;
    btr->ksize      = (uchar)ksize;
    btr->keyofst    = sizeof(bt_n);
    uint32 nodeofst = btr->keyofst + n * ksize;
    btr->nodeofst   = (ushort)nodeofst;
    btr->t          = t;
    btr->iainc      = iainc;
    int nbits       = real_log2(n, sizeof(int) * 8) + 1;
    nbits           = 1 << (real_log2(nbits, sizeof(int) * 8) + 1);
    btr->nbits      = (uchar)nbits;
    btr->nbyte      = nbyte;
    btr->kbyte      = kbyte;
    btr->root       = allocbtreenode(btr, 1);
    btr->numnodes   = 1;
    //bt_dump_info(btr);
    return btr;
}

static int findkindex(bt *btr, bt_n *x, bt_data_t k, int *r, btIterator *iter) {
    if (x->n == 0) return -1;
    int b, tr;
    int *rr = (r == NULL) ? &tr : r; /* rr: key is greater than current entry */
    int  i  = 0;
    int  a  = x->n - 1;
    while (a > 0) {
        b            = _log2(a, (int)btr->nbits);
        int slot     = (1 << b) + i;
        bt_data_t k2 = KEYS(btr, x, slot);
        if ((*rr = btr->cmp(k, k2)) < 0) {
            a        = (1 << b) - 1;
        } else {
            a       -= (1 << b);
            i       |= (1 << b);
        }
    }
    if ((*rr = btr->cmp(k, KEYS(btr, x, i))) < 0)  i--;
    if (iter) {
        iter->bln->in = (i > 0) ? i : 0;
        iter->bln->ik = (i > 0) ? i : 0;
    }
    return i;
}

static bool split_leaf2left(bt *btr, bt_n *x, bt_n *y, bt_n *xp, int i) {
    ushort  t  = btr->t;
    int blocksize = (t - 1) * btr->ksize;
    /* 1.) move x to xp_mid */
    setKey(btr, AKEYS(btr, xp, t - 1), KEYS(btr, x, i - 1));
    /* 2.) move y_mid to x */
    setKey(btr, AKEYS(btr, x, i - 1), KEYS(btr, y, t - 1));
    /* 3.) move begin(y) to mid(xp) */
    memmove(AKEYS(btr, xp, t), AKEYS(btr, y, 0), blocksize);
    xp->n += t;
    /* 4.) move y back t keys */
    memmove(AKEYS(btr, y, 0), AKEYS(btr, y, t), blocksize);
    y->n = t - 1;
    return 0;
}
static bool btreesplitchild(bt *btr, bt_n *x, int i, bt_n *y) {
    ushort  t = btr->t;
    if (i && y->leaf && btr->iainc) { /* AutoIncrementInteger split-left */
        bt_n *xp = NODES(btr, x)[i - 1];
        if ((xp->n <= t - 1)) return split_leaf2left(btr, x, y, xp, i);
    }
    bt_n   *z = allocbtreenode(btr, y->leaf);
    z->leaf   = y->leaf; /* duplicate leaf setting */
    for (int j = 0; j < t - 1; j++) { // TODO single memcpy()
        setKey(btr, AKEYS(btr, z, j), KEYS(btr, y, j + t));
    }
    y->n = t - 1; /* half num_nodes in Y */
    z->n = t - 1; /* half num_nodes in Z */
    if (!y->leaf) { /* if it's an internal node, copy the ptr's too */
        for (int j = 0; j < t; j++) {
            NODES(btr, z)[j] = NODES(btr, y)[j + t];
        }
    }
    for (int j = x->n; j > i; j--) { /* move nodes in parent down one */
        NODES(btr, x)[j + 1] = NODES(btr, x)[j];
    }
    NODES(btr, x)[i + 1] = z; /* store new node */

    /* adjust the keys from previous move, and store new key */
    for (int j = x->n - 1; j >= i; j--) {
        setKey(btr, AKEYS(btr, x, j + 1), KEYS(btr, x, j));
    }
    setKey(btr, AKEYS(btr, x, i), KEYS(btr, y, y->n));
    x->n++;
    return 1;
}
#define GETN(btr) ((2 * btr->t) - 1)
static void btreeinsertnonfull(bt *btr, bt_n *x, bt_data_t k) {
    int i = x->n - 1;
    if (x->leaf) { /* we are a leaf, just add it in */
        i = findkindex(btr, x, k, NULL, NULL);
        if (i != x->n - 1) {
            memmove(AKEYS(btr, x, i + 2), AKEYS(btr, x, i + 1),
                    (x->n - i - 1) * btr->ksize);
        }
        setKey(btr, AKEYS(btr, x, i + 1), k);
        x->n++;
    } else { /* not leaf */
        i = findkindex(btr, x, k, NULL, NULL) + 1;
        /* make sure that the next node isn't full */
        if (NODES(btr, x)[i]->n == GETN(btr)) {
            if (btreesplitchild(btr, x, i, NODES(btr, x)[i])) {
                if (btr->cmp(k, KEYS(btr, x, i)) > 0) i++;
            }
        }
        btreeinsertnonfull(btr, NODES(btr, x)[i], k);
    }
}
void bt_insert(bt *btr, bt_data_t k) {
    bt_n *r = btr->root;
    if (r->n == GETN(btr)) { /* NOTE: tree increase height */
        bt_n *s          = allocbtreenode(btr, 0);
        btr->root        = s;
        s->leaf          = 0;
        s->n             = 0;
        NODES(btr, s)[0] = r;
        btreesplitchild(btr, s, 0, r);
        r                = s;
        btr->numnodes++;
    }
    btreeinsertnonfull(btr, r, k);    /* finally insert the new node */
    btr->numkeys++;
}

/* NOTE: case_2c_ptr retains the deleted pointer to be passed to the caller */
void *case_2c_ptr = NULL;
/*
 * remove an existing key from the tree. KEY MUST EXIST
 * the s parameter:
   1.) for normal operation pass it as 0,
   2.) delete the max node, pass it as 1,
   3.) delete the min node, pass it as 2.
 */
static bt_data_t nodedeletekey(bt *btr, bt_n *x, bt_data_t k, int s) {
    bt_n *xp, *y, *z;
    bt_data_t kp;
    int       i, yn, zn;
    int       ks = btr->ksize;
    int       r  = -1;
    if (x == NULL) return 0;
    if (s) { /* min or max node deletion */
        if (!x->leaf) {
            switch (s) {
                case 1: r = 1;  break;
                case 2: r = -1; break;
            }
        } else {
            r = 0;
        }
        switch (s) {
            case 1:  i = x->n - 1; break; /* max node */
            case 2:  i = -1;       break; /* min node */
            default: i = 42;       break; /* should NOT happen */
        }
    } else {
        i = findkindex(btr, x, k, &r, NULL);
    }

    /* Case 1
     * If the key k is in node x and x is a leaf, delete the key k from x. */
    if (x->leaf) {
        if (s == 2) i++;
        kp = KEYS(btr, x, i);
        memmove(AKEYS(btr, x, i), AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
        x->n--;
        setKey(btr, AKEYS(btr, x, x->n), NULL); /* RUSS added for iterator */
        return kp;
    }

    if (r == 0) {
        /* Case 2
         * if the key k is in the node x, and x is an internal node */
        if ((yn = NODES(btr, x)[i]->n) >= btr->t) {
            /* Case 2a
             * if the child y that precedes k in node x has at
             * least t keys, then find the predecessor k' of
             * k in the subtree rooted at y.  Recursively delete
             * k', and replace k by k' in x.
             *   Currently the deletion isn't done in a single
             *   downward pass was that would require special
             *   unwrapping of the delete function. */
            xp = NODES(btr, x)[i];
            kp = KEYS(btr, x, i);
            setKey(btr, AKEYS(btr, x, i), nodedeletekey(btr, xp, NULL, 1));
            return kp;
        }
        if ((zn = NODES(btr, x)[i + 1]->n) >= btr->t) {
            /* Case 2b
             * if the child z that follows k in node x has at
             * least t keys, then find the successor k' of
             * k in the subtree rooted at z.  Recursively delete
             * k', and replace k by k' in x.
             *   See above for comment on single downward pass. */
            xp = NODES(btr, x)[i + 1];
            kp = KEYS(btr, x, i);
            setKey(btr, AKEYS(btr, x, i), nodedeletekey(btr, xp, NULL, 2));
            return kp;
        }
        if (yn == btr->t - 1 && zn == btr->t - 1) {
            /* Case 2c
             * if both y and z have only t - 1 keys, merge k
             * and all of z into y, so that x loses both k and
             * the pointer to z, and y now contains 2t - 1
             * keys.
             *   RUSS fixed a bug here, the return ptr was wrong */
            if (!case_2c_ptr) case_2c_ptr = KEYS(btr, x, i);
            y = NODES(btr, x)[i];
            z = NODES(btr, x)[i + 1];
            setKey(btr, AKEYS(btr, y, y->n++), k);
            memmove(AKEYS(btr, y, y->n), AKEYS(btr, z, 0), z->n * ks);
            if (!y->leaf) memmove(NODES(btr, y) + y->n, NODES(btr, z),
                                  (z->n + 1) * VOIDSIZE);
            y->n += z->n;
            memmove(AKEYS(btr, x, i),AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                    (x->n - i - 1) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(z, btr);
            return nodedeletekey(btr, y, k, s);
        }
    }
    /* Case 3
     * if k is not present in internal node x, determine the root x' of
     * the appropriate subtree that must contain k, if k is in the tree
     * at all.  If x' has only t - 1 keys, execute step 3a or 3b as
     * necessary to guarantee that we descend to a node containing at
     * least t keys.  Finish by recursing on the appropriate child of x. */
    i++;
    if ((xp = NODES(btr, x)[i])->n == btr->t - 1) { /* !x->leaf */
        /* Case 3a
         * If x' has only t - 1 keys but has a sibling with at
         * least t keys, give x' an extra key by moving a key
         * from x down into x', moving a key from x''s immediate
         * left or right sibling up into x, and moving the
         * appropriate child from the sibling into x'. */
        if (i > 0 && (y = NODES(btr, x)[i - 1])->n >= btr->t) {
            /* left sibling has t keys */
            memmove(AKEYS(btr, xp, 1), AKEYS(btr, xp, 0), xp->n * ks);
            if (!xp->leaf) memmove(NODES(btr, xp) + 1, NODES(btr, xp),
                                   (xp->n + 1) * VOIDSIZE);
            setKey(btr, AKEYS(btr, xp, 0), KEYS(btr, x, i - 1));
            setKey(btr, AKEYS(btr, x, i - 1), KEYS(btr, y, y->n - 1));
            if (!xp->leaf) NODES(btr, xp)[0] = NODES(btr, y)[y->n];
            y->n--;
            xp->n++;
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n >= btr->t) {
            /* right sibling has t keys */
            setKey(btr, AKEYS(btr, xp, xp->n++), KEYS(btr, x, i));
            setKey(btr, AKEYS(btr, x, i), KEYS(btr, y, 0));
            if (!xp->leaf) NODES(btr, xp)[xp->n] = NODES(btr, y)[0];
            y->n--;
            memmove(AKEYS(btr, y, 0), AKEYS(btr, y, 1), y->n * ks);
            if (!y->leaf) memmove(NODES(btr, y), NODES(btr, y) + 1,
                                  (y->n + 1) * VOIDSIZE);
        }
        /* Case 3b
         * If x' and all of x''s siblings have t - 1 keys, merge
         * x' with one sibling, which involves moving a key from x
         * down into the new merged node to become the median key
         * for that node.  */
        else if (i > 0 && (y = NODES(btr, x)[i - 1])->n == btr->t - 1) {
            /* merge i with left sibling */
            setKey(btr, AKEYS(btr, y, y->n++), KEYS(btr, x, i - 1));
            memmove(AKEYS(btr, y, y->n), AKEYS(btr, xp, 0), xp->n * ks);
            if (!xp->leaf) memmove(NODES(btr, y) + y->n, NODES(btr, xp),
                                    (xp->n + 1) * VOIDSIZE);
            y->n += xp->n;
            memmove(AKEYS(btr, x, i - 1), AKEYS(btr, x, i), (x->n - i) * ks);
            memmove(NODES(btr, x) + i, NODES(btr, x) + i + 1,
                    (x->n - i) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(xp, btr);
            xp = y;
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n == btr->t - 1) {
            /* merge i with right sibling */
            setKey(btr, AKEYS(btr, xp, xp->n++), KEYS(btr, x, i));
            memmove(AKEYS(btr, xp, xp->n), AKEYS(btr, y, 0), y->n * ks);
            if (!xp->leaf) memmove(NODES(btr, xp) + xp->n, NODES(btr, y),
                                   (y->n + 1) * VOIDSIZE);
            xp->n += y->n;
            memmove(AKEYS(btr, x, i),AKEYS(btr, x, i + 1), (x->n - i - 1) * ks);
            memmove(NODES(btr, x) + i + 1, NODES(btr, x) + i + 2,
                    (x->n - i - 1) * VOIDSIZE);
            x->n--;
            bt_free_btreenode(y, btr);
        }
    }
    return nodedeletekey(btr, xp, k, s);
}

bt_data_t bt_delete(bt *btr, bt_data_t k) {
    bt_n *x;
    case_2c_ptr = NULL;
    bt_data_t r = nodedeletekey(btr, btr->root, k, 0);

    /* remove empty non-leaf node from root, */
    if (!btr->root->n && !btr->root->leaf) { /* NOTE: tree decrease height */
        btr->numnodes--;
        x         = btr->root;
        btr->root = NODES(btr, x)[0];
        bt_free_btreenode(x, btr);
    }
    btr->numkeys--;
    return case_2c_ptr ? case_2c_ptr : r;
}

static bt_data_t findmaxnode(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, x->n - 1);
    else         return findmaxnode(btr, NODES(btr, x)[x->n]);
}
static bt_data_t findminnode(bt *btr, bt_n *x) {
    if (x->leaf) return KEYS(btr, x, 0);
    else         return findminnode(btr, NODES(btr, x)[0]);
}
bt_data_t bt_max(bt *btr) {
    if (!btr->root) return NULL;
    return findmaxnode(btr, btr->root);
}
bt_data_t bt_min(bt *btr) {
    if (!btr->root) return NULL;
    return findminnode(btr, btr->root);
}

static bt_data_t findnodekey(bt *btr, bt_n *x, bt_data_t k) {
    int i, r;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return KEYS(btr, x, i);
        if (x->leaf)          return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}
bt_data_t bt_find(bt *btr, bt_data_t k) {
    return findnodekey(btr, btr->root, k);
}

/* copy of findnodekey */
int bt_init_iterator(bt *btr, bt_data_t k, btIterator *iter) {
    if (!btr->root) return -1;
    int i, r;
    uchar  only_right = 1;
    bt_n  *x          = btr->root;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, iter);

        if (i >= 0 && r == 0) return 0;

        if (r < 0 || i != (x->n - 1)) only_right = 0;
        if (x->leaf) {
            if (i != (x->n - 1)) only_right = 0;
            return only_right ? RET_ONLY_RIGHT : RET_LEAF_EXIT;
        }

        iter->bln->child = get_new_iter_child(iter);
        x                = NODES(btr, x)[i + 1];
        become_child(iter, x);
    }
    return -1;
}

/* copy of findnodekey */
static bt_data_t findnodekeyreplace(bt *btr, bt_n *x,
                                    bt_data_t k, bt_data_t val) {
    if (!btr->root) return NULL;
    int i, r;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) {
            bt_data_t b     = KEYS(btr, x, i);
            setKey(btr, AKEYS(btr, x, i), val);
            return b;
        }
        if (x->leaf) return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}
bt_data_t bt_replace(bt *btr, bt_data_t k, bt_data_t val) {
    return findnodekeyreplace(btr, btr->root, k, val);
}

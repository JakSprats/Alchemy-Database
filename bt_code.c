/*-
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
 *
 * Pseudo code used to implement this data structure was obtained from:
 * Introduction to Algorithms / Thomas H. Cormen, Charles E. Leiserson,
 *     Ronald L. Rivest.
 *
 * The delete key routine was not provided in pseudo code and is my own
 * creation following their cases to maintain balance.
 *
 */

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "btree.h"
#include "btreepriv.h"
#include "bt.h"
#include "bt_iterator.h"
#include "redis.h"
#include "common.h"

#define RL4 redisLog(4,
#define RL7 if (iter && iter->which == 0) redisLog(4,

/* FROM zmalloc.c */
extern size_t          used_memory;
extern int             zmalloc_thread_safe;
extern pthread_mutex_t used_memory_mutex;

#if 0
static void bt_internal_dump(struct btree *btr) {
    bt_dumptree(btr, btr->ktype, REDIS_BTREE);
}

//RL4 "%p: INC BT size: %d TOT: %d size: %d", btr, btr->malloc_size, used_memory, size);
//RL4 "%p: BT OBJ size: %d", btr, size);
int loops = 0;
//loops++; if (loops%100 == 0) RL4 "MALLOC NODE: %d BTR: %d DATA: %d RATIO: %f", size, btr->malloc_size, btr->data_size, ((double)btr->malloc_size/(double)btr->data_size));
//RL4 "MALLOC TREE: %d", size);
//RL4 "%p: DEC BT size: %d TOT: %d", btr, btr->malloc_size, used_memory);
RL4 "bt_malloc: %d BTR: %d DATA: %d RATIO: %f", size, btr->malloc_size, btr->data_size, ((double)btr->malloc_size/(double)btr->data_size));
#endif


#define NODE_SIZE sizeof(struct btreenode) + btr->textra
static void bt_increment_used_memory(bt *btr, size_t size) {
    //RL4 "ADD:  bt_increment_used_memory: curr: %d size: %d", btr->malloc_size, size);
    btr->malloc_size += (ull)size;
    increment_used_memory(size);
}

void *bt_malloc(int size, bt *btr) {
    void *v = malloc(size);
    bt_increment_used_memory(btr, size);
    btr->data_size += size;
    return v;
}

static struct btreenode *allocbtreenode(bt *btr) {
    size_t            size = NODE_SIZE;
    struct btreenode *btn  = malloc(size);
    bt_increment_used_memory(btr, size);
    bzero(btn, size);
    btn->leaf              = 1;
    return btn;
}

void *bt_malloc_btree() {
    int  size = sizeof(struct btree);
    bt  *btr  = (bt *)malloc(size);
    assert(btr);
    bzero(btr, size);
    bt_increment_used_memory(btr, size);
    return btr;
}

static struct btree *allocbtree(void) {
    struct btree *btr = bt_malloc_btree();
    btr->root         = NULL;
    btr->cmp          = NULL;
    btr->keyoff       = 0;
    btr->nodeptroff   = 0;
    btr->t            = 0;
    btr->nbits        = 0;
    btr->textra       = 0;
    btr->numkeys      = 0;
    btr->numnodes     = 0;
    return btr;
}

static void bt_decrement_used_memory(bt *btr, size_t size) {
    //RL4 "LESS: bt_decrement_used_memory: curr: %d size: %d", btr->malloc_size, size);
    btr->malloc_size -= (ull)size;
    decrement_used_memory(size);
}
void bt_free(void *v, bt *btr, int size) {
    bt_decrement_used_memory(btr, size);
    btr->data_size -= size;
    free(v);
}
void bt_free_btreenode(void *v, bt *btr) {
    size_t size = NODE_SIZE;
    bt_decrement_used_memory(btr, size);
    free(v);
}
void bt_free_btree(void *v, bt *btr) {
    int  size = sizeof(struct btree);
    if (btr) bt_decrement_used_memory(btr, size);
    free(v);
}

static inline int _log2(unsigned int a, int nbits);
struct btree *bt_create(bt_cmp_t cmp, int size) {
    struct btree *btr    = NULL;
    int           textra = 0;

    size   -= sizeof(struct btreenode);
    size   -= sizeof(struct btreenode *);

    /*calculate maximum t that will fix in size, and that t >= 2 */
    int n   = size / (sizeof(struct btreenode *) + sizeof(bt_data_t));
    assert(n > 0);
    int t   = (n + 1) / 2;
    assert(t >= 2);
    n       = 2 * t - 1;
    textra += sizeof (btr->root) +
                           n * (sizeof(struct btreenode *) + sizeof(bt_data_t));

    btr                     = allocbtree();
    btr->cmp                = cmp;
    btr->keyoff             = sizeof(struct btreenode);
    unsigned int nodeptroff = btr->keyoff + n * sizeof(bt_data_t);
    assert(nodeptroff <= USHRT_MAX);
    btr->nodeptroff         = (unsigned short)nodeptroff;
    assert(n          <= USHRT_MAX);
    assert(t          <= USHRT_MAX);
    btr->t                  = t;
    int nbits               = _log2(n, sizeof(int) * 8) + 1;
    nbits                   = 1 << (_log2(nbits, sizeof(int) * 8) + 1);
    assert(nbits      <= UCHAR_MAX);
    btr->nbits              = nbits;
    assert(textra     <= USHRT_MAX);
    btr->textra             = textra;
    btr->numnodes++;
    btr->root               = allocbtreenode(btr);
    assert(btr->root);

    //RL4 "BT t: %d nbits: %d textra: %d keyoff: %d nodeptroff: %d",
           //btr->t, btr->nbits, btr->textra, btr->keyoff, btr->nodeptroff);
    return btr;
}

/*
 * This is the real log2 function.  It is only called when we don't have
 * a value in the table. -> which is basically never
 */
static inline int real_log2(unsigned int a, int nbits) {
    unsigned int i;
    unsigned int b;

    /* divide in half rounding up */
    b = (nbits + 1) / 2;
    i = 0;
    while (b) {
        i = (i << 1);
        if (a >= (unsigned int)(1 << b)) {
            /* select the top half and mark this bit */
            a /= (1 << b);
            i = i | 1;
        } else
            /* select the bottom half and don't set the bit */
            a &= (1 << b) - 1;
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
    static signed char  *table   = NULL;
    static unsigned int  alloced = 0;
    unsigned int i;

    if (a > alloced) {
        table = realloc(table, (a + 1) * sizeof *table);
        for (i = alloced; i < a + 1; i++) table[i] = -1;
        alloced = a + 1;
    }

    if (table[a] == -1) table[a] = real_log2(a, nbits);

    return table[a];
}

static int findkindex(struct btree      *btr,
                      struct btreenode  *x,
                      bt_data_t          k,
                      int               *r,
                      struct btIterator *iter) {
    int a, b, i, tr;
    int *rr; /* rr means key is greater than current entry */

    if (r == NULL) rr = &tr;
    else           rr = r;

    if (x->n == 0) return -1;

    i = 0;
    a = x->n - 1;
    while (a > 0) {
        b            = _log2(a, (int)btr->nbits);
        int slot     = (1 << b) + i;
        bt_data_t k2 = KEYS(btr, x)[slot];
        if ((*rr = btr->cmp(k, k2)) < 0) {
            a  = (1 << b) - 1;
        } else {
            a -= (1 << b);
            i |= (1 << b);
        }
    }
    if ((*rr = btr->cmp(k, KEYS(btr, x)[i])) < 0)  i--;

    //RL7 "findkindex: i: %d r: %d", i, *rr);
    if (iter) {
        iter->bln->in = (i > 0) ? i : 0;
        iter->bln->ik = (i > 0) ? i : 0;
    }

    return i;
}

static void btreesplitchild(struct btree     *btr,
                            struct btreenode *x,
                            int               i,
                            struct btreenode *y) {
    int j;
    struct btreenode *z;

    btr->numnodes++;
    if ((z = allocbtreenode(btr)) == NULL) exit(1);

    /* duplicate leaf setting, and store number of nodes */
    z->leaf = y->leaf;
    z->n    = btr->t - 1;

    /* copy the last half of y into z */
    for (j = 0; j < btr->t - 1; j++) {
        KEYS(btr, z)[j] = KEYS(btr, y)[j + btr->t];
    }

    /* if it's an internal node, copy the ptr's too */
    if (!y->leaf) {
        for (j = 0; j < btr->t; j++) {
            NODES(btr, z)[j] = NODES(btr, y)[j + btr->t];
        }
    }

    /* store resulting number of nodes in old part */
    y->n = btr->t - 1;

    /* move node ptrs in parent node down one, and store new node */
    for (j = x->n; j > i; j--) { // TODO this can be a single memcpy()
        NODES(btr, x)[j + 1] = NODES(btr, x)[j];
    }
    NODES(btr, x)[i + 1] = z;

    /* adjust the keys from previous move, and store new key */
    for (j = x->n - 1; j >= i; j--) {
        KEYS(btr, x)[j + 1] = KEYS(btr, x)[j];
    }
    KEYS(btr, x)[i] = KEYS(btr, y)[btr->t - 1];
    x->n++;
}

#define n(x) ((2*x)-1)
static void btreeinsertnonfull(struct btree     *btr,
                               struct btreenode *x,
                               bt_data_t         k) {
    int i = x->n - 1;
    if (x->leaf) {
        /* we are a leaf, just add it in */
        i = findkindex(btr, x, k, NULL, NULL);
        if (i != x->n - 1) {
            memmove(KEYS(btr, x) + i + 2, KEYS(btr, x) + i + 1,
                    (x->n - i - 1) * sizeof k);
        }
        KEYS(btr, x)[i + 1] = k;
        x->n++;
    } else {
        i = findkindex(btr, x, k, NULL, NULL) + 1;

        /* make sure that the next node isn't full */
        if (NODES(btr, x)[i]->n == n(btr->t)) {
            btreesplitchild(btr, x, i, NODES(btr, x)[i]);
            if (btr->cmp(k, KEYS(btr, x)[i]) > 0) i++;
        }
        btreeinsertnonfull(btr, NODES(btr, x)[i], k);
    }
}

void bt_insert(struct btree *btr, bt_data_t k) {
    struct btreenode *r, *s;

    btr->numkeys++;
    r = btr->root;
    if (r->n == n(btr->t)) {
        /* this is the ONLY place that the tree can grown in height */
        btr->numnodes++;
        if ((s = allocbtreenode(btr)) == NULL) exit(1);
        btr->root        = s;
        s->leaf          = 0;
        s->n             = 0;
        NODES(btr, s)[0] = r;
        btreesplitchild(btr, s, 0, r);
        r                = s;
    }
    /* finally insert the new node */
    btreeinsertnonfull(btr, r, k);
}

void *case_2c_ptr = NULL;
/*
 * remove an existing key from the tree, if the key doesn't exist in it,
 * unexpected results may happen.
 *
 * the s parameter is kinda special, for normal operation you need to pass
 * it as 0, if you want to delete the max node, pass it as 1, or if you
 * want to delete the min node, pass it as 2.
 */
static bt_data_t nodedeletekey(struct btree     *btr,
                               struct btreenode *x,
                               bt_data_t         k,
                               int               s) {
    struct btreenode *xp, *y, *z;
    bt_data_t         kp;
    int               yn, zn;
    int               i;
    int               r = -1;

    if (x == NULL) return 0;

    if (s) {
        if (!x->leaf) {
            switch (s) {
                case 1:
                    r = 1;
                    break;
                case 2:
                    r = -1;
                    break;
            }
        } else {
            r = 0;
        }
        switch (s) {
            case 1:
                i = x->n - 1;
                break;
            case 2:
                i = -1;
                break;
            default:
                i = 42;
                break;
        }
    } else {
        i = findkindex(btr, x, k, &r, NULL);
    }

    /* Case 1
     * If the key k is in node x and x is a leaf, delete the key k from x. */
    if (x->leaf) {
        if (s == 2) i++;
        kp = KEYS(btr, x)[i];
        memmove(KEYS(btr, x) + i,
                KEYS(btr, x) + i + 1,
                (x->n - i - 1) * sizeof k);
        x->n--;
        KEYS(btr, x)[x->n] = NULL; /* RUSS added for iterator */
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
             *
             * Currently the deletion isn't done in a signle
             * downward pass was that would require special
             * unwrapping of the delete function. */
            xp              = NODES(btr, x)[i];
            kp              = KEYS(btr, x)[i];
            KEYS(btr, x)[i] = nodedeletekey(btr, xp, NULL, 1);
            return kp;
        }
        if ((zn = NODES(btr, x)[i + 1]->n) >= btr->t) {
            /* Case 2b
             * if the child z that follows k in node x has at
             * least t keys, then find the successor k' of
             * k in the subtree rooted at z.  Recursively delete
             * k', and replace k by k' in x.
             *
             * See above for comment on single downward pass. */
            xp              = NODES(btr, x)[i + 1];
            kp              = KEYS(btr, x)[i];
            KEYS(btr, x)[i] = nodedeletekey(btr, xp, NULL, 2);
            return kp;
        }
        if (yn == btr->t - 1 && zn == btr->t - 1) {
            /* Case 2c
             * if both y and z have only t - 1 keys, merge k
             * and all of z into y, so that x loses both k and
             * the pointer to z, and y now contains 2t - 1
             * keys. */
            /* RUSS fixed a bug here, the return ptr was wrong */
            if (!case_2c_ptr) case_2c_ptr = KEYS(btr, x)[i];

            y                    = NODES(btr, x)[i];
            z                    = NODES(btr, x)[i + 1];
            KEYS(btr, y)[y->n++] = k;
            memmove(KEYS(btr, y)  + y->n, KEYS(btr, z),   z->n      * sizeof k);
            memmove(NODES(btr, y) + y->n, NODES(btr, z), (z->n + 1) * sizeof y);
            y->n += z->n;
   
            memmove(KEYS(btr, x) + i,
                    KEYS(btr, x) + i + 1,
                    (x->n - i - 1) * sizeof k);
            memmove(NODES(btr, x) + i + 1,
                    NODES(btr, x) + i + 2,
                    (x->n - i - 1) * sizeof k);
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
    /* !x->leaf */
    if ((xp = NODES(btr, x)[i])->n == btr->t - 1) {
        /* Case 3a
         * If x' has only t - 1 keys but has a sibling with at
         * least t keys, give x' an extra key by moving a key
         * from x down into x', moving a key from x''s immediate
         * left or right sibling up into x, and moving the
         * appropriate child from the sibling into x'. */
        if (i > 0 && (y = NODES(btr, x)[i - 1])->n >= btr->t) {
            /* left sibling has t keys */
            memmove(KEYS(btr, xp) + 1, KEYS(btr, xp), xp->n * sizeof k);
            memmove(NODES(btr, xp) + 1, NODES(btr, xp), (xp->n + 1) * sizeof x);
            KEYS(btr, xp)[0]    = KEYS(btr, x)[i - 1];
            KEYS(btr, x)[i - 1] = KEYS(btr, y)[y->n - 1];
            NODES(btr, xp)[0]   = NODES(btr, y)[y->n];
            y->n--;
            xp->n++;
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n >= btr->t) {
            /* right sibling has t keys */
            KEYS(btr, xp)[xp->n++] = KEYS(btr, x)[i];
            KEYS(btr, x)[i]        = KEYS(btr, y)[0];
            NODES(btr, xp)[xp->n]  = NODES(btr, y)[0];
            y->n--;
            memmove(KEYS(btr, y), KEYS(btr, y) + 1, y->n * sizeof k);
            memmove(NODES(btr, y), NODES(btr, y) + 1, (y->n + 1) * sizeof x);
        }
        /* Case 3b
         * If x' and all of x''s siblings have t - 1 keys, merge
         * x' with one sibling, which involves moving a key from x
         * down into the new merged node to become the median key
         * for that node.  */
          else if (i > 0 && (y = NODES(btr, x)[i - 1])->n == btr->t - 1) {
            /* merge i with left sibling */
            KEYS(btr, y)[y->n++] = KEYS(btr, x)[i - 1];
            memmove(KEYS(btr, y) + y->n, KEYS(btr, xp), xp->n * sizeof k);
            memmove(NODES(btr, y) + y->n,
                    NODES(btr, xp),
                    (xp->n + 1) * sizeof x);
            y->n += xp->n;
            memmove(KEYS(btr, x) + i - 1,
                    KEYS(btr, x) + i,
                    (x->n - i) * sizeof k);
            memmove(NODES(btr, x) + i,
                    NODES(btr, x) + i + 1,
                    (x->n - i) * sizeof x);
            x->n--;
            bt_free_btreenode(xp, btr);
            xp = y;
        } else if (i < x->n && (y = NODES(btr, x)[i + 1])->n == btr->t - 1) {
            /* merge i with right sibling */
            KEYS(btr, xp)[xp->n++] = KEYS(btr, x)[i];
            memmove(KEYS(btr, xp) + xp->n, KEYS(btr, y), y->n * sizeof k);
            memmove(NODES(btr, xp) + xp->n,
                    NODES(btr, y),
                    (y->n + 1) * sizeof x);
            xp->n += y->n;
            memmove(KEYS(btr, x) + i,
                    KEYS(btr, x) + i + 1,
                    (x->n - i - 1) * sizeof k);
            memmove(NODES(btr, x) + i + 1,
                    NODES(btr, x) + i + 2,
                    (x->n - i - 1) * sizeof x);
            x->n--;
            bt_free_btreenode(y, btr);
        }
    }
    return nodedeletekey(btr, xp, k, s);
}

bt_data_t bt_delete(struct btree *btr, bt_data_t k) {
    struct btreenode *x;
    bt_data_t r;

    case_2c_ptr = NULL;
    r = nodedeletekey(btr, btr->root, k, 0);

    /* remove an empty, non-leaf node from root, this is the ONLY
     * place that a tree can decrease in height */
    if (btr->root->n == 0 && btr->root->leaf == 0) {
        btr->numnodes--;
        x         = btr->root;
        btr->root = NODES(btr, x)[0];
        bt_free_btreenode(x, btr);
    }
    btr->numkeys--;

    if (case_2c_ptr) return case_2c_ptr;
    else             return r;
}

static bt_data_t findmaxnode(struct btree *btr, struct btreenode *x) {
    if (x->leaf) return KEYS(btr, x)[x->n - 1];
    else         return findmaxnode(btr, NODES(btr, x)[x->n]);
}

static bt_data_t findminnode(struct btree *btr, struct btreenode *x) {
    if (x->leaf) return KEYS(btr, x)[0];
    else         return findminnode(btr, NODES(btr, x)[0]);
}

bt_data_t bt_max(struct btree *btr) {
    return findmaxnode(btr, btr->root);
}

bt_data_t bt_min(struct btree *btr) {
    return findminnode(btr, btr->root);
}

static bt_data_t findnodekey(struct btree     *btr,
                             struct btreenode *x,
                             bt_data_t         k) {
    int i, r;
    while (x != NULL) {
        i = findkindex(btr, x, k, &r, NULL);
        if (i >= 0 && r == 0) return KEYS(btr, x)[i];
        if (x->leaf)          return NULL;
        x = NODES(btr, x)[i + 1];
    }
    return NULL;
}

bt_data_t bt_find(struct btree *btr, bt_data_t k) {
    return findnodekey(btr, btr->root, k);
}

/* copy of findnodekey */
int bt_init_iterator(struct btree *btr, bt_data_t k, struct btIterator *iter) {
    if (!btr->root) return -1;

    int i = 0;
    int r = 0;

    struct btreenode *x          = btr->root;
    unsigned char     only_right = 1;
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

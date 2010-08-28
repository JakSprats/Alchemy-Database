/* B-trees Implementation.
 *
 * This file implements in the iterators for the Btree
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __BTREE_ITERATOR_H
#define __BTREE_ITERATOR_H

#include "alsosql.h"
#include "btreepriv.h"
#include "common.h"

typedef struct btEntry {
    void *key;
    void *val;
} btEntry;

typedef struct bTreeLinkedListNode { // 3ptr(24) 2int(8) -> 32 bytes
    struct bTreeLinkedListNode *parent;
    struct btreenode           *self;
    struct bTreeLinkedListNode *child;
    int                         ik;
    int                         in;
} bt_ll_n;

/* using 16 as 8^16 can hold 2.8e14 elements (8 is min members in a btn)*/
#define MAX_BTREE_DEPTH 16
typedef struct btIterator { // 2*ptr(16) int(4) 3*char(3) long(8) ptr(8)
                            // btEntry(16) 2*robj(?200?)
                            // int(4) 16*bt_ll_n(512) -> i.e. dont malloc
    bt            *btr;
    bt_ll_n       *bln;
    int            depth;
    unsigned char  ktype;
    unsigned char  vtype;
 
    bool           finished;
    unsigned long  high;
    char          *highc;
    unsigned char  num_nodes;
    int            which;

    btEntry        be;  /* used to transfrom stream into key&val robj's below */
    robj           key;
    robj           val;

    bt_ll_n        nodes[MAX_BTREE_DEPTH];
} btIterator;

#define RET_LEAF_EXIT  1
#define RET_ONLY_RIGHT 2

bt_ll_n *get_new_iter_child(btIterator *iter);
void become_child(btIterator *iter, bt_n* self);

btIterator *btGetRangeIterator(    robj *o, void *low, void *high, bool virt);
btIterator *btGetFullRangeIterator(robj *o, bool asc, bool virt);
btEntry    *btRangeNext(           btIterator *iter, bool asc);
void        btReleaseRangeIterator(btIterator *iter);
//int         btNumRecsRange(bt *ht, void *low, void *high);

/* for parallel JOINS */
btIterator *btGetGenericRangeIterator(robj *o,
                                      void *low,
                                      void *high,
                                      bool  virt,
                                      int   which);
#endif /* __BTREE_ITERATOR_H */

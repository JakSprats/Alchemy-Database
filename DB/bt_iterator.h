/* B-trees Implementation.
 *
 * This file implements in the iterators for the Btree
 *

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
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

#ifndef __BTREE_ITERATOR_H
#define __BTREE_ITERATOR_H

#include "alsosql.h"
#include "btreepriv.h"
#include "bt.h"
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

typedef void iter_single(struct btIterator *iter);

/* NOTE: btIterator is generic */
/* using 16 as 8^16 can hold 2.8e14 elements (8 is min members in a btn)*/
#define MAX_BTREE_DEPTH 16
typedef struct btIterator { // 5*ptr(40) int(4) 2*char(2) long(8) float(4)
                            // ptr(8) int(4) 16*bt_ll_n(512) -> i.e. dont malloc
    bt          *btr;
    bt_ll_n     *bln;
    int          depth;
 
    iter_single *iNode; /* function to iterate on node's */
    iter_single *iLeaf; /* function to iterate on leaf's */

    void        *data;  /* iNode and iLeaf can change "data" */
    bool         finished;

    ulong        high;  /* for INT & LONG */
    float        highf;
    char        *highs;

    uchar        num_nodes;
    bt_ll_n      nodes[MAX_BTREE_DEPTH];
} btIterator;

typedef struct btSIter { // btIterator(?500?) 1*char(1) int(1)
                         // btEntry(16) 2*aobj(23) i.e. dont malloc
    btIterator x;
    uchar      ktype;
    int        which;
    btEntry    be;
    aobj       key; /* static AOBJ for be.key */
} btSIter;

#define RET_LEAF_EXIT  1
#define RET_ONLY_RIGHT 2

bt_ll_n *get_new_iter_child(btIterator *iter);
void     become_child(btIterator *iter, bt_n* self);
int      init_iterator(bt *btr, bt_data_t simkey, struct btIterator *iter);
void    *btNext(btIterator *iter);

btSIter *btGetRangeIter    (bt *btr, aobj *alow, aobj *ahigh,         bool asc);
btSIter *btGetXthIter      (bt *btr, aobj *alow, aobj *ahigh, long x);
btSIter *btGetFullXthIter  (bt *btr,                          long x);
btSIter *btGetFullRangeIter(bt *btr,                                  bool asc);

btEntry *btRangeNext           (btSIter *iter,                        bool asc);
void     btReleaseRangeIterator(btSIter *iter);

bool assignMinKey(bt *btr, aobj *key);
bool assignMaxKey(bt *btr, aobj *key);

#endif /* __BTREE_ITERATOR_H */

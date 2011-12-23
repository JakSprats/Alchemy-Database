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
    void   *key;
    void   *val;
    void   *stream; // some iterators need the raw stream (INDEX CURSORS)
    bt_n   *x;      // some iterators need the position in the bt_n
    int     i;      // some iterators need the position in the bt_n
    bool    missed;
    uint32  dr;     // RANGE DELETEs simulate Keys using DR
} btEntry;

typedef struct bTreeLinkedListNode { // 3ptr(24) 2int(8) -> 32 bytes
    struct bTreeLinkedListNode *parent;
    struct btreenode           *self;
    struct bTreeLinkedListNode *child;
    int                         ik;
    int                         in; //TODO in not needed, ik & logic is enough
} bt_ll_n;

typedef void iter_single(struct btIterator *iter);

/* using 16 as 8^16 can hold 2.8e14 elements (8 is min members in a btn)*/
#define MAX_BTREE_DEPTH 16
typedef struct btIterator { // 60B + 16*bt_ll_n(512) -> dont malloc
    bt          *btr;
    bt_ll_n     *bln;
    int          depth;
    iter_single *iNode;     // function to iterate on node's
    iter_single *iLeaf;     // function to iterate on leaf's
    void        *data;      // iNode and iLeaf can change "data"
    bool         finished;
    ulong        high;      // HIGH for INT & LONG
    uint128      highx;     // HIGH for U128
    float        highf;     // HIGH for FLOAT
    char        *highs;     // HIGH for TEXT
    uchar        num_nodes; // \/-slot in nodes[]
    bt_ll_n      nodes[MAX_BTREE_DEPTH];
} btIterator;

typedef struct btSIter { // btIterator 500+ bytes -> STACK (globals) ALLOCATE
    btIterator x;
    bool       missed; // CURRENT iteration is miss
    uint32     mdelta; // CURRENT iteration missed by how much (delta)
    bool       nim;    // NEXT    iteration is miss
    bool       empty;
    bool       scan;
    uchar      ktype;
    int        which; // which BT_Iterators[] slot
    btEntry    be;
    aobj       key;    // static AOBJ for be.key
} btSIter;

#define II_FAIL       -1
#define II_OK          0
#define II_LEAF_EXIT   1
#define II_ONLY_RIGHT  2
#define II_MISS        3
#define II_L_MISS      4

bt_ll_n *get_new_iter_child(btIterator *iter);
void     to_child(btIterator *iter, bt_n* self);
int      init_iterator(bt *btr, bt_data_t simkey, struct btIterator *iter);

btSIter *btGetRangeIter    (bt *btr, aobj *alow, aobj *ahigh,         bool asc);
btSIter *btGetXthIter      (bt *btr, aobj *alow, aobj *ahigh, long x, bool asc);
btSIter *btGetFullRangeIter(bt *btr,             bool asc, cswc_t *w);
btSIter *btGetFullXthIter  (bt *btr,     long x, bool asc, cswc_t *w, long lim);

btEntry *btRangeNext           (btSIter *iter,                        bool asc);
void     btReleaseRangeIterator(btSIter *iter);

bool assignMinKey(bt *btr, aobj *key);
bool assignMaxKey(bt *btr, aobj *key);

#endif /* __BTREE_ITERATOR_H */

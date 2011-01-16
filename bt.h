/* B-tree Implementation.
 *
 * This file implements in memory b-tree with insert/del/replace/find/ ops
 *

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
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

#ifndef __ALSO_SQL_BT_H
#define __ALSO_SQL_BT_H

#include "row.h"
#include "btreepriv.h"
#include "aobj.h"
#include "common.h"

bt *abt_resize(bt *obtr, int new_size);

robj *createBtreeObject(uchar ktype, int num, uchar btype);
robj *createEmptyBtreeObject(); /*used for virtual indices */
void freeBtreeObject(robj *o);

/* different Btree types */
#define BTREE_TABLE           0
#define BTREE_INDEX           1
#define BTREE_INDEX_NODE      2
#define BTREE_JOIN_RESULT_SET 3

/* INT Indexes have been optimised */
#define INODE(btr) \
  (btr->btype == BTREE_INDEX_NODE && btr->ktype == COL_TYPE_INT)

void btDestroy(bt *nbtr, bt *btr);
int   btAdd(    bt *btr, aobj *apk, void *val);
void  btReplace(bt *btr, aobj *apk, void *val);
int   btDelete( bt *btr, aobj *apk);
void *btFindVal(bt *btr, aobj *apk);

void  btIndAdd(    bt *btr, aobj *akey, bt  *nbtr);
bt   *btIndFindVal(bt *btr, aobj *akey);
int   btIndDelete( bt *btr, aobj *akey);

bt   *createIndexNode(uchar pktype);
void  btIndNodeAdd(   bt *btr, aobj *apk);
int   btIndNodeDelete(bt *btr, aobj *apk);

/* JOINS */
typedef struct joinRowEntry {
    robj *key;
    void *val;
} joinRowEntry;

bt  *createJoinResultSet(uchar pktype);
int  btJoinAddRow(   bt *jbtr, joinRowEntry *key);
void *btJoinFindVal( bt *jbtr, joinRowEntry *key);
int  btJoinDeleteRow(bt *jbtr, joinRowEntry *key);
void btJoinRelease(bt  *jbtr,
                   int  ncols,
                   bool is_ob,
                   void (*freer)(list *s, int ncols, bool is_ob));

#endif /* __ALSO_SQL_BT_H */

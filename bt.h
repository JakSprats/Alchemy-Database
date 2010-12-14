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
#include "common.h"

bt *abt_resize(bt *obtr, int new_size);

unsigned int getStreamMallocSize(uchar         *stream,
                                 int            vtype,
                                 uchar  is_index);

robj *createBtreeObject(uchar ktype, int num, uchar is_index);
robj *createEmptyBtreeObject(); /*used for virtual indices */
void freeBtreeObject(robj *o);

int btStreamCmp(void *a, void *b);

/* different Btree types */
#define BTREE_TABLE           0
#define BTREE_INDEX           1
#define BTREE_INDEX_NODE      2
#define BTREE_JOIN_RESULT_SET 3

void btDestroy(bt *nbtr, bt *btr);
int   btAdd(    robj *o,       void *key, void *val, int ctype);
int   btReplace(robj *o,       void *key, void *val, int ctype);
int   btDelete( robj *o, const void *key,            int ctype);
robj *btFindVal(robj *o, const void *key,            int ctype);


int   btIndAdd(    bt *btr, void *key,       bt  *nbtr, int k_type);
robj *btIndFindVal(bt *btr, const void *key,            int k_type);
int   btIndDelete( bt *btr, const void *key,            int k_type);

bt   *createIndexNode(uchar pktype);
int   btIndNodeAdd(   bt *btr, void *key,               int k_type);
int   btIndNodeDelete(bt *btr, const void *key,         int k_type);

char *createSimKey(       const robj   *key,
                          int           ktype,
                          bool          *med,
                          uchar         *sflag,
                          unsigned int *ksize);
char *createSimKeyFromRaw(void         *key_ptr,
                          int           ktype,
                          bool         *med,
                          uchar        *sflag,
                          unsigned int *ksize);
void  destroySimKey(char *simkey, bool  med);

/* convert stream to robj's */
void destroyAssignKeyRobj(robj *key);
void assignKeyRobj(uchar *stream,            robj *key);
void assignValRobj(uchar *stream, int ctype, robj *val, uchar is_index);

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

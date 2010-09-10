/* B-tree Implementation.
 *
 * This file implements in memory b-tree with insert/del/replace/find/ ops
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ALSO_SQL_BT_H
#define __ALSO_SQL_BT_H

#include "row.h"
#include "btreepriv.h"
#include "common.h"

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

bt   *btCreate( uchar ctype, int num, uchar is_index);
void  btRelease(bt *node_btr, bt *btr);
int   btAdd(    robj *o,       void *key, void *val, int ctype);
int   btReplace(robj *o,       void *key, void *val, int ctype);
int   btDelete( robj *o, const void *key,            int ctype);
robj *btFindVal(robj *o, const void *key,            int ctype);


int   btIndAdd(    bt *btr, void *key,       void *val, int k_type);
robj *btIndFindVal(bt *btr, const void *key,            int k_type);
int   btIndDelete( bt *btr, const void *key,            int k_type);

robj *createIndexNode(uchar pktype);
int   btIndNodeAdd(   bt *btr, void *key,        int k_type);
int   btIndNodeDelete(bt *btr, const void *key, int k_type);

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
                   void (*freer)(list *s, int ncols));

#endif /* __ALSO_SQL_BT_H */

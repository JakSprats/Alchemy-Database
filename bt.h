/* B-tree Implementation.
 *
 * This file implements in memory b-tree with insert/del/replace/find/ ops
  COPYRIGHT: RUSS
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

#define BTREE_TABLE      0
#define BTREE_INDEX      1
#define BTREE_INDEX_NODE 2
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

//DEBUG
void assignKeyRobj(uchar *stream,            robj *key);
void assignValRobj(uchar *stream, int ctype, robj *val, uchar is_index);

#endif /* __ALSO_SQL_BT_H */

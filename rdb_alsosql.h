/* Alsosql save to rdb
 *
 * This file implements saving alsosql datastructures to rdb files
  COPYRIGHT: RUSS
 */

#ifndef __ALSQSQL_RDB_H
#define __ALSQSQL_RDB_H

#include "redis.h"

int   rdbSaveBT(FILE *fp, robj *o);
robj *rdbLoadBT(FILE *fp, redisDb *db);
void  rdbLoadFinished(    redisDb *db);

int buildIndex(bt *btr, bt_n *x, bt *ind, int icol, int itbl);

#endif /* __ALSQSQL_RDB_H */

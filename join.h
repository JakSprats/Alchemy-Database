/*
COPYRIGHT: RUSS
 */

#ifndef __JOINSTORE__H
#define __JOINSTORE__H

#include "redis.h"
#include "common.h"

void freeJoinRowObject(robj *o);
void freeAppendSetObject(robj *o);
void freeValSetObject(robj *o);

int multiColCheckOrReply(redisClient *c,
                         char        *col_list,
                         int          j_tbls[],
                         int          j_cols[]);

int parseIndexedColumnListOrReply(redisClient *c, char *ilist, int j_indxs[]);

void joinGeneric(redisClient *c,
                 redisClient *fc,
                 int          j_indxs[],
                 int          j_tbls [],
                 int          j_cols[],
                 int          n_ind, 
                 int          qcols, 
                 robj        *low,  
                 robj        *high, 
                 int          sto);

void jstoreCommit(redisClient *c,
                  int          sto,
                  robj        *range,
                  robj        *newname,
                  int          j_indxs[MAX_JOIN_INDXS],
                  int          j_tbls [MAX_JOIN_INDXS],
                  int          j_cols [MAX_JOIN_INDXS],
                  int          n_ind,
                  int          qcols);

#endif /* __JOINSTORE__H */ 

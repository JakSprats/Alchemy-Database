/*
COPYRIGHT: RUSS
 */

#ifndef __STORE__H
#define __STORE__H

#include "redis.h"
#include "common.h"

#define NUM_STORAGE_TYPES 11

unsigned char respOk(redisClient *c);
bool performStoreCmdOrReply(redisClient *c, redisClient *fc, int sto);

void istoreCommit(redisClient *c,
                  int          tmatch,
                  int          imatch,
                  char        *sto_type,
                  char        *col_list,
                  char        *range,
                  robj        *new_name);

void legacyTableCommand(redisClient *c); /* LEGACY syntax for createTable() */
void legacyInsertCommand(redisClient *c);

#define CHECK_STORE_TYPE_OR_REPLY(cargv1ptr)                      \
    char *store_type = cargv1ptr;                                 \
    for (int i = 0; i < NUM_STORAGE_TYPES; i++) {                 \
        if (!strcasecmp(store_type, StorageCommands[i].name)) {   \
            sto = i;                                              \
            break;                                                \
        }                                                         \
    }                                                             \
    if (sto == -1) {                                              \
        addReply(c, shared.storagetypeunkown);                    \
        return;                                                   \
    }

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols, 
                         int          j_tbls [],
                         int          j_cols[]);
#endif /* __STORE__H */ 

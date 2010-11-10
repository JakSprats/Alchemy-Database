/*
 * Implements istore and iselect
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __STORE__H
#define __STORE__H

#include "redis.h"
#include "adlist.h"

#include "alsosql.h"
#include "common.h"

#define NUM_STORAGE_TYPES 11

unsigned char respOk(redisClient *c);

bool internalCreateTable(redisClient *c,
                         redisClient *fc,
                         int          qcols, 
                         int          cmatchs[],
                         int          tmatch);

bool performStoreCmdOrReply(redisClient *c, redisClient *fc, int sto);

bool prepareToStoreReply(redisClient  *c,
                         cswc_t       *w,
                         char        **nname,
                         int          *nlen,
                         bool         *sub_pk,
                         int          *nargc,
                         char        **last,
                         int           qcols);
void istoreCommit(redisClient *c,
                  cswc_t      *w,
                  int          tmatch,
                  int          cmatchs[MAX_COLUMN_PER_TABLE],
                  int          qcols);

void legacyTableCommand(redisClient *c); /* LEGACY syntax for createTable() */
void legacyInsertCommand(redisClient *c);

#define CHECK_STORE_TYPE_OR_REPLY(cargv1ptr,sto,ret)              \
    char *store_type = cargv1ptr;                                 \
    sto              = -1;                                        \
    for (int i = 0; i < NUM_STORAGE_TYPES; i++) {                 \
        if (!strcasecmp(store_type, StorageCommands[i].name)) {   \
            sto = i;                                              \
            break;                                                \
        }                                                         \
    }                                                             \
    if (sto == -1) {                                              \
        addReply(c, shared.storagetypeunkown);                    \
        return ret;                                               \
    }

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols, 
                         int          j_tbls [],
                         int          j_cols[]);
#endif /* __STORE__H */ 

/*
 * This file implements the indexing logic of Alsosql
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __INDEX__H
#define __INDEX__H

#include "redis.h"
#include "btreepriv.h"
#include "common.h"

int find_index( int tmatch, int cmatch);
int match_index(int tmatch, int indices[]);
int match_index_name(char *iname);
int checkIndexedColumnOrReply(redisClient *c, char *curr_tname);

/* MATCH_INDICES(tmatch)
     creates (int indices[], int matches)     */
#define MATCH_INDICES(tmatch)                      \
    int   indices[REDIS_DEFAULT_DBNUM];            \
    int   matches = match_index(tmatch, indices);

void newIndex(redisClient *c, char *iname, int tmatch, int cmatch, bool virt);
void createIndex(redisClient *c);
void legacyIndexCommand(redisClient *c);

void iAdd(bt *btr, robj *i_key, robj *i_val, uchar pktype);

void addToIndex(  redisDb *db, robj *pko, char *vals, uint32 cofsts[], int inum);
void delFromIndex(redisDb *db, robj *old_pk, robj *row, int inum, int tmatch);
void updateIndex( redisDb *db,
                  robj    *old_pk,
                  robj    *new_pk,
                  robj    *new_val,
                  robj    *row,
                  int      inum,
                  uchar    pk_update,
                  int      tmatch);

/* RANGE_CHECK_OR_REPLY(char *cargv3ptr) -
     creates (robj *low, robj *high)     */
#define RANGE_CHECK_OR_REPLY(cargv3ptr)                              \
    robj *low, *high;                                                \
    {                                                                \
        char *local_range = cargv3ptr;                               \
        char *local_nextc = strchr(local_range, CMINUS);             \
        if (!local_nextc) {                                          \
            addReply(c, shared.invalidrange);                        \
            return;                                                  \
        }                                                            \
        *local_nextc = '\0';                                         \
        local_nextc++;                                               \
        low  = createStringObject(local_range, strlen(local_range)); \
        high = createStringObject(local_nextc, strlen(local_nextc)); \
    }

void dropIndex(redisClient *c);

void iselectAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          i_match,
                   char        *col_list,
                   int          obc,
                   bool         asc,
                   int          lim);
void ideleteAction(redisClient *c, char *range, int tmatch, int imatch);
void iupdateAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          imatch,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]);

ull get_sum_all_index_size_for_table(redisClient *c, int tmatch);

#endif /* __INDEX__H */ 

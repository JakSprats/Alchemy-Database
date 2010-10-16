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

/* RANGE_CHECK_OR_REPLY(char *range, [retval]) -
     creates (robj *low, robj *high)     */
#define RANGE_CHECK_OR_REPLY(RANGE, RET)                             \
    robj *low, *high;                                                \
    {                                                                \
        char *local_range = RANGE;                                   \
        char *local_nextc = strchr(local_range, CMINUS);             \
        if (!local_nextc) {                                          \
            addReply(c, shared.invalidrange);                        \
            return RET;                                              \
        }                                                            \
        *local_nextc = '\0';                                         \
        local_nextc++;                                               \
        low  = createStringObject(local_range, strlen(local_range)); \
        high = createStringObject(local_nextc, strlen(local_nextc)); \
    }

void dropIndex(redisClient *c);

void iselectAction(redisClient *c,
                   robj        *rng,
                   int          tmatch,
                   int          i_match,
                   char        *col_list,
                   int          obc,
                   bool         asc,
                   int          lim,
                   list        *inl);
void ideleteAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          imatch,
                   int          obc,
                   bool         asc,
                   int          lim);
void iupdateAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          imatch,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[],
                   int          obc,
                   bool         asc,
                   int          lim);

ull get_sum_all_index_size_for_table(redisClient *c, int tmatch);

#define RANGE_QUERY_LOOKUP_START                                              \
    btEntry *be, *nbe;                                                        \
    robj *o       = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);      \
    robj *ind     = Index[server.dbid][imatch].obj;                           \
    bool  virt    = Index[server.dbid][imatch].virt;                          \
    robj *bt      = virt ? o : lookupKey(c->db, ind);                         \
    int   ind_col = (int)Index[server.dbid][imatch].column;                   \
    bool  pktype  = Tbl[server.dbid][tmatch].col_type[0];                     \
    bool  brk_pk  = (asc && obc == 0);                                        \
    bool  q_pk    = (!asc || (obc != -1 && obc != 0));                        \
    bool  brk_fk  = (asc  && obc != -1 && obc == ind_col);                    \
    bool  q_fk    = (obc != -1);                                              \
    qed           = virt ? q_pk : q_fk;                                       \
    btStreamIterator *bi  = btGetRangeIterator(bt, low, high, virt);          \
    btStreamIterator *nbi = NULL;                                             \
    while ((be = btRangeNext(bi, 1)) != NULL) {     /* iterate btree */       \
        if (virt) {                                                           \
            if (brk_pk && (uint32)lim == card) break; /* ORDER BY PK LIMIT */ \
            robj *key = be->key;                                              \
            robj *row = be->val;
            /* PK operation specific code comes here */
#define RANGE_QUERY_LOOKUP_MIDDLE                                             \
            card++;                                                           \
        } else {                                                              \
            robj *val = be->val;                                              \
            nbi       = btGetFullRangeIterator(val, 0, 0);                    \
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {  /* iter8 NodeBT */ \
                if (brk_fk && (uint32)lim == card) break;                     \
                robj *key = nbe->key;                                         \
                robj *row = btFindVal(o, key, pktype);
                /* FK operation specific code comes here */
#define RANGE_QUERY_LOOKUP_END                                                \
                card++;                                                       \
            }                                                                 \
            btReleaseRangeIterator(nbi);                                      \
            nbi = NULL; /* explicit in case of goto's in inner loop */        \
        }                                                                     \
    }                                                                         \
    btReleaseRangeIterator(bi);                                               \
    bi = NULL; /* explicit in case of goto's in inner loop */


#endif /* __INDEX__H */ 

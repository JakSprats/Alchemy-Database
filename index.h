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

#include "alsosql.h"
#include "btreepriv.h"
#include "parser.h"
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

void newIndex(redisClient *c,
              char        *iname,
              int          tmatch,
              int          cmatch,
              bool         virt,
              d_l_t       *nrlind);
void createIndex(redisClient *c);
void legacyIndexCommand(redisClient *c);

void iAdd(bt *btr, robj *i_key, robj *i_val, uchar pktype);

void addToIndex(redisDb *db, robj *pko, char *vals, uint32 cofsts[], int inum);
void delFromIndex(redisDb *db, robj *old_pk, robj *row, int inum, int tmatch);
void updateIndex( redisDb *db,
                  robj    *old_pk,
                  robj    *new_pk,
                  robj    *new_val,
                  robj    *row,
                  int      inum,
                  uchar    pk_update,
                  int      tmatch);

void freeNrlIndexObject(robj *o);
sds genNRL_Cmd(d_l_t  *nrlind,
               robj   *pko,
               char   *vals,
               uint32  cofsts[],
               bool    from_insert,
               robj   *row,
               int     tmatch);
void runCmdInFakeClient(sds s);
sds rebuildOrigNRLcmd(robj *o);


void dropIndex(redisClient *c);

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar);

void ideleteAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch);

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]);

ull get_sum_all_index_size_for_table(redisClient *c, int tmatch);

#define RANGE_QUERY_LOOKUP_START                                              \
    btEntry *be, *nbe;                                                        \
    robj *o       = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);      \
    robj *ind     = Index[server.dbid][w->imatch].obj;                        \
    bool  virt    = Index[server.dbid][w->imatch].virt;                       \
    robj *btt     = virt ? o : lookupKey(c->db, ind);                         \
    int   ind_col = (int)Index[server.dbid][w->imatch].column;                \
    bool  pktype  = Tbl[server.dbid][tmatch].col_type[0];                     \
    bool  brk_pk  = (w->asc && w->obc == 0);                                  \
    bool  q_pk    = (!w->asc || (w->obc != -1 && w->obc != 0));               \
    bool  brk_fk  = (w->asc  && w->obc != -1 && w->obc == ind_col);           \
    bool  q_fk    = (w->obc != -1);                                           \
    qed           = virt ? q_pk : q_fk;                                       \
    bi            = btGetRangeIterator(btt, w->low, w->high, virt);           \
    while ((be = btRangeNext(bi, 1)) != NULL) {     /* iterate btree */       \
        if (virt) {                                                           \
            if (w->ofst > 0) {                                                \
                w->ofst--;                                                    \
                continue;                                                     \
            }                                                                 \
            if (brk_pk && (uint32)w->lim == card) break; /* ORDRBY PK LIM */  \
            robj *key = be->key;                                              \
            robj *row = be->val;
            /* PK operation specific code comes here */
#define RANGE_QUERY_LOOKUP_MIDDLE                                             \
            card++;                                                           \
        } else {                                                              \
            robj *val = be->val;                                              \
            if (cstar) {                                                      \
                bt *nbtr = (bt *)val->ptr;                                    \
                card    += nbtr->numkeys;                                     \
            } else {                                                          \
                nbi       = btGetFullRangeIterator(val, 0, 0);                \
                while ((nbe = btRangeNext(nbi, 1)) != NULL) {  /* NodeBT */   \
                    if (w->ofst > 0) {                                        \
                        w->ofst--;                                            \
                        continue;                                             \
                    }                                                         \
                    if (brk_fk && (uint32)w->lim == card) break;              \
                    robj *key = nbe->key;                                     \
                    robj *row = btFindVal(o, key, pktype);
                    /* FK operation specific code comes here */
    #define RANGE_QUERY_LOOKUP_END                                            \
                    card++;                                                   \
                }                                                             \
            }                                                                 \
            btReleaseRangeIterator(nbi);                                      \
            nbi = NULL; /* explicit in case of goto's in inner loop */        \
        }                                                                     \
    }                                                                         \
    btReleaseRangeIterator(bi);                                               \
    bi = NULL; /* explicit in case of goto's in inner loop */



#define IN_QUERY_LOOKUP_START                                                 \
    listNode  *ln;                                                            \
    bool  virt   = Index[server.dbid][w->imatch].virt;                        \
    robj *o      = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);       \
    bool  pktype = Tbl[server.dbid][tmatch].col_type[0];                      \
    listIter         *li  = listGetIterator(w->inl, AL_START_HEAD);           \
    if (virt) {                                                               \
        bool  brk_pk  = (w->asc && w->obc == 0);                              \
        bool  q_pk    = (!w->asc || (w->obc != -1 && w->obc != 0));           \
        qed           = q_pk;                                                 \
        while((ln = listNext(li)) != NULL) {                                  \
            if (w->ofst > 0) {                                                \
                w->ofst--;                                                    \
                continue;                                                     \
            }                                                                 \
            if (brk_pk && (uint32)w->lim == card) break; /* ORDRBY PK LIM */  \
            robj *key = convertRobj(ln->value, pktype);                       \
            robj *row = btFindVal(o, key, pktype);                            \
            if (row) {
            /* PK operation specific code comes here */
#define IN_QUERY_LOOKUP_MIDDLE                                                \
               card++;                                                        \
            }                                                                 \
            decrRefCount(key); /* from addRedisCmdToINList() */               \
         }                                                                    \
     } else {                                                                 \
        btEntry *nbe;                                                         \
        robj *ind     = Index[server.dbid][w->imatch].obj;                    \
        robj *ibt     = lookupKey(c->db, ind);                                \
        int   ind_col = (int)Index[server.dbid][w->imatch].column;            \
        bool  fktype  = Tbl[server.dbid][tmatch].col_type[ind_col];           \
        bool  brk_fk  = (w->asc  && w->obc != -1 && w->obc == ind_col);       \
        bool  q_fk    = (w->obc != -1);                                       \
        qed           = q_fk;                                                 \
        while((ln = listNext(li)) != NULL) {                                  \
            robj *ikey = convertRobj(ln->value, fktype);                      \
            robj *val  = btIndFindVal(ibt->ptr, ikey, fktype);                \
            if (val) {                                                        \
                if (cstar) {                                                  \
                    bt *nbtr = (bt *)val->ptr;                                \
                    card    += nbtr->numkeys;                                 \
                } else {                                                      \
                    nbi = btGetFullRangeIterator(val, 0, 0);                  \
                    while ((nbe = btRangeNext(nbi, 1)) != NULL) {             \
                        if (w->ofst > 0) {                                    \
                            w->ofst--;                                        \
                            continue;                                         \
                        }                                                     \
                        if (brk_fk && (uint32)w->lim == card) break;          \
                        robj *key = nbe->key;                                 \
                        robj *row = btFindVal(o, key, pktype);
               /* FK operation specific code comes here */
#define IN_QUERY_LOOKUP_END                                                   \
                        card++;                                               \
                    }                                                         \
                }                                                             \
                btReleaseRangeIterator(nbi);                                  \
                nbi = NULL; /* explicit in case of goto's in inner loop */    \
            }                                                                 \
            decrRefCount(ikey); /* from addRedisCmdToINList() */              \
        }                                                                     \
    }                                                                         \
    listReleaseIterator(li);

#endif /* __INDEX__H */ 

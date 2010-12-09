/*
 * This file implements Range OPS (iselect, idelete, iupdate) for AlchemyDB
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

#ifndef __RANGE__H
#define __RANGE__H

#include "redis.h"

#include "alsosql.h"
#include "btreepriv.h"
#include "parser.h"
#include "common.h"

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar);

void ideleteAction(redisClient *c,
                   cswc_t      *w);

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]);


#define RANGE_QUERY_LOOKUP_START                                              \
    btEntry *be, *nbe;                                                        \
    robj *ind     = Index[server.dbid][w->imatch].obj;                        \
    bool  virt    = Index[server.dbid][w->imatch].virt;                       \
    int   ind_col = (int)Index[server.dbid][w->imatch].column;                \
    bool  pktype  = Tbl[server.dbid][w->tmatch].col_type[0];                  \
    bool  q_pk    = (!w->asc || (w->obc > 0));                                \
    bool  brk_pk  = (w->asc && w->obc == 0);                                  \
    bool  q_fk    = (w->obc > 0 && w->obc != ind_col);                        \
    bool  brk_fk  = (w->asc && !q_fk);                                        \
    robj *o       = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);   \
    robj *btt     = virt ? o : lookupKey(c->db, ind);                         \
    qed           = virt ? q_pk : q_fk;                                       \
    long  loops   = -1;                                                       \
    bi            = btGetRangeIterator(btt, w->low, w->high, virt);           \
    while ((be = btRangeNext(bi, 1)) != NULL) {     /* iterate btree */       \
        if (virt) {                                                           \
            loops++;                                                          \
            if (brk_pk) {                                                     \
                if (w->ofst != -1 && loops < w->ofst) continue;               \
                if ((uint32)w->lim == card) break; /* ORDRBY PK LIM */        \
            }                                                                 \
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
                    loops++;                                                  \
                    if (brk_fk) {                                             \
                        if (w->ofst != -1 && loops < w->ofst) continue;       \
                        if ((uint32)w->lim == card) break; /* ORDRBY FK LIM */\
                    }                                                         \
                    robj *key = nbe->key;                                     \
                    robj *row = btFindVal(o, key, pktype);
                    /* FK operation specific code comes here */
#define RANGE_QUERY_LOOKUP_END                                                \
                    card++;                                                   \
                }                                                             \
                if (brk_fk && (uint32)w->lim == card) break; /*ORDRBY FK LIM*/\
            }                                                                 \
            btReleaseRangeIterator(nbi);                                      \
            nbi = NULL; /* explicit in case of GOTO in inner loop */          \
        }                                                                     \
    }                                                                         \
    btReleaseRangeIterator(bi);                                               \
    bi = NULL; /* explicit in case of GOTO in inner loop */


#define IN_QUERY_LOOKUP_START                                                 \
    listNode  *ln;                                                            \
    bool      virt   = Index[server.dbid][w->imatch].virt;                    \
    bool      pktype = Tbl[server.dbid][w->tmatch].col_type[0];               \
    robj     *o      = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);\
    listIter *li     = listGetIterator(w->inl, AL_START_HEAD);                \
    if (virt) {                                                               \
        bool  brk_pk  = (w->asc && w->obc == 0);                              \
        bool  q_pk    = (!w->asc || (w->obc != -1 && w->obc != 0));           \
        qed           = q_pk;                                                 \
        while((ln = listNext(li)) != NULL) {                                  \
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
        bool  fktype  = Tbl[server.dbid][w->tmatch].col_type[ind_col];        \
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
                        if (brk_fk && (uint32)w->lim == card) break;          \
                        robj *key = nbe->key;                                 \
                        robj *row = btFindVal(o, key, pktype);
               /* FK operation specific code comes here */
#define IN_QUERY_LOOKUP_END                                                   \
                        card++;                                               \
                    }                                                         \
                }                                                             \
                btReleaseRangeIterator(nbi);                                  \
                nbi = NULL; /* explicit in case of GOTO in inner loop */      \
            }                                                                 \
            decrRefCount(ikey); /* from addRedisCmdToINList() */              \
        }                                                                     \
    }                                                                         \
    listReleaseIterator(li);

#endif /* __RANGE__H */ 

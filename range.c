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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>

#include "redis.h"
#include "adlist.h"

#include "bt.h"
#include "bt_iterator.h"
#include "orderby.h"
#include "index.h"
#include "alsosql.h"
#include "common.h"
#include "range.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer server;

extern char    *Col_type_defs[];
extern r_tbl_t  Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_DB][MAX_NUM_INDICES];


#define ISELECT_OPERATION(Q)                                               \
    if (!cstar) {                                                          \
        robj *r = outputRow(row, qcols, cmatchs, key, w->tmatch, 0);       \
        if (Q) addORowToRQList(ll, r, row, w->obc, key, w->tmatch, ctype); \
        else   addReplyBulk(c, r);                                         \
        decrRefCount(r);                                                   \
    }

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar) {
    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (w->obc != -1) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][w->tmatch].col_type[w->obc];
    }

    bool     qed = 0;
    btSIter *bi  = NULL;
    btSIter *nbi = NULL;
    LEN_OBJ
    if (w->low) { /* RANGE QUERY */
        RANGE_QUERY_LOOKUP_START
            ISELECT_OPERATION(q_pk)
        RANGE_QUERY_LOOKUP_MIDDLE
                ISELECT_OPERATION(q_fk)
        RANGE_QUERY_LOOKUP_END
    } else {    /* IN () QUERY */
        IN_QUERY_LOOKUP_START
            ISELECT_OPERATION(q_pk)
        IN_QUERY_LOOKUP_MIDDLE
                ISELECT_OPERATION(q_fk)
        IN_QUERY_LOOKUP_END
    }

    int sent = 0;
    if (card) {
        if (qed) {
            obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = vector[k];
                    addReplyBulk(c, ob->row);
                }
            }
            sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
            free(vector);
        } else {
            sent = card;
        }
    }
    if (ll) listRelease(ll);

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}


#define BUILD_RQ_OPERATION(Q)                                         \
    if (Q) {                                                          \
        addORowToRQList(ll, key, row, w->obc, key, w->tmatch, ctype); \
    } else {                                                          \
        robj *cln  = cloneRobj(key); /* clone orig is BtRobj */       \
        listAddNodeTail(ll, cln);                                     \
    }


#define BUILD_RANGE_QUERY_LIST                                                \
    list *ll    = listCreate();                                               \
    uchar ctype = COL_TYPE_NONE;                                              \
    if (w->obc != -1) {                                                       \
        ctype = Tbl[server.dbid][w->tmatch].col_type[w->obc];                 \
    }                                                                         \
                                                                              \
    bool     cstar = 0;                                                       \
    bool     qed   = 0;                                                       \
    ulong    card  = 0;                                                       \
    btSIter *bi    = NULL;                                                    \
    btSIter *nbi   = NULL;                                                    \
    if (w->low) { /* RANGE QUERY */                                           \
        RANGE_QUERY_LOOKUP_START                                              \
            BUILD_RQ_OPERATION(q_pk)                                          \
        RANGE_QUERY_LOOKUP_MIDDLE                                             \
                BUILD_RQ_OPERATION(q_fk)                                      \
        RANGE_QUERY_LOOKUP_END                                                \
    } else {    /* IN () QUERY */                                             \
        IN_QUERY_LOOKUP_START                                                 \
            BUILD_RQ_OPERATION(q_pk)                                          \
        IN_QUERY_LOOKUP_MIDDLE                                                \
                BUILD_RQ_OPERATION(q_fk)                                      \
        IN_QUERY_LOOKUP_END                                                   \
    }

void ideleteAction(redisClient *c,
                   cswc_t      *w) {
    BUILD_RANGE_QUERY_LIST

    MATCH_INDICES(w->tmatch)

    int sent = 0;
    if (card) {
        if (qed) {
            obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = vector[k];
                    robj *nkey = ob->row;
                    deleteRow(c, w->tmatch, nkey, matches, indices);
                }
            }
            sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
            free(vector);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                deleteRow(c, w->tmatch, nkey, matches, indices);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
}

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]) {
    BUILD_RANGE_QUERY_LIST

    bool pktype = Tbl[server.dbid][w->tmatch].col_type[0];
    int  sent   = 0;
    if (card) {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][w->tmatch].name);
        if (qed) {
            obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = vector[k];
                    robj *nkey = ob->row;
                    robj *row  = btFindVal(o, nkey, pktype);
                    updateRow(c, o, nkey, row, w->tmatch, ncols,
                              matches, indices, vals, vlens, cmiss);
                }
            }
            sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
            free(vector);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                robj *row  = btFindVal(o, nkey, pktype);
                updateRow(c, o, nkey, row, w->tmatch, ncols, matches, indices,
                          vals, vlens, cmiss);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
}

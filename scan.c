/*
 * This file implements the "SCANSELECT"
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
#include <assert.h>

#include "adlist.h"
#include "redis.h"

#include "bt_iterator.h"
#include "row.h"
#include "sql.h"
#include "alsosql.h"
#include "orderby.h"
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
static int col_cmp(char *a, char *b, int ctype) {
    if (ctype == COL_TYPE_INT) {
        return atoi(a) - atoi(b);
    } else if (ctype == COL_TYPE_FLOAT) {
        float f = atof(a) - atof(b);
        return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
    } else if (ctype == COL_TYPE_STRING) {
        return strcmp(a, b);
    }
    return 0;
}

// SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN
// SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN SCAN

/* TODO too many args for a full table scan -> pack into a struct */
static void condSelectReply(redisClient   *c,
                            cswc_t        *w,
                            robj          *o,
                            robj          *key,
                            robj          *row,
                            int            tmatch,
                            int            qcols,
                            int            cmatchs[],
                            unsigned long *card,
                            bool           no_wc,
                            uchar          ctype,
                            list          *ll,
                            bool           cstar) {
    char  *s;
    robj  *cr  = NULL;
    uchar  hit = 0;
    if (no_wc) {
        s = key->ptr;
        hit = 1;
    } else {
        cr       = createColObjFromRow(row, w->cmatch, NULL, tmatch); // freeME
        s        = cr->ptr;
        int type = Tbl[server.dbid][tmatch].col_type[w->cmatch];
        if (w->low) { /* RANGE QUERY */
            if (col_cmp(s, w->low->ptr,  type) >= 0 &&
                col_cmp(s, w->high->ptr, type) <= 0) {
                hit = 1;
            }
        } else {    /* IN () QUERY */
            listNode  *ln;
            listIter *li  = listGetIterator(w->inl, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *ink = ln->value;     
                if (col_cmp(s, ink->ptr, type) == 0) {
                    hit = 1;
                    break;
                }
            }
            listReleaseIterator(li);
        }
    }

    if (hit) {
        if (!cstar) {
            uchar  pktype = Tbl[server.dbid][tmatch].col_type[0];
            robj  *row    = btFindVal(o, key, pktype);
            robj  *r      = outputRow(row, qcols, cmatchs, key, tmatch, 0);
            if (w->obc != -1) {
                addORowToRQList(ll, r, row, w->obc, key, tmatch, ctype);
            } else {
                addReplyBulk(c, r);
                decrRefCount(r);
            }
        }
        *card = *card + 1;
    }
    if (cr) decrRefCount(cr);
}

void tscanCommand(redisClient *c) {
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  = 0;
    int  qcols  = 0;
    int  tmatch = -1;
    bool join   = 0;
    bool no_wc;       /* NO WHERE CLAUSE */
    sds  where = (c->argc > 4) ? c->argv[4]->ptr : NULL;
    sds  wc    = (c->argc > 5) ? c->argv[5]->ptr : NULL;
    if (!parseSelectReply(c, &no_wc, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          c->argv[3]->ptr, where)) return;
    if (join) {
        addReply(c, shared.scan_join);
        return;
    }

    uchar  sop = SQL_SCANSELECT;
    cswc_t w;
    init_check_sql_where_clause(&w, wc);

    if (no_wc && c->argc > 4) {
        if (!parseWCAddtlSQL(c, c->argv[4]->ptr, &w, tmatch, 0)) return;
        if (w.obc != -1) no_wc = 1; /* ORDER BY or STORE w/o WHERE CLAUSE */
    }

    bool rq    = 0;
    if (!no_wc && w.obc == -1) {
        uchar wtype  = checkSQLWhereClauseReply(c, &w, tmatch, sop, 0, 1);
        if (wtype == SQL_ERR_LOOKUP) goto tscan_cmd_err;
        if (w.imatch != -1) {/* disallow SCANSELECT on indexed columns */
            addReply(c, shared.scan_on_index);
            goto tscan_cmd_err;
        }

        if (wtype == SQL_RANGE_QUERY) {
            rq          = 1;
        } else { /* TODO this should be an SINGLE op */
            w.low  = w.key;
            w.high = w.key;
        }
    }

    uchar ctype = COL_TYPE_NONE;
    list *ll    = NULL;
    if (w.obc != -1) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][tmatch].col_type[w.obc];
    }

    LEN_OBJ
    btEntry *be;
    robj    *o  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    btSIter *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        condSelectReply(c, &w, o, be->key, be->val, tmatch, qcols, cmatchs,
                        &card, no_wc, ctype, ll, cstar);
    }
    btReleaseRangeIterator(bi);

    int sent = 0;
    if (w.obc != -1 && card) {
        obsl_t **vector = sortOrderByToVector(ll, ctype, w.asc);
        for (int k = 0; k < (int)listLength(ll); k++) {
            if (w.lim != -1 && sent == w.lim) break;
            if (w.ofst > 0) {
                w.ofst--;
            } else {
                sent++;
                obsl_t *ob = vector[k];
                addReplyBulk(c, ob->row);
            }
        }
        sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
        free(vector);
    }

    if (w.lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

tscan_cmd_err:
    if (!rq) {
        w.low  = NULL;
        w.high = NULL;
    }
    destroy_check_sql_where_clause(&w);
}

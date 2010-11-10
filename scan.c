/*
 * This file implements the "SCANSELECT"
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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
static int col_cmp(char *a, char *b, int type, bool bchar) {
    if (type == COL_TYPE_INT) {
        if (bchar) {
            return (int)(long)a - atoi(b);
        } else {
            return a - b;
        }
    } else if (type == COL_TYPE_STRING) {
        return strcmp(a, b);
    } else {
        assert(!"col_cmp DOUBLE not supported");
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
                            bool           icol,
                            list          *ll,
                            bool           cstar,
                            bool           bchar) {
    char *s;
    robj *cr = NULL;
    if (w->cmatch == -1) {
        s = key->ptr;
    } else {
        cr = createColObjFromRow(row, w->cmatch, NULL, tmatch); // freeME
        s = cr->ptr;
    }

    unsigned char hit = 0;
    if (no_wc) {
        hit = 1;
    } else {
        int type = Tbl[server.dbid][tmatch].col_type[w->cmatch];
        if (w->low) { /* RANGE QUERY */
            if (col_cmp(s, w->low->ptr,  type, bchar) >= 0 &&
                col_cmp(s, w->high->ptr, type, bchar) <= 0) {
                hit = 1;
            }
        } else {    /* IN () QUERY */
            listNode  *ln;
            listIter *li  = listGetIterator(w->inl, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *ink = ln->value;     
                if (col_cmp(s, ink->ptr, type, bchar) == 0) {
                    hit = 1;
                    break;
                }
            }
            listReleaseIterator(li);
        }
    }

    if (hit) {
        if (!cstar) {
            robj *row = btFindVal(o, key, Tbl[server.dbid][tmatch].col_type[0]);
            robj *r   = outputRow(row, qcols, cmatchs, key, tmatch, 0);
            if (w->obc != -1) {
                addORowToRQList(ll, r, row, w->obc, key, tmatch, icol);
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
    bool bchar = 0; /* means the b(2nd) arg in cmp() is type: "char *" */
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
            bchar = 1;
            w.low  = w.key;
            w.high = w.key;
        }
    }

    bool  icol = 0;
    list *ll   = NULL;
    if (w.obc != -1) {
        ll    = listCreate();
        icol  = (Tbl[server.dbid][tmatch].col_type[w.obc] == COL_TYPE_INT);
        bchar = 1;
    }

    LEN_OBJ
    btEntry *be;
    robj    *o  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    btSIter *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        condSelectReply(c, &w, o, be->key, be->val, tmatch, qcols, cmatchs,
                        &card, no_wc, icol, ll, cstar, bchar);
    }
    btReleaseRangeIterator(bi);

    if (w.obc != -1 && card) {
        obsl_t **vector = sortOrderByToVector(ll, icol, w.asc);
        for (int k = 0; k < (int)listLength(ll); k++) {
            if (w.lim != -1 && k == w.lim) break;
            obsl_t *ob = vector[k];
            addReplyBulk(c, ob->row);
        }
        sortedOrderByCleanup(vector, listLength(ll), icol, 1);
        free(vector);
    }

    if (w.lim != -1 && (uint32)w.lim < card) card = w.lim;
    if (cstar) lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    else       lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

tscan_cmd_err:
    if (!rq) {
        w.low  = NULL;
        w.high = NULL;
    }
    destroy_check_sql_where_clause(&w);
}

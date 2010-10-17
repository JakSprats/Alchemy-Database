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
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern char  CCOMMA;
extern char  CMINUS;
extern char *COMMA;

extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
static int col_cmp(char *a, char *b, int type, bool binl) {
    if (type == COL_TYPE_INT) {
        if (binl) {
            return a - b;
        } else {
            return a - atoi(b);
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
static void condSelectReply(redisClient   *c,
                            robj          *o,
                            robj          *key,
                            robj          *row,
                            int            tmatch,
                            int            cmatch,
                            int            qcols,
                            int            cmatchs[],
                            robj          *low,
                            robj          *high,
                            unsigned long *card,
                            bool           no_w_c,
                            list          *inl) {
    char *s;
    robj *cr = NULL;
    if (!cmatch) {
        s = key->ptr;
    } else {
        cr = createColObjFromRow(row, cmatch, NULL, tmatch); // freeME
        s = cr->ptr;
    }

    unsigned char hit = 0;
    if (no_w_c) {
        hit = 1;
    } else {
        int type = Tbl[server.dbid][tmatch].col_type[cmatch];
        if (low) { /* RANGE QUERY */
            if (col_cmp(s, low->ptr,  type, 0) >= 0 &&
                col_cmp(s, high->ptr, type, 0) <= 0) {
                hit = 1;
            }
        } else {    /* IN () QUERY */
            listNode  *ln;
            listIter *li  = listGetIterator(inl, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *ink = ln->value;     
                if (col_cmp(s, ink->ptr, type, 1) == 0) {
                    hit = 1;
                    break;
                }
            }
            listReleaseIterator(li);
        }
    }

    if (hit) {
        robj *row = btFindVal(o, key, Tbl[server.dbid][tmatch].col_type[0]);
        robj *r   = outputRow(row, qcols, cmatchs, key, tmatch, 0);
        addReplyBulk(c, r);
        decrRefCount(r);
        *card = *card + 1;
    }
    if (cr) decrRefCount(cr);
}

void tscanCommand(redisClient *c) {
    int   argn;
    uchar sop   = 3; /*used in ARGN_OVERFLOW() */
    robj *pko   = NULL, *rng  = NULL;
    sds   clist = sdsempty();
    for (argn = 1; argn < c->argc; argn++) { /* parse col_list */
        sds y = c->argv[argn]->ptr;
        if (!strcasecmp(y, "FROM")) break;

        if (*y == CCOMMA) {
             if (sdslen(y) == 1) continue;
             y++;
        }
        char *nextc = y;
        while ((nextc = strrchr(nextc, CCOMMA))) {
            *nextc = '\0';
            nextc++;
            if (sdslen(clist)) clist  = sdscatlen(clist, COMMA, 1);
            clist  = sdscat(clist, y);
            y      = nextc;
        }
        if (*y) {
            if (sdslen(clist)) clist  = sdscatlen(clist, COMMA, 1);
            clist  = sdscat(clist, y);
        }
    }

    if (argn == c->argc) {
        addReply(c, shared.selectsyntax_nofrom);
        goto tscan_cmd_err;
    }
    ARGN_OVERFLOW()

    TABLE_CHECK_OR_REPLY(c->argv[argn]->ptr,)

    bool no_w_c = 0; /* NO WHERE CLAUSE */
    if (argn == (c->argc - 1)) no_w_c = 1;

    if (!no_w_c) {
        ARGN_OVERFLOW()
    }

    int    imatch = -1;
    int    cmatch = -1;
    robj *rq_low  = NULL;
    robj *rq_high = NULL;
    bool  rq      = 0;
    list *inl     = NULL;


    int cmatchs[MAX_COLUMN_PER_TABLE];
    int qcols = parseColListOrReply(c, tmatch, clist, cmatchs);
    if (!qcols) goto tscan_cmd_err;

    if (!no_w_c) {
        bool  bdum;
        int   obc   = -1; /* ORDER BY col */
        bool  asc   = 1;
        int   lim   = -1;
        uchar wtype = checkSQLWhereClauseReply(c, &pko, &rng, &imatch,
                                               &cmatch, &argn, tmatch, 0, 0,
                                               &obc, &asc, &lim, &bdum, &inl);
        if (!wtype) goto tscan_cmd_err;
        if (wtype == SQL_RANGE_QUERY) {
            char *range = rng ? rng->ptr : NULL;
            if (range && !range_check_or_reply(c, range, &rq_low, &rq_high))
                return;
            rq          = 1;
        } else {
            rq_low  = pko;
            rq_high = pko;
        }
    }

    LEN_OBJ
    btEntry          *be;
    robj             *o  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    btStreamIterator *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        robj *key = be->key;
        robj *row = be->val;
        condSelectReply(c, o, key, row, tmatch, cmatch,
                        qcols, cmatchs, rq_low, rq_high, &card, no_w_c, inl);
    }
    btReleaseRangeIterator(bi);

    if (rq) {
        decrRefCount(rq_low);
        decrRefCount(rq_high);
    }

    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

tscan_cmd_err:
    if (inl) listRelease(inl);
    if (pko) decrRefCount(pko);
    if (rng) decrRefCount(rng);
    sdsfree(clist);
}

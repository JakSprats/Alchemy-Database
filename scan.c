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

#include "redis.h"
#include "bt_iterator.h"
#include "row.h"
#include "sql.h"
#include "alsosql.h"
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

// GLOBALS
extern char  CCOMMA;
extern char  CMINUS;
extern char *COMMA;

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
static int col_cmp(char *a, char *b, int type) {
    if (type == COL_TYPE_INT) {
        int i = atoi(a);
        int j = atoi(b);
        return i > j ? 1 : (i == j) ? 0 : -1;
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
                            unsigned long *card) {
    char *s;
    robj *r = NULL;
    if (!cmatch) {
        s = key->ptr;
    } else {
        r = createColObjFromRow(row, cmatch, NULL, tmatch); // freeME
        s = r->ptr;
    }

    unsigned char hit = 0;
    int type = Tbl_col_type[tmatch][cmatch];
    if (col_cmp(s, low->ptr,  type) >= 0 &&
        col_cmp(s, high->ptr, type) <= 0) {
        hit = 1;
    }

    if (hit) {
        selectReply(c, o, key, tmatch, cmatchs, qcols);
        *card = *card + 1;
    }
    if (r) decrRefCount(r);
}

void tscanCommand(redisClient *c) {
    int   argn;
    int   which = 3; /*used in ARGN_OVERFLOW() */
    robj *pko   = NULL, *range  = NULL;
    sds   clist = sdsempty();
    for (argn = 1; argn < c->argc; argn++) {
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
    ARGN_OVERFLOW()

    int cmatchs[MAX_COLUMN_PER_TABLE];
    int qcols = parseColListOrReply(c, tmatch, clist, cmatchs);
    if (!qcols) goto tscan_cmd_err;

    int            imatch = -1,    cmatch = -1;
    unsigned char  where  = checkSQLWhereClauseOrReply(c, &pko, &range, &imatch,
                                                       &cmatch, &argn, tmatch,
                                                       0, 0);
    if (!where) goto tscan_cmd_err;

    robj *o = lookupKeyRead(c->db, Tbl_name[tmatch]);
    LEN_OBJ
    bool rq = (where == 2); /* RANGE QUERY */
    robj *rq_low, *rq_high;
    if (rq) {
        RANGE_CHECK_OR_REPLY(range->ptr)
        rq_low  = low;
        rq_high = high;
    } else {
        rq_low  = pko;
        rq_high = pko;
    }

    btEntry          *be;
    btStreamIterator *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        robj *key = be->key;
        robj *row = be->val;
        condSelectReply(c, o, key, row, tmatch, cmatch,
                        qcols, cmatchs, rq_low, rq_high, &card);
    }
    btReleaseRangeIterator(bi);
    if (rq) {
        decrRefCount(rq_low);
        decrRefCount(rq_high);
    }

    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
tscan_cmd_err:
    sdsfree(clist);
    if (pko)   decrRefCount(pko);
    if (range) decrRefCount(range);
}

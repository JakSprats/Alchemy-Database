/*
 * This file implements the "SCANSELECT" and "NORMALIZE" commands
 *

MIT License

Copyright (c) 2010 Russell Sullivan

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
#include "index.h"
#include "store.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

// GLOBALS
extern char  CCOMMA;
extern char  CEQUALS;
extern char  CMINUS;
extern char *EQUALS;
extern char *COMMA;

extern char *Col_type_defs[];

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern robj          *Index_obj     [MAX_NUM_INDICES];
extern int            Indexed_column[MAX_NUM_INDICES];

#define STO_FUNC_INSERT 6

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
    int   which = 3; /*used in ARGN_OVERFLOW */
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
    ARGN_OVERFLOW

    TABLE_CHECK_OR_REPLY(c->argv[argn]->ptr,)
    ARGN_OVERFLOW

    int cmatchs[MAX_COLUMN_PER_TABLE];
    int qcols = parseColListOrReply(c, tmatch, clist, cmatchs);
    if (!qcols) goto tscan_cmd_err;

    int            imatch = -1,    cmatch = -1;
    unsigned char  where  = checkSQLWhereClauseOrReply(c, &pko, &range, &imatch,
                                                       &cmatch, &argn, tmatch,
                                                       0);
    if (!where) goto tscan_cmd_err;

    robj *o = lookupKeyReadOrReply(c, Tbl_name[tmatch], shared.nullbulk);
    if (!o) goto tscan_cmd_err;

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

    btEntry    *be;
    btIterator *bi = btGetFullRangeIterator(o, 0, 1);
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

// NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM
// NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM
static robj *makeWildcard(sds pattern, int plen, char *token, int token_len) {
    robj *r = createStringObject(pattern, plen);
    if (token_len) {
        r->ptr = sdscatlen(r->ptr, ":*:", 3);
        r->ptr = sdscatlen(r->ptr, token, token_len);
    }
    r->ptr  = sdscatlen(r->ptr, ":*", 2);
    return r;
}
static robj *makeTblName(sds pattern, int plen, char *token, int token_len) {
    robj *r = createStringObject(pattern, plen);
    if (token_len) {
        r->ptr = sdscatlen(r->ptr, "_", 1);
        r->ptr = sdscatlen(r->ptr, token, token_len);
    }
    return r;
}

#define MAX_NUM_NORM_TBLS 16
void normCommand(redisClient *c) {
    sds pattern = c->argv[1]->ptr;
    int plen    = sdslen(pattern);

    robj *new_tbls[MAX_NUM_NORM_TBLS];
    robj *ext_patt[MAX_NUM_NORM_TBLS];
    int   n_ep  = 0;
    sds   start = c->argv[2]->ptr; 
    char *token = start;
    char *nextc = start;
    while ((nextc = strchr(nextc, CCOMMA))) {
        ext_patt[n_ep] = makeWildcard(pattern, plen, token, nextc - token);
        new_tbls[n_ep] = makeTblName( pattern, plen, token, nextc - token);
        n_ep++;
        nextc++;
        token          = nextc;
    }
    ext_patt[n_ep] = makeWildcard(pattern, plen, token,
                                   sdslen(start) - (token - start));
    new_tbls[n_ep] = makeTblName( pattern, plen, token,
                                   sdslen(start) - (token - start));
    n_ep++;
    ext_patt[n_ep] = makeWildcard(pattern, plen, NULL, 0);
    new_tbls[n_ep] = makeTblName( pattern, plen, NULL, 0);
    n_ep++;

    robj          *col_defs[MAX_NUM_NORM_TBLS];
    robj          *h_data  [MAX_NUM_NORM_TBLS];
    unsigned char  pk_type[MAX_NUM_NORM_TBLS];
    for (int i = 0; i < n_ep; i++) {
        col_defs[i] = createSetObject();
        h_data  [i] = createSetObject();
        pk_type [i] = COL_TYPE_INT;
    }

    dictEntry    *de;
    int           nrows = 0;
    dictIterator *di = dictGetIterator(c->db->dict);
    while((de = dictNext(di)) != NULL) {
        robj *keyobj = dictGetEntryKey(de);
        sds   key    = keyobj->ptr;
        robj *valobj = dictGetEntryVal(de);
        for (int i = 0; i < n_ep; i++) {
            if (stringmatchlen(ext_patt[i]->ptr, sdslen(ext_patt[i]->ptr),
                               key, sdslen(key), 0)) {

                char      *cname = strrchr(key, ':') + 1;
                robj      *col   = createStringObject(cname, strlen(cname));
                dictEntry *edef  = dictFind((dict *)col_defs[i]->ptr, col);
                if (!edef) {
                    dictAdd((dict *)col_defs[i]->ptr, col, valobj);
                }

                char      *pk    = strchr(key, ':') + 1;
                int        pklen = strchr(pk, ':') - pk;
                robj      *pko   = createStringObject(pk, pklen);
                {
                    char *endptr;
                    long val = strtol(pko->ptr, &endptr, 10); /* test is INT */
                    if (endptr[0] != '\0') pk_type[i] = COL_TYPE_STRING;
                    val = 0; /* compiler warning */
                }

                dictEntry *epk  = dictFind((dict *)h_data[i]->ptr, pko);
                if (!epk) {
                    nrows++;
                    robj *set = createSetObject();
                    dictAdd((dict *)set->ptr,       col, valobj);
                    dictAdd((dict *)h_data[i]->ptr, pko, set);
                } else {
                    robj *set = epk->val;
                    dictEntry *ecol  = dictFind((dict *)set->ptr, col);
                    if (!ecol) {
                        dictAdd((dict *)set->ptr, col, valobj);
                    } else {
                        dictReplace((dict *)set->ptr, col, valobj);
                    }
                }

                break;
            }
        }
    }
    dictReleaseIterator(di);

    if (!nrows) {
        addReply(c, shared.czero);
        return;
    }

    robj               *argv[STORAGE_MAX_ARGC + 1];
    struct redisClient *fc = createFakeClient();
    fc->argv               = argv;

    LEN_OBJ
    for (int i = 0; i < n_ep; i++) {
        dictEntry    *de, *ide;
        dictIterator *di, *idi;
        argv[1]    = createStringObject(new_tbls[i]->ptr,
                                         sdslen(new_tbls[i]->ptr));
        robj *cdef = pk_type[i] == COL_TYPE_STRING ?
                                         createStringObject("pk=TEXT", 7) :
                                         createStringObject("pk=INT",  6); 

        di         = dictGetIterator(col_defs[i]->ptr);
        while((de = dictNext(di)) != NULL) {
            robj *key = de->key;
            robj *val = de->val;
            cdef->ptr = sdscatprintf(cdef->ptr, "%s%s%s%s",
                                      COMMA, (char *)key->ptr,
                                      EQUALS, Col_type_defs[val->encoding]);
        }
        dictReleaseIterator(di);

        robj *resp = createStringObject("table ", 6);
        resp->ptr  = sdscatlen(resp->ptr, new_tbls[i]->ptr, 
                                          sdslen(new_tbls[i]->ptr));
        resp->ptr  = sdscatlen(resp->ptr, " ", 1);
        resp->ptr  = sdscatlen(resp->ptr, cdef->ptr, sdslen(cdef->ptr));

        argv[2]    = cdef;
        fc->argc   = 3;

        legacyTableCommand(fc);
        if (!respOk(fc)) { /* most likely table already exists */
            listNode *ln = listFirst(fc->reply);
            robj     *o  = (robj *)ln->value;
            /* lenobj has already been addReply()d */
            sds       s  = sdsnewlen(o->ptr, sdslen(o->ptr));
            lenobj->ptr  = s;
            incrRefCount(lenobj);
            goto norm_err;
        }
        decrRefCount(cdef);

        di = dictGetIterator(h_data[i]->ptr);
        while((de = dictNext(di)) != NULL) {
            robj *key   = de->key;
            robj *val   = de->val;
            robj *ic    = createStringObject(key->ptr, sdslen(key->ptr));
            dict *row   = (dict*)val->ptr;
            idi         = dictGetIterator(col_defs[i]->ptr);
            while((ide = dictNext(idi)) != NULL) {
                ic->ptr         = sdscatlen(ic->ptr, COMMA, 1);
                robj      *ckey = ide->key;
                dictEntry *erow = dictFind(row, ckey);
                if (erow) {
                    robj *cval = erow->val;
                    if (cval->encoding != REDIS_ENCODING_RAW) {
                        ic->ptr = sdscatprintf(ic->ptr, "%ld",
                                                         (long)(cval->ptr));
                    } else {
                        ic->ptr = sdscatlen(ic->ptr, cval->ptr,
                                                      sdslen(cval->ptr));
                    }
                }
            }
            dictReleaseIterator(idi);

            argv[2]    = ic;
            fc->argc   = 3;
            if (!performStoreCmdOrReply(c, fc, STO_FUNC_INSERT)) {
                dictReleaseIterator(di);
                decrRefCount(resp);
                decrRefCount(ic);
                goto norm_err;
            }
            decrRefCount(ic);
        }
        dictReleaseIterator(di);

        addReplyBulk(c, resp);
        decrRefCount(resp);
        card++;
    }
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

norm_err:
    for (int i = 0; i < n_ep; i++) {
        decrRefCount(ext_patt[i]);
        decrRefCount(col_defs[i]);
        decrRefCount(h_data  [i]);
    }
    freeFakeClient(fc);
}

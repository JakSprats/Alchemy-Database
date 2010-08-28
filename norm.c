/*
 * This file implements the "SCANSELECT" and "NORMALIZE" commands
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

#include "common.h"
#include "bt_iterator.h"
#include "row.h"
#include "sql.h"
#include "index.h"
#include "store.h"
#include "redis.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

// GLOBALS
extern char  CCOMMA;
extern char  CEQUALS;
extern char *EQUALS;
extern char *COMMA;

extern char *Col_type_defs[];

#define STO_FUNC_INSERT 6

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
static bool is_int(robj *pko) {
    char *endptr;
    long val = strtol(pko->ptr, &endptr, 10);
    val = 0; /* compiler warning */
    if (endptr[0] != '\0') return 0;
    else                   return 1;
}

static void handleTableCreationError(redisClient *fc, robj *lenobj) {
    listNode *ln = listFirst(fc->reply);
    robj     *o  = (robj *)ln->value;
    /* UGLY: lenobj has already been addReply()d */
    sds       s  = sdsnewlen(o->ptr, sdslen(o->ptr));
    lenobj->ptr  = s;
    incrRefCount(lenobj);
}

/* build response string (which are the table definitions) */
static robj *createNormRespStringObject(sds nt, robj *cdef) {
    robj *resp   = createStringObject("CREATE TABLE ", 13);
    resp->ptr    = sdscatlen(resp->ptr, nt, sdslen(nt));
    resp->ptr    = sdscatlen(resp->ptr, " (", 2);
    sds sql_cdef = sdsnewlen(cdef->ptr, sdslen(cdef->ptr));
    for (uint i = 0; i < sdslen(sql_cdef); i++) {
        if (sql_cdef[i] == CEQUALS) sql_cdef[i] = ' ';
    }
    resp->ptr    = sdscatlen(resp->ptr, sql_cdef, sdslen(sql_cdef));
    sdsfree(sql_cdef);
    resp->ptr    = sdscatlen(resp->ptr, ")", 1);
    return resp;
}

// NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM
// NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM NORM
static robj *makeWildcard(sds pattern, int plen, char *token, int token_len) {
    robj *r = createStringObject(pattern, plen);
    if (token_len) {
        r->ptr = sdscatlen(r->ptr, ":*:", 3);
        r->ptr = sdscatlen(r->ptr, token, token_len);
        r->ptr = sdscatlen(r->ptr, "*", 1);
    } else {
        r->ptr = sdscatlen(r->ptr, ":*", 2);
    }
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

static void assignPkAndColToRow(robj *pko,
                                char *cname,
                                robj *valobj,
                                robj *cdefs[],
                                robj *rowvals[],
                                int  *nrows,
                                int   ep) {
    void *enc    = (void *)(long)valobj->encoding;
    /* If a string is an INT, we will store it as an INT */
    if (valobj->encoding == REDIS_ENCODING_RAW) {
        if (is_int(valobj)) enc = (void *)REDIS_ENCODING_INT;
    }

    robj      *col   = createStringObject(cname, strlen(cname));
    dictEntry *cdef  = dictFind((dict *)cdefs[ep]->ptr, col);
    if (!cdef) {                                /* new column */
        dictAdd((dict *)cdefs[ep]->ptr, col, enc);
    } else {                                    /* known column */
        void *cenc = cdef->val;
        if (cenc == (void *)REDIS_ENCODING_INT) { /* check enc */
            if (cenc != enc) { /* string in INT col -> TEXT col */
                dictReplace((dict *)cdefs[ep]->ptr, col, enc);
            }
        }
    }

    dictEntry *row  = dictFind((dict *)rowvals[ep]->ptr, pko);
    if (!row) {         /* row does not yet have columns defined */
        *nrows = *nrows + 1;
        robj *set = createSetObject();
        dictAdd((dict *)set->ptr,       col, valobj);
        dictAdd((dict *)rowvals[ep]->ptr, pko, set);
    } else {            /* row has at least one column defined */
        robj      *set  = row->val;
        dictAdd((dict *)set->ptr, col, valobj);
    }
}

#define MAX_NUM_NORM_TBLS 16
void normCommand(redisClient *c) {
    robj *new_tbls[MAX_NUM_NORM_TBLS];
    robj *ext_patt[MAX_NUM_NORM_TBLS];
    uint  ext_len [MAX_NUM_NORM_TBLS];
    sds   pattern = c->argv[1]->ptr;
    int   plen    = sdslen(pattern);
    int   n_ep    = 0;

    /* First: create wildcards and their destination tablenames */
    if (c->argc > 2) {
        sds   start = c->argv[2]->ptr; 
        char *token = start;
        char *nextc = start;
        while ((nextc = strchr(nextc, CCOMMA))) {
            ext_patt[n_ep] = makeWildcard(pattern, plen, token, nextc - token);
            ext_len [n_ep] = sdslen(ext_patt[n_ep]->ptr);
            new_tbls[n_ep] = makeTblName( pattern, plen, token, nextc - token);
            n_ep++;
            nextc++;
            token          = nextc;
        }
        int token_len  = sdslen(start) - (token - start);
        ext_patt[n_ep] = makeWildcard(pattern, plen, token, token_len);
        ext_len [n_ep] = sdslen(ext_patt[n_ep]->ptr);
        new_tbls[n_ep] = makeTblName( pattern, plen, token, token_len);
        n_ep++;
    }
    ext_patt[n_ep] = makeWildcard(pattern, plen, NULL, 0);
    sds e          = ext_patt[n_ep]->ptr;
    ext_len [n_ep] = strrchr(e, ':') - e + 1;
    new_tbls[n_ep] = makeTblName( pattern, plen, NULL, 0);
    n_ep++;

    robj  *cdefs   [MAX_NUM_NORM_TBLS];
    robj  *rowvals [MAX_NUM_NORM_TBLS];
    uchar  pk_type [MAX_NUM_NORM_TBLS];
    for (int i = 0; i < n_ep; i++) {
        cdefs  [i] = createSetObject();
        rowvals[i] = createSetObject();
        pk_type[i] = COL_TYPE_INT;
    }

    /* Second: search ALL keys and create column_definitions for wildcards */
    dictEntry    *de;
    int           nrows = 0;
    dictIterator *di = dictGetIterator(c->db->dict);
    while((de = dictNext(di)) != NULL) {                   /* search ALL keys */
        robj *keyobj = dictGetEntryKey(de);
        sds   key    = keyobj->ptr;
        for (int i = 0; i < n_ep; i++) {
            sds e = ext_patt[i]->ptr;
            if (stringmatchlen(e, sdslen(e), key, sdslen(key), 0)) { /* MATCH */
                robj *val    = dictGetEntryVal(de);
                char *pk     = strchr(key, ':') + 1;
                char *end_pk = strchr(pk, ':');
                int   pklen  = end_pk ? end_pk - pk : (int)strlen(pk);
                robj *pko    = createStringObject(pk, pklen);
                /* a single STRING means the PK is TEXT */
                if (!is_int(pko)) pk_type[i] = COL_TYPE_STRING;

                if (val->type == REDIS_HASH) { /* each hash key is a colname */
                    hashIterator *hi = hashInitIterator(val);
                    while (hashNext(hi) != REDIS_ERR) {
                        robj *hkey = hashCurrent(hi, REDIS_HASH_KEY);
                        robj *hval = hashCurrent(hi, REDIS_HASH_VALUE);
                        assignPkAndColToRow(pko, hkey->ptr, hval,
                                            cdefs, rowvals, &nrows, i);
                    }
                    hashReleaseIterator(hi);
                    break; /* match FIRST ext_patt[] */
                } else if (val->type == REDIS_STRING) {
                    char *cname;
                    if (i == (n_ep - 1)) { /* primary key */
                        cname = strchr(key + ext_len[i], ':') + 1;
                    } else {
                        /* 2ndary matches of REDIS_STRINGs MUST have the format
                            "primarywildcard:pk:secondarywildcard:columnname" */
                        if (sdslen(key) <= (ext_len[i] + 1) || /* ":" -> +1 */
                            key[ext_len[i] - 1] != ':') {
                            decrRefCount(pko);
                            continue;
                        }
                        cname = key + ext_len[i];
                    }
                    assignPkAndColToRow(pko, cname, val,
                                         cdefs, rowvals, &nrows, i);
                    break; /* match FIRST ext_patt[] */
                }
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
        /* Third: CREATE TABLE with column definitions from Step 2*/
        sds   nt   = new_tbls[i]->ptr;
        bool  cint = (pk_type[i] == COL_TYPE_STRING);
        robj *cdef = cint ?  createStringObject("pk=TEXT", 7) :
                             createStringObject("pk=INT",  6); 
        di         = dictGetIterator(cdefs[i]->ptr);
        while((de = dictNext(di)) != NULL) {
            robj *key = de->key;
            long  enc = (long)de->val;
            cdef->ptr = sdscatprintf(cdef->ptr, "%s%s%s%s",
                                      COMMA, (char *)key->ptr,
                                      EQUALS, Col_type_defs[enc]);
        }
        dictReleaseIterator(di);

        robj *resp  = createNormRespStringObject(nt, cdef);
        fc->argv[1] = createStringObject(nt, sdslen(nt));
        fc->argv[2] = cdef;
        fc->argc    = 3;

        legacyTableCommand(fc);
        if (!respOk(fc)) { /* most likely table already exists */
            handleTableCreationError(fc, lenobj);
            goto norm_err;
        }
        decrRefCount(cdef);

        /* Fourth: INSERT INTO new_table with rows from Step 2 */
        di = dictGetIterator(rowvals[i]->ptr);
        while((de = dictNext(di)) != NULL) {
            robj *key  = de->key;
            robj *val  = de->val;
            /* create SQL-ROW from key & loop[vals] */
            robj *ir   = createStringObject(key->ptr, sdslen(key->ptr));
            dict *row  = (dict*)val->ptr;
            idi        = dictGetIterator(cdefs[i]->ptr);
            while((ide = dictNext(idi)) != NULL) {
                ir->ptr         = sdscatlen(ir->ptr, COMMA, 1);
                robj      *ckey = ide->key;
                dictEntry *erow = dictFind(row, ckey);
                if (erow) {
                    robj *cval = erow->val;
                    void *cvp  = cval->ptr;
                    bool rwenc = (cval->encoding != REDIS_ENCODING_RAW);
                    ir->ptr = rwenc ? sdscatprintf(ir->ptr, "%ld", (long)(cvp)):
                                      sdscatlen(   ir->ptr, cvp,   sdslen(cvp));
                }
            }
            dictReleaseIterator(idi);

            fc->argv[1] = createStringObject(nt, sdslen(nt));
            fc->argv[2] = ir;
            fc->argc    = 3;
            if (!performStoreCmdOrReply(c, fc, STO_FUNC_INSERT)) {
                dictReleaseIterator(di);
                decrRefCount(resp);
                decrRefCount(ir);
                goto norm_err;
            }
            decrRefCount(ir);
        }
        dictReleaseIterator(di);

        addReplyBulk(c, resp);
        decrRefCount(resp);
        card++;
    }
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);

norm_err:
    for (int i = 0; i < n_ep; i++) {
        decrRefCount(new_tbls[i]);
        decrRefCount(ext_patt[i]);
        decrRefCount(cdefs   [i]);
        decrRefCount(rowvals [i]);
    }
    freeFakeClient(fc);
}

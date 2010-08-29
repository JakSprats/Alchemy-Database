/*
 *
 * This file implements "CREATE TABLE x AS redis_datastructure"
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

#include "redis.h"
#include "index.h"
#include "store.h"
#include "alsosql.h"
#include "sql.h"
#include "join.h"
#include "denorm.h"
#include "bt_iterator.h"
#include "row.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern int            Tbl_col_count[MAX_NUM_TABLES];
extern robj          *Tbl_col_name [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];

stor_cmd AccessCommands[NUM_ACCESS_TYPES];
char *DUMP = "DUMP";

static robj *_createStringObject(char *s) {
    return createStringObject(s, strlen(s));
}

static bool addSingle(redisClient *c,
                      redisClient *fc,
                      robj        *key,
                      long        *instd,
                      bool         is_insert) {
    robj *vals  = createObject(REDIS_STRING, NULL);
    if (is_insert) {
        vals->ptr   = sdsnewlen(key->ptr, sdslen(key->ptr));
    } else {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s",  *instd, (char *)key->ptr):
            sdscatprintf(sdsempty(), "%ld,%ld", *instd, (long)  key->ptr);
    }
    fc->argv[2] = vals;
    //RL4 "SGL: INSERTING [1]: %s [2]: %s", fc->argv[1]->ptr, fc->argv[2]->ptr);
    legacyInsertCommand(fc);
    decrRefCount(vals);
    if (!respOk(fc)) { /* insert error */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    *instd = *instd + 1;
    return 1;
}

static bool addDouble(redisClient *c,
                     redisClient *fc,
                     robj        *key,
                     robj        *val,
                     long        *instd,
                     bool         val_is_dbl) {
    robj *vals  = createObject(REDIS_STRING, NULL);
    if (val_is_dbl) {
        double d = *((double *)val);
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%f", 
                          *instd, (char *)key->ptr, d) :
            sdscatprintf(sdsempty(), "%ld,%ld,%f",
                          *instd, (long)  key->ptr, d);
    } else if (val->encoding == REDIS_ENCODING_RAW) {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%s", 
                          *instd, (char *)key->ptr, (char *)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%s",
                          *instd, (long)  key->ptr, (char *)val->ptr);
    } else {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%ld", 
                          *instd, (char *)key->ptr, (long)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%ld",
                          *instd, (long)  key->ptr, (long)val->ptr);
    }
    fc->argv[2] = vals;
    //RL4 "DBL: INSERTING [1]: %s [2]: %s", fc->argv[1]->ptr, fc->argv[2]->ptr);
    legacyInsertCommand(fc);
    decrRefCount(vals);
    if (!respOk(fc)) { /* insert error */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    *instd = *instd + 1;
    return 1;
}

/* TODO the protocol-parsing does not exactly follow the line protocol,
        it follow what the code does ... the code could change */

/* NOTE: this function implements a fakeClient pipe */
void createTableAsObjectOperation(redisClient *c, bool is_insert) {
    robj               *wargv[3];
    struct redisClient *wfc    = createFakeClient(); /* one client to write */
    wfc->argc                  = 3;
    wfc->argv                  = wargv;
    wfc->argv[1]               = c->argv[2]; /* table name */

    robj               **rargv = malloc(sizeof(robj *) * c->argc);
    struct redisClient *rfc    = createFakeClient(); /* one client to read */
    rfc->argv                  = rargv;
    for (int i = 5; i < c->argc; i++) {
        rfc->argv[i - 4] = c->argv[i];
    }
    rfc->argc                  = c->argc - 4;
    rfc->db                    = c->db;

    struct redisCommand *cmd = lookupCommand(c->argv[4]->ptr);
    cmd->proc(rfc);

    long instd = 1; /* ZER0 as pk is sometimes bad */
    listNode *ln;
    listIter li;
    robj *o;
    listRewind(rfc->reply,&li);
    while((ln = listNext(&li))) {
        o     = ln->value;
        sds s = o->ptr;
        /* ignore protocol, we just want data */
        if (*s == '*' || *s == '\r') continue;
        if (*s == '-')               break; /* error */
        if (*s == '$') { /* parse doubles which are w/in this list element */
            char *x = strchr(s, '\r');
            uint line_len = x - s;
            if (line_len + 2 < sdslen(s)) { /* got a double */
                x += 2; /* move past \r\n */
                char *y = strchr(x, '\r'); /* kill the final \r\n */
                *y = '\0';
                robj *r = createStringObject(x, strlen(x));
                if (!addSingle(c, wfc, r, &instd, is_insert)) goto cr8tbloo_err;
            }
            continue;
        }
        /* all ranges are single */
        if (!addSingle(c, wfc, o, &instd, is_insert)) goto cr8tbloo_err;
    }

cr8tbloo_err:
    freeFakeClient(rfc);
    freeFakeClient(wfc);
    free(rargv);
    addReply(c, shared.ok);
    return;
}

void createTableAsObject(redisClient *c) {
    robj *axs_type = c->argv[4];
    int   axs      = -1;
    for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
        if (!strcasecmp(axs_type->ptr, AccessCommands[i].name)) {
            axs = i;
            break;
        }
    }
  
    if (axs != -1) {
        if (c->argc < (4 + AccessCommands[axs].argc)) {
            addReply(c, shared.create_table_as_access_num_args);
            return;
        }
    } else {
        if (strcasecmp(axs_type->ptr, DUMP)) {
            addReply(c, shared.create_table_as_function_not_found);
            return;
        }
        if (c->argc < 6) {
            addReply(c, shared.create_table_as_dump_num_args);
            return;
        }
    }

    robj *cdef;
    bool  single;
    robj *o  = NULL;
    if (axs == ACCESS_SELECT_COMMAND_NUM) {
        int   which = 0; /*used in ARGN_OVERFLOW */
        int   argn  = 5;
        sds   clist = sdsempty();

        parseSelectColumnList(c, &clist, &argn);
        /* parseSelectColumnList edits the cargv ----------------\/           */
        sdsfree(c->argv[5]->ptr);                             /* so free it   */
        c->argv[5]->ptr = sdsnewlen(clist, sdslen(clist));;   /* and recr8 it */
        ARGN_OVERFLOW                      /* skip SQL keyword"FROM" */

        robj               *argv[3];
        bool                ret   = 1;
        int                 qcols = 0;
        struct redisClient *rfc   = createFakeClient();
        rfc->argv                 = argv;
        rfc->argv[1]              = c->argv[2];
        if (strchr(clist, '.')) { /* CREATE TABLE AS SELECT JOIN */
            int j_tbls [MAX_JOIN_INDXS];
            int j_cols [MAX_JOIN_INDXS];
            qcols = multiColCheckOrReply(c, clist, j_tbls, j_cols);
            if (qcols) ret = createTableFromJoin(c, rfc, qcols, j_tbls, j_cols);
        } else {
            TABLE_CHECK_OR_REPLY(c->argv[argn]->ptr,);
            int cmatchs[MAX_COLUMN_PER_TABLE];
            qcols = parseColListOrReply(c, tmatch, clist, cmatchs);
            if (qcols) ret = internalCreateTable(c, rfc, qcols,
                                                 cmatchs, tmatch);
        }
        freeFakeClient(rfc);
        if (!ret || !qcols) return;

        createTableAsObjectOperation(c, 1);

        addReply(c, shared.ok);
        return;
    } else {
        robj *key = c->argv[5];
        o         = lookupKeyReadOrReply(c, key, shared.nullbulk);
        if (!o) return;

        if (axs != -1) { /* all ranges are single */
            cdef = _createStringObject("pk=INT,value=TEXT");
            single = 1;
        } else if (o->type == REDIS_LIST) {
            cdef = _createStringObject("pk=INT,lvalue=TEXT");
            single = 1;
        } else if (o->type == REDIS_SET) {
            cdef = _createStringObject("pk=INT,svalue=TEXT");
            single = 1;
        } else if (o->type == REDIS_ZSET) {
            cdef = _createStringObject("pk=INT,zkey=TEXT,zvalue=TEXT");
            single = 0;
        } else if (o->type == REDIS_HASH) {
            cdef = _createStringObject("pk=INT,hkey=TEXT,hvalue=TEXT");
            single = 0;
        } else {
            addReply(c, shared.createtable_as_on_wrong_type);
            return;
        }
    }

    { /* CREATE TABLE */
        robj               *argv[3];
        struct redisClient *fc = createFakeClient();
        fc->argv               = argv;
        fc->argv[1]            = c->argv[2];
        fc->argv[2]            = cdef;
        fc->argc               = 3;

        legacyTableCommand(fc);
        if (!respOk(fc)) { /* most likely table already exists */
            listNode *ln = listFirst(fc->reply);
            addReply(c, ln->value);
            freeFakeClient(fc);
            return;
        }
        freeFakeClient(fc);
    }

    if (axs != -1) {
        createTableAsObjectOperation(c, 0);
    } else {
        robj               *argv[3];
        struct redisClient *dfc    = createFakeClient();
        dfc->argv                  = argv;
        dfc->argv[1]               = c->argv[2]; /* table name */
        long                instd = 1;          /* ZER0 as PK can be bad */
        if (o->type == REDIS_LIST) {
            list     *list = o->ptr;
            listNode *ln   = list->head;
            while (ln) {
                robj *key = listNodeValue(ln);
                if (!addSingle(c, dfc, key, &instd, 0)) goto cr8tbldmp_err;
                ln = ln->next;
            }
        } else if (o->type == REDIS_SET) {
            dictEntry    *de;
            dict         *set = o->ptr;
            dictIterator *di  = dictGetIterator(set);
            while ((de = dictNext(di)) != NULL) {   
                robj *key  = dictGetEntryKey(de);
                if (!addSingle(c, dfc, key, &instd, 0)) goto cr8tbldmp_err;
            }
            dictReleaseIterator(di);
        } else if (o->type == REDIS_ZSET) {
            dictEntry    *de;
            zset         *zs  = o->ptr;
            dictIterator *di  = dictGetIterator(zs->dict);
            while ((de = dictNext(di)) != NULL) {   
                robj *key = dictGetEntryKey(de);
                robj *val = dictGetEntryVal(de);
                if (!addDouble(c, dfc, key, val, &instd, 1)) goto cr8tbldmp_err;
            }
            dictReleaseIterator(di);
        } else {
            hashIterator *hi = hashInitIterator(o);
            while (hashNext(hi) != REDIS_ERR) {
                robj *key = hashCurrent(hi, REDIS_HASH_KEY);
                robj *val = hashCurrent(hi, REDIS_HASH_VALUE);
                if (!addDouble(c, dfc, key, val, &instd, 0)) goto cr8tbldmp_err;
            }
            hashReleaseIterator(hi);
        }
        addReply(c, shared.ok);

cr8tbldmp_err:
        freeFakeClient(dfc);
    }
}

void denormCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    sds wildcard = c->argv[2]->ptr;
    if (!strchr(wildcard, '*')) {
        addReply(c, shared.denorm_wildcard_no_star);
        return;
    }

    uint wlen = sdslen(wildcard);
    uint spot = 0;
    for (uint i = 0; i < wlen; i++) {
        if (wildcard[i] == '*') {
            spot = i;
            break;
        }
    }
    uint  restlen  = (spot < wlen - 2) ? wlen - spot - 1: 0;
    sds   s_wldcrd = sdsnewlen(wildcard, spot);
    s_wldcrd       = sdscatlen(s_wldcrd, "%s", 2);
    if (restlen) s_wldcrd = sdscatlen(s_wldcrd, &wildcard[spot + 1], restlen);
    sds   d_wldcrd = sdsdup(s_wldcrd);
    char *fmt      = strstr(d_wldcrd, "%s") + 1;
    *fmt           = 'd';

    robj               *argv[4];
    struct redisClient *fc = createFakeClient();
    fc->argv               = argv;
    fc->argc               = 4;

    btEntry    *be;
    robj       *o  = lookupKeyRead(c->db, Tbl_name[tmatch]);
    btIterator *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        robj *key = be->key;
        robj *row = be->val;

        sds hname = sdsempty();
        if (key->encoding == REDIS_ENCODING_RAW) {
            hname = sdscatprintf(hname, s_wldcrd, key->ptr);
        } else {
            hname = sdscatprintf(hname, d_wldcrd, key->ptr);
        }
        fc->argv[1] = createStringObject(hname, sdslen(hname));
        sdsfree(hname);

        for (int i = 1; i < Tbl_col_count[tmatch]; i++) { /* PK is in name */
            robj *r     = createColObjFromRow(row, i, key, tmatch);
            sds tname   = Tbl_col_name[tmatch][i]->ptr;
            fc->argv[2] = createStringObject(tname, sdslen(tname));
            sds cname   = r->ptr;
            fc->argv[3] = createStringObject(cname, sdslen(cname));
            hsetCommand(fc);
        }
    }

    addReply(c, shared.ok);
    sdsfree(s_wldcrd);
    sdsfree(d_wldcrd);
    freeFakeClient(fc);
}

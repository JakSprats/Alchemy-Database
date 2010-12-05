/*
 *
 * This file implements "CREATE TABLE x AS redis_datastructure"
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

#include "redis.h"
#include "zmalloc.h"

#include "sql.h"
#include "store.h"
#include "join.h"
#include "bt_iterator.h"
#include "row.h"
#include "bt.h"
#include "denorm.h" /* for fakeClientPipe */
#include "parser.h"
#include "legacy.h"
#include "alsosql.h"
#include "cr8tblas.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern int      Num_tbls     [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

stor_cmd AccessCommands[NUM_ACCESS_TYPES];

static robj *_createStringObject(char *s) {
    return createStringObject(s, strlen(s));
}

static bool addSingle(redisClient *c,
                      void        *x,
                      robj        *key,
                      long        *card,
                      int          is_ins,
                      int          nlines) {
    nlines = 0; /* compiler warning */
    redisClient *fc    = (redisClient *)x;
    robj        *vals  = createObject(REDIS_STRING, NULL);
    if (is_ins) {
        vals->ptr      = sdsnewlen(key->ptr, sdslen(key->ptr));
    } else {
        vals->ptr      = (key->encoding == REDIS_ENCODING_RAW) ?
                 sdscatprintf(sdsempty(), "%ld,%s",  *card, (char *)key->ptr) :
                 sdscatprintf(sdsempty(), "%ld,%ld", *card, (long)  key->ptr);
    }
    fc->argv[2]        = vals;
    //RL4 "SGL: INSERTING [1]: %s [2]: %s", fc->argv[1]->ptr, fc->argv[2]->ptr);
    legacyInsertCommand(fc);
    decrRefCount(vals);
    if (!respOk(fc)) { /* insert error */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    *card = *card + 1;
    return 1;
}

static bool addDouble(redisClient *c,
                      redisClient *fc,
                      robj        *key,
                      robj        *val,
                      long        *card,
                      bool         val_is_dbl) {
    robj *vals  = createObject(REDIS_STRING, NULL);
    if (val_is_dbl) {
        double d = *((double *)val);
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%f", 
                          *card, (char *)key->ptr, d) :
            sdscatprintf(sdsempty(), "%ld,%ld,%f",
                          *card, (long)  key->ptr, d);
    } else if (val->encoding == REDIS_ENCODING_RAW) {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%s", 
                          *card, (char *)key->ptr, (char *)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%s",
                          *card, (long)  key->ptr, (char *)val->ptr);
    } else {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%ld", 
                          *card, (char *)key->ptr, (long)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%ld",
                          *card, (long)  key->ptr, (long)val->ptr);
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
    *card = *card + 1;
    return 1;
}

void createTableAsObjectOperation(redisClient  *c,
                                  int           is_ins,
                                  robj        **rargv,
                                  int           rargc) {
    robj               *wargv[3];
    struct redisClient *wfc    = rsql_createFakeClient(); /* client to write */
    wfc->argc                  = 3;
    wfc->argv                  = wargv;
    wfc->argv[1]               = c->argv[2]; /* table name */

    /* TODO parse as_cmd */
    struct redisClient *rfc    = rsql_createFakeClient(); /* client to read */
    rfc->argv                  = rargv;
    rfc->argc                  = rargc;
    rfc->db                    = c->db;

    flag flg = 0;
    fakeClientPipe(c, rfc, wfc, is_ins, &flg, addSingle, emptyNoop);

    rsql_freeFakeClient(rfc);
    rsql_freeFakeClient(wfc);
    zfree(rargv);
    addReply(c, shared.ok); /* TODO return rows created */
    return;
}

void createTableAsSelect(redisClient *c, char *as_cmd) {
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  = 0;
    int  qcols  = 0;
    int  tmatch = -1;
    bool join   = 0;

    int rargc = 6;
    robj **rargv = parseSelectCmdToArgv(as_cmd);

    if (!parseSelectReply(c, 0, &tmatch, cmatchs, &qcols, &join,
                          &cstar,  rargv[1]->ptr, rargv[2]->ptr,
                          rargv[3]->ptr, rargv[4]->ptr)) return;
    if (cstar) {
        addReply(c, shared.select_store_count);
        return;
    }

    robj        *argv[3];
    bool         ret     = 0;
    bool         ok      = 0;
    redisClient *rfc     = rsql_createFakeClient();
    rfc->argv            = argv;
    rfc->argv[1]         = c->argv[2];
    if (join) { /* CREATE TABLE AS SELECT JOIN */
        jb_t jb;
        init_join_block(&jb, rargv[5]->ptr);
        ok    = parseJoinReply(c, 1, &jb, rargv[1]->ptr, rargv[3]->ptr);
        qcols = jb.qcols;
        if (ok && qcols) {
            ret = createTableFromJoin(c, rfc, qcols, jb.j_tbls, jb.j_cols);
        }
        destroy_join_block(&jb);
    } else  if (qcols) { /* check WHERE clause for syntax */
        uchar  sop = SQL_SELECT;
        cswc_t w;
        init_check_sql_where_clause(&w, rargv[5]->ptr);
        uchar wtype  = checkSQLWhereClauseReply(c, &w, tmatch, sop, 1, 0);
        if (wtype != SQL_ERR_LOOKUP) {
            ret = internalCreateTable(c, rfc, qcols, cmatchs, tmatch);
            ok  = 1;
        }
        destroy_check_sql_where_clause(&w);
    }
    rsql_freeFakeClient(rfc);
    if (!ret || !ok || !qcols) {
        addReply(c, shared.create_table_as_select);
        return;
    }

    createTableAsObjectOperation(c, 1, rargv, rargc);

    addReply(c, shared.ok);
    return;
}

int getAccessCommNum(char *as_cmd) {
    int   axs    = -1;
    for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
        if (!strncasecmp(as_cmd, AccessCommands[i].name,
                                 strlen(AccessCommands[i].name))) {
            char *x = as_cmd + strlen(AccessCommands[i].name);
            if (*x == ' ') {
                axs = i;
                break;
            }
        }
    }
    return axs;
}

void createTableAsObject(redisClient *c) {
    char *as     = c->argv[3]->ptr;
    char *as_cmd = next_token(as);
    if (!as_cmd) {
        addReply(c, shared.create_table_as_access_num_args);
        return;
    }
    int   axs    = getAccessCommNum(as_cmd);
  
    char *dumpee = NULL;
    if (axs == -1) { /* quick argc parsing validation */
        if (strncasecmp(as_cmd, "DUMP ", 5)) {
            addReply(c, shared.create_table_as_function_not_found);
            return;
        }
    }

    if (axs == ACCESS_SELECT_COMMAND_NUM) {
        createTableAsSelect(c, as_cmd);
        return;
    }

    dumpee = next_token(as_cmd);
    if (!dumpee) {
        addReply(c, shared.create_table_as_dump_num_args);
        return;
    }
    robj *cdef;
    bool  single;
    robj *o  = NULL;
    if (dumpee) {
        robj *key = createStringObject(dumpee, get_token_len(dumpee));
        o         = lookupKeyReadOrReply(c, key, shared.nullbulk);
        decrRefCount(key);
        if (!o) return;
    }

    bool table_created = 0;
    if (axs != -1) { /* all Redis COMMANDS produce single results */
        cdef = _createStringObject("pk=INT,value=TEXT");
        single = 1;
    } else if (o->type == REDIS_BTREE) { /* DUMP one table to another */
        bt *btr = (bt *)o->ptr;
        if (btr->is_index != BTREE_TABLE) {
            addReply(c, shared.createtable_as_index);
            return;
        }
        TABLE_CHECK_OR_REPLY(dumpee,)

        bool bdum;
        int  cmatchs[MAX_COLUMN_PER_TABLE];
        int  qcols  = 0;
        parseCommaSpaceListReply(NULL, "*", 1, 0, 0, tmatch, cmatchs,
                                 0, NULL, NULL, NULL, &qcols, &bdum);

        robj               *argv[3];
        struct redisClient *cfc = rsql_createFakeClient();
        cfc->argv               = argv;
        cfc->argv[1]            = c->argv[2]; /* new tablename */

        bool ret = internalCreateTable(c, cfc, qcols, cmatchs, tmatch);
        rsql_freeFakeClient(cfc);
        if (!ret) return;
        table_created = 1;
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

    if (!table_created) { /* CREATE TABLE */
        robj               *argv[3];
        struct redisClient *fc = rsql_createFakeClient();
        fc->argv               = argv;
        fc->argv[1]            = c->argv[2];
        fc->argv[2]            = cdef;
        fc->argc               = 3;

        legacyTableCommand(fc);
        if (!respOk(fc)) { /* most likely table already exists */
            listNode *ln = listFirst(fc->reply);
            addReply(c, ln->value);
            rsql_freeFakeClient(fc);
            return;
        }
        rsql_freeFakeClient(fc);
    }

    if (axs != -1) { /* EXEC "redis_command redis_args" to table */
        int    rargc;
        robj **rargv = parseCmdToArgv(as_cmd, &rargc);
        createTableAsObjectOperation(c, 0, rargv, rargc);
    } else {         /* DUMP "Redis_object"             to table */
        robj               *argv[3];
        struct redisClient *dfc  = rsql_createFakeClient();
        dfc->argv                = argv;
        dfc->argv[1]             = c->argv[2]; /* table name */
        long                card = 1;          /* ZER0 as PK can be bad */
        if (o->type == REDIS_LIST) {
            list     *list = o->ptr;
            listNode *ln   = list->head;
            while (ln) {
                robj *key = listNodeValue(ln);
                if (!addSingle(c, dfc, key, &card, 0, 0)) goto cr8tbldmp_err;
                ln = ln->next;
            }
        } else if (o->type == REDIS_SET) {
            dictEntry    *de;
            dict         *set = o->ptr;
            dictIterator *di  = dictGetIterator(set);
            while ((de = dictNext(di)) != NULL) {   
                robj *key  = dictGetEntryKey(de);
                if (!addSingle(c, dfc, key, &card, 0, 0)) goto cr8tbldmp_err;
            }
            dictReleaseIterator(di);
        } else if (o->type == REDIS_ZSET) {
            dictEntry    *de;
            zset         *zs  = o->ptr;
            dictIterator *di  = dictGetIterator(zs->dict);
            while ((de = dictNext(di)) != NULL) {   
                robj *key = dictGetEntryKey(de);
                robj *val = dictGetEntryVal(de);
                if (!addDouble(c, dfc, key, val, &card, 1)) goto cr8tbldmp_err;
            }
            dictReleaseIterator(di);
        } else if (o->type == REDIS_HASH) {
            hashIterator *hi = hashInitIterator(o);
            while (hashNext(hi) != REDIS_ERR) {
                robj *key = hashCurrent(hi, REDIS_HASH_KEY);
                robj *val = hashCurrent(hi, REDIS_HASH_VALUE);
                if (!addDouble(c, dfc, key, val, &card, 0)) goto cr8tbldmp_err;
            }
            hashReleaseIterator(hi);
        } else if (o->type == REDIS_BTREE) {
            btEntry          *be;
            /* table just created */
            int      tmatch = Num_tbls[server.dbid] - 1;
            int      pktype = Tbl[server.dbid][tmatch].col_type[0];
            robj    *tname  = Tbl[server.dbid][tmatch].name;
            robj    *new_o  = lookupKeyWrite(c->db, tname);
            btSIter *bi     = btGetFullRangeIterator(o, 0, 1);
            while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
                btAdd(new_o, be->key, be->val, pktype); /* row-to-row copy */
            }
        }
        addReply(c, shared.ok);

cr8tbldmp_err:
        rsql_freeFakeClient(dfc);
    }
}

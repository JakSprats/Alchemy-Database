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
#include "zmalloc.h"

#include "sql.h"
#include "index.h"
#include "store.h"
#include "alsosql.h"
#include "join.h"
#include "bt_iterator.h"
#include "row.h"
#include "bt.h"
#include "parser.h"
#include "denorm.h"

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

bool emptyNoop(redisClient *c) {
    c = NULL; /* compiler warning */
    return 1;
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

/* NOTE: this function implements a fakeClient pipe */
long fakeClientPipe(redisClient *c,
                    redisClient *rfc,
                    void        *wfc, /* can be redisClient,list,LuaState */
                    int          is_ins,
                    flag        *flg,
                    bool (* adder)
                    (redisClient *c, void *x, robj *key, long *l, int b, int n),
                    bool (* emptyer) (redisClient *c)) {
    struct redisCommand *cmd = lookupCommand(rfc->argv[0]->ptr);
    cmd->proc(rfc);

    listNode *ln;
    *flg             = PIPE_NONE_FLAG;
    int       nlines = 0;
    long      card   = 1; /* ZER0 as pk can cause problems */
    bool      fline  = 1;
    listIter  *li = listGetIterator(rfc->reply, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *o    = ln->value;
        sds   s    = o->ptr;
        bool  o_fl = fline;
        fline = 0;
        //RL4 "PIPE: %s", s);
        /* ignore protocol, we just want data */
        if (*s == '\r' && *(s + 1) == '\n') continue;
         /* TODO introduce more state -> data starting w/ '\r\n' ignored */
        if (o_fl) {
            if (*s == '-') {
                *flg = PIPE_ERR_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* error */
            }
            if (*s == '+') {
                *flg = PIPE_ONE_LINER_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* OK */
            }
            if (*s == ':') {
                char *x = s + 1;
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                robj *r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
                break; /* single integer reply */
            }
            if (*s == '*') {
                nlines = atoi(s+1); /* some pipes need to know num_lines */
                if (nlines == 0) {
                    *flg = PIPE_EMPTY_SET_FLAG;
                    break;
                }
                continue;
            }
        }
        if (*s == '$') { /* parse doubles which are w/in this list element */
            if (*(s + 1) == '-') continue; /* $-1 -> nil */
            char   *x    = strchr(s, '\r');
            uint32  llen = x - s;
            if (llen + 2 < sdslen(s)) { /* got a double */
                x += 2; /* move past \r\n */
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                robj *r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
            }
            continue;
        }
        /* all ranges are single */
        if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
    }
    listReleaseIterator(li);
    if (card == 1) { /* empty response from rfc */
        if (!(*emptyer)(c)) return -1;
    }
    return card - 1; /* started at 1 */
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

void createTableAsObject(redisClient *c) {
    char *as     = c->argv[3]->ptr;
    char *as_cmd = next_token(as);
    int   axs    = -1;
    for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
        if (!strncasecmp(as_cmd, AccessCommands[i].name,
                         strlen(AccessCommands[i].name))) {
            axs = i;
            break;
        }
    }
  
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

    if (axs != -1) { /* DUMP AS redis_command redis_args */
        int    rargc;
        robj **rargv = parseCmdToArgv(as_cmd, &rargc);
        createTableAsObjectOperation(c, 0, rargv, rargc);
    } else { /* DUMP object to table */
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

void denormCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    sds wildcard = c->argv[2]->ptr;
    if (!strchr(wildcard, '*')) {
        addReply(c, shared.denorm_wildcard_no_star);
        return;
    }

    uint32 wlen = sdslen(wildcard);
    uint32 spot = 0;
    for (uint32 i = 0; i < wlen; i++) {
        if (wildcard[i] == '*') {
            spot = i;
            break;
        }
    }
    uint32  restlen  = (spot < wlen - 2) ? wlen - spot - 1: 0;
    sds     s_wldcrd = sdsnewlen(wildcard, spot);
    s_wldcrd         = sdscatlen(s_wldcrd, "%s", 2);
    if (restlen) s_wldcrd = sdscatlen(s_wldcrd, &wildcard[spot + 1], restlen);
    sds     d_wldcrd = sdsdup(s_wldcrd);
    char   *fmt      = strstr(d_wldcrd, "%s") + 1;
    *fmt             = 'd';

    robj               *argv[4];
    struct redisClient *fc = rsql_createFakeClient();
    fc->argv               = argv;
    fc->argc               = 4;

    btEntry          *be;
    robj             *o  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    btSIter *bi = btGetFullRangeIterator(o, 0, 1);
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

        /* PK is in name */
        for (int i = 1; i < Tbl[server.dbid][tmatch].col_count; i++) {
            robj *r     = createColObjFromRow(row, i, key, tmatch);
            sds tname   = Tbl[server.dbid][tmatch].col_name[i]->ptr;
            fc->argv[2] = createStringObject(tname, sdslen(tname));
            sds cname   = r->ptr;
            fc->argv[3] = createStringObject(cname, sdslen(cname));
            hsetCommand(fc);
        }
    }

    addReply(c, shared.ok);
    sdsfree(s_wldcrd);
    sdsfree(d_wldcrd);
    rsql_freeFakeClient(fc);
}

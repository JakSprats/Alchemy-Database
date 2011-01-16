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

#include "wc.h"
#include "join.h"
#include "bt_iterator.h"
#include "row.h"
#include "bt.h"
#include "rpipe.h"
#include "parser.h"
#include "legacy.h"
#include "colparse.h"
#include "alsosql.h"
#include "aobj.h"
#include "cr8tblas.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern int      Num_tbls       [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

extern char *Col_type_defs[];

#define MAX_TBL_DEF_SIZE     1024

stor_cmd AccessCommands[NUM_ACCESS_TYPES];

/* NOTE: INTERNAL_CREATE_TABLE_ERRORs shouldnt happen -> means theres a bug */
//#define FORCE_BUG_1
//#define FORCE_BUG_2
//#define FORCE_BUG_3
//#define FORCE_BUG_4

#define INTERNAL_INSERT_ERR_MSG \
 "-ERR CREATE TABLE AS DUMP Redis_Object - Automatic INSERT failed with error: "
static bool addSingle(redisClient *c,
                      void        *x,
                      robj        *key,
                      long        *card,
                      int          is_ins,
                      int          nlines) {
    nlines = 0; /* compiler warning */
    redisClient *rfc  = (redisClient *)x;
    robj        *vals = createObject(REDIS_STRING, NULL);
    if (is_ins) {
        vals->ptr     = sdsnewlen(key->ptr, sdslen(key->ptr));
    } else {
        vals->ptr     = (key->encoding == REDIS_ENCODING_RAW) ?
                 sdscatprintf(sdsempty(), "%ld,%s",  *card, (char *)key->ptr) :
                 sdscatprintf(sdsempty(), "%ld,%ld", *card, (long)  key->ptr);
    }
    rfc->argv[2]      = vals;
#ifdef FORCE_BUG_1
    rfc->argv[2] = createStringObject("", 0);
#endif
    //RL4 "SGL: INSERT [1]: %s [2]: %s", rfc->argv[1]->ptr, rfc->argv[2]->ptr);
    rsql_resetFakeClient(rfc);
    legacyInsertCommand(rfc);
    decrRefCount(vals);
    if (!replyIfNestedErr(c, rfc, INTERNAL_INSERT_ERR_MSG)) return 0;
    *card = *card + 1;
    return 1;
}

static bool addDouble(redisClient *c,
                      redisClient *rfc,
                      robj        *key,
                      robj        *val,
                      long        *card,
                      bool         val_is_dbl) {
    robj *vals = createObject(REDIS_STRING, NULL);
    if (val_is_dbl) {
        double d  = *((double *)val);
        vals->ptr = (key->encoding == REDIS_ENCODING_RAW) ?
                        sdscatprintf(sdsempty(), "%ld,%s,%f", 
                                                 *card, (char *)key->ptr, d) :
                        sdscatprintf(sdsempty(), "%ld,%ld,%f",
                                                 *card, (long)  key->ptr, d);
    } else if (val->encoding == REDIS_ENCODING_RAW) {
        vals->ptr = (key->encoding == REDIS_ENCODING_RAW) ?
                        sdscatprintf(sdsempty(), "%ld,%s,%s", 
                                    *card, (char *)key->ptr, (char *)val->ptr) :
                        sdscatprintf(sdsempty(), "%ld,%ld,%s",
                                    *card, (long)  key->ptr, (char *)val->ptr);
    } else {
        vals->ptr = (key->encoding == REDIS_ENCODING_RAW) ?
                        sdscatprintf(sdsempty(), "%ld,%s,%ld", 
                                      *card, (char *)key->ptr, (long)val->ptr) :
                        sdscatprintf(sdsempty(), "%ld,%ld,%ld",
                                      *card, (long)  key->ptr, (long)val->ptr);
    }
    rfc->argv[2] = vals;
#ifdef FORCE_BUG_2
    rfc->argv[2] = createStringObject("BUG", 3);
#endif
    //RL4 "DBL: INSERT [1]: %s [2]: %s", rfc->argv[1]->ptr, rfc->argv[2]->ptr);
    rsql_resetFakeClient(rfc);
    legacyInsertCommand(rfc);
    decrRefCount(vals);
    if (!replyIfNestedErr(c, rfc, INTERNAL_INSERT_ERR_MSG)) return 0;
    *card = *card + 1;
    return 1;
}

/* INTERNAL_CREATE_TABLE INTERNAL_CREATE_TABLE INTERNAL_CREATE_TABLE */
/* INTERNAL_CREATE_TABLE INTERNAL_CREATE_TABLE INTERNAL_CREATE_TABLE */
static void cpyColDef(char *cdefs,
                      int  *slot,
                      int   tmatch,
                      int   cmatch,
                      int   qcols,
                      int   loop,
                      bool  has_conflicts,
                      bool  cname_cflix[]) {
    robj *col = Tbl[server.dbid][tmatch].col_name[cmatch];
    if (has_conflicts && cname_cflix[loop]) { // prepend tbl_name
        robj *tbl  = Tbl[server.dbid][tmatch].name;
        memcpy(cdefs + *slot, tbl->ptr, sdslen(tbl->ptr));
        *slot     += sdslen(tbl->ptr);        // tblname
        memcpy(cdefs + *slot, ".", 1);
        *slot      = *slot + 1;
    }
    memcpy(cdefs + *slot, col->ptr, sdslen(col->ptr));
    *slot        += sdslen(col->ptr);            // colname
    memcpy(cdefs + *slot, "=", 1);
    *slot = *slot + 1;
    char *ctype   = Col_type_defs[Tbl[server.dbid][tmatch].col_type[cmatch]];
    int   ctlen   = strlen(ctype);               // [INT,STRING]
    memcpy(cdefs + *slot, ctype, ctlen);
    *slot        += ctlen;
    if (loop != (qcols - 1)) {
        memcpy(cdefs + *slot, ",", 1);
        *slot = *slot + 1;                       // ,
    }
}

#define INTERNAL_CREATE_TABLE_ERR_MSG \
  "-ERR CREATE TABLE AS SELECT - Automatic Table Creation failed with error: "
static bool _internalCreateTable(redisClient *c,
                                 redisClient *rfc,
                                 int          qcols,
                                 int          cmatchs[],
                                 int          tmatch,
                                 int          j_tbls[],
                                 int          j_cols[],
                                 bool         cname_cflix[]) {
    if (find_table(c->argv[2]->ptr) > 0) return 1;

    char cdefs[MAX_TBL_DEF_SIZE];
    int  slot  = 0;
    for (int i = 0; i < qcols; i++) {
        if (tmatch != -1) {
            cpyColDef(cdefs, &slot, tmatch, cmatchs[i], qcols, i,
                      0, cname_cflix);
        } else {
            cpyColDef(cdefs, &slot, j_tbls[i], j_cols[i], qcols, i,
                      1, cname_cflix);
        }
    }
    rfc->argc    = 3;
    rfc->argv[2] = createStringObject(cdefs, slot);
#ifdef FORCE_BUG_3
    rfc->argv[2] = createStringObject("BUG", 3);
#endif
    rsql_resetFakeClient(rfc);
    legacyTableCommand(rfc);
    if (!replyIfNestedErr(c, rfc, INTERNAL_CREATE_TABLE_ERR_MSG)) return 0;
    else                                                          return 1;
}

bool internalCreateTable(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          cmatchs[],
                         int          tmatch) {
    int  idum[1];
    bool bdum[1];
    return _internalCreateTable(c, fc, qcols, cmatchs, tmatch,
                                idum, idum, bdum);
}

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          j_tbls [],
                         int          j_cols[]) {
    bool cname_cflix[MAX_JOIN_INDXS];
    for (int i = 0; i < qcols; i++) {
        for (int j = 0; j < qcols; j++) {
            if (i == j) continue;
            if (!strcmp(Tbl[server.dbid][j_tbls[i]].col_name[j_cols[i]]->ptr,
                        Tbl[server.dbid][j_tbls[j]].col_name[j_cols[j]]->ptr)) {
                cname_cflix[i] = 1;
                break;
            } else {
                cname_cflix[i] = 0;
            }
        }
    }

    int idum[1];
    return _internalCreateTable(c, fc, qcols, idum, -1,
                                j_tbls, j_cols, cname_cflix);
}

/* CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS */
/* CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS */
#define CR8TBL_SELECT_ERR_MSG \
  "-ERR CREATE TABLE AS SELECT - SELECT command had error: "
#define CR8TBL_DUMP_ERR_MSG \
  "-ERR CREATE TABLE AS DUMP - DUMP redis_object had error: "
static void createTableAsObjectOperation(redisClient  *c,
                                         int           is_ins,
                                         robj        **rargv,
                                         int           rargc,
                                         bool          dmp) {
    robj        *wargv[3];
    char        *msg  = dmp ? CR8TBL_DUMP_ERR_MSG : CR8TBL_SELECT_ERR_MSG;
    redisClient *wfc = rsql_createFakeClient(); /* client to write */
    wfc->argc        = 3;
    wfc->argv        = wargv;
    wfc->argv[1]     = c->argv[2]; /* table name */

    redisClient *rfc = rsql_createFakeClient(); /* client to read */
    rfc->argv        = rargv;
    rfc->argc        = rargc;
    rfc->db          = c->db;

    flag flg  = 0;
    long card = fakeClientPipe(c, rfc, wfc, is_ins, &flg, addSingle, emptyNoop);
    bool err  = 0;
    if (!replyIfNestedErr(c, rfc, msg)) err = 1; /* should not happen */

    rsql_freeFakeClient(rfc);
    rsql_freeFakeClient(wfc);
    if (!err) addReplyLongLong(c, card);
}

static void createTableAsSelect(redisClient *c, char *as_cmd, int axs) {
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  =  0;
    int  qcols  =  0;
    int  tmatch = -1;
    bool join   =  0;

    int    rargc;
    robj **rargv = (*AccessCommands[axs].parse)(as_cmd, &rargc);
    //TODO support CREATE TABLE AS SCANSELECT
    if (!strcmp(rargv[0]->ptr, "SCANSELECT")) {
        addReply(c, shared.cr8tbl_scan);
        goto cr8_tblassel_end;
    }

    if (!parseSelectReply(c, 0, NULL, &tmatch, cmatchs, &qcols, &join,
                          &cstar,  rargv[1]->ptr, rargv[2]->ptr,
                          rargv[3]->ptr, rargv[4]->ptr)) goto cr8_tblassel_end;
    if (cstar) {
        addReply(c, shared.create_table_as_count);
        goto cr8_tblassel_end;
    }

    robj        *argv[3];
    bool         ok  = 0;
    char        *msg = CR8TBL_SELECT_ERR_MSG;
    redisClient *rfc = rsql_createFakeClient();
    rfc->argv        = argv;
    rfc->argv[1]     = c->argv[2];
    if (join) { /* CREATE TABLE AS SELECT JOIN */
        jb_t jb;
        init_join_block(&jb, rargv[5]->ptr);
        parseJoinReply(rfc, &jb, rargv[1]->ptr, rargv[3]->ptr);
        qcols = jb.qcols;
        if (replyIfNestedErr(c, rfc, msg)) {
            ok = createTableFromJoin(c, rfc, qcols, jb.j_tbls, jb.j_cols);
        }
        destroy_join_block(&jb);
    } else  {   /* CREATE TABLE AS SELECT RANGE QUERY */
        cswc_t w;
        init_check_sql_where_clause(&w, tmatch, rargv[5]->ptr);
        parseWCReply(rfc, &w, SQL_SELECT, 0);
        if (replyIfNestedErr(c, rfc, msg)) {
            ok = internalCreateTable(c, rfc, qcols, cmatchs, w.tmatch);
        }
        destroy_check_sql_where_clause(&w);
    }
    rsql_freeFakeClient(rfc);

    if (ok) createTableAsObjectOperation(c, 1, rargv, rargc, 0);

cr8_tblassel_end:
    zfree(rargv);
}

int getAccessCommNum(char *as_cmd) {
    int   axs    = -1;
    for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
        if (!strncasecmp(as_cmd, AccessCommands[i].name,
                                 strlen(AccessCommands[i].name))) {
            char *x = as_cmd + strlen(AccessCommands[i].name);
            if (*x == ' ') { /* needs to be space delimited as well */
                axs = i;
                break;
            }
        }
    }
    return axs;
}

static bool createInternalTableForCmdAndDump(redisClient *c,
                                             robj        *name,
                                             robj        *cdef) {
    robj               *argv[3];
    redisClient *rfc = rsql_createFakeClient();
    rfc->argv        = argv;
    rfc->argv[1]     = name;
    rfc->argv[2]     = cdef;
    rfc->argc        = 3;
#ifdef FORCE_BUG_4
    rfc->argv[2]     = createStringObject("BUG", 3);
#endif
    rsql_resetFakeClient(rfc);
    legacyTableCommand(rfc);
    if (!replyIfNestedErr(c, rfc, INTERNAL_CREATE_TABLE_ERR_MSG)) return 0;
    rsql_freeFakeClient(rfc);
    return 1;
}

void createTableAsObject(redisClient *c) {
    char *as     = c->argv[3]->ptr;
    char *as_cmd = next_token(as);
    if (!as_cmd) {
        addReply(c, shared.create_table_as_num_args);
        return;
    }
    int   axs    = getAccessCommNum(as_cmd);
    if (axs == -1) { /* quick argc parsing validation */
        if (strncasecmp(as_cmd, "DUMP ", 5)) {
            addReply(c, shared.create_table_as_function_not_found);
            return;
        }
    }

    if (AccessCommands[axs].parse) { /* CREATE TABLE AS SELECT */
        createTableAsSelect(c, as_cmd, axs);
        return;
    }

    char *dumpee = next_token(as_cmd);
    if (!dumpee) {
        addReply(c, shared.create_table_as_dump_num_args);
        return;
    }
    robj *cdef;
    bool  single;
    robj *o  = NULL;
    if (dumpee) { /* check that object exists */
        robj *key = createStringObject(dumpee, get_token_len(dumpee));
        o         = lookupKeyReadOrReply(c, key, shared.nullbulk);
        decrRefCount(key);
        if (!o) return;
    }

    /* Destination table's columns will be defined by either
        1.) REDIS_CMD    -> always outputs a single value
        2.) DUMP table   -> copy SRC table's columns
        3.) REDIS_OBJECT -> [LIST & SET -> one val],[ZSET & HASH -> 2 vals] */
    bool table_created = 0;
    if (axs != -1) { /* all Redis COMMANDS produce single results */
        cdef   = _createStringObject("pk=INT,value=TEXT");
        single = 1;
    } else if (o->type == REDIS_BTREE) { /* DUMP one table to another */
        bt *btr = (bt *)o->ptr;
        if (btr->btype != BTREE_TABLE) {
            addReply(c, shared.createtable_as_index);
            return;
        }
        TABLE_CHECK_OR_REPLY(dumpee,)

        robj        *argv[3];
        int          cmatchs[MAX_COLUMN_PER_TABLE];
        int          qcols = get_all_cols(tmatch, cmatchs);
        redisClient *cfc   = rsql_createFakeClient();
        cfc->argv          = argv;
        cfc->argv[1]       = c->argv[2]; /* new tablename */

        bool ok = internalCreateTable(c, cfc, qcols, cmatchs, tmatch);
        if (!replyIfNestedErr(c, cfc, INTERNAL_CREATE_TABLE_ERR_MSG)) ok = 0;
        rsql_freeFakeClient(cfc);
        if (!ok) return;
        table_created = 1;
    } else if (o->type == REDIS_LIST) {
        cdef   = _createStringObject("pk=INT,lvalue=TEXT");
        single = 1;
    } else if (o->type == REDIS_SET) {
        cdef   = _createStringObject("pk=INT,svalue=TEXT");
        single = 1;
    } else if (o->type == REDIS_ZSET) {
        cdef   = _createStringObject("pk=INT,zkey=TEXT,zvalue=FLOAT");
        single = 0;
    } else if (o->type == REDIS_HASH) {
        cdef   = _createStringObject("pk=INT,hkey=TEXT,hvalue=TEXT");
        single = 0;
    } else {
        addReply(c, shared.createtable_as_on_wrong_type);
        return;
    }

    if (axs != -1) { /* CREATE TABLE AS CMD: "redis_command redis_args" */
        int    rargc;
        robj **rargv = parseCmdToArgvReply(c, as_cmd, &rargc);
        if (!rargv) return;
        if (createInternalTableForCmdAndDump(c, c->argv[2], cdef)) {
            createTableAsObjectOperation(c, 0, rargv, rargc, 1);
        }
        zfree(rargv);
        return;
    }

    /* CREATE TABLE AS DUMP: "Redis_object"*/
    if (!table_created) { /* CREATE the TABLE */
        if (!createInternalTableForCmdAndDump(c, c->argv[2], cdef)) return;
    }
    robj               *argv[3];
    dictIterator *di   = NULL; /* B4 GOTO */
    hashIterator *hi   = NULL; /* B4 GOTO */
    btSIter      *bi   = NULL; /* B4 GOTO */
    redisClient  *dfc  = rsql_createFakeClient();
    dfc->argv          = argv;
    dfc->argv[1]       = c->argv[2]; /* table name */
    long          card = 1;          /* ZER0 as PK can be bad */
    if (o->type == REDIS_LIST) {
        list     *list = o->ptr;
        listNode *ln   = list->head;
        while (ln) {
            robj *key = listNodeValue(ln);
            if (!addSingle(c, dfc, key, &card, 0, 0)) goto cr8tbldmp_end;
            ln = ln->next;
        }
    } else if (o->type == REDIS_SET) {
        dictEntry *de;
        dict      *set = o->ptr;
        di             = dictGetIterator(set);
        while ((de = dictNext(di)) != NULL) {   
            robj *key  = dictGetEntryKey(de);
            if (!addSingle(c, dfc, key, &card, 0, 0)) goto cr8tbldmp_end;
        }
    } else if (o->type == REDIS_ZSET) {
        dictEntry *de;
        zset      *zs = o->ptr;
        di            = dictGetIterator(zs->dict);
        while ((de = dictNext(di)) != NULL) {   
            robj *key = dictGetEntryKey(de);
            robj *val = dictGetEntryVal(de);
            if (!addDouble(c, dfc, key, val, &card, 1)) goto cr8tbldmp_end;
        }
    } else if (o->type == REDIS_HASH) {
        hi = hashInitIterator(o);
        while (hashNext(hi) != REDIS_ERR) {
            robj *key = hashCurrent(hi, REDIS_HASH_KEY);
            robj *val = hashCurrent(hi, REDIS_HASH_VALUE);
            if (!addDouble(c, dfc, key, val, &card, 0)) goto cr8tbldmp_end;
        }
    } else if (o->type == REDIS_BTREE) {
        btEntry *be;
        /* table created above */
        int      tmatch  = Num_tbls[server.dbid] - 1;
        robj    *tname   = Tbl[server.dbid][tmatch].name;
        robj    *new_btt = lookupKeyWrite(c->db, tname);
        bt      *new_btr = (bt *)new_btt->ptr;
        bt      *btr     = (bt *)o->ptr;
        bi               = btGetFullRangeIterator(btr);
        while ((be = btRangeNext(bi)) != NULL) {      // iterate btree
            btAdd(new_btr, be->key, be->val); /* row-to-row copy */
        }
    }
    addReplyLongLong(c, (card -1));

cr8tbldmp_end:
    if (di) dictReleaseIterator(di);
    if (hi) hashReleaseIterator(hi);
    if (bi) btReleaseRangeIterator(bi);
    rsql_freeFakeClient(dfc);
}

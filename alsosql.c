/*
  *
  * This file implements the basic SQL commands of Alsosql (single row ops)
  *  and calls the range-query and join ops
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
#include "bt.h"
#include "row.h"
#include "index.h"
#include "join.h"
#include "store.h"
#include "zmalloc.h"
#include "alsosql.h"
#include "denorm.h"
#include "sql.h"
#include "common.h"


// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
int Num_tbls[MAX_NUM_DB];
// Redisql table information is stored here
r_tbl_t Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

char  CCOMMA       = ',';
char  CEQUALS      = '=';
char  CPERIOD      = '.';
char  CMINUS       = '-';

char *EQUALS       = "=";
char *EMPTY_STRING = "";
char *OUTPUT_DELIM = ",";
char *COLON        = ":";
char *COMMA        = ",";
char *PERIOD       = ".";
char *SPACE        = " ";

char *STORE        = "STORE";

char *Col_type_defs[] = {"TEXT", "INT" };


extern int     Num_indx[MAX_NUM_DB];
extern r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

sds   Curr_range;

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
robj *cloneRobj(robj *r) { // must be decrRefCount()ed
    if (r->encoding == REDIS_ENCODING_RAW) {
        return createStringObject(r->ptr, sdslen(r->ptr));
    } else {
        robj *n     = createObject(REDIS_STRING, r->ptr);
        n->encoding = REDIS_ENCODING_INT;
        return n;
    }
}

int find_table(char *tname) {
    for (int i = 0; i < Num_tbls[server.dbid]; i++) {
        if (Tbl[server.dbid][i].name) {
            if (!strcmp(tname, (char *)Tbl[server.dbid][i].name->ptr)) {
                return i;
            }
        }
    }
    return -1;
}

int find_column(int tmatch, char *column) {
    if (!Tbl[server.dbid][tmatch].name) return -1;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        if (Tbl[server.dbid][tmatch].col_name[j]) {
            if (!strcmp(column, (char *)Tbl[server.dbid][tmatch].col_name[j]->ptr)) {
                return j;
            }
        }
    }
    return -1;
}

static int find_column_n(int tmatch, char *column, int len) {
    if (!Tbl[server.dbid][tmatch].name) return -1;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        if (Tbl[server.dbid][tmatch].col_name[j]) {
            if (!strncmp(column, (char *)Tbl[server.dbid][tmatch].col_name[j]->ptr, len)) {
                return j;
            }
        }
    }
    return -1;
}

static char *str_next_unescaped_chr(char *beg, char *s, int x) {
    char *nextc = s;
    while ((nextc = strchr(nextc, x))) {
        if (nextc - beg > 1) { /* handle backslash-escaped commas */
            if  (*(nextc - 1) == '\\') {
                char *backslash = nextc - 1;
                while (backslash >= beg) {
                    if (*backslash != '\\') break;
                    backslash--;
                }
                int num_backslash = nextc - backslash - 1;
                if (num_backslash % 2 == 1) {
                    nextc++;
                    continue;
                }
            }
        }
        return nextc;
    }
    return NULL;
}
static char *parseRowVals(sds    vals,
                          char **pk,
                          int   *pklen,
                          int    ncols,
                          uint   cofsts[]) {
    if (vals[sdslen(vals) - 1] == ')') vals[sdslen(vals) - 1] = '\0';
    if (*vals == '(') vals++;

    int   fieldnum = 0;
    char *token    = vals;
    char *nextc    = vals;
    while ((nextc = str_next_unescaped_chr(vals, nextc, CCOMMA))) {
        if (!*pk) {
            int   len = (nextc - vals);
            if (!len) return NULL;
            char *buf = zmalloc(len); /* z: sds hash-key */
            memcpy(buf, vals, len);
            *pk       = buf;
            *pklen    = len;
        }
        nextc++;
        token               = nextc;
        cofsts[fieldnum] = token - vals;
        fieldnum++;
    }
    int len             = strlen(token);
    cofsts[fieldnum] = (token - vals) + len + 1; // points 2 NULL terminatr
    fieldnum++;
    if (fieldnum != ncols) {
        return NULL;
    }
    return vals;
}

int parseColListOrReply(redisClient   *c,
                        int            tmatch,
                        char          *clist,
                        int            cmatchs[]) {
    if (*clist == '*') {
        for (int i = 0; i < Tbl[server.dbid][tmatch].col_count; i++) {
            cmatchs[i] = i;
        }
        return Tbl[server.dbid][tmatch].col_count;
    }

    int   qcols  = 0;
    char *nextc = clist;
    while ((nextc = strchr(nextc, CCOMMA))) {
        *nextc = '\0';
        nextc++;
        COLUMN_CHECK_OR_REPLY(clist, 0)
        cmatchs[qcols] = cmatch;
        qcols++;
        clist = nextc;
    }
    COLUMN_CHECK_OR_REPLY(clist, 0)
    cmatchs[qcols] = cmatch;
    qcols++;
    return qcols;
}

int parseUpdateColListReply(redisClient  *c,
                            int           tmatch,
                            char         *cname,
                            int           cmatchs[],
                            char         *vals   [],
                            uint          vlens  []) {
    char *o_cname = cname;
    int   qcols   = 0;
    while (1) {
        char *val   = str_next_unescaped_chr(o_cname, cname, CEQUALS);
        char *nextc = str_next_unescaped_chr(o_cname, cname, CCOMMA);
        if (val) {
            //*val = '\0';
            val++;
        } else {
            addReply(c, shared.invalidupdatestring);
            return 0;
        }
        if (nextc) {
            //*nextc = '\0';
            nextc++;
        }

        int cmatch = find_column_n(tmatch, cname, (val -cname - 1));
        if (cmatch == -1) {
            addReply(c, shared.nonexistentcolumn);
            return 0;
        }

        uint val_len   = nextc ?  nextc - val - 1 : (uint)strlen(val);
        cmatchs[qcols] = cmatch;
        vals   [qcols] = val;
        vlens  [qcols] = val_len;
        qcols++;

        if (!nextc) break;
        cname = nextc;
    }
    return qcols;
}

// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
bool cCpyOrReply(redisClient *c, char *src, char *dest, uint len) {
    if (len >= MAX_COLUMN_NAME_SIZE) {
        addReply(c, shared.columnnametoobig);
        return 0;
    }
    memcpy(dest, src, len);
    dest[len] = '\0';
    return 1;
}

void createTableCommitReply(redisClient *c,
                            char         cnames[][MAX_COLUMN_NAME_SIZE],
                            int          ccount,
                            char        *tname) {
    if (ccount < 2) {
        addReply(c, shared.toofewcolumns);
        return;
    }

    // check for repeat column names
    for (int i = 0; i < ccount; i++) {
        for (int j = 0; j < ccount; j++) {
            if (i == j) continue;
            if (!strcmp(cnames[i], cnames[j])) {
                addReply(c, shared.nonuniquecolumns);
                return;
            }
        }
    }
    int ntbls = Num_tbls[server.dbid];

    addReply(c, shared.ok);
    // commit table definition
    for (int i = 0; i < ccount; i++) {
        Tbl[server.dbid][ntbls].col_name[i] = createStringObject(cnames[i],
                                                    strlen(cnames[i]));
    }
    robj *tbl            = createStringObject(tname, strlen(tname));
    Tbl[server.dbid][ntbls].col_count = ccount;
    Tbl[server.dbid][ntbls].name      = tbl;
    robj *bt             = createBtreeObject(Tbl[server.dbid][ntbls].col_type[0],
                                             ntbls, BTREE_TABLE);
    dictAdd(c->db->dict, tbl, bt);
    // BTREE implies an index on "tbl:pk:index" -> autogenerate
    robj *iname = createStringObject(tname, strlen(tname));
    iname->ptr  = sdscatprintf(iname->ptr, "%s%s%s%s",
                                       COLON, cnames[0], COLON, INDEX_DELIM);
    newIndex(c, iname->ptr, Num_tbls[server.dbid], 0, 1);
    decrRefCount(iname);
    Num_tbls[server.dbid]++;
}

void createTable(redisClient *c) {
    if (Num_tbls[server.dbid] >= MAX_NUM_TABLES) {
        addReply(c, shared.toomanytables);
        return;
    }

    int   len   = sdslen(c->argv[2]->ptr);
    char *tname = c->argv[2]->ptr;
    /* Mysql denotes strings w/ backticks */
    tname       = rem_backticks(tname, &len);
    if (find_table(tname) != -1) {
        addReply(c, shared.nonuniquetablenames);
        return;
    }

    if (!strcasecmp(c->argv[3]->ptr, "AS")) {
        createTableAsObject(c);
        return;
    }

    //TODO break out into function -> sql.c
    char  cnames [MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    char *o_token[MAX_COLUMN_PER_TABLE * 3]; /* can be 2+ times more */
    int   ccount      = 0;
    int   parsed_argn = 0;

    if (parseCreateTable(c, cnames, &ccount, &parsed_argn, o_token))
        createTableCommitReply(c, cnames, ccount, tname);

    for (int j = 0; j < parsed_argn; j++) {
        sdsfree(o_token[j]);
    }
}

void createCommand(redisClient *c) {
    if (c->argc < 4) {
        addReply(c, shared.createsyntax);
        return;
    }
    bool create_table = 0;
    bool create_index = 0;
    if (!strcasecmp(c->argv[1]->ptr, "table")) {
        create_table = 1;
    }
    if (!strcasecmp(c->argv[1]->ptr, "index")) {
        create_index = 1;
    }

    if (!create_table && !create_index) {
        addReply(c, shared.createsyntax);
        return;
    }

    if (create_table) createTable(c);
    if (create_index) createIndex(c);
    server.dirty++; /* for appendonlyfile */
}

void insertCommitReply(redisClient *c,
                       sds          vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[]) {
    uint  cofsts[MAX_COLUMN_PER_TABLE];
    char *pk     = NULL;
    int   pklen  = 0; /* init avoids compiler warning*/
    vals         = parseRowVals(vals, &pk, &pklen, ncols, cofsts);
    if (!vals) {
        addReply(c, shared.insertcolumnmismatch);
        return;
    }

    int   pktype = Tbl[server.dbid][tmatch].col_type[0];
    robj *o      = lookupKeyWrite(c->db, Tbl[server.dbid][tmatch].name);

    robj *pko = createStringObject(pk, pklen);
    if (pktype == COL_TYPE_INT) {
        long l   = atol(pko->ptr);
        if (l >= TWO_POW_32) {
            addReply(c, shared.uint_pk_too_big);
            return;
        } else if (l < 0) {
            addReply(c, shared.uint_no_negative_values);
            return;
        }
    }
    robj *row = btFindVal(o, pko, pktype);
    if (row) {
        zfree(pk);
        decrRefCount(pko);
        addReply(c, shared.insertcannotoverwrite);
        return;
    }
    if (matches) { /* indices */
        for (int i = 0; i < matches; i++) {
            addToIndex(c->db, pko, vals, cofsts, indices[i]);
        }
    }

    /* createRow modifies vals' buffer in place */
    robj *nrow = createRow(c, tmatch, ncols, vals, cofsts);
    if (!nrow) return;
    int   len  = btAdd(o, pko, nrow, pktype); /* copy[pk & val] */
    sdsfree(Curr_range); /* used by InSwapMode */
    Curr_range = sdsdup(pko->ptr);
    decrRefCount(pko);
    decrRefCount(nrow);
    zfree(pk);

    int new_size = 0;
    if (c->argc > 5 &&
        !strcasecmp(c->argv[5]->ptr, "RETURN") &&
        !strcasecmp(c->argv[6]->ptr, "SIZE")      ) {
        new_size = len;
    }

    if (new_size) {
        char buf[128];
        bt  *btr        = (bt *)o->ptr;
        ull  index_size = get_sum_all_index_size_for_table(c, tmatch);
        sprintf(buf,
              "INFO: BYTES: [ROW: %d BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
                   new_size, btr->data_size, btr->malloc_size, index_size);
        robj *r = createStringObject(buf, strlen(buf));
        addReplyBulk(c, r);
        decrRefCount(r);
    } else {
        addReply(c, shared.ok);
    }

    if (c->argc > 4) { /* do not do for legacyInsert */
        /* write back in final ")" for AOF and slaves */
        sds l_argv = c->argv[4]->ptr;
        l_argv[sdslen(l_argv) - 1] = ')';
    }

}

void insertCommand(redisClient *c) {
   if (strcasecmp(c->argv[1]->ptr, "INTO")) {
        addReply(c, shared.insertsyntax_no_into);
        return;
    }

    int   len   = sdslen(c->argv[2]->ptr);
    char *t     = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliance */
    TABLE_CHECK_OR_REPLY(t,)
    int   ncols = Tbl[server.dbid][tmatch].col_count;
    MATCH_INDICES(tmatch)

    /* TODO column ordering is IGNORED */
    if (strcasecmp(c->argv[3]->ptr, "VALUES")) {
        char *x = c->argv[3]->ptr;
        if (*x == '(') addReply(c, shared.insertsyntax_col_declaration);
        else           addReply(c, shared.insertsyntax_no_values);
        return;
    }

    /* NOTE: INSERT requires (vals,,,,,) to be its own cargv (CLIENT SIDE REQ)*/
    sds vals = c->argv[4]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices);
}

void selectReply(redisClient  *c,
                 robj         *o,
                 robj         *pko,
                 int           tmatch,
                 int           cmatchs[],
                 int           qcols) {
    robj *row = btFindVal(o, pko, Tbl[server.dbid][tmatch].col_type[0]);
    if (!row) {
        addReply(c, shared.nullbulk);
        return;
    }

    robj *r = outputRow(row, qcols, cmatchs, pko, tmatch, 0);
    addReplyBulk(c, r);
    decrRefCount(r);
}

void parseSelectColumnList(redisClient *c, sds *clist, int *argn) {
    for (; *argn < c->argc; *argn = *argn + 1) {
        sds y = c->argv[*argn]->ptr;
        if (!strcasecmp(y, "FROM")) break;

        if (*y == CCOMMA) {
             if (sdslen(y) == 1) continue;
             y++;
        }
        char *nextc = y;
        while ((nextc = strrchr(nextc, CCOMMA))) {
            nextc++;
            if (sdslen(*clist)) *clist  = sdscatlen(*clist, COMMA, 1);
            *clist  = sdscatlen(*clist, y, nextc - y - 1);
            y      = nextc;
        }
        if (*y) {
            if (sdslen(*clist)) *clist  = sdscatlen(*clist, COMMA, 1);
            *clist  = sdscat(*clist, y);
        }
    }
}

void selectALSOSQLCommand(redisClient *c) {
    if (c->argc == 2) { /* this is a REDIS "select DB" command" */
        selectCommand(c);
        return;
    }
    int   argn = 1;
    int   which = 0; /*used in ARGN_OVERFLOW() */
    robj *pko   = NULL, *range = NULL;
    sds   clist = sdsempty();

    parseSelectColumnList(c, &clist, &argn);

    if (argn == c->argc) {
        addReply(c, shared.selectsyntax_nofrom);
        goto sel_cmd_err;
    }
    ARGN_OVERFLOW()
    sds  tbl_list = c->argv[argn]->ptr;

    if (strchr(clist, '.')) {
        joinReply(c, clist, argn);
    } else {
        TABLE_CHECK_OR_REPLY(tbl_list,);
        ARGN_OVERFLOW()

        int   imatch = -1;
        uchar where  = checkSQLWhereClauseOrReply(c, &pko, &range, &imatch,
                                                  NULL, &argn, tmatch, 0, 0);
        if (!where) goto sel_cmd_err;

        if (argn < (c->argc - 1)) { /* DENORM e.g.: STORE LPUSH list */
            ARGN_OVERFLOW()
            if (!strcasecmp(c->argv[argn]->ptr, STORE)) {
                if (!range) {
                    addReply(c, shared.selectsyntax_store_norange);
                    goto sel_cmd_err;
                }
                ARGN_OVERFLOW()
                int i = argn;
                ARGN_OVERFLOW()
                istoreCommit(c, tmatch, imatch, c->argv[i]->ptr, clist,
                             range->ptr, c->argv[argn]);
            } else {
                addReply(c, shared.selectsyntax);
            }
        } else if (where == 2) { /* RANGE QUERY */
            iselectAction(c, range->ptr, tmatch, imatch, clist);
        } else {
            int cmatchs[MAX_COLUMN_PER_TABLE];
            int qcols = parseColListOrReply(c, tmatch, clist, cmatchs);
            if (!qcols) goto sel_cmd_err;
    
            robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
            selectReply(c, o, pko, tmatch, cmatchs, qcols);
        }
    }

sel_cmd_err:
    sdsfree(clist);
    if (pko)   decrRefCount(pko);
    if (range) decrRefCount(range);
}

void deleteCommand(redisClient *c) {
    if (strcasecmp(c->argv[1]->ptr, "FROM")) {
        addReply(c, shared.deletesyntax);
        return;
    }

    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)

    int    imatch = -1;
    robj  *pko    = NULL, *range = NULL;
    int    argn   = 3;
    uchar  where  = checkSQLWhereClauseOrReply(c, &pko, &range, &imatch,
                                               NULL, &argn, tmatch, 1, 0);
    if (!where) return;

    sdsfree(Curr_range);
    if (where == 2) { /* RANGE QUERY */
        Curr_range = sdsdup(range->ptr);
        ideleteAction(c, range->ptr, tmatch, imatch);
    } else {
        Curr_range = sdsdup(pko->ptr);
        MATCH_INDICES(tmatch)
        deleteRow(c, tmatch, pko, matches, indices) ? addReply(c, shared.cone) :
                                                      addReply(c, shared.czero);
    }
    if (pko)   decrRefCount(pko);
    if (range) decrRefCount(range);
}

void updateCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)

    if (strcasecmp(c->argv[2]->ptr, "SET")) {
        addReply(c, shared.updatesyntax);
        return;
    }

    int    cmatchs[MAX_COLUMN_PER_TABLE];
    char  *mvals  [MAX_COLUMN_PER_TABLE];
    uint   mvlens [MAX_COLUMN_PER_TABLE];
    char  *nvals = c->argv[3]->ptr;
    int    ncols = Tbl[server.dbid][tmatch].col_count;
    int    qcols = parseUpdateColListReply(c, tmatch, nvals, cmatchs,
                                           mvals, mvlens);
    if (!qcols) return;
    int pk_up_col = -1;
    for (int i = 0; i < qcols; i++) {
        if (!cmatchs[i]) {
            pk_up_col = i;
            break;
        }
    }
    MATCH_INDICES(tmatch)

    ASSIGN_UPDATE_HITS_AND_MISSES

    int    imatch = -1;
    robj  *pko    = NULL, *range = NULL;
    int    argn   = 4;
    uchar  where  = checkSQLWhereClauseOrReply(c, &pko, &range, &imatch,
                                               NULL, &argn, tmatch, 2, 0);
    if (!where) goto update_cmd_err;

    sdsfree(Curr_range);
    if (where == 2) { /* RANGE QUERY */
        if (pk_up_col != -1) {
            addReply(c, shared.update_pk_range_query);
            goto update_cmd_err;
        }
        Curr_range = sdsdup(range->ptr);
        iupdateAction(c, range->ptr, tmatch, imatch, ncols, matches, indices,
                      vals, vlens, cmiss);
    } else {
        Curr_range = sdsdup(pko->ptr);
        robj *o    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
        robj *row = btFindVal(o, pko, Tbl[server.dbid][tmatch].col_type[0]);
        if (!row) {
            addReply(c, shared.czero);
            goto update_cmd_err;
        }
        if (pk_up_col != -1) { /* disallow pk updts that overwrite other rows */
            robj *xo   = createStringObject(mvals[pk_up_col],
                                            strlen(mvals[pk_up_col]));
            robj *xrow = btFindVal(o, xo, Tbl[server.dbid][tmatch].col_type[0]);
            decrRefCount(xo);
            if (xrow) {
                addReply(c, shared.update_pk_overwrite);
                goto update_cmd_err;
            }
        }

        if (!updateRow(c, o, pko, row, tmatch, ncols, matches, indices, 
                       vals, vlens, cmiss)) goto update_cmd_err;

        addReply(c, shared.cone);
    }

update_cmd_err:
    if (pko)   decrRefCount(pko);
    if (range) decrRefCount(range);
}


unsigned long tableEmpty(redisDb *db, int tmatch) {
    if (!Tbl[server.dbid][tmatch].name) return 0; /* already deleted */

    MATCH_INDICES(tmatch)
    unsigned long deleted = 0;
    if (matches) { // delete indices first
        robj *index_del_list[MAX_COLUMN_PER_TABLE];
        for (int i = 0; i < matches; i++) { // build list of robj's to delete
            int inum          = indices[i];
            index_del_list[i] = Index[server.dbid][inum].obj;
        }
        for (int i = 0; i < matches; i++) { //delete index robj's
            int inum                        = indices[i];
            Index[server.dbid][inum].obj    = NULL;
            Index[server.dbid][inum].table  = -1;
            Index[server.dbid][inum].column = -1;
            Index[server.dbid][inum].type   = 0;
            Index[server.dbid][inum].virt   = 0;
            deleteKey(db, index_del_list[i]);
            deleted++;
        }
        //TODO shuffle indices to make space for deleted indices
    }


    deleteKey(db, Tbl[server.dbid][tmatch].name);
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        decrRefCount(Tbl[server.dbid][tmatch].col_name[j]);
        Tbl[server.dbid][tmatch].col_name[j] = NULL;
        Tbl[server.dbid][tmatch].col_type[j] = -1;
    }
    Tbl[server.dbid][tmatch].col_count = 0;
    Tbl[server.dbid][tmatch].virt_indx = -1;
    Tbl[server.dbid][tmatch].name      = NULL;

    deleted++;
    //TODO shuffle tables to make space for deleted indices

    return deleted;
}

static void dropTable(redisClient *c) {
    char *tname           = c->argv[2]->ptr;
    TABLE_CHECK_OR_REPLY(tname,)
    unsigned long deleted = tableEmpty(c->db, tmatch);
    addReplyLongLong(c, deleted);
    server.dirty++;
}

void dropCommand(redisClient *c) {
    bool drop_table = 0;
    bool drop_index = 0;
    if (!strcasecmp(c->argv[1]->ptr, "table")) {
        drop_table = 1;
    }
    if (!strcasecmp(c->argv[1]->ptr, "index")) {
        drop_index = 1;
    }

    if (!drop_table && !drop_index) {
        addReply(c, shared.dropsyntax);
        return;
    }

    if (drop_table) dropTable(c);
    if (drop_index) dropIndex(c);
}

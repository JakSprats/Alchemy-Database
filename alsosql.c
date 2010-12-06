/*
  *
  * This file implements the basic SQL commands of Alsosql (single row ops)
  *  and calls the range-query and join ops
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
#include <ctype.h>
#include <assert.h>

#include "redis.h"

#include "bt.h"
#include "row.h"
#include "index.h"
#include "range.h"
#include "desc.h"
#include "join.h"
#include "store.h"
#include "zmalloc.h"
#include "cr8tblas.h"
#include "sql.h"
#include "parser.h"
#include "common.h"
#include "alsosql.h"


// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
int Num_tbls[MAX_NUM_DB];
// Redisql table information is stored here
r_tbl_t Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

char *EQUALS       = "=";
char *EMPTY_STRING = "";
char *OUTPUT_DELIM = ",";
char *COLON        = ":";
char *COMMA        = ",";
char *PERIOD       = ".";
char *SPACE        = " ";

char *Col_type_defs[] = {"TEXT", "INT", "FLOAT" };

extern int     Num_indx[MAX_NUM_DB];
extern r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

/* HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS */
/* HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS */
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

int find_table_n(char *tname, int len) {
    for (int i = 0; i < Num_tbls[server.dbid]; i++) {
        if (Tbl[server.dbid][i].name) {
            sds x = Tbl[server.dbid][i].name->ptr;
            if (((int)sdslen(x) == len) && !strncmp(tname, x, len)) {
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
            if (!strcmp(column,
                        (char *)Tbl[server.dbid][tmatch].col_name[j]->ptr)) {
                return j;
            }
        }
    }
    return -1;
}

int find_column_n(int tmatch, char *column, int len) {
    if (!Tbl[server.dbid][tmatch].name) return -1;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        if (Tbl[server.dbid][tmatch].col_name[j]) {
            char *x = Tbl[server.dbid][tmatch].col_name[j]->ptr;
            if (((int)sdslen(x) == len) && !strncmp(column, x, len)) {
                return j;
            }
        }
    }
    return -1;
}

/* PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE */
/* PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE */
static char *parseRowVals(sds      vals,
                          char   **pk,
                          int     *pklen,
                          int      ncols,
                          uint32   cofsts[]) {
    /* TODO NULLING here is not ok, it effects the AOF line */
    if (vals[sdslen(vals) - 1] == ')') vals[sdslen(vals) - 1] = '\0';
    if (*vals == '(') vals++;

    int   fieldnum = 0;
    char *token    = vals;
    char *nextc    = vals;
    while ((nextc = str_next_unescaped_chr(vals, nextc, ','))) {
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

bool parseCol(int   tmatch,
              char *cname,
              int   clen,
              int   cmatchs[],
              int  *qcols,
              bool *cstar) {
    if (*cname == '*') {
        for (int i = 0; i < Tbl[server.dbid][tmatch].col_count; i++) {
            cmatchs[i] = i;
        }
        *qcols = Tbl[server.dbid][tmatch].col_count;
        return 1;
    }
    if (!strcasecmp(cname, "COUNT(*)")) {
        *cstar = 1;
        *qcols = 1;
        return 1;
    }
    int cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) return 0;
    cmatchs[*qcols] = cmatch;
    *qcols = *qcols + 1;
    return 1;
}

static bool parseJoinColsReply(redisClient *c,
                               char        *y,
                               int          len,
                               int         *numt,
                               int          tmatchs[],
                               int          j_tbls[],
                               int          j_cols[],
                               int        *qcols,
                               bool       *cstar) {
   if (!strcasecmp(y, "COUNT(*)")) {
        *cstar = 1;
        return 1;
    }
    if (*y == '*') {
        for (int i = 0; i < *numt; i++) {
            int tmatch = tmatchs[i];
            for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
                j_tbls[*qcols] = tmatch;
                j_cols[*qcols] = j;
                *qcols         = *qcols + 1;
            }
        }
        return 1;
    }

    char *nextp = _strnchr(y, '.', len);
    if (!nextp) {
        addReply(c, shared.indextargetinvalid);
        return 0;
    }

    char *tname  = y;
    int   tlen   = nextp - y;
    int   tmatch = find_table_n(tname, tlen);
    if (tmatch == -1) {
        addReply(c,shared.nonexistenttable);
        return 0;
    }

    char *cname  = nextp + 1;
    int   clen   = len - tlen - 1;
    int   cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) {
        addReply(c,shared.nonexistentcolumn);
        return 0;
    }

    j_tbls[*qcols] = tmatch;
    j_cols[*qcols] = cmatch;
    *qcols         = *qcols + 1;
    return 1;
}

bool parseCommaSpaceListReply(redisClient *c,
                              char        *y,
                              bool         col_check,
                              bool         tbl_check,
                              bool         join_check,
                              int          tmatch,    /* COL or TBL */
                              int          cmatchs[], /* COL or TBL */
                              int         *numt,      /* JOIN */
                              int          tmatchs[], /* JOIN */
                              int          j_tbls[],  /* JOIN */
                              int          j_cols[],  /* JOIN */
                              int         *qcols,
                              bool        *cstar) {
    while (1) {
        char *dend  = NULL;
        char *rend  = NULL;
        char *nexts = strchr(y, ' ');
        char *nextc = strchr(y, ',');
        if (nexts && nextc) {
            if (nexts > nextc) {
                dend = nextc;
                rend = nexts;
            } else {
                dend = nexts;
                rend = nextc;
            }
        } else if (nexts) {
            rend = dend = nexts;
        } else if (nextc) {
            rend = dend = nextc;
        }

        int len = dend ? (dend - y) : (int)strlen(y);

        if (len) {
            if (col_check) {
                if (!parseCol(tmatch, y, len, cmatchs, qcols, cstar)) {
                    addReply(c, shared.nonexistentcolumn);
                    return 0;
                }
            } else if (tbl_check) {
                int tm = find_table_n(y, len);
                if (tm == -1) {
                    addReply(c, shared.nonexistenttable);
                    return 0;
                }
                tmatchs[*numt] = tm;
                *numt          = *numt + 1;
            } else if (join_check) {
                if (!parseJoinColsReply(c, y, len, numt, tmatchs,
                                        j_tbls, j_cols, qcols, cstar)) return 0;
            }
        }

        if (!rend) break;
        y = rend;
        y++;
        while (isspace(*y)) y++; /* for "col1 ,  col2 " */
    }
    return 1;
}

int parseUpdateColListReply(redisClient  *c,
                            int           tmatch,
                            char         *cname,
                            int           cmatchs[],
                            char         *vals   [],
                            uint32        vlens  []) {
    char *o_cname = cname;
    int   qcols   = 0;
    while (1) {
        char *val   = str_next_unescaped_chr(o_cname, cname, '=');
        char *nextc = str_next_unescaped_chr(o_cname, cname, ',');
        if (!val) {
            addReply(c, shared.invalidupdatestring);
            return 0;
        }
        char *endval = val - 1;
        while (isblank(*endval)) endval--;
        val++;
        while (isblank(*val)) val++;

        int cmatch = find_column_n(tmatch, cname, (endval - cname + 1));
        if (cmatch == -1) {
            addReply(c, shared.nonexistentcolumn);
            return 0;
        }

        char *vc = strchr(val, ',');
        char *vp = strchr(val, ' ');
        uint32 val_len;
        if (vc && vp) {
            if (vc > vp) {
                val_len = vp - val;
            } else {
                val_len = vc - val;
            }
        } else if (vc) {
            val_len = vc - val;
        } else if (vp) {
            val_len = vp - val;
        } else {
            val_len = strlen(val);
        }
        cmatchs[qcols] = cmatch;
        vals   [qcols] = val;
        vlens  [qcols] = val_len;
        qcols++;

        if (!nextc) break;
        nextc++;
        while (isblank(*nextc)) nextc++;
        cname = nextc;
    }
    return qcols;
}

/* CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE */
/* CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE CREATE */
bool cCpyOrReply(redisClient *c, char *src, char *dest, uint32 len) {
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
                            char        *tname,
                            int          tlen) {
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
    int   pktype = Tbl[server.dbid][ntbls].col_type[0];
    robj *tbl    = createStringObject(tname, tlen);
    robj *bt     = createBtreeObject(pktype, ntbls, BTREE_TABLE);
    Tbl[server.dbid][ntbls].name      = tbl;
    Tbl[server.dbid][ntbls].col_count = ccount;
    dictAdd(c->db->dict, tbl, bt);
    // BTREE implies an index on "tbl:pk:index" -> autogenerate
    robj *iname = cloneRobj(tbl);
    iname->ptr  = sdscatprintf(iname->ptr, "%s%s%s%s",
                                       COLON, cnames[0], COLON, INDEX_DELIM);
    newIndex(c, iname->ptr, Num_tbls[server.dbid], 0, 1, NULL);
    decrRefCount(iname);
    Num_tbls[server.dbid]++;
}

static void createTable(redisClient *c) {
    if (Num_tbls[server.dbid] >= MAX_NUM_TABLES) {
        addReply(c, shared.toomanytables);
        return;
    }

    int   tlen  = sdslen(c->argv[2]->ptr);
    char *tname = c->argv[2]->ptr;
    /* Mysql denotes strings w/ backticks */
    tname       = rem_backticks(tname, &tlen);
    if (find_table_n(tname, tlen) != -1) {
        addReply(c, shared.nonuniquetablenames);
        return;
    }

    if (!strncasecmp(c->argv[3]->ptr, "AS ", 3)) {
        createTableAsObject(c);
        return;
    }

    char cnames [MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    int  ccount = 0;
    if (parseCreateTable(c, cnames, &ccount, c->argv[3]->ptr))
        createTableCommitReply(c, cnames, ccount, tname, tlen);
}

void createCommand(redisClient *c) {
    if (c->argc < 4) {
        addReply(c, shared.createsyntax);
        return;
    }
    bool create_table = 0;
    bool create_index = 0;
    if (!strcasecmp(c->argv[1]->ptr, "TABLE")) {
        create_table = 1;
    }
    if (!strcasecmp(c->argv[1]->ptr, "INDEX")) {
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

/* INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT */
/* INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT */
void insertCommitReply(redisClient *c,
                       sds          vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[]) {
    uint32  cofsts[MAX_COLUMN_PER_TABLE];
    char   *pk     = NULL;
    int     pklen  = 0; /* init avoids compiler warning*/
    vals           = parseRowVals(vals, &pk, &pklen, ncols, cofsts);
    if (!vals) {
        addReply(c, shared.insertcolumnmismatch);
        return;
    }

    int   pktype = Tbl[server.dbid][tmatch].col_type[0];
    robj *o      = lookupKeyWrite(c->db, Tbl[server.dbid][tmatch].name);

    robj *nrow = NULL; /* must come before first GOTO */
    robj *pko  = createStringObject(pk, pklen);
    if (pktype == COL_TYPE_INT) {
        long l = atol(pko->ptr);
        if (l >= TWO_POW_32) {
            addReply(c, shared.uint_pk_too_big);
            goto insert_commit_err;
        } else if (l < 0) {
            addReply(c, shared.uint_no_negative_values);
            goto insert_commit_err;
        }
    }
    robj *row = btFindVal(o, pko, pktype);
    if (row) {
        addReply(c, shared.insertcannotoverwrite);
        goto insert_commit_err;
    }

    bool   ret_size = 0;
    if (c->argc > 6 && !strcasecmp(c->argv[5]->ptr, "RETURN")) {
        if (!strcasecmp(c->argv[6]->ptr, "SIZE")) ret_size = 1;
    }

    /* NOTE: createRow replaces final ")" with a NULL */
    nrow       = createRow(c, tmatch, ncols, vals, cofsts);
    if (!nrow) goto insert_commit_err; /* value did not match col_def */

    if (matches) { /* Add to Indices */
        for (int i = 0; i < matches; i++) {
            addToIndex(c->db, pko, vals, cofsts, indices[i]);
        }
    }
    int len    = btAdd(o, pko, nrow, pktype);

    if (ret_size) {
        char buf[128];
        bt  *btr        = (bt *)o->ptr;
        ull  index_size = get_sum_all_index_size_for_table(c, tmatch);
        snprintf(buf, 127,
              "INFO: BYTES: [ROW: %d BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
                   len, btr->data_size, btr->malloc_size, index_size);
        buf[127] = '\0';
        robj *r = createStringObject(buf, strlen(buf));
        addReplyBulk(c, r);
        decrRefCount(r);
    } else {
        addReply(c, shared.ok);
    }

    if (c->argc > 4) { /* do not do for legacyInsert (from AOF) */
        /* write back in final ")" for AOF and slaves -> TODO: HACK */
        sds l_argv = c->argv[4]->ptr;
        l_argv[sdslen(l_argv) - 1] = ')';
    }

insert_commit_err:
    zfree(pk);
    decrRefCount(pko);
    if (nrow) decrRefCount(nrow);

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

    if (strcasecmp(c->argv[3]->ptr, "VALUES")) {
        char *x = c->argv[3]->ptr;
        if (*x == '(') addReply(c, shared.insertsyntax_col_decl);
        else           addReply(c, shared.insertsyntax_no_values);
        return;
    }

    /* NOTE: INSERT requires (vals,,,,,) to be its own cargv (CLIENT SIDE REQ)*/
    sds vals = c->argv[4]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices);
}

/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
static void selectSinglePKReply(redisClient  *c,
                                robj         *o,
                                robj         *key,
                                int           tmatch,
                                int           cmatchs[],
                                int           qcols) {
    robj *row = btFindVal(o, key, Tbl[server.dbid][tmatch].col_type[0]);
    if (!row) {
        addReply(c, shared.nullbulk);
        return;
    }

    robj *lenobj = createStringObject("*1\r\n", 4);
    addReply(c, lenobj);
    decrRefCount(lenobj);
    robj *r = outputRow(row, qcols, cmatchs, key, tmatch, 0);
    addReplyBulk(c, r);
    decrRefCount(r);
}

bool parseSelectReply(redisClient *c,
                      bool        *no_wc,
                      int         *tmatch,
                      int          cmatchs[MAX_COLUMN_PER_TABLE],
                      int         *qcols,
                      bool        *join,
                      bool        *cstar,
                      char        *clist,
                      char        *from,
                      char        *tlist,
                      char        *where) {
    if (strcasecmp(from, "FROM")) {
        addReply(c, shared.selectsyntax_nofrom);
        return 0;
    }
    if (!where) {
        *no_wc = 1;
    } else if (strcasecmp(where, "WHERE")) {
        if (!no_wc) {
            addReply(c, shared.selectsyntax_nowhere);
            return 0;
        } else {
            *no_wc = 1;
        }
    }

    if (strchr(tlist, ',')) {
        *join = 1;
        return 1;
    }
    *join = 0;

    *tmatch = find_table_n(tlist, get_token_len(tlist));
    if (*tmatch == -1) {
        addReply(c, shared.nonexistenttable);
        return 0;
    }

    return parseCommaSpaceListReply(c, clist, 1, 0, 0, *tmatch, cmatchs,
                                    0, NULL, NULL, NULL, qcols, cstar);
}

void init_check_sql_where_clause(cswc_t *w, sds token) {
    w->key    = NULL;
    w->low    = NULL;
    w->high   = NULL;
    w->inl    = NULL;
    w->stor   = NULL;
    w->lvr    = NULL;
    w->imatch = -1;
    w->cmatch = -1;
    w->obc    = -1;
    w->obt    = -1;
    w->lim    = -1;
    w->ofst   = -1;
    w->sto    = -1;
    w->asc    = 1;
    w->token  = token;
}

void destroy_check_sql_where_clause(cswc_t *w) {
    if (w->key)  decrRefCount(w->key);
    if (w->inl)  listRelease(w->inl);
    if (w->low)  decrRefCount(w->low);
    if (w->high) decrRefCount(w->high);
}

/* TODO need a single FK iterator ... built into RANGE_QUERY_LOOKUP_START */
void singleFKHack(cswc_t *w, uchar *wtype) {
    *wtype  = SQL_RANGE_QUERY;
    w->low  = cloneRobj(w->key);
    w->high = cloneRobj(w->key);
}

bool leftoverParsingReply(redisClient *c, cswc_t *w) {
    if (w->lvr) {
        char *x = w->lvr;
        while (isblank(*x)) x++;
        if (*x) {
            addReplySds(c, sdscatprintf(sdsempty(),
                                        "-ERR could not parse '%s'\r\n", x));
            return 0;
        }
    }
    return 1;
}

/* SELECT cols,,,,, FROM tbls,,,, WHERE clause - (6 args) */
void sqlSelectCommand(redisClient *c) {
    if (c->argc == 2) { /* this is a REDIS "select DB" command" */
        selectCommand(c);
        return;
    }

    if (c->argc != 6) {
        addReply(c, shared.selectsyntax);
        return;
    }
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  =  0;
    int  qcols  =  0;
    int  tmatch = -1;
    bool join   =  0;
    sds  tlist  = c->argv[3]->ptr;
    if (!parseSelectReply(c, NULL, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          tlist, c->argv[4]->ptr)) return;
    if (join) {
        joinReply(c);
        return;
    }

    uchar  sop = SQL_SELECT;
    cswc_t w;
    init_check_sql_where_clause(&w, c->argv[5]->ptr);
    uchar wtype  = checkSQLWhereClauseReply(c, &w, tmatch, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)      goto select_cmd_err;
    if (!leftoverParsingReply(c, &w)) goto select_cmd_err;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (w.stor) { /* DENORM e.g.: STORE LPUSH list */
        if (!w.low && !w.inl) {
            addReply(c, shared.selectsyntax_store_norange);
            goto select_cmd_err;
        } else if (cstar) {
            addReply(c, shared.select_store_count);
            goto select_cmd_err;
        }
        if (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
            addReplySds(c, sdsnew(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n"));
            goto select_cmd_err;
        }
        istoreCommit(c, &w, tmatch, cmatchs, qcols);
    } else if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) { /* RQ */
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto select_cmd_err;
        }
        iselectAction(c, &w, tmatch, cmatchs, qcols, cstar);
    } else {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
        selectSinglePKReply(c, o, w.key, tmatch, cmatchs, qcols);
    }

select_cmd_err:
    destroy_check_sql_where_clause(&w);
}

/* DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE */
/* DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE */
void deleteCommand(redisClient *c) {
    if (strcasecmp(c->argv[1]->ptr, "FROM")) {
        addReply(c, shared.deletesyntax);
        return;
    }

    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)

    if (strcasecmp(c->argv[3]->ptr, "WHERE")) {
        addReply(c, shared.deletesyntax_nowhere);
        return;
    }

    uchar  sop = SQL_DELETE;
    cswc_t w;
    init_check_sql_where_clause(&w, c->argv[4]->ptr);
    uchar wtype  = checkSQLWhereClauseReply(c, &w, tmatch, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)      goto delete_cmd_err;
    if (!leftoverParsingReply(c, &w)) goto delete_cmd_err;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto delete_cmd_err;
        }
        ideleteAction(c, &w, tmatch);
    } else {
        MATCH_INDICES(tmatch)
        bool del = deleteRow(c, tmatch, w.key, matches, indices);
        addReply(c, del ? shared.cone :shared.czero);
    }

delete_cmd_err:
    destroy_check_sql_where_clause(&w);
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
void updateCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)

    if (strcasecmp(c->argv[2]->ptr, "SET")) {
        addReply(c, shared.updatesyntax);
        return;
    }

    if (strcasecmp(c->argv[4]->ptr, "WHERE")) {
        addReply(c, shared.updatesyntax_nowhere);
        return;
    }

    int      cmatchs[MAX_COLUMN_PER_TABLE];
    char    *mvals  [MAX_COLUMN_PER_TABLE];
    uint32   mvlens [MAX_COLUMN_PER_TABLE];
    char    *nvals = c->argv[3]->ptr;
    int      ncols = Tbl[server.dbid][tmatch].col_count;
    int      qcols = parseUpdateColListReply(c, tmatch, nvals, cmatchs,
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

    uchar  sop = SQL_UPDATE;
    cswc_t w;
    init_check_sql_where_clause(&w, c->argv[5]->ptr);
    uchar wtype  = checkSQLWhereClauseReply(c, &w, tmatch, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)      goto update_cmd_err;
    if (!leftoverParsingReply(c, &w)) goto update_cmd_err;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (pk_up_col != -1) {
            addReply(c, shared.update_pk_range_query);
            goto update_cmd_err;
        }
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto update_cmd_err;
        }
        iupdateAction(c, &w, tmatch, ncols, matches, indices,
                      vals, vlens, cmiss);
    } else {
        robj *o    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
        robj *row = btFindVal(o, w.key, Tbl[server.dbid][tmatch].col_type[0]);
        if (!row) {
            addReply(c, shared.czero);
            goto update_cmd_err;
        }
        if (pk_up_col != -1) { /* disallow pk updts that overwrite other rows */
            char *x    = mvals[pk_up_col];
            robj *xo   = createStringObject(x, strlen(x));
            robj *xrow = btFindVal(o, xo, Tbl[server.dbid][tmatch].col_type[0]);
            decrRefCount(xo);
            if (xrow) {
                addReply(c, shared.update_pk_overwrite);
                goto update_cmd_err;
            }
        }

        if (!updateRow(c, o, w.key, row, tmatch, ncols, matches, indices, 
                       vals, vlens, cmiss)) goto update_cmd_err;

        addReply(c, shared.cone);
    }

update_cmd_err:
    destroy_check_sql_where_clause(&w);
}

/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
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

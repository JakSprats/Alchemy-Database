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
extern ulong              CurrCard;

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

int get_all_cols(int tmatch, int cmatchs[]) {
    for (int i = 0; i < Tbl[server.dbid][tmatch].col_count; i++) {
        cmatchs[i] = i;
    }
    return Tbl[server.dbid][tmatch].col_count;
}

/* set "OFFSET var" for next cursor iteration */
void incrOffsetVar(redisClient *c, cswc_t *w, long incr) {
    robj *ovar = createStringObject(w->ovar, sdslen(w->ovar));
    if (w->lim > incr) {
        deleteKey(c->db, ovar);
    } else {
        lolo  value = (w->ofst == -1) ? (lolo)incr :
                                        (lolo)w->ofst + (lolo)incr;
        robj *val   = createStringObjectFromLongLong(value);
        int   ret   = dictAdd(c->db->dict, ovar, val);
        if (ret == DICT_ERR) dictReplace(c->db->dict, ovar, val);
    }
    server.dirty++;
}

/* PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE */
/* PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE */
static char *parseRowVals(char    *vals,
                          char   **pk,
                          int     *pklen,
                          int      ncols,
                          uint32   cofsts[]) {
    if (vals[sdslen(vals) - 1] != ')' || *vals != '(') return NULL;
    vals++;

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
        token            = nextc;
        cofsts[fieldnum] = token - vals;
        fieldnum++;
    }
    int len          = strlen(token);
    cofsts[fieldnum] = (token - vals) + len; // points 2 NULL terminatr
    fieldnum++;
    if (fieldnum != ncols) return NULL;
    return vals;
}

static bool parseSelectCol(int   tmatch,
                           char *cname,
                           int   clen,
                           int   cmatchs[],
                           int  *qcols,
                           bool *cstar) {
    if (*cname == '*') {
        *qcols = get_all_cols(tmatch, cmatchs);
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
        char *dend  = NULL; /* lesser  of nexts and nextc */
        char *rend  = NULL; /* greater of nexts and nextc */
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
                if (!parseSelectCol(tmatch, y, len, cmatchs, qcols, cstar)) {
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

bool parseSelectReply(redisClient *c,
                      bool         is_scan,
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

    if (!where || strcasecmp(where, "WHERE")) {
        if (is_scan) {
            *no_wc = 1;
        } else {
            addReply(c, shared.selectsyntax_nowhere);
            return 0;
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

static int parseUpdateColListReply(redisClient  *c,
                                   int           tmatch,
                                   char         *vallist,
                                   int           cmatchs[],
                                   char         *vals   [],
                                   uint32        vlens  []) {
    int qcols = 0;
    while (1) {
        char *val = strchr(vallist, '=');
        if (!val) {
            addReply(c, shared.invalidupdatestring);
            return 0;
        }
        char *endval = val - 1;
        while (isblank(*endval)) endval--;
        val++;
        while (isblank(*val)) val++;

        int cmatch = find_column_n(tmatch, vallist, (endval - vallist + 1));
        if (cmatch == -1) {
            addReply(c, shared.nonexistentcolumn);
            return 0;
        }

        char   *nextc   = str_next_unescaped_chr(val, val, ',');
        uint32  val_len = nextc ? nextc - val : (uint32)strlen(val);
        cmatchs[qcols]  = cmatch;
        vals   [qcols]  = val;
        vlens  [qcols]  = val_len;
        qcols++;

        if (!nextc) break;
        nextc++;
        while (isblank(*nextc)) nextc++;
        vallist = nextc;
    }
    return qcols;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
char PLUS   = '+';
char MINUS  = '-';
char MULT   = '*';
char DIVIDE = '/';
char MODULO = '%';
char POWER  = '^';
char STRCAT = '|';
static char isExpression(char *val, uint32 vlen) {
    for (uint32 i = 0; i < vlen; i++) {
        char x = val[i];
        if (x == PLUS)   return PLUS;
        if (x == MINUS)  return MINUS;
        if (x == MULT)   return MULT;
        if (x == DIVIDE) return DIVIDE;
        if (x == POWER)  return POWER;
        if (x == MODULO) return MODULO;
        if (x == STRCAT && i != (vlen - 1) && val[i + 1] == STRCAT)
            return STRCAT;
    }
    return 0;
}

static char Up_col_buf[64];
static uchar determineExprType(char *pred, int plen) {
    if (*pred == '\'') {
        if (plen < 3)                           return UETYPE_ERR;
        if (_strnchr(pred + 1, '\'', plen - 1)) return UETYPE_STRING;
        else                                    return UETYPE_ERR;
    }
    if (*pred == '"') {
        if (plen < 3)                           return UETYPE_ERR;
        if (_strnchr(pred + 1, '"', plen - 1))  return UETYPE_STRING;
        else                                    return UETYPE_ERR;
    }
    if (plen >= 64)                             return UETYPE_ERR;
    memcpy(Up_col_buf, pred, plen);
    Up_col_buf[plen] = '\0';
    if (is_int(Up_col_buf))                     return UETYPE_INT;
    if (is_float(Up_col_buf))                   return UETYPE_FLOAT;
    return UETYPE_ERR;
}

static bool parseExprReply(redisClient *c,
                           char         e,
                           int          tmatch,
                           int          cmatch,
                           uchar        ctype,
                           char        *val,
                           uint32       vlen,
                           ue_t        *ue) {
    char  *cname = val;
    while (isblank(*cname)) cname++;       /* cant fail "e" already found */
    char  *espot = _strnchr(val, e, vlen); /* cant fail - "e" already found */
    if (((espot - val) == (vlen - 1)) ||
        ((e == STRCAT) && ((espot - val) == (vlen - 2)))) {
        addReply(c, shared.update_expr);
        return 0;
    }
    char  *cend     = espot - 1;
    while (isblank(*cend)) cend--;
    int    uec1match = find_column_n(tmatch, cname, cend - cname + 1);
    if (uec1match == -1) {
        addReply(c, shared.update_expr_col);
        return 0;
    }
    if (uec1match != cmatch) {
        addReply(c, shared.update_expr_col_other);
        return 0;
    }
    char *pred = espot + 1;
    if (e == STRCAT) pred++;
    while (isblank(*pred)) {        /* find predicate (after blanks) */
        pred++;
        if ((pred - val) == vlen) {
            addReply(c, shared.update_expr);
            return 0;
        }
    }
    char *pend = val + vlen -1;     /* start from END */
    while (isblank(*pend)) pend--;  /* find end of predicate */
    int   plen  = pend - pred + 1;
    uchar uetype = determineExprType(pred, plen);

    if (uetype == UETYPE_ERR) {
        addReply(c, shared.update_expr_col);
        return 0;
    }

    /* RULES FOR UPDATE EXPRESSIONS */
    if (uetype == UETYPE_STRING && ctype != COL_TYPE_STRING) {
        addReply(c, shared.update_expr_math_str);
        return 0;
    }
    if (e == MODULO && ctype != COL_TYPE_INT) {
        addReply(c, shared.update_expr_mod);
        return 0;
    }
    if (e == STRCAT && (ctype != COL_TYPE_STRING || uetype != UETYPE_STRING)) {
        addReply(c, shared.update_expr_cat);
        return 0;
    }
    if (ctype == COL_TYPE_STRING && e != STRCAT) {
        addReply(c, shared.update_expr_str);
        return 0;
    }

    if (uetype == UETYPE_STRING) { /* ignore string delimiters */
        pred++;
        plen -= 2;
        if (plen == 0) {
            addReply(c, shared.update_expr_empty_str);
            return 0;
        }
    }

    ue->c1match = uec1match;
    ue->type    = uetype;
    ue->pred    = pred;
    ue->plen    = plen;
    ue->op      = e;
    return 1;
}

/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
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
    if (Num_tbls[server.dbid] == MAX_NUM_TABLES) {
        addReply(c, shared.toomanytables);
        return;
    }
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

    /* BTREE implies an index on "tbl:pk:index" -> autogenerate */
    sds  iname = sdscatprintf(sdsempty(), "%s:%s:%s",
                                          tname, cnames[0], INDEX_DELIM);
    if (!newIndexReply(c, iname, Num_tbls[server.dbid], 0, 1, NULL)) {
        sdsfree(iname);
        return;
    }
    sdsfree(iname);

    int ntbls = Num_tbls[server.dbid];

    addReply(c, shared.ok);
    // commit table definition
    for (int i = 0; i < ccount; i++) {
        Tbl[server.dbid][ntbls].col_name[i] = _createStringObject(cnames[i]);
    }
    int   pktype = Tbl[server.dbid][ntbls].col_type[0];
    robj *bt     = createBtreeObject(pktype, ntbls, BTREE_TABLE);
    robj *tbl    = createStringObject(tname, tlen);
    Tbl[server.dbid][ntbls].name      = tbl;
    Tbl[server.dbid][ntbls].col_count = ccount;
    dictAdd(c->db->dict, tbl, bt);

    Num_tbls[server.dbid]++;
}

static void createTable(redisClient *c) {
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

    char cnames[MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    int  ccount = 0;
    if (parseCreateTable(c, cnames, &ccount, c->argv[3]->ptr))
        createTableCommitReply(c, cnames, ccount, tname, tlen);
}

void createCommand(redisClient *c) {
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
                       char        *vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[],
                       bool         ret_size) {
    uint32  cofsts[MAX_COLUMN_PER_TABLE];
    robj   *nrow   = NULL; /* B4 GOTO */
    robj   *pko    = NULL; /* B4 GOTO */
    char   *pk     = NULL;
    int     pklen  = 0; /* init avoids compiler warning*/
    vals           = parseRowVals(vals, &pk, &pklen, ncols, cofsts);
    if (!vals) {
        addReply(c, shared.insertcolumnmismatch);
        goto insert_commit_end;
    }

    int   pktype = Tbl[server.dbid][tmatch].col_type[0];
    robj *o      = lookupKeyWrite(c->db, Tbl[server.dbid][tmatch].name);
    pko          = createStringObject(pk, pklen);
    if (pktype == COL_TYPE_INT) {
        long l = atol(pko->ptr);
        if (!checkUIntReply(c, l, 1)) goto insert_commit_end;
    }

    robj *row = btFindVal(o, pko, pktype);
    if (row) {
        addReply(c, shared.insertcannotoverwrite);
        goto insert_commit_end;
    }

    nrow = createRow(c, tmatch, ncols, vals, cofsts);
    if (!nrow) goto insert_commit_end; /* value did not match cdef */

    EMPTY_LEN_OBJ
    bool nrl = 0;
    if (matches) { /* Add to Indices */
        for (int i = 0; i < matches; i++) {
            if (Index[server.dbid][indices[i]].nrl) {
                if (!nrl) { INIT_LEN_OBJ }
                nrl = 1;
            }
            addToIndex(c->db, pko, vals, cofsts, indices[i]);
        }
    }
    int len = btAdd(o, pko, nrow, pktype);
    if (ret_size) { /* print SIZE stats */
        char buf[128];
        bt  *btr        = (bt *)o->ptr;
        ull  index_size = get_sum_all_index_size_for_table(c, tmatch);
        snprintf(buf, 127,
              "INFO: BYTES: [ROW: %d BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
                   len, btr->data_size, btr->malloc_size, index_size);
        buf[127] = '\0';
        robj *r  = _createStringObject(buf);
        addReplyBulk(c, r);
        decrRefCount(r);
    } else {
        if (nrl) { /* responses come from NonRelationalIndex Cmd responses */
            card        = CurrCard;
            lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
        } else {
            addReply(c, shared.ok);
        }
    }

insert_commit_end:
    if (pk)   zfree(pk);
    if (pko)  decrRefCount(pko);
    if (nrow) decrRefCount(nrow);
}

/* SYNTAX: INSERT INTO tbl VALUES (aaa,bbb,ccc) [RETURN SIZE] */
void insertCommand(redisClient *c) {
   if (strcasecmp(c->argv[1]->ptr, "INTO")) {
        addReply(c, shared.insertsyntax_no_into);
        return;
    }

    int   len   = sdslen(c->argv[2]->ptr);
    char *tname = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliance */
    TABLE_CHECK_OR_REPLY(tname,)
    int   ncols = Tbl[server.dbid][tmatch].col_count;
    MATCH_INDICES(tmatch)

    if (strcasecmp(c->argv[3]->ptr, "VALUES")) {
        addReply(c, shared.insertsyntax_no_values);
        return;
    }

    bool ret_size = 0;
    if (c->argc == 6) {
        leftoverParsingReply(c, c->argv[5]->ptr);
        return;
    } else if (c->argc > 6) {
        if (!strcasecmp(c->argv[5]->ptr, "RETURN") &&
            !strcasecmp(c->argv[6]->ptr, "SIZE")) {
           ret_size = 1;
        } else {
            sds s = sdsnewlen(c->argv[5]->ptr, sdslen(c->argv[5]->ptr));
            s = sdscatprintf(s, " %s", (char *)c->argv[6]->ptr);
            leftoverParsingReply(c, s);
            sdsfree(s);
            return;
        }
    }
    
    sds vals = c->argv[4]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices, ret_size);
}

/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
static void selectSinglePKReply(redisClient  *c,
                                robj         *o,
                                robj         *key,
                                int           tmatch,
                                int           cmatchs[],
                                int           qcols,
                                bool          cstar) {
    robj *row = btFindVal(o, key, Tbl[server.dbid][tmatch].col_type[0]);
    if (!row) {
        addReply(c, shared.nullbulk);
        return;
    }
    if (cstar) {
        addReply(c, shared.cone);
        return;
    }

    addReply(c, shared.singlerow);
    robj *r = outputRow(row, qcols, cmatchs, key, tmatch, 0);
    addReplyBulk(c, r);
    decrRefCount(r);
}

void init_check_sql_where_clause(cswc_t *w, int tmatch, sds token) {
    w->key    = NULL;
    w->low    = NULL;
    w->high   = NULL;
    w->inl    = NULL;
    w->stor   = NULL;
    w->lvr    = NULL;
    w->ovar   = NULL;
    w->imatch = -1;
    w->tmatch = tmatch;
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
    if (w->low)  decrRefCount(w->low);
    if (w->high) decrRefCount(w->high);
    if (w->inl)  listRelease(w->inl);
    if (w->ovar) sdsfree(w->ovar);
}

/* TODO need a single FK iterator ... built into RANGE_QUERY_LOOKUP_START */
void singleFKHack(cswc_t *w, uchar *wtype) {
    *wtype  = SQL_RANGE_QUERY;
    w->low  = cloneRobj(w->key);
    w->high = cloneRobj(w->key);
}

bool leftoverParsingReply(redisClient *c, char *x) {
    if (x) {
        while (isblank(*x)) x++;
        if (*x) {
            addReplySds(c,
                  sdscatprintf(sdsempty(), "-ERR could not parse '%s'\r\n", x));
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
    if (!parseSelectReply(c, 0, NULL, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          tlist, c->argv[4]->ptr)) return;
    if (join) {
        joinReply(c);
        return;
    }

    cswc_t w;
    uchar  sop   = SQL_SELECT;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr);
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto select_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto select_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (cstar && w.obc != -1) { /* SELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);
        goto select_cmd_end;
    }
    if (w.stor) { /* DENORM e.g.: STORE LPUSH list */
        if (!w.low && !w.inl) {
            addReply(c, shared.selectsyntax_store_norange);
            goto select_cmd_end;
        } else if (cstar) {
            addReply(c, shared.select_store_count);
            goto select_cmd_end;
        }
        if (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
            addReplySds(c, sdsnew(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n"));
            goto select_cmd_end;
        }
        istoreCommit(c, &w, cmatchs, qcols);
    } else if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) { /* RQ */
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto select_cmd_end;
        }

        iselectAction(c, &w, cmatchs, qcols, cstar);
    } else {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][w.tmatch].name);
        selectSinglePKReply(c, o, w.key, w.tmatch, cmatchs, qcols, cstar);
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

select_cmd_end:
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

    cswc_t w;
    uchar  sop   = SQL_DELETE;
    init_check_sql_where_clause(&w, tmatch, c->argv[4]->ptr);
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto delete_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto delete_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto delete_cmd_end;
        }
        ideleteAction(c, &w);
    } else {
        MATCH_INDICES(w.tmatch)
        bool del = deleteRow(c, w.tmatch, w.key, matches, indices);
        addReply(c, del ? shared.cone :shared.czero);
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

delete_cmd_end:
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
    ue_t     ue     [MAX_COLUMN_PER_TABLE];
    char    *vallist = c->argv[3]->ptr;
    int      ncols   = Tbl[server.dbid][tmatch].col_count;
    int      qcols   = parseUpdateColListReply(c, tmatch, vallist, cmatchs,
                                               mvals, mvlens);
    if (!qcols) return;

    int pk_up_col = -1; /* PK UPDATEs that OVERWRITE rows disallowed */
    for (int i = 0; i < qcols; i++) {
        if (!cmatchs[i]) {
            pk_up_col = i;
            break;
        }
    }
    MATCH_INDICES(tmatch)

    /* Figure out which columns get updated(hit) and which dont(miss) */
    unsigned char  cmiss[MAX_COLUMN_PER_TABLE];
    char          *vals [MAX_COLUMN_PER_TABLE];
    unsigned int   vlens[MAX_COLUMN_PER_TABLE];
    for (int i = 0; i < ncols; i++) {
        unsigned char miss = 1;
        ue[i].yes = 0;
        for (int j = 0; j < qcols; j++) {
            if (i == cmatchs[j]) {
                miss     = 0;
                vals[i]  = mvals[j];
                vlens[i] = mvlens[j];
                char e   = isExpression(vals[i], vlens[i]);
                if (e) {
                    uchar ctype = Tbl[server.dbid][tmatch].col_type[i];
                    if (!parseExprReply(c, e, tmatch, cmatchs[j], ctype,
                                        vals[i], vlens[i], &ue[i])) return;
                    ue[i].yes = 1;
                }
                break;
            }
        }
        cmiss[i] = miss;
    }

    cswc_t w;
    uchar  sop   = SQL_UPDATE;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr); /* ERR now GOTO */
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto update_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto update_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (pk_up_col != -1) {
            addReply(c, shared.update_pk_range_query);
            goto update_cmd_end;
        }
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto update_cmd_end;
        }
        iupdateAction(c, &w, ncols, matches, indices,
                      vals, vlens, cmiss, ue);
    } else {
        uchar  pktype = Tbl[server.dbid][w.tmatch].col_type[0];
        robj  *o      = lookupKeyRead(c->db, Tbl[server.dbid][w.tmatch].name);
        robj  *row    = btFindVal(o, w.key, pktype);
        if (!row) { /* no row to update */
            addReply(c, shared.czero);
            goto update_cmd_end;
        }
        if (pk_up_col != -1) { /* disallow pk updts that overwrite other rows */
            char *x    = mvals[pk_up_col];
            robj *xo   = _createStringObject(x);
            robj *xrow = btFindVal(o, xo, pktype);
            decrRefCount(xo);
            if (xrow) {
                addReply(c, shared.update_pk_overwrite);
                goto update_cmd_end;
            }
        }

        if (!updateRow(c, o, w.key, row, w.tmatch, ncols, matches, indices, 
                       vals, vlens, cmiss, ue)) goto update_cmd_end;

        addReply(c, shared.cone);
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

update_cmd_end:
    destroy_check_sql_where_clause(&w);
}

/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
unsigned long emptyTable(redisDb *db, int tmatch) {
    if (!Tbl[server.dbid][tmatch].name) return 0; /* already deleted */

    MATCH_INDICES(tmatch)
    unsigned long deleted = 0;
    if (matches) { // delete indices first
        for (int i = 0; i < matches; i++) { // build list of robj's to delete
            emptyIndex(db, indices[i]);
            deleted++;
        }
        //TODO shuffle indices to make space for deleted indices
    }

    //TODO there is a bug in redis' dictDelete (need to update redis code)
    //int retval = 
    dictDelete(db->dict,Tbl[server.dbid][tmatch].name);
    //assert(retval == DICT_OK);
    Tbl[server.dbid][tmatch].name      = NULL;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        decrRefCount(Tbl[server.dbid][tmatch].col_name[j]);
        Tbl[server.dbid][tmatch].col_name[j] = NULL;
        Tbl[server.dbid][tmatch].col_type[j] = -1;
    }
    Tbl[server.dbid][tmatch].col_count = 0;
    Tbl[server.dbid][tmatch].virt_indx = -1;

    deleted++;
    //TODO shuffle tables to make space for deleted indices

    return deleted;
}

static void dropTable(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)
    unsigned long deleted = emptyTable(c->db, tmatch);
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

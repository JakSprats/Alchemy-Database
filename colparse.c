/*
  *
  * This file implements the parsing of columns in SELECT and UPDATE statements
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
#include "zmalloc.h"

#include "bt.h"
#include "row.h"
#include "index.h"
#include "range.h"
#include "desc.h"
#include "join.h"
#include "cr8tblas.h"
#include "wc.h"
#include "parser.h"
#include "aobj.h"
#include "common.h"
#include "colparse.h"

// FROM redis.c
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
int Num_tbls[MAX_NUM_DB];
// AlchemyDB table information is stored here
r_tbl_t Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

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
char *parseRowVals(char    *vals,
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
            *pklen    = (nextc - vals);
            if (!*pklen) return NULL;
            char *s   = malloc(*pklen + 1);              /* FREE ME 021 */
            memcpy(s, vals, *pklen);
            s[*pklen] = '\0';
            *pk       = s;
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

bool parseSelectCol(int   tmatch,
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

bool parseJoinColsReply(redisClient *c,
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

int parseUpdateColListReply(redisClient  *c,
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
char isExpression(char *val, uint32 vlen) {
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
uchar determineExprType(char *pred, int plen) {
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

bool parseExprReply(redisClient *c,
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

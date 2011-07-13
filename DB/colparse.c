/*
  *
  * This file implements the parsing of columns in SELECT and UPDATE statements
  * and "CREATE TABLE" parsing

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of ALCHEMY_DATABASE

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <ctype.h>
#include <limits.h>
char *strcasestr(const char *haystack, const char *needle); /*compiler warning*/

#include "redis.h"
#include "zmalloc.h"

#include "query.h"
#include "parser.h"
#include "bt_iterator.h"
#include "alsosql.h"
#include "common.h"
#include "colparse.h"

// GLOBALS
ja_t JTAlias[MAX_JOIN_COLS];
int  NumJTAlias   = 0;
int  LastJTAmatch = -1;

int     Num_tbls;
r_tbl_t Tbl[MAX_NUM_TABLES];/* ALCHEMY_DATABASE table info stored here */

char   *Ignore_keywords[]   = {"PRIMARY", "CONSTRAINT", "UNIQUE", "KEY",
                               "FOREIGN" };
int     Ignore_keywords_lens[] = {7, 10, 6, 3, 7};
uint32  Num_ignore_keywords = 5;

/* HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS */
int find_table(char *tname) { /* NOTE does not use JTAlias[] */
    for (int i = 0; i < Num_tbls; i++) {
        if (Tbl[i].name) {
            if (!strcmp(tname, (char *)Tbl[i].name->ptr)) return i;
        }
    }
    return -1;
}
int find_table_n(char *tname, int len) {
    LastJTAmatch = -1;
    for (int i = 0; i < NumJTAlias; i++) {     /* first check Join Aliases */
        sds x = JTAlias[i].alias;
        if (((int)sdslen(x) == len) && !strncmp(tname, x, len)) {
            LastJTAmatch = i; return JTAlias[i].tmatch;
        }
    }
    for (int i = 0; i < Num_tbls; i++) { /* then check TableNames */
        if (Tbl[i].name) {
            sds x = Tbl[i].name->ptr;
            if (((int)sdslen(x) == len) && !strncmp(tname, x, len)) {
                LastJTAmatch = i + USHRT_MAX; /* DONT collide w/ NumJTAlias */
                return i;
            }
        }
    }
    return -1;
}
sds getJoinAlias(int jan) {
    if (jan == -1) return NULL;
    return (jan < NumJTAlias) ? JTAlias[jan].alias :
                                Tbl[(jan - USHRT_MAX)].name->ptr;
}
int find_column(int tmatch, char *column) {
    if (!Tbl[tmatch].name) return -1;
    for (int j = 0; j < Tbl[tmatch].col_count; j++) {
        if (Tbl[tmatch].col_name[j]) {
            char *cname = (char *)Tbl[tmatch].col_name[j]->ptr;
            if (!strcmp(column, cname)) return j;
        }
    }
    return -1;
}
int find_column_n(int tmatch, char *column, int len) {
    if (!Tbl[tmatch].name) return -1;
    for (int j = 0; j < Tbl[tmatch].col_count; j++) {
        if (Tbl[tmatch].col_name[j]) {
            char *x = Tbl[tmatch].col_name[j]->ptr;
            if (((int)sdslen(x) == len) && !strncmp(column, x, len)) return j;
        }
    }
    return -1;
}
int get_all_cols(int tmatch, int cs[], bool lru2) {
    r_tbl_t *rt = &Tbl[tmatch];
    for (int i = 0; i < rt->col_count; i++) {
        if (!lru2 && rt->lrud && rt->lruc == i) continue; /* DONT PRINT LRU */
        cs[i] = i;
    }
    return lru2 ? rt->col_count : rt->lrud ? rt->col_count - 1 : rt->col_count;
}

/* set "OFFSET var" for next cursor iteration */
void incrOffsetVar(redisClient *c, wob_t *wb, long incr) {
    robj *ovar = createStringObject(wb->ovar, sdslen(wb->ovar));
    if (wb->lim > incr) {
        dbDelete(c->db, ovar);
    } else {
        lolo  value = (wb->ofst == -1) ? (lolo)incr :
                                         (lolo)wb->ofst + (lolo)incr;
        robj *val   = createStringObjectFromLongLong(value);
        int   ret   = dictAdd(c->db->dict, ovar, val);
        if (ret == DICT_ERR) dictReplace(c->db->dict, ovar, val);
    }
    server.dirty++;
}

/* PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE PARSE */
char *parse_insert_val_list_nextc(char *start, uchar ctype, bool *err) {
    if (C_IS_S(ctype)) { /* column must be \' delimited */
        *err  = 1;                           /* presume failure */
        if (*start != '\'') return NULL;
        start++;
        start = str_next_unescaped_chr(start, start, '\'');
        if (!start)         return NULL;
        *err  = 0;                           /* negate presumed failure */
        start++;
    }
    return str_next_unescaped_chr(start, start, ',');
}

static void assign_auto_inc_pk(char **pk, int *pklen, int tmatch) {
    r_tbl_t *rt  = &Tbl[tmatch];
    char PKBuf[32]; snprintf(PKBuf, 32, "%lu", ++rt->ainc); /* AUTO_INCREMENT */
    *pklen       = strlen(PKBuf);
    *pk          = _strdup(PKBuf);
}
static bool assign_pk(int tmatch, int *pklen, char **pk, char *cstart) {
    if (!*pklen) {
        uchar pktyp = Tbl[tmatch].col_type[0];
        if (C_IS_NUM(pktyp)) assign_auto_inc_pk(pk, pklen, tmatch);
        else                 return 0;
    } else {
        char *s    = malloc(*pklen + 1);              /* FREE ME 021 */
        memcpy(s, cstart, *pklen);
        s[*pklen]  = '\0';
        *pk        = s;
    }
    return 1;
}
char *parseRowVals(sds vals,  char   **pk,        int    *pklen,
                   int ncols, twoint   cofsts[],  int     tmatch,
                   int pcols, int      cmatchs[]) {
    if (vals[sdslen(vals) - 1] != ')' || *vals != '(') return NULL;
    char   *mvals  = vals + 1; SKIP_SPACES(mvals)
    int     numc   = 0;
    char   *token  = mvals;
    char   *nextc  = mvals;
    int     cmatch = pcols ? cmatchs[numc] : numc;
    bool    err   = 0;
    while (1) {
        int   cmatch = pcols ? cmatchs[numc] : numc;
        uchar ctype  = Tbl[tmatch].col_type[cmatch];
        nextc        = parse_insert_val_list_nextc(nextc, ctype, &err);
        if (err)    return NULL;
        if (!nextc) break;
        if (!cmatch) { /* parse PK */
            char *cstart = token;
            char *cend   = nextc - 1; /* skip comma */
            REV_SKIP_SPACES(cend)
            if (C_IS_S(ctype)) { /* ignore leading & trailing \' TEXT PK */
                if (*cstart != '\'') return NULL; cstart++;
                if (*cend   != '\'') return NULL; cend--;
            }
            *pklen       = (cend - cstart) + 1;
            if (!assign_pk(tmatch, pklen, pk, cstart)) return NULL;
        }
        cofsts[cmatch].i = token - mvals;
        token            = nextc;
        cofsts[cmatch].j = token - mvals;
        nextc++; token = nextc; SKIP_SPACES(nextc) numc++;
    }
    int   len      = strlen(token);
    char *end      = token + len - 2;    /* skip trailing ')' */
    char *cend     = end; REV_SKIP_SPACES(cend)/*ignore finalcols trailn space*/
    len           -= (end - cend);
    cmatch         = pcols ? cmatchs[numc] : numc;
    cofsts[cmatch ].i = (token - mvals);
    cofsts[cmatch ].j = (token - mvals) + len - 1;
    if (!cmatch) { /* PK */
        *pklen = len - 1; if (!assign_pk(tmatch, pklen, pk, token)) return NULL;
    }
    numc++;
    /* NOTE: create PK if none exists for INT & LONG */
    if (!*pklen && !assign_pk(tmatch, pklen, pk, token)) return NULL;
    if (pcols) { if (numc != pcols) return NULL; }
    else if         (numc != ncols) return NULL;
    return mvals;
}
bool parseSelectCol(int   tmatch, char *cname, int   clen,
                    int   cs[],   int  *qcols, bool *cstar) {
    if (*cname == '*') {
        *qcols = get_all_cols(tmatch, cs, 0); return 1;
    }
    if (!strcasecmp(cname, "COUNT(*)")) {
        *cstar = 1; *qcols = 1; return 1;
    }
    int cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) return 0;
    cs[*qcols] = cmatch;
    INCR(*qcols);
    return 1;
}
static bool parseJCols(cli   *c,   char *y,     int  len,  int *numt,
                       int   ts[], int  jans[], jc_t js[], int *qcols,
                       bool *cstar) {
   if (!strcasecmp(y, "COUNT(*)")) {
        *cstar = 1;
        for (int i = 0; i < *numt; i++) {
            js[*qcols].t   = ts[i];
            js[*qcols].c   = 0;
            js[*qcols].jan = jans[i];
            INCR(*qcols);
        }
        return 1;
    }
    if (*y == '*') {
        for (int i = 0; i < *numt; i++) {
            int tmatch = ts[i];
            for (int j = 0; j < Tbl[tmatch].col_count; j++) {
                r_tbl_t *rt = &Tbl[tmatch];
                if (rt->lrud && rt->lruc == j) continue; /* DONT PRINT LRU */
                js[*qcols].t   = tmatch;
                js[*qcols].c   = j;
                js[*qcols].jan = jans[i];
                INCR(*qcols);
            }
        }
        return 1;
    }
    char *nextp = _strnchr(y, '.', len);
    if (!nextp) { addReply(c, shared.indextargetinvalid); return 0; }
    char *tname  = y;
    int   tlen   = nextp - y;
    int   tmatch = find_table_n(tname, tlen);
    if (tmatch == -1) { addReply(c,shared.nonexistenttable); return 0; }
    int   jan    = LastJTAmatch;
    char *cname  = nextp + 1;
    int   clen   = len - tlen - 1;
    if (clen == 1 && *cname == '*') {
        for (int j = 0; j < Tbl[tmatch].col_count; j++) {
            js[*qcols].t   = tmatch;
            js[*qcols].c   = j;
            js[*qcols].jan = jan;
            INCR(*qcols);
        }
        return 1;
    }
    int   cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) { addReply(c,shared.nonexistentcolumn); return 0; }
    js[*qcols].t   = tmatch;
    js[*qcols].c   = cmatch;
    js[*qcols].jan = jan;
    INCR(*qcols);
    return 1;
}
static bool addJoinAlias(redisClient *c, char *tkn, char *space, int len) {
    if (NumJTAlias == MAX_JOIN_COLS) {
        addReply(c, shared.toomanyindicesinjoin); return 0;
    }
    int tlen                   = space - tkn;
    SKIP_SPACES(space);
    JTAlias[NumJTAlias].alias  = sdsnewlen(space, (tkn + len - space));//DEST049
    JTAlias[NumJTAlias].tmatch = find_table_n(tkn, tlen);
    LastJTAmatch               = NumJTAlias; /* NOTE: needed on JoinColParse */
    NumJTAlias++;
    return 1;
}
bool parseCommaSpaceList(cli  *c,         char *tkn,
                         bool  col_check, bool  tbl_check, bool join_check,
        /* COL or TBL */ int   tmatch,    int   cs[],
        /* JOIN */       int  *numt,      int   ts[], int jans[], jc_t js[],
                         int  *qcols,     bool *cstar) {
    while (1) {
        int   len;
        SKIP_SPACES(tkn)
        char *nextc = strchr(tkn, ',');
        if (nextc) {
            char *endc = nextc - 1;
            REV_SKIP_SPACES(endc);
            len = endc - tkn + 1;
        } else {
            len = strlen(tkn);
        }
        if (col_check) {
            if (!parseSelectCol(tmatch, tkn, len, cs, qcols, cstar)) {
                addReply(c, shared.nonexistentcolumn); return 0;
            }
        } else if (tbl_check) {
            int   jan   = -1;
            char *alias = _strnchr(tkn, ' ', len);
            if (alias) {
                if (!addJoinAlias(c, tkn, alias, len)) return 0;
                len = alias - tkn;
                jan = LastJTAmatch;         /* from addJoinAlias() */
            }
            int tm = find_table_n(tkn, len);
            if (tm == -1) { addReply(c, shared.nonexistenttable); return 0; }
            if (!alias) jan = LastJTAmatch; /* from find_table_n() */
            ts  [*numt] = tm;
            jans[*numt] = jan;
            INCR(*numt);
        } else if (join_check) {
            if (!parseJCols(c, tkn, len, numt, ts, jans, js, qcols, cstar))
                return 0;
        }
        if (!nextc) break;
        tkn = nextc + 1;
    }
    return 1;
}

bool parseSelectReply(cli  *c,     bool  is_scan, bool *no_wc, int  *tmatch,
                      int   cs[],  int  *qcols,   bool *join,  bool *cstar,
                      char *clist, char *from,    char *tlist, char *where) {
    if (strcasecmp(from, "FROM")) {
        addReply(c, shared.selectsyntax_nofrom); return 0;
    }
    if (!where || strcasecmp(where, "WHERE")) {
        if (is_scan) *no_wc = 1;
        else         { addReply(c, shared.selectsyntax_nowhere); return 0; }
    }
    if (strchr(tlist, ',')) { *join = 1; return 1; }
    *join = 0;
    *tmatch = find_table_n(tlist, get_token_len(tlist));
    if (*tmatch == -1) { addReply(c, shared.nonexistenttable); return 0; }
    return parseCommaSpaceList(c, clist, 1, 0, 0, *tmatch, cs,
                               0, NULL, NULL, NULL, qcols, cstar);
}

char *parse_update_val_list_nextc(char *start, uchar ctype, bool *err) {
    if (C_IS_S(ctype)) { /* column must be \' delimited */
        *err = 1;         /* presume failure */
        while(1) {
            char c = *start;
            if (!c)            return NULL;
            if      (c == ',') return start;
            else if (c == '\'') {
                *err = 0; /* negate presumed failure */
                start++;
                if (!*start) return NULL;
                start = str_next_unescaped_chr(start, start, '\'');
                if (!start) return NULL;
            }
            start++;
        }
    } else {
        return str_next_unescaped_chr(start, start, ',');
    }
}
int parseUpdateColListReply(cli *c,    int   tmatch, char   *vallist,
                            int  cs[], char *vals[], uint32 vlens[]) {
    int qcols = 0;
    while (1) {
        SKIP_SPACES(vallist)
        char *val = strchr(vallist, '=');
        if (!val) { addReply(c, shared.invalidupdatestring);       return 0; }
        char *endval = val - 1; /* skip '=' */
        REV_SKIP_SPACES(endval) /* search backwards */
        val++;                  /* skip '=' */
        SKIP_SPACES(val)        /* search forwards */
        int cmatch = find_column_n(tmatch, vallist, (endval - vallist + 1));
        if (cmatch == -1) { addReply(c, shared.nonexistentcolumn); return 0; }
        uchar ctype = Tbl[tmatch].col_type[cmatch];
        bool  err   = 0;
        char *nextc = parse_update_val_list_nextc(val, ctype, &err);
        if (err) { addReply(c, shared.invalidupdatestring);        return 0; }
        char *end    = nextc ? nextc - 1 : val + strlen(val);
        REV_SKIP_SPACES(end)
        cs   [qcols] = cmatch;
        vals [qcols] = val;
        vlens[qcols] = end - val + 1;
        qcols++;
        if (!nextc) break;
        nextc++;
        SKIP_SPACES(nextc)
        vallist = nextc;
    }
    return qcols;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
static char Up_col_buf[64];
char PLUS   = '+'; char MINUS  = '-';
char MULT   = '*'; char DIVIDE = '/';
char MODULO = '%'; char POWER  = '^'; char STRCAT = '|';
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
bool parseExpr(cli   *c,     char  e,   int    tmatch, int   cmatch,
               uchar  ctype, char *val, uint32 vlen,   ue_t *ue) {
    char  *cname = val;
    SKIP_SPACES(cname)
    char  *espot = _strnchr(val, e, vlen); /* cant fail - "e" already found */
    if (((espot - val) == (vlen - 1)) ||
        ((e == STRCAT) && ((espot - val) == (vlen - 2)))) {
        addReply(c, shared.update_expr); return 0;
    }
    char  *cend      = espot - 1;
    REV_SKIP_SPACES(cend)
    int    uec1match = find_column_n(tmatch, cname, cend - cname + 1);
    if (uec1match == -1) { addReply(c, shared.update_expr_col); return 0; }
    if (uec1match != cmatch) {
        addReply(c, shared.update_expr_col_other); return 0;
    }
    char *pred = espot + 1;
    if (e == STRCAT) pred++;
    while (ISBLANK(*pred)) {        /* find predicate (after blanks) */
        pred++;
        if ((pred - val) == vlen) { addReply(c, shared.update_expr); return 0; }
    }
    char *pend   = val + vlen -1;     /* start from END */
    SKIP_SPACES(pend)               /* find end of predicate */
    int   plen   = pend - pred + 1;
    uchar uetype = determineExprType(pred, plen);
    if (uetype == UETYPE_ERR) { addReply(c, shared.update_expr_col); return 0; }

    /* RULES FOR UPDATE EXPRESSIONS */
    if (uetype == UETYPE_STRING && ctype != COL_TYPE_STRING) {
        addReply(c, shared.update_expr_math_str); return 0;
    }
    if (e == MODULO && (ctype != COL_TYPE_INT && ctype != COL_TYPE_LONG)) {
        addReply(c, shared.update_expr_mod); return 0;
    }
    if (e == STRCAT && (ctype != COL_TYPE_STRING || uetype != UETYPE_STRING)) {
        addReply(c, shared.update_expr_cat); return 0;
    }
    if (C_IS_S(ctype) && e != STRCAT) {
        addReply(c, shared.update_expr_str); return 0;
    }
    if (uetype == UETYPE_STRING) { /* ignore string delimiters */
        pred++;
        plen -= 2;
        if (plen == 0) { addReply(c, shared.update_expr_empty_str); return 0; }
    }
    ue->c1match = uec1match;
    ue->type    = uetype;
    ue->pred    = pred;
    ue->plen    = plen;
    ue->op      = e;
    return 1;
}

/* CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS */
bool ignore_cname(char *tkn, int tlen) {
    for (uint32 i = 0; i < Num_ignore_keywords; i++) {
        int len = MAX(tlen, Ignore_keywords_lens[i]);
        if (!strncasecmp(tkn, Ignore_keywords[i], len)) {
            return 1;
        }
    }
    return 0;
}
bool cCpyOrReply(redisClient *c, char *src, char *dest, uint32 len) {
    if (len >= MAX_COLUMN_NAME_SIZE) {
        addReply(c, shared.columnnametoobig);
        return 0;
    }
    memcpy(dest, src, len);
    dest[len] = '\0';
    return 1;
}
bool parseColType(cli *c, sds type, uchar *col_type) {
    if        (strcasestr(type, "INT")) {    *col_type = COL_TYPE_INT;
    } else if (strcasestr(type, "BIGINT") ||
               strcasestr(type, "LONG")) {   *col_type = COL_TYPE_LONG;
    } else if (strcasestr(type, "FLOAT") ||
               strcasestr(type, "REAL")  ||
               strcasestr(type, "DOUBLE")) { *col_type = COL_TYPE_FLOAT;
    } else if (strcasestr(type, "CHAR") ||
               strcasestr(type, "TEXT")  ||
               strcasestr(type, "BLOB")  ||
               strcasestr(type, "BYTE")  ||
               strcasestr(type, "BINARY")) { *col_type = COL_TYPE_STRING;
    } else { addReply(c, shared.undefinedcolumntype); return 0; }
    return 1;
}
bool parseCreateTable(cli *c, //TODO ADD ctypes[] using rt->col_types[] is BAD
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      sds           col_decl) { //printf("parseCreateTable\n");
    char *token = col_decl;
    if (*token == '(') token++;
    if (!*token) { /* empty or only '(' */
        addReply(c, shared.createsyntax); return 0;
    }
    SKIP_SPACES(token)
    while (token) {
        if (*ccount == MAX_COLUMN_PER_TABLE) {
            addReply(c, shared.toomanycolumns); return 0;
        }
        int clen;
        while (token) { /* first parse column name */
            clen      = get_token_len(token);
            token     = rem_backticks(token, &clen);
            if (!ignore_cname(token, clen)) break;
            token = get_next_token_nonparaned_comma(token);
        }
        SKIP_SPACES(token)
        if (!token) break;
        if (!cCpyOrReply(c, token, cnames[*ccount], clen)) return 0;

        token       = next_token_delim3(token, ',', ')'); /* parse ctype*/
        if (!token) break;
        sds   type  = sdsnewlen(token, get_tlen_delim3(token, ',', ')')); //D070
        token       = get_next_token_nonparaned_comma(token);

        int ntbls = Num_tbls;
        bool ok = parseColType(c, type, &Tbl[ntbls].col_type[*ccount]);
        sdsfree(type);                                   /* DESTROYED 070 */
        if (!ok) return 0;
        INCR(*ccount);
    }
    return 1;
}

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

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "redis.h"
#include "zmalloc.h"

#include "parser.h"
#include "bt_iterator.h"
#include "find.h"
#include "alsosql.h"
#include "query.h"
#include "common.h"
#include "colparse.h"

extern cli     *CurrClient;
extern r_tbl_t *Tbl;

// GLOBALS
ja_t JTAlias[MAX_JOIN_COLS]; // TODO MOVE to JoinBlock + convert 2 (dict *)

// CONSTANT GLOBALS
char    *Ignore_KW[]    = {"PRIMARY", "CONSTRAINT", "UNIQUE", "KEY", "FOREIGN"};
int      Ignore_KW_lens[] = {  7,         10,          6,       3,       7};
uint32   Num_Ignore_KW    = 5;

// PROTOTYPES
// from ddl.h
void addColumn(int tmatch, char *cname, int ctype);

/* set "OFFSET var" for next cursor iteration */
void incrOffsetVar(redisClient *c, wob_t *wb, long incr) {
    robj *ovar = createStringObject(wb->ovar, sdslen(wb->ovar));
    if (wb->lim > incr) dbDelete(c->db, ovar);
    else {
        lolo  value = (wb->ofst == -1) ? (lolo)incr :
                                         (lolo)wb->ofst + (lolo)incr;
        robj *val   = createStringObjectFromLongLong(value);
        setKey(c->db, ovar, val);
    }
    server.dirty++;
}

// INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
static char *parse_insert_val_list_nextc(char *start, uchar ctype, bool *err) {
    if (C_IS_S(ctype)) { /* column must be \' delimited */
        *err  = 1;                           /* presume failure */
        if (*start != '\'')        return NULL;
        start++; start = str_next_unescaped_chr(start, start, '\'');
        if (!start)                return NULL;
        start++; *err  = 0;                  /* negate presumed failure */
    }
    return str_next_unescaped_chr(start, start, ',');
}

static void assign_auto_inc_pk(char **pk, int *pklen, int tmatch) {
    r_tbl_t *rt  = &Tbl[tmatch];
    char PKBuf[32]; snprintf(PKBuf, 32, "%lu", ++rt->ainc); /* AUTO_INCREMENT */
    *pklen       = strlen(PKBuf);
    *pk          = _strdup(PKBuf);
}
static bool assign_pk(int tmatch, int *pklen, char **pk, char *cstart,
                      bool *ai) {
    if (!*pklen) {
        uchar pktyp = Tbl[tmatch].col[0].type;
        if (C_IS_NUM(pktyp)) { *ai = 1; assign_auto_inc_pk(pk, pklen, tmatch); }
        else                 return 0;
    } else {
        char *s    = malloc(*pklen + 1);              /* FREE ME 021 */
        memcpy(s, cstart, *pklen);
        s[*pklen]  = '\0';
        *pk        = s;
    }
    return 1;
}
#define DEBUG_PARSE_ROW_VALS                                    \
  printf("addColumn: type: %d ctcol: %d name: %s cmatch: %d\n", \
          ctype, rt->ctcol, rt->tcnames[rt->ctcol - 1], cmatch);

static bool determineColType(char *nextc, uchar *ctype, int tmatch) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (*nextc == '\'') *ctype = COL_TYPE_STRING;
    else {
        char   *nxc = str_next_unescaped_chr(nextc, nextc, ',');
        uint32  len = nxc ? nxc - nextc - 1 : (uint32)strlen(nextc) - 1;
        uchar   uet = getExprType(nextc, len);
        if      (uet == UETYPE_FLT)    *ctype = COL_TYPE_FLOAT;
        else if (uet == UETYPE_INT)    *ctype = COL_TYPE_LONG;
        else /* (uet == UETYPE_ERR) */ return 0;
    }
    addColumn(tmatch, rt->tcnames[rt->ctcol], *ctype); rt->ctcol++; return 1;
}
char *parseRowVals(sds vals,  char   **pk,        int    *pklen,
                   int ncols, twoint   cofsts[],  int     tmatch,
                   int pcols, int      cmatchs[], int     lncols, bool *ai) {
    if (vals[sdslen(vals) - 1] != ')' || *vals != '(') return NULL;
    int      cmatch;
    r_tbl_t *rt     = &Tbl[tmatch];
    char    *mvals  = vals + 1; SKIP_SPACES(mvals)
    char    *token  = mvals, *nextc = mvals;
    int      numc   = 0;
    bool     err    = 0;
    while (1) {
        cmatch = pcols ? cmatchs[numc] : numc;
        if (cmatch >= ncols) return NULL;
        uchar ctype = (cmatch == -1) ? COL_TYPE_NONE : rt->col[cmatch].type;
        if (C_IS_N(ctype)) { // HASHABILITY -> determine ctype
            if (!determineColType(nextc, &ctype, tmatch)) return NULL;
            cmatchs[numc] = cmatch = rt->col_count - 1;  //DEBUG_PARSE_ROW_VALS
        }
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
            if (!assign_pk(tmatch, pklen, pk, cstart, ai)) return NULL;
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
        *pklen = len - 1;
        if (!assign_pk(tmatch, pklen, pk, token, ai)) return NULL;
    }
    numc++;
    /* NOTE: create PK if none exists for INT & LONG */
    if (!*pklen) { if (!assign_pk(tmatch, pklen, pk, token, ai)) return NULL; }
    if (pcols) { if (numc != pcols)  return NULL; }
    else if         (numc != lncols) return NULL;
    return mvals;
}

// SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
#define DEBUG_PARSE_SEL_COL_ISI                         \
  printf("parseSelCol MISS -> ADD COL: %s tcols: %d\n", \
          rt->tcnames[rt->tcols - 1], rt->tcols - 1);

static bool parseSelCol(int  tmatch, char *cname, int  clen,  list *cs,
                        int *qcols,  bool *cstar, bool exact, bool  isi) {
    if (*cname == '*') { *qcols = get_all_cols(tmatch, cs, 0, 0); return 1; }
    if (!strcasecmp(cname, "COUNT(*)")) { *cstar = 1; *qcols = 1; return 1; }
    int cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) {
        r_tbl_t *rt = &Tbl[tmatch];
        if (rt->hashy) {
            if      (isi)    { // remember cname to later addColumn(cname)
                sds *tcnames = malloc(sizeof(sds) * (rt->tcols + 1));//FREEME106
                if (rt->tcnames) {
                    memmove(tcnames, rt->tcnames, sizeof(sds) * rt->tcols);
                    free(rt->tcnames);                               //FREED 106
                }
                rt->tcnames            = tcnames;
                rt->tcnames[rt->tcols] = sdsnewlen(cname, clen);     //FREEME107
                rt->tcols++;                         // DEBUG_PARSE_SEL_COL_ISI
            } else if (exact) return 0; // NOTE: used by LUATRIGGER
            //else hashy && !isi && !exact-> [SCAN|SELECT] cmatch(-1)-> emptycol
        } else return 0;
    }
    listAddNodeTail(cs, VOIDINT cmatch); INCR(*qcols);
    return 1;
}
static bool parseJCols(cli   *c,    char *y,    int   len,
                       list *ts,    list *jans, list *js,  int *qcols,
                       bool *cstar, bool  exact) {
   if (!strcasecmp(y, "COUNT(*)")) {
        listNode *ln, *lnj;
        *cstar        = 1;
        listIter *li  = listGetIterator(ts,   AL_START_HEAD);
        listIter *lij = listGetIterator(jans, AL_START_HEAD);
        while ((ln = listNext(li)) && (lnj = listNext(lij))) {
            jc_t *jc  = malloc(sizeof(jc_t));
            jc->t     = (int)(long)ln->value;
            jc->c     = 0;                     // PK of each table in join
            jc->jan   = (int)(long)lnj->value;
            listAddNodeTail(js, jc); INCR(*qcols);
        } listReleaseIterator(li); listReleaseIterator(lij);
        return 1;
    }
    if (*y == '*') {
        listNode *ln, *lnj;
        listIter *li  = listGetIterator(ts,   AL_START_HEAD);
        listIter *lij = listGetIterator(jans, AL_START_HEAD);
        while ((ln = listNext(li)) && (lnj = listNext(lij))) {
            int       tmatch = (int)(long)ln->value;
            r_tbl_t  *rt     = &Tbl[tmatch];
            for (int j = 0; j < rt->col_count; j++) {
                if (rt->lrud && rt->lruc == j) continue; /* DONT PRINT LRU */
                if (rt->lfu  && rt->lfuc == j) continue; /* DONT PRINT LFU */
                jc_t     *jc  = malloc(sizeof(jc_t));
                jc->t         = tmatch;
                jc->c         = j;
                jc->jan       = (int)(long)lnj->value;
                listAddNodeTail(js, jc); INCR(*qcols);
            }
        } listReleaseIterator(li); listReleaseIterator(lij);
        return 1;
    }
    char    *nextp = _strnchr(y, '.', len);
    if (!nextp) { addReply(c, shared.indextargetinvalid); return 0; }
    char    *tname  = y;
    int      tlen   = nextp - y;
    int      tmatch = find_table_n(tname, tlen);
    if (tmatch == -1) { addReply(c,shared.nonexistenttable); return 0; }
    r_tbl_t *rt     = &Tbl[tmatch];
    int      jan    = CurrClient->LastJTAmatch;
    char    *cname  = nextp + 1;
    int      clen   = len - tlen - 1;
    if (clen == 1 && *cname == '*') {
        for (int j = 0; j < rt->col_count; j++) {
            jc_t *jc = malloc(sizeof(jc_t));
            jc->t    = tmatch; jc->c    = j; jc->jan  = jan;
            listAddNodeTail(js, jc); INCR(*qcols);
        }
        return 1;
    }
    int   cmatch = find_column_n(tmatch, cname, clen);
    if (cmatch == -1) {
        if (!(Tbl[tmatch].hashy && !exact)) {
            addReply(c,shared.nonexistentcolumn); return 0;
        }
    }
    jc_t *jc = malloc(sizeof(jc_t));
    jc->t    = tmatch; jc->c = cmatch; jc->jan  = jan;
    listAddNodeTail(js, jc);
    INCR(*qcols);
    return 1;
}
static bool addJoinAlias(redisClient *c, char *tkn, char *space, int len) {
    if (CurrClient->NumJTAlias == MAX_JOIN_COLS) {
        addReply(c, shared.toomanyindicesinjoin); return 0;
    }
    int tlen                   = space - tkn;
    SKIP_SPACES(space);
    int nja                  = CurrClient->NumJTAlias;
    JTAlias[nja].alias       = sdsnewlen(space, (tkn + len - space)); //DEST049
    JTAlias[nja].tmatch      = find_table_n(tkn, tlen);
    CurrClient->LastJTAmatch = nja; /* NOTE: needed on JoinColParse */
    CurrClient->NumJTAlias++;
    return 1;
}
bool parseCommaSpaceList(cli  *c,         char  *tkn,
                         bool  col_check, bool   tbl_check, bool  join_check,
                         bool  exact,     bool   isi,
        /* COL or TBL */ int   tmatch,    list  *cs,
        /* JOIN */       list *ts,        list  *jans,      list *js,
                         int  *qcols,     bool  *cstar) {
    while (1) {
        int   len;
        SKIP_SPACES(tkn)
        char *nextc = strchr(tkn, ',');
        if (nextc) {
            char *endc = nextc - 1; REV_SKIP_SPACES(endc); len = endc - tkn + 1;
        } else len = strlen(tkn);
        if (col_check) {
            if (!parseSelCol(tmatch, tkn, len, cs, qcols, cstar, exact, isi)) {
                addReply(c, shared.nonexistentcolumn); return 0;
            }
        } else if (tbl_check) {
            int   jan   = -1;
            char *alias = _strnchr(tkn, ' ', len);
            if (alias) {
                if (!addJoinAlias(c, tkn, alias, len)) return 0;
                len     = alias - tkn;
                jan     = CurrClient->LastJTAmatch;   /* from addJoinAlias() */
            }
            int   tm    = find_table_n(tkn, len);
            if (tm == -1) { addReply(c, shared.nonexistenttable); return 0; }
            if (!alias) jan = CurrClient->LastJTAmatch;/* from find_table_n() */
            listAddNodeTail(ts,   VOIDINT tm);
            listAddNodeTail(jans, VOIDINT jan);
        } else if (join_check) {
            if (!parseJCols(c, tkn, len, ts, jans, js, qcols, cstar, exact))
                return 0;
        }
        if (!nextc) break;
        tkn = nextc + 1;
    }
    return 1;
}

bool parseSelect(cli  *c,     bool  is_scan, bool *no_wc, int  *tmatch,
                 list *cs,    int  *qcols,   bool *join,  bool *cstar,
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
    return parseCommaSpaceList(c, clist, 1, 0, 0, 0, 0, *tmatch, cs,
                               NULL, NULL, NULL, qcols, cstar);
}

// UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
int parseUpdateColListReply(cli  *c,  int   tmatch, char *vallist,
                            list *cs, list *vals,   list *vlens) {
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
        bool  err   = 0;
        char *nextc = get_next_comma_ignore_quotes_n_parens(val);
        if (err) { addReply(c, shared.invalidupdatestring);        return 0; }
        char *end;
        if (nextc) { char *s = nextc;  while(*s != ',') s--; end = s - 1; }
        else       end = val + strlen(val);
        REV_SKIP_SPACES(end)
        listAddNodeTail(cs,    VOIDINT cmatch);
        listAddNodeTail(vals,          val);
        listAddNodeTail(vlens, VOIDINT (end - val + 1));            
        qcols++;
        if (!nextc) break;
        if (*nextc == ',') { nextc++; SKIP_SPACES(nextc) }
        vallist = nextc;
    }
    return qcols;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
char PLUS   = '+'; char MINUS  = '-';
char MULT   = '*'; char DIVIDE = '/';
char MODULO = '%'; char POWER  = '^';
uchar getExprType(char *pred, int plen) {
    if (plen >= 64)           return UETYPE_ERR;
    char up_col_buf[64];
    memcpy(up_col_buf, pred, plen); up_col_buf[plen] = '\0';
    if (is_int  (up_col_buf)) return UETYPE_INT;
    if (is_float(up_col_buf)) return UETYPE_FLT;
    return UETYPE_ERR;
}
int parseExpr(cli *c, int tmatch, int cmatch, char *val, uint32 vlen, ue_t *ue){
    uint32  i;
    uchar   ctype = Tbl[tmatch].col[cmatch].type;
    for (i = 0; i < vlen; i++) { char e = *(val + i); if (!ISALNUM(e)) break; }
    if (!i || i == vlen)                         return 0;
    char   *cend  = val + i;
    int     ucm   = find_column_n(tmatch, val, (cend - val));
    if (ucm == -1 || ucm != cmatch)              return 0;
    char   *x     = cend; SKIP_SPACES(x)
    i             = (x - val); if (i == vlen)    return 0;
    char    e     = *x;      // TODO \/ use OpDelim[] array
    if (!((e == PLUS) || (e == MINUS) || (e == MULT) || (e == DIVIDE) ||
          (e == POWER) || (e == MODULO)))        return 0;
    char   *pred  = val + i + 1;
    while (ISBLANK(*pred)) {        /* find predicate (after blanks) */
        pred++; if ((pred - val) == vlen)        return 0;
    }
    char    *pend = val + vlen -1; REV_SKIP_SPACES(pend)
    int      plen = pend - pred + 1;
    uchar    uet  = getExprType(pred, plen);
    if (uet == UETYPE_ERR)                       return 0;
    if (e == MODULO && (ctype != COL_TYPE_INT && ctype != COL_TYPE_LONG)) {
        addReply(c, shared.update_expr_mod); return -1;
    }
    ue->c1match = ucm;
    ue->type    = uet;
    ue->pred    = pred;
    ue->plen    = plen;
    ue->op      = e;
    return 1;
}

// LuaDelims[] defines characters that CAN border column-names
static bool Inited_LuaDelims = 0;
static bool LuaDelims[256];
static void init_LuaDelims() {
    bzero(LuaDelims, 256);
    LuaDelims[0]    = 1;
    LuaDelims['\n'] = 1; LuaDelims['\t'] = 1; LuaDelims[' '] = 1;
    LuaDelims['+']  = 1; LuaDelims['-']  = 1; LuaDelims['*'] = 1;
    LuaDelims['/']  = 1; LuaDelims['%']  = 1; LuaDelims['^'] = 1;
    LuaDelims['#']  = 1; LuaDelims['=']  = 1; LuaDelims['~'] = 1;
    LuaDelims['<']  = 1; LuaDelims['>']  = 1; LuaDelims['('] = 1;
    LuaDelims[')']  = 1; LuaDelims['{']  = 1; LuaDelims['}'] = 1;
    LuaDelims['[']  = 1; LuaDelims[']']  = 1; LuaDelims[';'] = 1;
    LuaDelims[':']  = 1; LuaDelims[',']  = 1; LuaDelims['.'] = 1;
}

bool parseLuaExpr(int tmatch, int cmatch, char *val, uint32 vlen, lue_t *le) {
    if (!Inited_LuaDelims) { init_LuaDelims(); Inited_LuaDelims = 1; }
    r_tbl_t *rt    = &Tbl[tmatch];
    sds      expr  = sdsnewlen(val, vlen);                    // FREE ME 097
    char    *beg   = expr, *s = expr;
    sds      mexpr = sdsempty();                              // FREE ME 098
    while (*s) { // do NOT search STRINGS for cnames (->strip STRINGS)
        if (*s == '\'') {
            if (s != beg) mexpr = sdscatlen(mexpr, beg, (s - beg));
            s++; s = str_next_unescaped_chr(s, s, '\'');
            if (!s) goto prs_lua_expr_end;
            s++;
            beg = s;
        } else s++;
    }
    if (*beg) mexpr = sdscat(mexpr, beg);
    list    *cl    = listCreate();                            // FREE ME 101
    for (int i = 0; i < rt->col_count; i++) {
        char *hit = strstr(mexpr, rt->col[i].name);
        if (hit) {
            char *before = (hit == mexpr) ? NULL : hit - 1;
            char *after  = hit + sdslen(rt->col[i].name);
            if ((!before || LuaDelims[(int)*before]) &&
                            LuaDelims[(int)*after]) {
                listNode *ln = listSearchKey(cl, VOIDINT i);
                if (!ln) listAddNodeTail(cl, VOIDINT i);
            }
        }
    }
    listNode *ln;
    le->yes         = 1;
    le->ncols       = cl->len;
    if (le->ncols) le->cmatchs = (int *)malloc(le->ncols * sizeof(int));//FRE096
    sds       lfunc = sdsnew("return (function (...) ");      // FREE ME 100
    le->fname       = sdscatprintf(sdsempty(), "luf_%d", cmatch);
    if (cl->len) {
        uint32    i     = 0;
        listIter *li    = listGetIterator(cl, AL_START_HEAD);
        while((ln = listNext(li))) {
            int cmatch = (int)(long)ln->value;
            lfunc      = sdscatprintf(lfunc, "local %s = arg[%d]; ",
                                             rt->col[cmatch].name, (i + 1));
            le->cmatchs[i] = (int)(long)ln->value;
            i++;
        } listReleaseIterator(li);
    }
    lfunc = sdscatprintf(lfunc, " return (%s); end)(...)", expr);
    //printf("lfunc: %s\n", lfunc);
    int ret = luaL_loadstring(server.lua, lfunc);
    sdsfree(lfunc);                                           // FREED 100
    if (ret) le->yes = 0;
    else lua_setglobal(server.lua, le->fname);
    listRelease(cl);                                          // FREED 101

prs_lua_expr_end:
    sdsfree(expr);                                            // FREED 097
    sdsfree(mexpr);                                           // FREED 098
    return le->yes;
}

/* CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS */
bool ignore_cname(char *tkn, int tlen) {
    for (uint32 i = 0; i < Num_Ignore_KW; i++) {
        int len = MAX(tlen, Ignore_KW_lens[i]);
        if (!strncasecmp(tkn, Ignore_KW[i], len)) return 1;
    }
    return 0;
}
bool parseColType(cli *c, sds type, uchar *ctype) {
    if      (strcasestr(type, "BIGINT") ||
             strcasestr(type, "LONG"))    *ctype = COL_TYPE_LONG;
    else if (strcasestr(type, "INT"))     *ctype = COL_TYPE_INT;
    else if (strcasestr(type, "FLOAT") ||
             strcasestr(type, "REAL")  ||
             strcasestr(type, "DOUBLE"))  *ctype = COL_TYPE_FLOAT;
    else if (strcasestr(type, "CHAR") ||
             strcasestr(type, "TEXT")  ||
             strcasestr(type, "BLOB")  ||
             strcasestr(type, "BYTE")  ||
             strcasestr(type, "BINARY"))  *ctype = COL_TYPE_STRING;
    else { addReply(c, shared.undefinedcolumntype); return 0; }
    return 1;
}
bool parseCreateTable(cli    *c,      list *ctypes,  list *cnames,
                      int    *ccount, sds   col_decl) {
    char *token = col_decl;
    if (*token == '(') token++;
    if (!*token) { /* empty or only '(' */
        addReply(c, shared.createsyntax); return 0;
    }
    SKIP_SPACES(token)
    while (token) {
        int clen;
        while (token) { /* first parse column name */
            clen      = get_token_len(token);
            token     = rem_backticks(token, &clen);
            if (!ignore_cname(token, clen)) break;
            token = get_next_token_nonparaned_comma(token);
        }
        SKIP_SPACES(token)
        if (!token) break;
   
        sds cname = sdsnewlen(token, clen);
        if (!strcasecmp(cname, "LRU") || !strcasecmp(cname, "LFU")) {
            addReply(c, shared.kw_cname); return 0;
        }
        listAddNodeTail(cnames, cname);

        token       = next_token_delim3(token, ',', ')'); /* parse ctype*/
        if (!token) break;
        sds   type  = sdsnewlen(token, get_tlen_delim3(token, ',', ')')); //D070
        token       = get_next_token_nonparaned_comma(token);

        uchar  ctype;
        bool   ok  = parseColType(c, type, &ctype);
        sdsfree(type);                                   /* DESTROYED 070 */
        if (!ok) return 0;
        listAddNodeTail(ctypes, VOIDINT ctype);
        INCR(*ccount);
    }
    return 1;
}

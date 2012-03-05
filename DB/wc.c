/*
 * This file implements sql parsing for ALCHEMY_DATABASE WhereClauses
 *

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

#include "sds.h"
#include "zmalloc.h"
#include "redis.h"

#include "debug.h"
#include "prep_stmt.h"
#include "embed.h"
#include "internal_commands.h"
#include "join.h"
#include "index.h"
#include "qo.h"
#include "filter.h"
#include "cr8tblas.h"
#include "colparse.h"
#include "rpipe.h"
#include "parser.h"
#include "find.h"
#include "alsosql.h"
#include "common.h"
#include "wc.h"

/* WC TODO LIST
    1.) doJoin(): prepareJoin AFTER optimiseJoinPlan
    2.) parseWCTokRelation(): parse error on "WHERE x = 4 WTF AND -> WTF?"
*/

extern int      Num_tbls;
extern r_tbl_t *Tbl;
extern r_ind_t *Index;

extern ja_t     JTAlias[MAX_JOIN_COLS];

extern stor_cmd AccessCommands[];
extern uchar    OP_len[NOP];

// CONSTANT GLOBALS
static char CLT = '<';
static char CGT = '>';
static char CEQ = '=';
static char CNE = '!';

#define TOK_TYPE_KEY    0
#define TOK_TYPE_RANGE  1
#define TOK_TYPE_IN     2

#define IN_RCMD_ERR_MSG \
  "-ERR SELECT FROM WHERE col IN(Redis_cmd) - inner command had error: "

static uchar genericParseError(cli *c, uchar sop) {
    if      (sop == SQL_SELECT)   addReply(c, shared.selectsyntax);
    else if (sop == SQL_DELETE)   addReply(c, shared.deletesyntax);
    else       /*   SQL_UPDATE */ addReply(c, shared.updatesyntax);
    return SQL_ERR_LKP;
}
static enum OP findOperator(char *val, uint32 vlen, char **spot) {
    for (uint32 i = 0; i < vlen; i++) {
        char x = val[i];
        if (x == CEQ) { *spot  = val + i; return EQ; }
        if (x == CLT) {
            if (i != (vlen - 1) && val[i + 1] == CEQ) {
                *spot  = val + i + 1;     return LE;
            } else {
                *spot  = val + i;         return LT;
            }
        }
        if (x == CGT) {
            if (i != (vlen - 1) && val[i + 1] == CEQ) {
                *spot  = val + i + 1;     return GE;
            } else {
                *spot  = val + i;         return GT;
            }
        }
        if (x == CNE) {
            if (i != (vlen - 1) && val[i + 1] == CEQ) {
                *spot  = val + i + 1;     return NE;
            }
        }
    }
    *spot = NULL;                         return NONE;
}

/* "OFFSET M" if M is a redis variable, ALCHEMY CURSOR */
static bool setOffsetReply(cli *c, wob_t *wb, char *nextp) {
    if (ISALPHA(*nextp)) { /* OFFSET "var" - used in cursors */
        int   len  = get_token_len(nextp);
        robj *ovar = createStringObject(nextp, len);
        wb->ovar   = sdsdup(ovar->ptr);
        robj *o    = lookupKeyRead(c->db, ovar);
        decrRefCount(ovar);
        if (o) {
            long long value;
            if (!checkType(c, o, REDIS_STRING) &&
                 getLongLongFromObjectOrReply(c, o, &value,
                            "OFFSET variable is not an integer") == REDIS_OK) {
                wb->ofst = (long)value;
            } else { /* possibly variable was a ZSET,LIST,etc */
                sdsfree(wb->ovar);
                wb->ovar = NULL;
                return 0;
            }
        }
    } else { /* LIMIT N OFFSET X */
        wb->ofst = atol(nextp); /* OK: DELIM: [\ ,\0] */
    }
    return 1;
}

/* SYNTAX: ORDER BY {col [DESC],}+ [LIMIT n [OFFSET m]] */
static bool parseOBYcol(cli   *c,  char  **token, int   tmatch,
                        wob_t *wb, char  **fin,   bool *more,   bool isj) {
    //printf("parseOBYcol wb->nob: %d tmatch: %d token: %s\n",
    //        wb->nob, tmatch, *token);
    wb->le[wb->nob].yes = 0;
    char *nextc = get_next_nonparaned_comma(*token);
    *more       = nextc ? 1 : 0;
    sds   t2    = nextc ? sdsnewlen(*token, (nextc - *token)) : sdsnew(*token);
    if (nextc) { nextc++; SKIP_SPACES(nextc) }
    char *endc  = t2 + sdslen(t2) - 1;
    REV_SKIP_SPACES(endc)             //printf("more: %d t2: %s\n", *more, t2);
    if (!nextc) {
        char *lim = strstr_not_quoted(t2, " LIMIT");
        if (lim) { lim++;
            *token = *fin = lim;
            sds t3 = sdsnewlen(t2, lim - t2 - 1); sdsfree(t2); t2 = t3;
            endc   = t2 + sdslen(t2) - 1;
            REV_SKIP_SPACES(endc)
        } else *token = NULL;
    } else *token = nextc;    //printf("POST LIM: t2: %s fin: %s\n", t2, *fin);
    if ((endc - t2) > 5 && !strncasecmp((endc - 4), " DESC", 5)) {
        wb->asc[wb->nob] = 0;
        sds t3 = sdsnewlen(t2, (endc - 4) - t2); sdsfree(t2); t2 = t3;
        endc   = t2 + sdslen(t2) - 1;
        REV_SKIP_SPACES(endc)
    } else wb->asc[wb->nob] = 1; //printf("DESC: t2: %s fin: %s\n", t2, *fin);

    int  join  = 0;
    bool iscol = ISALPHA(*t2);
    if (iscol) {
        for (uint32 i = 1; i < sdslen(t2); i++) {
            char c = *(t2 + i);
            if (!ISALNUM(c) && !ISBLANK(c) && c != '.') { iscol = 0; break; }
            if (c == '.') join = i;
        }
    }
    if (!iscol) { // make it a Lua Function
        if (!checkOrCr8LFunc(tmatch, &wb->le[wb->nob], t2, 1)) {
            addReply(c, shared.order_by_col_not_found); sdsfree(t2); return 0;
        } else { wb->nob++;                             sdsfree(t2); return 1; }
    } 
    DECLARE_ICOL(ic, -1)
    wb->obt[wb->nob] = tmatch; cloneIC(&wb->obc[wb->nob], &ic); // DEFAULT
    if (isj) { // JOIN COLUMN [tbl.col]
        if ((wb->obt[wb->nob] = find_table_n(t2, join)) == -1) {
            addReply(c, shared.join_order_by_tbl); sdsfree(t2);      return 0;
        } else {
            char *cname = t2 + join + 1;
            int   clen  = (int)strlen(cname);
            wb->obc[wb->nob] = find_column_n(wb->obt[wb->nob], cname, clen);
            if (wb->obc[wb->nob].cmatch == -1) {
                addReply(c, shared.join_order_by_col); sdsfree(t2);  return 0;
            }
        }
    } else {  // SIMPLE COLUMN [col]
        wb->obc[wb->nob] = find_column_sds(tmatch, t2);
//TODO fimatch
        if (wb->obc[wb->nob].cmatch == -1) {
            addReply(c, shared.order_by_col_not_found); sdsfree(t2); return 0;
        }
    }
    if (wb->obt[wb->nob] != -1 && wb->obc[wb->nob].cmatch != -1 &&
        C_IS_O(Tbl[wb->obt[wb->nob]].col[wb->obc[wb->nob].cmatch].type) &&
        !wb->obc[wb->nob].nlo) {
            addReply(c, shared.order_by_luatbl);     sdsfree(t2);    return 0;
    }
    wb->nob++; sdsfree(t2); return 1;
}
static bool parseLimit(cli *c, char *token, wob_t *wb, char **fin) {
    if (!strncasecmp(token, "LIMIT ", 6)) {
        token  = next_token(token);
        if (!token) { addReply(c, shared.oby_lim_needs_num); return 0; }
        wb->lim = atol(token); /* OK: DELIM: [\ ,\0] */
        token  = next_token(token);
        if (token) {
            if (!strncasecmp(token, "OFFSET", 6)) {
                token  = next_token(token);
                if (!token) {
                    addReply(c, shared.oby_ofst_needs_num);  return 0;
                }
                if (!setOffsetReply(c, wb, token))           return 0;
                token   = next_token(token);
            }
        }
    }
    if (token) *fin = token; /* still something to parse */
    return 1;
}
static bool parseOrderBy(cli  *c, char *by, int tmatch, wob_t *wb, char **fin,
                         bool  isj) {
    if (strncasecmp(by, "BY ", 3)) {
        addReply(c, shared.wc_orderby_no_by);                         return 0;
    }
    char *token = next_token(by);
    if (!token) { addReply(c, shared.wc_orderby_no_by);               return 0;}
    if (strncasecmp(token, "LIMIT ", 6)) {
        bool more = 1; /* more OBC to parse */
        while (more) {
            if (wb->nob == MAX_ORDER_BY_COLS) {
                addReply(c, shared.toomany_nob);                      return 0;
            }
            if (!parseOBYcol(c, &token, tmatch, wb, fin, &more, isj)) return 0;
        }
    }
    if (token) return parseLimit(c, token, wb, fin);
    return 1;
}
bool parseWCEnd(redisClient *c, char *token, cswc_t *w, wob_t *wb, bool isj) {
    w->lvr         = token;   /* assume parse error */
    if (!strncasecmp(token, "ORDER ", 6)) {
        char *by      = next_token(token);
        if (!by) {
            w->lvr = NULL; addReply(c, shared.wc_orderby_no_by); return 0;
        }
        char *lfin    = NULL;
        if (!parseOrderBy(c, by, w->wf.tmatch, wb, &lfin, isj)) {
            w->lvr = NULL;                                       return 0;
        }
        if (lfin) token     = lfin;
        else      w->lvr    = NULL; /* negate parse error */
    }
    if (!strncasecmp(token, "LIMIT ", 6)) {
        char *lfin    = NULL;
        if (!parseLimit(c, token, wb, &lfin)) {
            w->lvr = NULL;                                       return 0;
        }
        if (lfin) token     = lfin;
        else      w->lvr    = NULL; /* negate parse error */
    }
    return 1;
}
static bool pRangeReply(cli *c, char *frst, char **fin, uchar ctype, f_t *flt) {
    int   slen;
    char *and    = next_token_wc_key(frst, ctype);
    if (!and) goto parse_range_err;
    int   flen   = and - frst;
    SKIP_SPACES(and) /* find start of value */
    if (strncasecmp(and, "AND ", 4)) {
        addReply(c, shared.whereclause_no_and); return 0;
    }
    char *second = next_token(and);
    if (!second) goto parse_range_err;
    char *tfin   = next_token_wc_key(second, ctype);
    if (tfin) {
        slen     = tfin - second;
        SKIP_SPACES(tfin)
        *fin     = tfin;
    } else {
        slen     = strlen(second);
        *fin     = NULL;
    }
    frst         = extract_string_col(frst,  &flen);
    if (!frst) return 0;
    flt->low     = sdsnewlen(frst,  flen);
    second       = extract_string_col(second, &slen);
    if (!second) return 0;
    flt->high    = sdsnewlen(second, slen);
    return 1;

parse_range_err:
    addReply(c, shared.whereclause_between);
    return 0;
}

static void addSelectToINL(void *v, lolo val, char *x, lolo xlen, long *card) {
    list **inl = (list **)v;
    if (x) listAddNodeTail(*inl, sdsnewlen(x, xlen));
    else   {
        sds s = sdscatprintf(sdsempty(), "%lld", val); // DESTROY ME ???
        listAddNodeTail(*inl, s);
    }
    INCR(*card);
}
static bool pWC_IN_CMD(redisClient *c, list **inl, char *s, int slen) {
    int axs = getAccessCommNum(s);
    if (axs == -1) { addReply(c, shared.accesstypeunknown); return 0; }
    int     argc;
    robj **rargv;
    sds x = sdsnewlen(s, slen);
    rargv = (*AccessCommands[axs].parse)(x, &argc);
    sdsfree(x);
    if (!rargv) { addReply(c, shared.where_in_select);      return 0; }
    redisClient *rfc = getFakeClient(); // frees last rfc->rargv[] + content
    rfc->argv        = rargv;
    rfc->argc        = argc;
    fakeClientPipe(rfc, inl, addSelectToINL);
    cleanupFakeClient(rfc);
    bool         err = !replyIfNestedErr(c, rfc, IN_RCMD_ERR_MSG);
    return !err;
}

static char *checkIN_Clause(redisClient *c, char *token) {
    char *end = str_matching_end_paren(token);
    if (!end || (*token != '(')) {
        addReply(c, shared.whereclause_in_err); return NULL;
    }
    return end;
}
// SYNTAX: IN (a,b,c)
static bool pWC_IN(cli *c, char *tok, list **inl, uchar ctype, char **fin) {
    char *end = checkIN_Clause(c, tok);
    if (!end) return 0;
    *inl       = listCreate();
    bool piped = 0;
    tok++;
    SKIP_SPACES(tok)
    if (*tok != '\'' && !ISDIGIT(*tok)) piped = 1;
    if (piped) {
        char *s    = tok;
        int   slen = end - s;
        if (!pWC_IN_CMD(c, inl, s, slen)) return 0;
    } else {
        char *s   = tok;
        char *beg = s;
        while (1) {
            if C_IS_S(ctype) {
                SKIP_SPACES(s)
                char *send = next_token_wc_key(s, ctype);
                if (!send) return 0;
                s++;                             /* skip leading \' */
                sds   x     = sdsnewlen(s, send - s - 1);
                listAddNodeTail(*inl, x);
                s           = strchr(send, ','); /* find next comma */
                if (!s || s > end) break;        /* strchr can go past end */
                s++;                             /* skip comma */
            } else { /* strnextunescapedchr() can go past end, safe but lame */
                char *nextc = str_next_unescaped_chr(beg, s, ',');
                if (!nextc || nextc > end) break;
                SKIP_SPACES(s)
                sds   x     = sdsnewlen(s, nextc - s);
                listAddNodeTail(*inl, x);
                nextc++;
                s           = nextc;
                SKIP_SPACES(s)
            }
        }
        if (!C_IS_S(ctype)) { /* no trailing ',' for final in list value */
            sds   x = sdsnewlen(s, end - s);
            listAddNodeTail(*inl, x);
        }
    }
    convertINLtoAobj(inl, ctype); /* convert from STRING to ctype */
    end++;
    if (*end) {
        SKIP_SPACES(end)
        *fin = end;
    }
    return 1;
}

typedef robj *parse_inum(char *token, int tlen, f_t *flt);
static robj *parseInumTblCol(char *token, int tlen, f_t *flt) {
    char *nextp = _strnchr(token, '.', tlen);
    if (!nextp)            return shared.badindexedcolumnsyntax;
    flt->tmatch = find_table_n(token, nextp - token);
    flt->jan    = server.alc.CurrClient->LastJTAmatch;
    if (flt->tmatch == -1) return shared.nonexistenttable;
    nextp++;
    flt->ic     = find_column_n(flt->tmatch, nextp, tlen - (nextp - token));
    if (flt->ic.cmatch == -1) return shared.nonexistentcolumn;
    flt->imatch = find_index(flt->tmatch, flt->ic);
    return NULL;
}
static robj *parseInumCol(char *token, int tlen, f_t *flt) {
    flt->jan = -1;
    flt->ic  = find_column_n(flt->tmatch, token, tlen);
    if      (flt->ic.fimatch != -1) {
        flt->imatch = flt->ic.fimatch;
    } else if (flt->ic.cmatch  == -1) {
        return shared.wc_col_not_found;
    } else {
        flt->imatch = find_index(flt->tmatch, flt->ic);
    }
    return NULL;
}
static uchar pWC_checkLuaFunc(cli *c, f_t *flt, sds tkn, char **fin, robj *ro) {
    char *oby =              strstr_not_quoted(tkn, " ORDER BY ");
    char *lim = oby ? NULL : strstr_not_quoted(tkn, " LIMIT ");
    sds s; // Do NOT include ORDERBY or LIMIT -> UGLY need tokenizer
    if      (oby) { s = sdsnewlen(tkn, (oby - tkn)); *fin = oby;  }
    else if (lim) { s = sdsnewlen(tkn, (oby - lim)); *fin = lim;  }
    else          { s = sdsnew   (tkn);              *fin = NULL; }
    bool ret = checkOrCr8LFunc(flt->tmatch, &flt->le, s, 1);
    sdsfree(s);
    if (ret) { flt->op = LFUNC;    return PRS_OK;       }
    else     {
        if (ro) { addReply(c, ro); return PRS_NEST_ERR; }
        else                       return PRS_GEN_ERR;
    }
}
#define PARSE_WC_CHECK_LUA                       \
  return pWC_checkLuaFunc(c, flt, tkn, fin, ro);

static uchar parseWCTokRelation(cli *c,   cswc_t *w,   sds    tkn, char **fin,
                                f_t *flt, bool    isj, uchar  ttype) {
    uchar ctype; int two_toklen;
    parse_inum *pif   = (isj) ? parseInumTblCol : parseInumCol;
    flt->tmatch       = w->wf.tmatch; /* Non-joins need tmatch in pif() */
    if (ttype == TOK_TYPE_KEY) {
        robj *ro      = NULL;
        char *tok2    = next_token(tkn);
        if (!tok2) two_toklen = strlen(tkn);
        else       two_toklen = (tok2 - tkn) + get_token_len(tok2);
        char *spot    = NULL;
        flt->op       = findOperator(tkn, two_toklen, &spot);
        if (flt->op == NONE)                              PARSE_WC_CHECK_LUA
        char *end     = spot - OP_len[flt->op];;
        REV_SKIP_SPACES(end)
        ro            = (*pif)(tkn, end - tkn + 1, flt);
        if (ro)                                           PARSE_WC_CHECK_LUA
        ctype         = CTYPE_FROM_FLT(flt)
        char *start   = spot + 1;
        SKIP_SPACES(start)
        if (!*start)                                      PARSE_WC_CHECK_LUA
        char *tfin    = next_token_wc_key(start, ctype);
        int   len     = tfin ? tfin - start : (uint32)strlen(start);
        if (*start == '\'') flt->iss = 1; // TODO COL_STRING must be iss
        start         = extract_string_col(start, &len);
        if (!start)                                       PARSE_WC_CHECK_LUA
        flt->key      = sdsnewlen(start, len);
        if (tfin) SKIP_SPACES(tfin)
        *fin       = tfin;
        if (!tfin) w->lvr = NULL;
    } else { /* RANGE_QUERY or IN_QUERY */
        robj *ro      = NULL;
        char *nextp   = strchr(tkn, ' ');
        if (!nextp)                                       PARSE_WC_CHECK_LUA
        ro            = (*pif)(tkn, nextp - tkn, flt);
        if (ro)                                           PARSE_WC_CHECK_LUA
        ctype         = CTYPE_FROM_FLT(flt)
        SKIP_SPACES(nextp)
        if (!strncasecmp(nextp, "IN ", 3)) {
            nextp     = next_token(nextp);
            if (!nextp)                                   PARSE_WC_CHECK_LUA
            if (!pWC_IN(c, nextp, &flt->inl, ctype, fin)) PARSE_WC_CHECK_LUA
            flt->op = IN;
        } else if (!strncasecmp(nextp, "BETWEEN ", 8)) { /* RANGE QUERY */
            nextp     = next_token(nextp);
            if (!nextp)                                   PARSE_WC_CHECK_LUA
            if (!pRangeReply(c, nextp, fin, ctype, flt))  PARSE_WC_CHECK_LUA
            flt->op = RQ;
        } else                                            PARSE_WC_CHECK_LUA
    }
    return PRS_OK;
}

static bool addIndexes2Join(redisClient *c, f_t *flt, jb_t *jb, list *ijl) {
    ijp_t *ij = malloc(sizeof(ijp_t)); init_ijp(ij);
    if (flt->key && !flt->iss && ISALPHA(*flt->key)) {   /* JOIN INDEX */
        if (flt->op != EQ) { addReply(c, shared.join_noteq);      return 0; }
        f_t f2; initFilter(&f2); /* NOTE f2 does not need to be released */
        robj *ro = parseInumTblCol(flt->key, sdslen(flt->key), &f2);
        if (ro) { addReply(c, ro);                                return 0; }
        uchar ctype1 = CTYPE_FROM_FLT(flt) uchar ctype2 = CTYPE_FROM_FLT(flt)
        if (ctype1 != ctype2) {
            addReply(c, shared.join_coltypediff);                 return 0;
        }
        memcpy(&ij->rhs, &f2, sizeof(f_t)); // NOTE: NOW: must return 1
    }
    memcpy(&ij->lhs, flt, sizeof(f_t));     // NOTE: NOW: must return 1
    free(flt); /* FREED cuz just memcpy'ed, so flt's refs will get destroyed */
    listAddNodeTail(ijl, ij); jb->n_jind++;
    return 1;
}
uchar parseWC(cli *c, cswc_t *w, wob_t *wb, jb_t *jb, list *ijl) {
    uchar   prs;
    f_t    *flt   = NULL;
    bool    isj   = jb ? 1 : 0;
    char   *line  = w->token;
    sds     token = NULL;
    while (1) {
        flt            = newEmptyFilter();
        uchar  ttype   = TOK_TYPE_KEY;
        char  *tfin    = NULL;
        char  *in      = strcasestr_blockchar(line, " IN ", '\'');
        char  *and     = strcasestr_blockchar(line, " AND ", '\'');
        if (in && (!and || in < and)) ttype = TOK_TYPE_IN;
        if (and && in && in < and) {     /* include nested "IN (...AND....)"  */
            and        = in + 4;
            SKIP_SPACES(and)
            if (!*and)                                goto p_wd_err;
            and        = checkIN_Clause(c, and); /* and now @ ')' */
            if (!and)                                 goto p_wd_err;
            and        = strcasestr_blockchar(and, " AND ", '\'');
        } else {                         /* include "BETWEEN x AND y" */
            char *btwn = strcasestr_blockchar(line, " BETWEEN ", '\'');
            if (and && btwn && btwn < and) {
                and    = strcasestr_blockchar(and + 5, " AND ", '\'');
                ttype  = TOK_TYPE_RANGE;
            }
        }
        int   tlen     = and ? and - line : (int)strlen(line);
        if (token) sdsfree(token);
        token = sdsnewlen(line, tlen);
        prs   = parseWCTokRelation(c, w, token, &tfin, flt, isj, ttype);
        if (prs != PRS_OK)                          goto p_wd_err;
        if (jb) {             /* JOINs */
            if (!addIndexes2Join(c, flt, jb, ijl))    goto p_wd_err;
            flt = NULL; // means do not destroy below
        } else {             /* RANGE SELECT/UPDATE/DELETE */
            if (!w->flist) w->flist = listCreate();
            listAddNodeTail(w->flist, flt);
            flt = NULL; // means do not destroy below
        }
        if (and) {
            line = and + 5; /* go past " AND " */
            SKIP_SPACES(line)
            if (!*line)   break;
        } else {
            if (!tfin)    break;
            line = tfin;
            SKIP_SPACES(line)
            if (!*line)   break;
            if (!parseWCEnd(c, line, w, wb, isj)) { prs = PRS_NEST_ERR; break; }
            break;
        }
    } //dumpW(printf, w);

p_wd_err:
    if (flt) destroyFilter(flt);

    if (w->lvr) { /* blank space in lvr is not actually lvr */
        SKIP_SPACES(w->lvr)
        if (*(w->lvr)) w->lvr = sdsnewlen(w->lvr, strlen(w->lvr));
        else           w->lvr = NULL;
    }
    if (token) sdsfree(token);
    return prs;
}

/* RANGE_QUERY RANGE_QUERY RANGE_QUERY RANGE_QUERY RANGE_QUERY RANGE_QUERY */
void parseWCplusQO(cli *c, cswc_t *w, wob_t *wb, uchar sop) {
    uchar prs = parseWC(c, w, wb, NULL, NULL);
    if (prs == PRS_GEN_ERR)              genericParseError(c, sop);
    if (prs != PRS_OK)                   return;
    if (!optimiseRangeQueryPlan(c, w, wb)) return;
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
void init_join_block(jb_t *jb) {
    bzero(jb, sizeof(jb_t));
    init_wob(&jb->wb);                                   /* DESTROY ME 066 */
    jb->hw = jb->fkimatch = -1;
}
static void releaseIJ(ijp_t *ij) {                     //printf("releaseIJ\n");
    releaseFilterR_KL(&ij->lhs);
    releaseFilterR_KL(&ij->rhs);
    releaseFlist(&ij->flist); /* flist is referential, dont destroy */
}
void destroy_join_block(cli *c, jb_t *jb) {
    if (jb->lvr) { sdsfree(jb->lvr); jb->lvr = NULL; }   /* DESTROYED 050 */
    destroyFlist(&jb->mciflist);                         /* DESTROYED 069 */
    destroy_wob (&jb->wb);                               /* DESTROYED 066 */
    for (uint32 i = 0; i < jb->n_jind; i++) releaseIJ(&jb->ij[i]);
    if (jb->js) { free(jb->js); jb->js = NULL; }
    if (jb->ij) { free(jb->ij); jb->ij = NULL; }
    releaseFlist(&jb->fflist); /* flist is referential, dont destroy */
    releaseFlist(&jb->fklist); /* klist is referential, dont destroy */
    if (jb->ob) destroy_obsl(jb->ob, OBY_FREE_ROBJ);     /* DESTROYED 057 */
    for (int i = 0; i < c->NumJTAlias; i++) sdsfree(JTAlias[i].alias);//DEST 049
}

static bool joinParseWC(redisClient *c, jb_t *jb, char *wc) {
    bool   ret = 0; /* presume failure */
    cswc_t w;
    init_check_sql_where_clause(&w, -1, wc);
    list  *ijl = listCreate();
    uchar  prs = parseWC(c, &w, &jb->wb, jb, ijl);
    jb->ij     = malloc(sizeof(ijp_t) * ijl->len);
    listNode *lnij; int iij = 0;
    listIter *liij = listGetIterator(ijl, AL_START_HEAD);
    while((lnij = listNext(liij))) {
        memcpy(&jb->ij[iij], lnij->value, sizeof(ijp_t)); iij++;
    } listReleaseIterator(liij);
    listRelease(ijl);
    if (prs == PRS_GEN_ERR) genericParseError(c, SQL_SELECT);
    if (prs != PRS_OK) goto j_p_wc_end;
    if (w.lvr) {
        jb->lvr = sdsdup(w.lvr);                 goto j_p_wc_end; // DEST ME 050
    }
    if (!validateJoinOrderBy(c, jb))             goto j_p_wc_end;
    if (jb->n_jind < 1) {
        addReply(c, shared.toofewindicesinjoin); goto j_p_wc_end;
    }
    ret = 1;        /* negate presumed failure */

j_p_wc_end:
    destroy_check_sql_where_clause(&w);
    return ret;
}

bool parseJoin(cli *c, jb_t *jb, char *clist, char *tlist, char *wc) {
    bool  ret = 0;
    list *tl = listCreate();   list *ls = listCreate();
    list *janl = listCreate(); list *jl = listCreate();
    /* Check tbls in "FROM tbls,,,," */
    if (!parseCSLJoinTable(c, tlist, tl, janl)) goto prs_join_end;
    /* Check all tbl.cols in "SELECT tbl1.col1, tbl2.col2,,,,," */
    if (!parseCSLJoinColumns(c, clist, 0, tl, janl, jl, &jb->qcols, &jb->cstar))
                                                goto prs_join_end;
    jb->js = malloc(sizeof(jc_t) * jl->len);
    listNode *lnjs; int ijs  = 0;
    listIter *lijs = listGetIterator(jl, AL_START_HEAD);
    while((lnjs = listNext(lijs))) {
        memcpy(&jb->js[ijs], lnjs->value, sizeof(jc_t)); ijs++;
    } listReleaseIterator(lijs);
    ret = joinParseWC(c, jb, wc);
    if (!leftoverParsingReply(c, jb->lvr))      goto prs_join_end;
    if (EREDIS) embeddedSaveJoinedColumnNames(jb);

prs_join_end:
    listRelease(tl); luasellistRelease(ls);
    listRelease(janl); listRelease(jl);
    //printf("parseJoin: ret: %d\n", ret); dumpJB(c, printf, jb);
    return ret;
}
static void addQcol(jb_t *jb, int tmatch) {
   bool hit = 0;
   for (int j = 0; j < jb->qcols; j++) {
       if (jb->js[j].t == tmatch) { hit = 1; break; }
   }
   if (!hit) {
       jb->js[jb->qcols].t = tmatch; jb->js[jb->qcols].c = 0; /* PK */
       jb->qcols++;
   }
}
bool executeJoin(cli *c, jb_t *jb) {
    if (jb->cstar) { /* SELECT PK per index - minimum cols for "COUNT(*)" */
       jb->qcols = 0;
       for (int i = 0; i < jb->hw; i++) addQcol(jb, jb->ij[i].lhs.tmatch);
       addQcol(jb, jb->ij[jb->hw - 1].rhs.tmatch);
    }
    return joinGeneric(c, jb);
}
bool doJoin(redisClient *c, sds clist, sds tlist, sds wclause) {
    jb_t jb; init_join_block(&jb);
    bool ok = parseJoin(c, &jb, clist, tlist, wclause);
    if (ok) { //TODO prepareJoin AFTER optimiseJoinPlan
        if      (c->Prepare) ok = prepareJoin(c, &jb);
        else if (optimiseJoinPlan(c, &jb) && validateChain(c, &jb)) {
            if (c->Explain) explainJoin(c, &jb);
            else            ok = executeJoin(c, &jb);
        }
    }
    destroy_join_block(c, &jb);
    return ok;
}

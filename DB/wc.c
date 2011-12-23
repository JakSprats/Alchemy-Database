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
char *strcasestr(const char *haystack, const char *needle); /*compiler warning*/

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

extern int      Num_tbls;
extern r_tbl_t *Tbl;
extern r_ind_t *Index;

extern ja_t     JTAlias[MAX_JOIN_COLS];

extern stor_cmd AccessCommands[];
extern uchar    OP_len[NOP];

extern uchar    OutputMode;

// CONSTANT GLOBALS
static char CLT = '<';
static char CGT = '>';
static char CEQ = '=';
static char CNE = '!';

#define TOK_TYPE_KEY    0
#define TOK_TYPE_RANGE  1
#define TOK_TYPE_IN     2

static bool Bdum; /* dummy variable */

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

#define PARSEOB_DONE(endt) \
  if (!endt || !(*endt)) { *more   = 0; *fin = NULL; *token  = NULL; return 1; }

#define PARSEOB_IFCOMMA_NEXTTOK(endt)                                 \
  if (*endt == ',') {                                                 \
      endt++; PARSEOB_DONE(endt) SKIP_SPACES(endt) PARSEOB_DONE(endt) \
      *more  = 1; *token = endt; return 1;                            \
  }

/* SYNTAX: ORDER BY {col [DESC],}+ [LIMIT n [OFFSET m]] */
static bool parseOBYcol(redisClient  *c,
                        char        **token,
                        int           tmatch,
                        wob_t        *wb,
                        char        **fin,
                        bool         *more) {
    int   tlen  = get_tlen_delim2(*token, ',');
    if (!tlen) tlen = (int)strlen(*token);
    char *prd   = _strnchr(*token, '.', tlen);
    if (prd) { /* JOIN */
        if ((wb->obt[wb->nob] = find_table_n(*token, prd - *token)) == -1) {
            addReply(c, shared.join_order_by_tbl);          return 0;
        }
        char *cname = prd + 1;
        int   clen  = get_tlen_delim2(cname, ',');
        if (!clen) clen = (int)strlen(cname);
        if (!clen) { addReply(c, shared.join_order_by_col); return 0; }
        wb->obc[wb->nob] = find_column_n(wb->obt[wb->nob], cname, clen);
        if (wb->obc[wb->nob] == -1) {
            addReply(c, shared.join_order_by_col);          return 0;
        }
    } else {  /* RANGE_QUERY or IN_QUERY */
        wb->obc[wb->nob] = find_column_n(tmatch, *token, tlen);
        if (wb->obc[wb->nob] == -1) {
            addReply(c, shared.order_by_col_not_found);     return 0;
        }
        wb->obt[wb->nob] = tmatch;
    }
    wb->asc[wb->nob] = 1; /* ASC by default */
    *more          = (*(*token + tlen) == ',') ? 1: 0;
    int nob        = wb->nob;
    wb->nob++;           /* increment as parsing may already be finished */

    char *nextt = next_token_delim2(*token, ',');
    PARSEOB_DONE(nextt)            /* e.g. "ORDER BY X" no DESC nor COMMA    */
    PARSEOB_IFCOMMA_NEXTTOK(nextt) /* e.g. "ORDER BY col1,col2,col3"         */
    SKIP_SPACES(nextt)             /* e.g. "ORDER BY col1    DESC" -> "DESC" */
    PARSEOB_IFCOMMA_NEXTTOK(nextt) /* e.g. "ORDER BY col1    ,   col2"       */
    PARSEOB_DONE(nextt)            /* e.g. "ORDER BY X   " no DESC nor COMMA */

    if (!strncasecmp(nextt, "DESC", 4)) {
        wb->asc[nob] = 0;
        if (*(nextt + 4) == ',') *more = 1;
        nextt       = next_token_delim2(nextt, ',');
        PARSEOB_DONE(nextt) /* for misformed ORDER BY ending in ',' */
    } else if (!strncasecmp(nextt, "ASC", 3)) {
        wb->asc[nob] = 1;
        if (*(nextt + 3) == ',') *more = 1;
        nextt       = next_token_delim2(nextt, ',');
        PARSEOB_DONE(nextt) /* for misformed ORDER BY ending in ',' */
    }
    PARSEOB_IFCOMMA_NEXTTOK(nextt) /* e.g. "ORDER BY c1 DESC ,c2" -> "c2" */
    *token = nextt;
    return 1;
}
static bool parseOrderBy(cli *c, char *by, int tmatch, wob_t *wb, char **fin) {
    if (strncasecmp(by, "BY ", 3)) {
        addReply(c, shared.wc_orderby_no_by);                    return 0;
    }
    char *token = next_token(by);
    if (!token) { addReply(c, shared.wc_orderby_no_by);          return 0; }
    bool more = 1; /* more OBC to parse */
    while (more) {
        if (wb->nob == MAX_ORDER_BY_COLS) {
            addReply(c, shared.toomany_nob);                     return 0;
        }
        if (!parseOBYcol(c, &token, tmatch, wb, fin, &more))     return 0;
    }
    if (token) {
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
    }
    if (token) *fin = token; /* still something to parse */
    return 1;
}
bool parseWCEnd(redisClient *c, char *token, cswc_t *w, wob_t *wb) {
    w->lvr         = token;   /* assume parse error */
    if (!strncasecmp(token, "ORDER ", 6)) {
        char *by      = next_token(token);
        if (!by) {
            w->lvr = NULL; addReply(c, shared.wc_orderby_no_by); return 0;
        }
        char *lfin    = NULL;
        if (!parseOrderBy(c, by, w->wf.tmatch, wb, &lfin)) {
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
    flt->low     = sdsnewlen(frst,  flen);
    second       = extract_string_col(second, &slen);
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
static bool parseWC_IN_CMD(redisClient *c, list **inl, char *s, int slen) {
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
/* SYNTAX: IN (a,b,c) OR IN($redis_command arg1 arg2) */
static bool parseWC_IN(cli *c, char *tok, list **inl, uchar ctype, char **fin) {
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
        if (!parseWC_IN_CMD(c, inl, s, slen)) return 0;
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
                if (!s || s > end) break;        /* strchr can bo past end */
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

typedef bool parse_inum(cli *c, char *token, int tlen, f_t *flt);
static bool parseInumTblCol(cli *c, char *token, int tlen, f_t *flt) {
    char *nextp = _strnchr(token, '.', tlen);
    if (!nextp) { addReply(c, shared.badindexedcolumnsyntax);      return 0; }
    flt->tmatch = find_table_n(token, nextp - token);
    flt->jan    = c->LastJTAmatch;
    if (flt->tmatch == -1) { addReply(c, shared.nonexistenttable); return 0; }
    nextp++;
    flt->cmatch = find_column_n(flt->tmatch, nextp, tlen - (nextp - token));
    if (flt->cmatch == -1) { addReply(c,shared.nonexistentcolumn); return 0; }
    flt->imatch = find_index(flt->tmatch, flt->cmatch);
    return 1;
}
static bool parseInumCol(cli *c, char *token, int tlen, f_t *flt) {
    flt->jan    = -1;
    flt->cmatch = find_column_n(flt->tmatch, token, tlen);
    if (flt->cmatch == -1) { addReply(c, shared.wc_col_not_found); return 0; }
    else                   flt->imatch = find_index(flt->tmatch, flt->cmatch);
    return 1;
}

static uchar parseWCTokRelation(cli *c,   cswc_t *w,   char  *token, char **fin,
                                f_t *flt, bool    isj, uchar  ttype) {
    uchar ctype; int two_toklen;
    parse_inum *pif   = (isj) ? parseInumTblCol : parseInumCol;
    flt->tmatch       = w->wf.tmatch; /* Non-joins need tmatch in pif() */
    if (ttype == TOK_TYPE_KEY) {
        char *tok2    = next_token(token);
        if (!tok2) two_toklen = strlen(token);
        else       two_toklen = (tok2 - token) + get_token_len(tok2);
        char *spot    = NULL;
        flt->op       = findOperator(token, two_toklen, &spot);
        if (flt->op == NONE)                         return PARSE_GEN_ERR;
        char *end     = spot - OP_len[flt->op];;
        REV_SKIP_SPACES(end)
        if (!(*pif)(c, token, end - token + 1, flt)) return PARSE_NEST_ERR;
        ctype         = CTYPE_FROM_FLT(flt)
        char *start   = spot + 1;
        SKIP_SPACES(start)
        if (!*start)                                 return PARSE_GEN_ERR;
        char *tfin    = next_token_wc_key(start, ctype);
        int   len     = tfin ? tfin - start : (uint32)strlen(start);
        if (*start == '\'') flt->iss = 1; // TODO COL_STRING must be iss
        start         = extract_string_col(start, &len);
        flt->key      = sdsnewlen(start, len);
        if (tfin) SKIP_SPACES(tfin)
        *fin       = tfin;
        if (!tfin) w->lvr = NULL; //TODO WHERE x = 4 WTF AND -> WTF?
    } else { /* RANGE_QUERY or IN_QUERY */
        char *nextp   = strchr(token, ' ');
        if (!nextp)                                  return PARSE_GEN_ERR;
        if (!(*pif)(c, token, nextp - token, flt))   return PARSE_NEST_ERR;
        ctype         = CTYPE_FROM_FLT(flt)
        SKIP_SPACES(nextp)
        if (!strncasecmp(nextp, "IN ", 3)) {
            nextp     = next_token(nextp);
            if (!nextp)                              return PARSE_GEN_ERR;
            if (!parseWC_IN(c, nextp, &flt->inl, ctype, fin))
                                                     return PARSE_NEST_ERR;
            flt->op = IN;
        } else if (!strncasecmp(nextp, "BETWEEN ", 8)) { /* RANGE QUERY */
            nextp     = next_token(nextp);
            if (!nextp)                              return PARSE_GEN_ERR;
            if (!pRangeReply(c, nextp, fin, ctype, flt))
                                                     return PARSE_NEST_ERR;
            flt->op = RQ;
        } else { return PARSE_GEN_ERR; }
    }
    return PARSE_OK;
}

static bool addIndexes2Join(redisClient *c, f_t *flt, jb_t *jb, list *ijl) {
    ijp_t *ij = malloc(sizeof(ijp_t)); init_ijp(ij);
    if (flt->key && !flt->iss && ISALPHA(*flt->key)) {   /* JOIN INDEX */
        if (flt->op != EQ) { addReply(c, shared.join_noteq);      return 0; }
        f_t f2; initFilter(&f2); /* NOTE f2 does not need to be released */
        if (!parseInumTblCol(c, flt->key, sdslen(flt->key), &f2)) return 0;
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
        if (prs != PARSE_OK)                          goto p_wd_err;
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
            if (!parseWCEnd(c, line, w, wb)) { prs = PARSE_NEST_ERR; break; }
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
    if (prs == PARSE_GEN_ERR)              genericParseError(c, sop);
    if (prs != PARSE_OK)                   return;
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
    if (prs == PARSE_GEN_ERR) genericParseError(c, SQL_SELECT);
    if (prs != PARSE_OK) goto j_p_wc_end;
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
    bool  ret  = 0;
    list *tl   = listCreate(); //TODO combine: [tl & janl]
    list *janl = listCreate();
    list *jl   = listCreate();
    /* Check tbls in "FROM tbls,,,," */
    if (!parseCommaSpaceList(c,  tlist, 0, 1, 0, 0, 0, -1, NULL, tl, janl,
                             NULL, NULL, &Bdum))          goto prs_join_end;
    /* Check all tbl.cols in "SELECT tbl1.col1, tbl2.col2,,,,," */
    if (!parseCommaSpaceList(c,  clist, 0, 0, 1, 0, 0, -1, NULL, tl, janl,
                             jl, &jb->qcols, &jb->cstar)) goto prs_join_end;
    jb->js = malloc(sizeof(jc_t) * jl->len);
    listNode *lnjs; int ijs  = 0;
    listIter *lijs = listGetIterator(jl, AL_START_HEAD);
    while((lnjs = listNext(lijs))) {
        memcpy(&jb->js[ijs], lnjs->value, sizeof(jc_t)); ijs++;
    } listReleaseIterator(lijs);
    ret = joinParseWC(c, jb, wc);
    if (!leftoverParsingReply(c, jb->lvr))                goto prs_join_end;
    if (EREDIS) embeddedSaveJoinedColumnNames(jb);

prs_join_end:
    listRelease(tl); listRelease(janl); listRelease(jl);
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

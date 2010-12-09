/*
 * This file implements the sql parsing routines for Alsosql
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
char *strcasestr(const char *haystack, const char *needle); /*compiler warning*/

#include "sds.h"
#include "zmalloc.h"
#include "redis.h"

#include "common.h"
#include "join.h"
#include "store.h"
#include "alsosql.h"
#include "index.h"
#include "cr8tblas.h"
#include "rpipe.h"
#include "parser.h"
#include "sql.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern int      Num_tbls[MAX_NUM_DB];
extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];
extern stor_cmd AccessCommands[];

char   *Ignore_keywords[]   = {"PRIMARY", "CONSTRAINT", "UNIQUE", "KEY",
                               "FOREIGN" };
uint32  Num_ignore_keywords = 5;

/* CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS CREATE_TABLE_HELPERS */
bool ignore_cname(char *tkn) {
    for (uint32 i = 0; i < Num_ignore_keywords; i++) {
        if (!strncasecmp(tkn, Ignore_keywords[i], strlen(Ignore_keywords[i]))) {
            return 1;
        }
    }
    return 0;
}

/* SYNTAX: CREATE TABLE t (id int , division INT,salary FLOAT, name TEXT)*/
bool parseCreateTable(redisClient *c,
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      sds           col_decl) {
    char *token = col_decl;
    if (*token == '(') token++;
    if (!*token) { /* empty or only '(' */
        addReply(c, shared.createsyntax);
        return 0;      
    }
    while (isblank(*token)) token++;
    while (token) {
        if (*ccount == MAX_COLUMN_PER_TABLE) {
            addReply(c, shared.toomanycolumns);
            return 0;
        }
        int clen;
        while (token) { /* first parse column name */
            clen      = get_token_len(token);
            token     = rem_backticks(token, &clen);
            if (!ignore_cname(token)) break;
            token = get_next_token_nonparaned_comma(token);
        }
        if (!token) break;
        if (!cCpyOrReply(c, token, cnames[*ccount], clen)) return 0;

        token       = next_token_delim(token, ',', ')'); /* parse ctype, flags*/
        if (!token) break;
        sds   type  = sdsnewlen(token, get_token_len_delim(token, ',', ')'));
        token       = get_next_token_nonparaned_comma(token);

        /* in type search for INT (but not BIGINT - too big @8 Bytes) */
        int ntbls = Num_tbls[server.dbid];
        if ((strcasestr(type, "INT") && !strcasestr(type, "BIGINT")) ||
                   strcasestr(type, "TIMESTAMP")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_INT;
        } else if (strcasestr(type, "FLOAT") ||
                   strcasestr(type, "REAL")  ||
                   strcasestr(type, "DOUBLE")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_FLOAT;
        } else if (strcasestr(type, "CHAR") ||
                   strcasestr(type, "TEXT")  ||
                   strcasestr(type, "BLOB")  ||
                   strcasestr(type, "BYTE")  ||
                   strcasestr(type, "BINARY")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_STRING;
        } else {
RL4 "type: %s", type);
            addReply(c, shared.undefinedcolumntype);
            return 0;
        }
        sdsfree(type);
        Tbl[server.dbid][ntbls].col_flags[*ccount] = 0; /* TODO flags */
        *ccount                                    = *ccount + 1;
    }
    return 1;
}

/* WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE */
/* WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE WHERE_CLAUSE */

/* SYNTAX: BETWEEN x AND y */
static uchar parseRangeReply(redisClient  *c,
                             char         *first,
                             cswc_t       *w,
                             char        **finish) {
    int   slen;
    char *and    = strchr(first, ' ');
    if (!and) goto parse_range_err;
    int   flen   = and - first;
    while (isblank(*and)) and++; /* find start of value */
    if (strncasecmp(and, "AND ", 4)) {
        addReply(c, shared.whereclause_no_and);
        return SQL_ERR_LOOKUP;
    }
    char *second = next_token(and);
    if (!second) goto parse_range_err;
    char *tokfin = strchr(second, ' ');
    if (tokfin) {
        slen    = tokfin - second;
        while (isblank(*tokfin)) tokfin++;
        *finish = tokfin;
    } else {
        slen    = strlen(second);
        *finish = NULL;
    }

    w->low  = createStringObject(first,  flen);
    w->high = createStringObject(second, slen);
    //RL4 "RANGEQUERY: low: %s high: %s", w->low->ptr, w->high->ptr);
    return SQL_RANGE_QUERY;

parse_range_err:
    addReply(c, shared.whereclause_between);
    return SQL_ERR_LOOKUP;
}

/* SYNTAX: ORDER BY col [DESC] [LIMIT n [OFFSET m]] */
static bool parseOrderBy(redisClient  *c,
                         char         *by,
                         int           tmatch,
                         cswc_t       *w,
                         char        **finish) {
    if (strncasecmp(by, "BY ", 3)) {
        addReply(c, shared.whereclause_orderby_no_by);
        return 0;
    }

    char *token = next_token(by);
    if (!token) {
        addReply(c, shared.whereclause_orderby_no_by);
        return 0;
    }
    char *nextp = strchr(token, ' ');
    int   tlen  = nextp ? nextp - token : (int)strlen(token);
    char *prd   = _strnchr(token, '.', tlen);
    if (prd) { /* JOIN */
        if ((w->obt = find_table_n(token, prd - token)) == -1) {
            addReply(c, shared.join_order_by_tbl);
            return 0;
        }
        char *cname = prd + 1;
        int   clen  = get_token_len(cname);
        if (!clen || (w->obc = find_column_n(w->obt, cname, clen)) == -1) {
            addReply(c, shared.join_order_by_col);
            return 0;
        }
    } else {  /* RANGE QUERY */
        w->obc = find_column_n(tmatch, token, tlen);
        if (w->obc == -1) {
            addReply(c, shared.order_by_col_not_found);
            return 0;
        }
    }

    if (!nextp) { /* "ORDER BY X" - no DESC, LIMIT, OFFSET */
        *finish = NULL;
        return 1;
    }
    while (isblank(*nextp)) nextp++;
    if (!nextp) { /* "ORDER BY X   " - no DESC, LIMIT, OFFSET */
        *finish = NULL;
        return 1;
    }

    if (!strncasecmp(nextp, "DESC", 4)) {
        w->asc = 0;
        nextp  = next_token(nextp);
    } else if (!strncasecmp(nextp, "ASC", 3)) {
        w->asc = 1;
        nextp  = next_token(nextp);
    }

    if (nextp) {
        if (!strncasecmp(nextp, "LIMIT", 5)) {
            nextp  = next_token(nextp);
            if (!nextp) {
                addReply(c, shared.orderby_limit_needs_number);
                return 0;
            }
            w->lim = atoi(nextp);
            nextp  = next_token(nextp);
            if (nextp) {
                if (!strncasecmp(nextp, "OFFSET", 6)) {
                    nextp  = next_token(nextp);
                    if (!nextp) {
                        addReply(c, shared.orderby_offset_needs_number);
                        return 0;
                    }
                    w->ofst = atoi(nextp); /* LIMIT N OFFSET X */
                    nextp   = next_token(nextp);
                }
            }
        }
    }

    if (nextp) *finish = nextp; /* still something to parse - maybe STORE */
    return 1;
}

bool parseWCAddtlSQL(redisClient *c, char *token, cswc_t *w) {
    bool check_sto = 1;
    w->lvr         = token;   /* assume parse error */
    if (!strncasecmp(token, "ORDER ", 6)) {
        char *by      = next_token(token);
        if (!by) {
            addReply(c, shared.whereclause_orderby_no_by);
            return 0;
        }

        char *lfin    = NULL;
        if (!parseOrderBy(c, by, w->tmatch, w, &lfin)) return 0;
        if (lfin) {
            check_sto = 1;
            token     = lfin;
        } else {
            check_sto = 0;
            w->lvr    = NULL; /* negate parse error */
        }
    }
    if (check_sto) {
        w->lvr = token; /* assume parse error */
        if (!strncasecmp(token, "STORE ", 6)) {
            w->stor = token;
            w->lvr  = NULL;   /* negate parse error */
        }
    }
    return 1;
}

static bool addRCmdToINList(redisClient *c,
                            void        *x,
                            robj        *key,
                            long        *card,
                            int          b,   /* variable ignored */
                            int          n) { /* variable ignored */
    c = NULL; b = 0; n = 0; /* compiler warnings */
    list **inl = (list **)x;
    listAddNodeTail(*inl, key);
    *card      = *card + 1;
    return 1;
}

#define IN_RCMD_ERR_MSG \
  "-ERR SELECT FROM WHERE col IN(Redis_cmd) - inner command had error: "

/* SYNTAX: IN (a,b,c) OR IN($redis_command arg1 arg2) */
static uchar parseWC_IN(redisClient  *c,
                        char         *token,
                        list        **inl,
                        char        **finish) {
    char *end = str_next_unescaped_chr(token, token, ')');
    if (!end || (*token != '(')) {
        addReply(c, shared.whereclause_in_err);
        return SQL_ERR_LOOKUP;
    }

    *inl       = listCreate();
    bool piped = 0;
    token++;
    if (*token == '$') piped = 1;

    if (piped) {
        char *s    = token + 1;
        int   slen = end - s;
        int   axs  = getAccessCommNum(s);
        if (axs == -1 ) {
            addReply(c, shared.accesstypeunknown);
            return SQL_ERR_LOOKUP;
        }

        int     argc;
        robj **rargv;
        if (axs == ACCESS_SELECT_COMMAND_NUM) { /* SELECT has 6 args */
            argc  = 6;
            sds x = sdsnewlen(s, slen);
            rargv = parseSelectCmdToArgv(x);
            sdsfree(x);
            if (!rargv) {
                addReply(c, shared.where_in_select);
                return SQL_ERR_LOOKUP;
            }
        } else {
            sds *argv  = sdssplitlen(s, slen, " ", 1, &argc);
            int  arity = AccessCommands[axs].argc;
            if ((arity > 0 && arity != argc) || (argc < -arity)) { 
                zfree(argv);
                addReply(c, shared.accessnumargsmismatch);
                return SQL_ERR_LOOKUP;
            }
            rargv = zmalloc(sizeof(robj *) * argc);
            for (int j = 0; j < argc; j++) {
                rargv[j] = createStringObject(argv[j], sdslen(argv[j]));
            }
            zfree(argv);
        }
        redisClient *rfc = rsql_createFakeClient();
        rfc->argv        = rargv;
        rfc->argc        = argc;

        uchar f   = 0;
        fakeClientPipe(c, rfc, inl, 0, &f, addRCmdToINList, emptyNoop);
        bool  err = 0;
        if (!replyIfNestedErr(c, rfc, IN_RCMD_ERR_MSG)) err = 1;

        rsql_freeFakeClient(rfc);
        zfree(rargv);
        if (err) return SQL_ERR_LOOKUP;
    } else {
        char *s   = token;
        char *beg = s;
        while (1) {
            char *nextc = str_next_unescaped_chr(beg, s, ',');
            if (!nextc) break;
            robj *r     = createStringObject(s, nextc - s);
            listAddNodeTail(*inl, r);
            nextc++;
            s           = nextc;
            while (isblank(*s)) s++;
        }
        robj *r = createStringObject(s, end - s);
        listAddNodeTail(*inl, r);
    }

    end++;
    if (*end) {
        while (isblank(*end)) end++;
        *finish = end;
    }
    return SQL_IN_LOOKUP;
}

typedef bool parse_inum_func(redisClient *c,
                             int         *tmatch, 
                             char        *token, 
                             int          tlen, 
                             int         *cmatch, 
                             int         *imatch,
                             bool         is_scan);

/* Parse "tablename.columnname" into [tmatch, cmatch, imatch */
static bool parseInumTblCol(redisClient *c,
                            int         *tmatch,
                            char        *token,
                            int          tlen,
                            int         *cmatch,
                            int         *imatch,
                            bool         is_scan) {
    is_scan = 0; /* compiler warning */
    char *nextp = _strnchr(token, '.', tlen);
    if (!nextp) {
        addReply(c, shared.badindexedcolumnsyntax);
        return 0;
    }
    *tmatch = find_table_n(token, nextp - token);
    if (*tmatch == -1) {
        addReply(c, shared.nonexistenttable);
        return 0;
    }
    nextp++;
    *cmatch = find_column_n(*tmatch, nextp, tlen - (nextp - token));
    if (*cmatch == -1) {
        addReply(c,shared.nonexistentcolumn);
        return 0;
    }
    *imatch = find_index(*tmatch, *cmatch);
    if (*imatch == -1) {
        addReply(c, shared.whereclause_col_not_indxd);
        return 0;
    }
    return 1;
}

/* Parse "columnname" from table Tbl[tmatch] into [cmatch, imatch */
static bool parseInumCol(redisClient *c,
                        int         *tmatch,
                        char        *token,
                        int          tlen,
                        int         *cmatch,
                        int         *imatch,
                        bool         is_scan) {
    *cmatch = find_column_n(*tmatch, token, tlen);
    if (*cmatch == -1) return 0;
    *imatch = (*cmatch == -1) ? -1 : find_index(*tmatch, *cmatch); 
    if (*cmatch == -1) {
        addReply(c, shared.whereclause_col_not_found);
        return 0;
    }
    if (!is_scan && *imatch == -1) { /* non-indexed column */
        addReply(c, shared.whereclause_col_not_indxd);
        return 0;
    }
    return 1;
}

/* SYNTAX Where Relation
     1.) "col = 4" ... can be "col=4", "col= 4", "col =4", "col = 4"
     2.) "col BETWEEN x AND y"
     3.) "col IN (,,,,,)" */
static uchar parseWCTokenRelation(redisClient  *c,
                                  cswc_t       *w,
                                  uchar         sop,
                                  char         *token,
                                  char        **finish,
                                  bool          is_scan,
                                  bool          is_j) {
    parse_inum_func *pif;
    int              two_tok_len;
    uchar            wtype = SQL_ERR_LOOKUP;
    if (is_j) {
        pif         = parseInumTblCol;
        char *tok2  = next_token(token);
        if (!tok2) two_tok_len = strlen(token);
        else       two_tok_len = (tok2 - token) + get_token_len(tok2);
    } else {
        pif         = parseInumCol;
        two_tok_len = strlen(token);
    }

    char *eq = _strnchr(token, '=', two_tok_len);
    if (eq) { /* "col = X" */
        char *end = eq - 1;
        while (isblank(*end)) end--; /* find end of PK */
        if (!(*pif)(c, &w->tmatch, token, end - token + 1,
                    &w->cmatch, &w->imatch, is_scan)) return SQL_ERR_LOOKUP;

        wtype        = w->cmatch ? SQL_SINGLE_FK_LOOKUP : SQL_SINGLE_LOOKUP;
        char *start  = eq + 1;
        while (isblank(*start)) start++; /* find start of value */
        if (!*start) goto sql_tok_rel_err;
        char *tokfin = strchr(start, ' ');
        int   len    = tokfin ? tokfin - start : (uint32)strlen(start);
        w->key       = createStringObject(start, len);

        if (tokfin) while (isblank(*tokfin)) tokfin++;
        *finish = tokfin;
        if (!w->cmatch || !tokfin) { /* PK=X(single row) OR nutn 2 parse */
            w->lvr = tokfin;
            return wtype;
        }
    } else { /* Range_Query  or IN_Query */
        char *nextp = strchr(token, ' ');
        if (!nextp) goto sql_tok_rel_err;
        if (!(*pif)(c, &w->tmatch, token, nextp - token,
                    &w->cmatch, &w->imatch, is_scan)) return SQL_ERR_LOOKUP;
        while (isblank(*nextp)) nextp++; /* find start of next token */
        if (!strncasecmp(nextp, "IN ", 3)) {
            nextp = next_token(nextp);
            if (!nextp) goto sql_tok_rel_err;
            wtype = parseWC_IN(c, nextp, &w->inl, finish);
        } else if (!strncasecmp(nextp, "BETWEEN ", 8)) { /* RANGE QUERY */
            nextp = next_token(nextp);
            if (!nextp) goto sql_tok_rel_err;
            wtype = parseRangeReply(c, nextp, w, finish);
        } else {
            goto sql_tok_rel_err;
        }
    }
    return wtype;

sql_tok_rel_err:
    if      (sop == SQL_SELECT) addReply(c, shared.selectsyntax);
    else if (sop == SQL_DELETE) addReply(c, shared.deletesyntax);
    else if (sop == SQL_UPDATE) addReply(c, shared.updatesyntax);
    else                        addReply(c, shared.scanselectsyntax);
    return SQL_ERR_LOOKUP;
}


uchar checkSQLWhereClauseReply(redisClient *c,
                               cswc_t      *w,
                               uchar        sop,
                               bool         is_scan) {
    char  *finish = NULL;
    char  *token  = w->token;
    while (isblank(*token)) token++; /* find start of next token */
    uchar  wtype  = parseWCTokenRelation(c, w, sop, token,
                                         &finish, is_scan, 0);
    if (finish && !parseWCAddtlSQL(c, finish, w)) return SQL_ERR_LOOKUP;
    return wtype;
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
static bool addInd2Join(redisClient *c,
                        int          ind,
                        int         *n_ind,
                        int          j_indxs[MAX_JOIN_INDXS]) {
    if (*n_ind == MAX_JOIN_INDXS) {
        addReply(c, shared.toomanyindicesinjoin);
        return 0;
    }
    for (int i = 0; i < *n_ind; i++) {
        if (j_indxs[i] == ind) return 1;
    }
    j_indxs[*n_ind] = ind;
    *n_ind          = *n_ind + 1;
    return 1;
}

/* SYNTAX: The join clause is parse into tokens delimited by "AND"
            the special case of "BETWEEN x AND y" overrides the "AND" delimter
           These tokens are then parses as "relations"
   NOTE: Joins currently work only on a single index and NOT on a star schema
*/
static bool joinParseWCReply(redisClient  *c, jb_t *jb) {
    uchar   sop    = SQL_SELECT;
    uchar   wtype  = SQL_ERR_LOOKUP;
    char   *finish = jb->w.token;
    cswc_t *w      = &jb->w;
    int     ntoks  = 0;
    sds     token  = NULL;
    bool    ret    = 0;

    while (1) {
        //TODO needs to be str_case_unescaped_quotes_str(" AND ")
        char *tokfin = NULL;
        char *and    = strcasestr(finish, " AND ");
        char *btwn   = strcasestr(finish, " BETWEEN ");
        if (and && btwn && btwn < and) and = strcasestr(and + 5, " AND ");
        int   tlen   = and ? and - finish : (int)strlen(finish);
        token        = sdsnewlen(finish, tlen);
        wtype        = parseWCTokenRelation(c, w, sop, token, &tokfin, 0, 1);
        if (wtype == SQL_ERR_LOOKUP) goto j_pcw_end;

        if (!addInd2Join(c, w->imatch, &jb->n_ind, jb->j_indxs)) goto j_pcw_end;

        sds key = w->key ? w->key->ptr : NULL;
        if (key) { /* parsed key may be "tablename.columnname" i.e. join_indx */
            if (isalpha(*key) && strchr(key, '.')) {
                if (!parseInumTblCol(c, &w->tmatch, key, sdslen(key),
                                    &w->cmatch, &w->imatch, 0))  goto j_pcw_end;
                if (!addInd2Join(c, w->imatch,
                                 &jb->n_ind, jb->j_indxs))       goto j_pcw_end;
                decrRefCount(w->key);
                w->key = NULL;
           } else { /* or if it is a key, it currently needs to be a range */
               singleFKHack(w, &wtype);
           }
        }
        ntoks++;

        if (and) {
            finish = and + 5; /* go past " AND " */
            while (isblank(*finish)) finish++;
            if (!*finish) break;
        } else {
            if (!tokfin) break;
            finish = tokfin;
            while (isblank(*finish)) finish++;
            if (!*finish) break;
            if (!parseWCAddtlSQL(c, finish, w)) goto j_pcw_end;
            break;
        }
    }
    if (w->lvr) { /* leftover from parsing */
        while (isblank(*(w->lvr))) w->lvr++;
        if (*(w->lvr)) goto j_pcw_end;
    }

    if (w->obc != -1 ) { /* ORDER BY -> Table must be in join */
        bool hit = 0;
        for (int i = 0; i < jb->qcols; i++) {
            if (jb->j_tbls[i] == w->obt) {
                hit = 1;
                break;
            }
        }
        if (!hit) {
            addReply(c, shared.join_table_not_in_query);
            goto j_pcw_end;
        }
    }
    if (jb->n_ind == 0) {
        addReply(c, shared.joinindexedcolumnlisterror);
        goto j_pcw_end;
    }
    if (jb->n_ind < 2) {
        addReply(c, shared.toofewindicesinjoin);
        goto j_pcw_end;
    }
    if (!w->low && !w->inl) {
        addReply(c, shared.join_requires_range);
        goto j_pcw_end;
    }
    if (jb->n_ind > ntoks) {
        addReply(c, shared.join_on_multi_col);
        goto j_pcw_end;
    }

    ret = 1;
j_pcw_end:
    //TODO w->stor and w->lvr are INVALID outside of this func
    //     they ref "token" which gets freed below -> HACK
    if (w->stor) w->stor = _strdup(w->stor);
    if (w->lvr)  w->lvr  = _strdup(w->lvr);
    if (token) sdsfree(token);
    return ret;
}

void init_join_block(jb_t *jb, char *wc) {
    init_check_sql_where_clause(&(jb->w), -1, wc);
    jb->n_ind = 0;
    jb->qcols = 0;
    jb->nname = NULL;
    jb->cstar = 0;
}
void destroy_join_block(jb_t *jb) {
    if (jb->nname) decrRefCount(jb->nname);
    destroy_check_sql_where_clause(&(jb->w));
}

bool parseJoinReply(redisClient *c,
                    jb_t        *jb,
                    char        *clist,
                    char        *tlist) {
    int  tmatchs[MAX_JOIN_INDXS];
    bool bdum;
    int  numt = 0;

    /* Check tbls in "FROM tbls,,,," */
    if (!parseCommaSpaceListReply(c, tlist, 0, 1, 0, -1, NULL,
                                  &numt, tmatchs, NULL, NULL,
                                  NULL, &bdum)) return 0;

    /* Check all tbl.cols in "SELECT tbl1.col1, tbl2.col2,,,,," */
    if (!parseCommaSpaceListReply(c, clist, 0, 0, 1, -1, NULL,
                                  &numt, tmatchs, jb->j_tbls, jb->j_cols,
                                  &jb->qcols, &jb->cstar)) return 0;

    bool ret = joinParseWCReply(c, jb);
    if (!leftoverParsingReply(c, jb->w.lvr)) return 0;
    return ret;
}

void joinReply(redisClient *c) {
    jb_t jb;
    init_join_block(&jb, c->argv[5]->ptr);
    if (!parseJoinReply(c, &jb, c->argv[1]->ptr, c->argv[3]->ptr)) goto j_end;

    if (jb.w.stor) {
        if (!jb.w.low && !jb.w.inl) {
            addReply(c, shared.selectsyntax_store_norange);
            goto j_end;
        } else if (jb.cstar) {
            addReply(c, shared.select_store_count);
            goto j_end;
        }
        if (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
            addReplySds(c, sdsnew(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n"));
            goto j_end;
        }
        joinStoreCommit(c, &jb);
    } else {
        if (jb.cstar) { /* SELECT PK per index - minimum cols for "COUNT(*)" */
           jb.qcols = 0;
           for (int i = 0; i < jb.n_ind; i++) {
               jb.j_tbls[i] = Index[server.dbid][jb.j_indxs[i]].table;
               jb.j_cols[i] = 0; /* PK */
               jb.qcols++;
           }
        }
        joinGeneric(c, NULL, &jb, 0, -1);
    }

j_end:
    if (jb.w.stor) free(jb.w.stor); /* HACK from joinParseWCReply */
    if (jb.w.lvr)  free(jb.w.lvr);  /* HACK from joinParseWCReply */
    destroy_join_block(&jb);
}

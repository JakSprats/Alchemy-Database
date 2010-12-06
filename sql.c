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

char   *Ignore_keywords[] = {"PRIMARY", "CONSTRAINT", "UNIQUE",
                              "KEY",    "FOREIGN" };
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
        if (strcasestr(type, "INT") && !strcasestr(type, "BIGINT")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_INT;
        } else if (strcasestr(type, "FLOAT") ||
                   strcasestr(type, "REAL")  ||
                   strcasestr(type, "DOUBLE")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_FLOAT;
        } else if (strcasestr(type, "CHAR") ||
                   strcasestr(type, "TEXT")  ||
                   strcasestr(type, "BLOB")  ||
                   strcasestr(type, "BINARY")) {
            Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_STRING;
        } else {
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
    *finish      = strchr(second, ' ');
    int slen;
    if (*finish) {
        slen = *finish - second;
        while (isblank(**finish)) *finish = *finish + 1;;
    } else {
        slen =  strlen(second);
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
    char *prd   = strchr(token, '.');
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

    if (!nextp) { /* ORDER BY X - no DESC, LIMIT, OFFSET */
        *finish = NULL;
        return 1;
    }
    while (isblank(*nextp)) nextp++;

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

bool parseWCAddtlSQL(redisClient *c,
                     char        *token,
                     cswc_t      *w,
                     int          tmatch) {
    bool check_sto = 1;
    w->lvr         = token;   /* assume parse error */
    if (!strncasecmp(token, "ORDER ", 6)) {
        char *by      = next_token(token);
        if (!by) {
            addReply(c, shared.whereclause_orderby_no_by);
            return 0;
        }

        char *lfin    = NULL;
        if (!parseOrderBy(c, by, tmatch, w, &lfin)) return 0;
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
                            long        *l,   /* variable ignored */
                            int          b,   /* variable ignored */
                            int          n) { /* variable ignored */
    c = NULL; l = NULL; b = 0; n = 0; /* compiler warnings */
    list **inl = (list **)x;
    robj *r    = cloneRobj(key);
    listAddNodeTail(*inl, r);
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

    *inl = listCreate();

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
                rargv[j]     = createStringObject(argv[j], sdslen(argv[j]));
            }
            zfree(argv);
        }
        redisClient *rfc = rsql_createFakeClient();
        rfc->argv        = rargv;
        rfc->argc        = argc;

        uchar f = 0;
        fakeClientPipe(c, rfc, inl, 0, &f, addRCmdToINList, emptyNoop);
        bool err = 0;
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

uchar checkSQLWhereClauseReply(redisClient *c,
                               cswc_t      *w,
                               int          tmatch,
                               uchar        sop,
                               bool         is_scan) {
    uchar  wtype;
    char  *finish = NULL;
    char  *eq     = strchr(w->token, '=');
    if (eq) { /* "col = X" */
        char *end = eq - 1;
        while (isblank(*end)) end--; /* find end of PK */
        w->cmatch = find_column_n(tmatch, w->token, end - w->token + 1);
        if (w->cmatch == -1) {
            addReply(c, shared.whereclause_col_not_found);
            return SQL_ERR_LOOKUP;
        }
        w->imatch = (w->cmatch == -1) ? -1 : find_index(tmatch, w->cmatch); 
        wtype     = w->cmatch ? SQL_SINGLE_FK_LOOKUP : SQL_SINGLE_LOOKUP;

        if (!is_scan && w->imatch == -1) { /* non-indexed column */
            addReply(c, shared.whereclause_col_not_indxd);
            return SQL_ERR_LOOKUP;
        }

        char *start  = eq + 1;
        while (isblank(*start)) start++; /* find start of value */
        if (!*start) goto check_sql_wc_err;
        finish       = strchr(start, ' ');
        int   len    = finish ? finish - start : (uint32)strlen(start);
        w->key       = createStringObject(start, len);
        if (!w->cmatch || !finish) { /* return on PK=X or nutn 2 parse */
            w->lvr = finish;
            return wtype;
        }
        while (isblank(*finish)) finish++; /* find start of next token */
    } else {
        char *nextp = strchr(w->token, ' ');
        if (!nextp) goto check_sql_wc_err;
        w->cmatch   = find_column_n(tmatch, w->token, nextp - w->token);
        w->imatch   = find_index(tmatch, w->cmatch); 

        char *tkn = nextp;
        while (isblank(*tkn)) tkn++; /* find start of next token */

        if (!strncasecmp(tkn, "IN ", 3)) {
            tkn   = next_token(tkn);
            if (!tkn) goto check_sql_wc_err;
            wtype = parseWC_IN(c, tkn, &w->inl, &finish);
        } else if (!strncasecmp(tkn, "BETWEEN ", 8)) { /* RANGE QUERY */
            tkn = next_token(tkn);
            if (!tkn) goto check_sql_wc_err;
            wtype = parseRangeReply(c, tkn, w, &finish);
        } else {
            goto check_sql_wc_err;
        }
        if (wtype == SQL_ERR_LOOKUP) return SQL_ERR_LOOKUP;
    }

    if (finish) { /* additional SQL */
        if (!parseWCAddtlSQL(c, finish, w, tmatch)) return SQL_ERR_LOOKUP;
    }
    return wtype;

check_sql_wc_err:
    if      (sop == SQL_SELECT) addReply(c, shared.selectsyntax);
    else if (sop == SQL_DELETE) addReply(c, shared.deletesyntax);
    else if (sop == SQL_UPDATE) addReply(c, shared.updatesyntax);
    else                        addReply(c, shared.scanselectsyntax);
    return SQL_ERR_LOOKUP;
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
static int parse_ind_from_tbl_col(redisClient *c, char *tname, char *nextc) {
    char *nextp = _strnchr(tname, '.', nextc - tname);
    if (!nextp) {
        addReply(c, shared.badindexedcolumnsyntax);
        return -1;
    }
    int tmatch = find_table_n(tname, nextp - tname);
    if (tmatch == -1) {
        addReply(c, shared.nonexistenttable);
        return -1;
    }
    nextp++;
    int cmatch = find_column_n(tmatch, nextp, nextc - nextp);
    if (cmatch == -1) {
        addReply(c,shared.nonexistentcolumn);
        return -1;
    }
    return find_index(tmatch, cmatch);
}

static int parseIndexedColumnListOrReply(redisClient *c,
                                         char        *ilist,
                                         int          j_indxs[]) {
    int   n_ind  = 0;
    char *tname  = ilist;
    char *nextc  = ilist;
    while ((nextc = strchr(nextc, ','))) {
        if (n_ind == MAX_JOIN_INDXS) {
            addReply(c, shared.toomanyindicesinjoin);
            return 0;
        }
        int imatch = parse_ind_from_tbl_col(c, tname, nextc);
        if (imatch == -1) {
            addReply(c, shared.nonexistentindex);
            return 0;
        }

        j_indxs[n_ind] = imatch;
        n_ind++;
        nextc++;
        tname     = nextc;
    }
    int imatch = parse_ind_from_tbl_col(c, tname, tname + strlen(tname));
    if (imatch == -1) {
        addReply(c, shared.nonexistentindex);
        return 0;
    }
    j_indxs[n_ind] = imatch;
    n_ind++;

    if (n_ind < 2) {
        addReply(c, shared.toofewindicesinjoin);
        return 0;
    }
    return n_ind;
}

static bool joinParseWCReply(redisClient  *c, jb_t *jb) {
    bool    ret   = 0;
    sds     icl   = sdsempty(); /* TODO: j_tbls[] j_indxs[] directly */
    robj   *jind1 = NULL;
    robj   *jind2 = NULL;
    char  **next  = &(jb->w.token);
    bool    fin  = 0;
    while (*next && !fin) {
        uchar wtype = SQL_ERR_LOOKUP;
        fin         = 1;
        jind1       = NULL;
        jind2       = NULL;
        //TODO needs to be str_unescaped_quotes_str(" AND ")
        char *and   = strstr(*next, " AND "); /* AND bounds the parse tuplet */
        int   alen  = and ? and - *next : (int)strlen(*next);
        char *tok1  = *next;
        char * eq   = _strnchr(tok1, '=', alen); /* "col = X" before AND */
        if (eq) { /* "col=" */
            char *end   = eq - 1;
            while (isblank(*end)) end--; /* find end of col */
            jind1       = createStringObject(tok1, (end - tok1 + 1));

            char *start  = eq + 1;
            while (isblank(*start)) start++; /* find start of value */

            char *finish  = strchr(start, ' ');
            int   len     = finish ? finish - start : (int)strlen(start);
            jind2         = createStringObject(start, len);

            *next         = next_token(start);
            wtype         = SQL_SINGLE_LOOKUP;
        } else { /* "col BETWEEN" or "col IN" */
            char *nextp   = strchr(tok1, ' ');
            if (!nextp) goto joincmd_end;
            jind1         = createStringObject(tok1, nextp - tok1);

            while (isblank(*nextp)) nextp++;
            char *tkn     = nextp;
            char *finish  = NULL;
            if (!strncasecmp(tkn, "IN ", 3)) {
                tkn       = next_token(tkn);
                if (!tkn) goto joincmd_end;
                wtype     = parseWC_IN(c, tkn, &jb->w.inl, &finish);
            } else if (!strncasecmp(tkn, "BETWEEN ", 8)) { /* RANGE QUERY */
                tkn       = next_token(tkn);
                if (!tkn) goto joincmd_end;
                wtype     = parseRangeReply(c, tkn, &jb->w, &finish);
            } else {
                goto joincmd_end;
            }
            *next = finish;
        }

        if (wtype == SQL_ERR_LOOKUP) goto joincmd_end;

// TODO START REPLACE
        //TODO on SQL_SINGLE_LOOKUP one col can be an INT
        // e.g. WHERE x.id = 4 AND x.id = y.id (reversed)
        sds  jp1      = jind1->ptr;
        sds  jp2      = jind2 ? jind2->ptr : NULL;
        bool mult_ind = ((sdslen(icl)) && !strstr(icl, jp1));
        if (jp2) mult_ind = (mult_ind && (!strstr(icl, jp2)));

        if (mult_ind) {
            addReply(c, shared.join_on_multi_col);
            goto joincmd_end;
        }
        if (!strstr(icl, jp1)) {
            if (sdslen(icl)) icl = sdscatlen(icl, ",", 1); 
            // TODO add to j_tbls & j_indxs DIRECTLY
            icl = sdscatlen(icl, jp1, sdslen(jp1));
        }
        if (jind2 && !strstr(icl, jp2)) {
                // TODO add to j_tbls & j_indxs DIRECTLY
            if (sdslen(icl)) icl = sdscatlen(icl, ",", 1); 
            icl = sdscatlen(icl, jp2, sdslen(jp2));
        }
// TODO END REPLACE

        if (*next) { /* Parse Next AND-Tuplet */
            if (!strncasecmp(*next, "AND ", 4)) {
                *next = next_token(*next);
                if (!*next) goto joincmd_end;
                fin = 0; /* i.e. loop */
            } else {
                if (!parseWCAddtlSQL(c, *next, &jb->w, -1)) goto joincmd_end;
                if (jb->w.obc != -1 ) { /* ORDER BY */
                    bool hit = 0;
                    for (int i = 0; i < jb->qcols; i++) {
                        if (jb->j_tbls[i] == jb->w.obt) {
                            hit = 1;
                            break;
                        }
                    }
                    if (!hit) {
                        addReply(c, shared.join_table_not_in_query);
                        goto joincmd_end;
                    }
                }
            }
        }

        if (jind1) decrRefCount(jind1);
        jind1 = NULL;
        if (jind2) decrRefCount(jind2);
        jind2 = NULL;
    }
    jb->w.lvr = *next;

    // TODO: combine this into the loop above
    jb->n_ind = parseIndexedColumnListOrReply(c, icl, jb->j_indxs);

    if (jb->n_ind == 0) {
        addReply(c, shared.joinindexedcolumnlisterror);
        goto joincmd_end;
    }
    if (!jb->w.low && !jb->w.inl) {
        addReply(c, shared.join_requires_range);
        goto joincmd_end;
    }

    ret = 1;
joincmd_end:
    sdsfree(icl);
    if (jind1) decrRefCount(jind1);
    if (jind2) decrRefCount(jind2);
    return ret;
}

void init_join_block(jb_t *jb, char *wc) {
    init_check_sql_where_clause(&(jb->w), wc);
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
    int  numt = 0;
    bool bdum;

    /* Check tbls in "FROM tbls,,,," */
    if (!parseCommaSpaceListReply(c, tlist, 0, 1, 0, -1, NULL,
                                  &numt, tmatchs, NULL, NULL,
                                  NULL, &bdum)) return 0;

    /* Check all tbl.cols in "SELECT tbl1.col1, tbl2.col2,,,,," */
    if (!parseCommaSpaceListReply(c, clist, 0, 0, 1, -1, NULL,
                                  &numt, tmatchs, jb->j_tbls, jb->j_cols,
                                  &jb->qcols, &jb->cstar)) return 0;

    return joinParseWCReply(c, jb);
}

void joinReply(redisClient *c) {
    jb_t jb;
    init_join_block(&jb, c->argv[5]->ptr);
    if (!parseJoinReply(c, &jb, c->argv[1]->ptr, c->argv[3]->ptr)) return;
    if (!leftoverParsingReply(c, jb.w.lvr)) goto join_end;

    if (jb.w.stor) {
        if (!jb.w.low && !jb.w.inl) {
            addReply(c, shared.selectsyntax_store_norange);
            goto join_end;
        } else if (jb.cstar) {
            addReply(c, shared.select_store_count);
            goto join_end;
        }
        if (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
            addReplySds(c, sdsnew(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n"));
            goto join_end;
        }
        joinStoreCommit(c, &jb);
    } else {
        if (jb.cstar) { /* get PK per column for "COUNT(*)" */
           jb.qcols = 0;
           for (int i = 0; i < jb.n_ind; i++) {
               jb.j_tbls[i] = Index[server.dbid][jb.j_indxs[i]].table;
               jb.j_cols[i] = 0; /* PK */
               jb.qcols++;
           }
        }
        joinGeneric(c, NULL, &jb, 0, -1);
    }

join_end:
    destroy_join_block(&jb);
}

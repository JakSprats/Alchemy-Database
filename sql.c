/*
 * This file implements the sql parsing routines for Alsosql
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
char *strcasestr(const char *haystack, const char *needle); /*compiler warning*/

#include "sds.h"
#include "zmalloc.h"
#include "redis.h"

#include "common.h"
#include "join.h"
#include "store.h"
#include "alsosql.h"
#include "index.h"
#include "denorm.h"
#include "sql.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern char  CCOMMA;
extern char  CEQUALS;
extern char  CPERIOD;
extern char  CMINUS;
extern char *COMMA;
extern char *STORE;

extern int      Num_tbls[MAX_NUM_DB];
extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];
extern stor_cmd StorageCommands[];
extern stor_cmd AccessCommands[];

char   *Col_keywords_to_ignore[] = {"PRIMARY", "CONSTRAINT", "UNIQUE",
                                    "KEY", "FOREIGN" };
uint32  Num_col_keywords_to_ignore = 5;

/* TODO legacy code based on "start-end" should be replaced */
static void convertFkValueToRange(sds fk_ptr, robj **rng) {
    *rng        = createStringObject(fk_ptr, sdslen(fk_ptr));
    (*rng)->ptr = sdscatprintf((*rng)->ptr, "-%s", (char *)fk_ptr);
}

char *rem_backticks(char *token, int *len) {
    if (*token == '`') {
        token++;
        *len = *len - 1;
    }
    if (token[*len - 1] == '`') {
        token[*len - 1] = '\0';
        *len = *len - 1;
    }
    return token;
}

static bool parseUntilNextCommaNotInParans(redisClient *c, int *argn) {
    bool got_open_parn = 0;
    uchar cflag        = 0;
    for (; *argn < c->argc; *argn = *argn + 1) {
        sds x = c->argv[*argn]->ptr;
        if (!strncasecmp(x, "AUTO_INCREMENT", 14)) cflag = 1;
        if (strchr(x, '(')) got_open_parn = 1;
        if (got_open_parn) {
            char *y = strchr(x, ')');
            if (y) {
                got_open_parn = 0;
                if (strchr(y, ',')) break;
            }
        } else {
            if (strchr(x, ',')) break;
        }
    }
    return cflag;
}

static bool parseCr8TblCName(redisClient *c,
                             int         *argn,
                             char        *tkn,
                             int          len,
                             bool        *p_cname,
                             bool        *continuer,
                             char         cnames[][MAX_COLUMN_NAME_SIZE],
                             int          ccount) {
    if (*tkn == '(') { /* delete "(" @ begin */
        tkn++;
        len--;
        if (!len) { /* lone "(" */
            *continuer = 1;
            return 1;
        }
    }
    tkn = rem_backticks(tkn, &len);

    for (uint32 i = 0; i < Num_col_keywords_to_ignore; i++) {
        if (!strcasecmp(tkn, Col_keywords_to_ignore[i])) {
            parseUntilNextCommaNotInParans(c, argn);
            *continuer = 1;
            break;
        }
    }
    if (*continuer) {
        *p_cname = 1;
    } else {
        if (!cCpyOrReply(c, tkn, cnames[ccount], len)) return 0;
        *p_cname = 0;
    }
    return 1;
}

bool parseCreateTable(redisClient *c,
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      int          *parsed_argn,
                      char         *o_token[]) {
    int   argn;
    bool  p_cname     = 1;
    char *trailer     = NULL;
    for (argn = 3; argn < c->argc; argn++) {
        sds token  = sdsdup(c->argv[argn]->ptr);
        o_token[*parsed_argn] = token;
        *parsed_argn = *parsed_argn + 1;
        if (p_cname) { /* first parse column name */
            int   len  = sdslen(token);
            char *tkn  = token; /* convert to "char *" to edit in raw form */
            bool  cont = 0;
            bool  ok = parseCr8TblCName(c, &argn, tkn, len, &p_cname, &cont,
                                        cnames, *ccount);
            if (!ok)  return 0;
            if (cont) continue;
        } else {      /* then parse column type + flags */
            if (trailer) { /* when last token ended with "int,x" */
                int  len  = strlen(trailer);
                bool cont = 0;
                bool ok   = parseCr8TblCName(c, &argn, trailer, len, &p_cname,
                                             &cont, cnames, *ccount);
                if (!ok)  return 0;
                if (cont) continue;
            }
            trailer = NULL;

            int   c_argn = argn;
            uchar cflag  = parseUntilNextCommaNotInParans(c, &argn);

            int   len   = sdslen(token);
            char *tkn   = token; /* convert to "char *" to edit in raw form */
            if (argn == c_argn) { /* this token has a comma */
                char *comma = strchr(tkn, CCOMMA);
                if ((comma - tkn) == (len - 1)) { /* "," @ end */
                    p_cname = 1;
                } else { /* means token ends w/ "int,x" */
                    trailer    = comma + 1;
                    p_cname = 0;
                }
            } else {
                p_cname = 1;
            }

            /* in 2nd word search for INT (but not BIGINT - too big @8 Bytes) */
            int ntbls = Num_tbls[server.dbid];
            if (strcasestr(tkn, "INT") && !strcasestr(tkn, "BIGINT")) {
                Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_INT;
            } else {
                Tbl[server.dbid][ntbls].col_type[*ccount] = COL_TYPE_STRING;
            }
            Tbl[server.dbid][ntbls].col_flags[*ccount] = cflag;
            *ccount                                    = *ccount + 1;
        }
    }
    return 1;
}

#define PARGN_OVERFLOW()            \
    *pargn = *pargn + 1;            \
    if (*pargn == c->argc) {        \
        WHERE_CLAUSE_ERROR_REPLY(0) \
    }

static uchar parseRangeReply(redisClient  *c,
                             char         *x,
                             int          *pargn,
                             robj        **rng,
                             uchar         sop,
                             bool          just_parse) {
    if (!strcasecmp(x, "BETWEEN")) { /* RANGE QUERY */
        PARGN_OVERFLOW()
        int start = *pargn;
        PARGN_OVERFLOW()
        if (strcasecmp(c->argv[*pargn]->ptr, "AND")) {
            addReply(c, shared.whereclause_no_and);
            return 0;
        }
        PARGN_OVERFLOW()
        int finish = *pargn;
        if (just_parse) return SQL_RANGE_QUERY;
        *rng = createStringObject(c->argv[start]->ptr,
                                    sdslen(c->argv[start]->ptr));
        (*rng)->ptr  = sdscatprintf((*rng)->ptr, "-%s",
                                    (char *)c->argv[finish]->ptr);
        //RL4 "RANGEQUERY: %s", (*rng)->ptr);
        return SQL_RANGE_QUERY;
    }
    if      (sop == SQL_SELECT) addReply(c, shared.selectsyntax_noequals);
    else if (sop == SQL_DELETE) addReply(c, shared.deletesyntax_noequals);
    else if (sop == SQL_UPDATE) addReply(c, shared.updatesyntax_noequals);
    else               addReply(c, shared.scanselectsyntax_noequals);
    return 0;
}

static bool parseOrderBy(redisClient *c,
                         int         *pargn,
                         int         *oba,
                         bool        *asc,
                         int         *lim) {
    uchar sop = 0;
    if (!strcasecmp(c->argv[*pargn]->ptr, "BY")) {
        PARGN_OVERFLOW()
        *oba = *pargn;
        if (*pargn != (c->argc - 1)) {
            if (!strcasecmp(c->argv[*pargn + 1]->ptr, "DESC")) {
                PARGN_OVERFLOW()
                *asc = 0;
            }
        }
        if (*pargn != (c->argc - 1)) {
            if (!strcasecmp(c->argv[*pargn + 1]->ptr, "LIMIT")) {
                PARGN_OVERFLOW()
                PARGN_OVERFLOW()
                *lim = atoi(c->argv[*pargn]->ptr);
            }
        }
        return 1;
    } else {
        addReply(c, shared.whereclause_orderby_no_by);
        return 0;
    }
}

#define CHECK_WHERE_CLAUSE_REPLY(X,Y,SOP)                            \
    if (strcasecmp(c->argv[X]->ptr, "WHERE")) {                      \
        if      (SOP == 0) addReply(c, shared.selectsyntax_nowhere); \
        else if (SOP == 1) addReply(c, shared.deletesyntax_nowhere); \
        else if (SOP == 2) addReply(c, shared.updatesyntax_nowhere); \
        return Y;                                                    \
    }

bool parseWCAddtlSQL(redisClient *c,
                     int         *pargn, 
                     int         *obc,
                     bool        *store,
                     uchar        sop,
                     bool        *asc,
                     int         *lim,
                     int          tmatch,
                     bool         reply) {
    PARGN_OVERFLOW()
    bool check_sto = 1;
    if (!strcasecmp(c->argv[*pargn]->ptr, "ORDER")) {
        PARGN_OVERFLOW()
        int oba   = -1;
        if (!parseOrderBy(c, pargn, &oba, asc, lim)) return 0;
        *obc = find_column(tmatch, c->argv[oba]->ptr);
        check_sto = 0;
        if (*pargn != (c->argc - 1)) {
            PARGN_OVERFLOW()
            check_sto = 1;
        }
    }
   if (check_sto) {
       if (!strcasecmp(c->argv[*pargn]->ptr, STORE)) {
            PARGN_OVERFLOW()
            *store = 1;
        } else {
            if (reply) {
                WHERE_CLAUSE_ERROR_REPLY(0);
            }
        }
    }
    return 1;
}

static bool addRedisCmdToINList(redisClient *c,
                                void        *x,
                                robj        *key,
                                long        *l,  /* variable ignored */
                                int          b) { /* variable ignored */
    c = NULL; l = NULL; b = 0; /* compiler warnings */
    list **inl = (list **)x;
    robj *r    = cloneRobj(key);
    listAddNodeTail(*inl, r);
    return 1;
}

/* NOTE: IN "(x, y, z)" must be one argv */
static bool parseWhereClauseIN(redisClient  *c,
                               int          *pargn,
                               list        **inl,
                               uchar         sop) {
    PARGN_OVERFLOW()
    sds carg = c->argv[*pargn]->ptr;
    if (!strchr(carg, '(') || !strchr(carg, ')')) {
        addReply(c, shared.whereclause_in_err);
        return 0;
    }
    sds arg = sdsdup(c->argv[*pargn]->ptr); /* sdsfree() in function */

    *inl    = listCreate();
    char *s = strchr(arg, '(');
    s++;
    while (1) {
        char *nextc = strchr(s, ',');
        if (!nextc) break;  /* TODO respect "\," */
        *nextc      = '\0';
        robj *r     = createStringObject(s, nextc - s);
        listAddNodeTail(*inl, r);
        nextc++;
        s           = nextc;
        while (*s == ' ') s++; /* ltrim */
    }
    char *t = strchr(s, ')');
    *t = '\0';
    if (listLength(*inl) == 0) { /* empty [except for sdsdup ptr] */
        int axs = -1;
        char *u = strchr(s, ' ');
        if (u) {
            int len = u - s;
            for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
                if (!strncasecmp(s, AccessCommands[i].name, len)) {
                    axs = i;
                    break;
                }
            }
            if (axs != -1 ) {
                int     argc;
                sds    *argv = sdssplitlen(s, strlen(s), " ", 1, &argc);
                robj **rargv = malloc(sizeof(robj *) * argc);
                for (int j = 0; j < argc; j++) {
                    rargv[j] = createObject(REDIS_STRING, argv[j]);
                }
                zfree(argv);
                redisClient *rfc = rsql_createFakeClient(); /* client to read */
                rfc->argv        = rargv;
                rfc->argc        = argc;

                fakeClientPipe(c, rfc, inl, 0, addRedisCmdToINList, emptyNoop);

                rsql_freeFakeClient(rfc);
                free(rargv);
                sdsfree(arg); /* sdsdup above */
                return 1;
            }
        }
    }
    while (*s == ' ') s++; /* ltrim */
    robj *r = createStringObject(s, t - s);
    listAddNodeTail(*inl, r);

    sdsfree(arg); /* sdsdup above */
    return 1;
}

/* TODO just_parse is not respected in all parsing routines */
uchar checkSQLWhereClauseReply(redisClient  *c,
                               robj       **key,
                               robj       **rng,
                               int         *imatch,
                               int         *cmatch,
                               int         *pargn,
                               int          tmatch,
                               uchar        sop,
                               bool         just_parse,
                               int         *obc,
                               bool        *asc,
                               int         *lim,
                               bool        *store,
                               list       **inl) {
    CHECK_WHERE_CLAUSE_REPLY(*pargn, 0, sop)
    PARGN_OVERFLOW()

    bool got_eq     = 0;
    sds  token      = c->argv[*pargn]->ptr;
    sds  eq         = strchr(token, CEQUALS); /* pk=X */
    int  tok_cmatch = -1;
    if (eq) {
        tok_cmatch  = find_column_n(tmatch, token, eq - token - 1);
        if (token[sdslen(token) - 1] == CEQUALS) {
            token[sdslen(token) - 1] = '\0';
            got_eq = 1;
        } else { // pk=X
            if (cmatch)     *cmatch = tok_cmatch;
            if (just_parse) return SQL_SINGLE_LOOKUP;
            *key = createStringObject(eq + 1, sdslen(token) - (eq - token) - 1);
            return SQL_SINGLE_LOOKUP;
        }
    } else {
        tok_cmatch  = find_column(tmatch, token);
    }

    bool  is_fk       = 0;
    int   im          = find_index(tmatch, tok_cmatch); 
    char *pk_col_name = Tbl[server.dbid][tmatch].col_name[0]->ptr;
    if (imatch) *imatch = im;
    if (strcasecmp(token, pk_col_name)) { /* not PK */
        if (im != -1) { /* FK query */
            is_fk = 1;
        } else {
            if (cmatch) {
                *cmatch = tok_cmatch;
            } else {
                if      (sop == SQL_SELECT) addReply(c, shared.select_notpk);
                else if (sop == SQL_DELETE) addReply(c, shared.delete_notpk);
                else if (sop == SQL_UPDATE) addReply(c, shared.update_notpk);
                return 0;
            }
        }
    } else {
        if (cmatch) *cmatch = tok_cmatch;
    }

    PARGN_OVERFLOW()
    token = c->argv[*pargn]->ptr;
    if (got_eq) {
        if (just_parse) return SQL_SINGLE_LOOKUP;
        *key = createStringObject(token, sdslen(token));
    } else {
        if (token[0] == CEQUALS) {
            if (just_parse) return SQL_SINGLE_LOOKUP;
            if (sdslen(token) == 1) {
                PARGN_OVERFLOW()
                token = c->argv[*pargn]->ptr;
                *key = createStringObject(token, sdslen(token));
            } else {
                char *k = token + 1;
                *key = createStringObject(k, sdslen(token) - 1);
            }
        } else {
            uchar wtype = 0;
            if (!strcasecmp(token, "IN")) {
                if (!parseWhereClauseIN(c, pargn, inl, sop)) return 0;
                wtype = SQL_IN_LOOKUP;
            } else {
                wtype = parseRangeReply(c, token, pargn, rng, sop,
                                        just_parse);
            }
            if (wtype) {
                if (*pargn != (c->argc - 1)) { /* additional SQL */
                    if (!parseWCAddtlSQL(c, pargn, obc, store, sop,
                                         asc, lim, tmatch, 1)) return 0;
                }
            }
            return wtype;
        }
    }

    if (just_parse) return SQL_SINGLE_LOOKUP;
    if (is_fk) { /* single FK lookup is HACKED into a range-query of length 1 */
        convertFkValueToRange((*key)->ptr, rng);
        if (*pargn != (c->argc - 1)) { /* additional SQL */
            if (!parseWCAddtlSQL(c, pargn, obc, store, sop,
                                 asc, lim, tmatch, 1)) return 0;
        }
        return SQL_RANGE_QUERY;
    } else {
        return SQL_SINGLE_LOOKUP;
    }
}

static uchar checkSQLWhereJoinReply(redisClient  *c,
                                    robj        **jind1,
                                    robj        **jind2,
                                    robj        **rng,
                                    int          *pargn,
                                    list        **inl) {
    uchar sop    = 0;
    bool  got_eq = 0;
    sds   token  = c->argv[*pargn]->ptr;
    sds   eq     = strchr(token, CEQUALS); /* pk=X */
    if (eq) {
        *jind1 = createStringObject(token, (eq - token));
        if (token[sdslen(token) - 1] == CEQUALS) {
            token[sdslen(token) - 1] = '\0'; /* TODO this must go */
            sdsupdatelen(token);
            got_eq = 1;
        } else { // "pk=X"
            if (!isalnum(*(eq + 1))) { /* sanity check - protocol abuse */ 
                addReply(c, shared.selectsyntax);
                return 0;
            }
            int len = sdslen(token) - (eq - token) - 1;
            *jind2  = createStringObject(eq + 1, len);
            return 1;
        }
    } else {
        *jind1 = createStringObject(token, sdslen(token));
    }

    PARGN_OVERFLOW()
    token = c->argv[*pargn]->ptr;
    if (got_eq) {
        *jind2 = createStringObject(token, sdslen(token));
    } else {
        if (token[0] == CEQUALS) {
            if (sdslen(token) == 1) {
                PARGN_OVERFLOW()
                token  = c->argv[*pargn]->ptr;
                *jind2 = createStringObject(token, sdslen(token));
            } else {
                char *k = token + 1;
                *jind2  = createStringObject(k, sdslen(token) - 1);
            }
        } else {
            if (!strcasecmp(token, "IN")) {
                if (!parseWhereClauseIN(c, pargn, inl, sop)) return 0;
                return SQL_IN_LOOKUP;
            }
            return parseRangeReply(c, token, pargn, rng, sop, 0);
        }
    }
    return 1;
}

static bool parseJoinTable(redisClient *c, char *tbl, int *obt, int *obc) {
    char *col = strchr(tbl, '.');
    if (!col) {
        addReply(c, shared.join_order_by_syntax);
        return 0;
    }
    *col = '\0';
    col++;
    if ((*obt = find_table(tbl)) == -1) {
        addReply(c, shared.join_order_by_tbl);
        return 0;
    }
    if ((*obc = find_column(*obt, col)) == -1) {
        addReply(c, shared.join_order_by_col);
        return 0;
    }
    return 1;
}

bool joinParseReply(redisClient  *c,
                    sds           clist,
                    int           argn,
                    int           j_indxs[],
                    int           j_tbls [],
                    int           j_cols [],
                    int          *qcols,
                    int          *sto,
                    robj        **nname,
                    robj        **rng,
                    int          *n_ind,
                    int          *obt,
                    int          *obc,
                    bool         *asc,
                    int          *lim,
                    list        **inl,
                    bool         *cntstr) {
    bool  ret = 0;
    uchar sop = 0;
    *qcols      = multiColCheckOrReply(c, clist, j_tbls, j_cols, cntstr);
    if (!*qcols) return 0;

    for (; argn < c->argc; argn++) {
        sds y = c->argv[argn]->ptr;
        if (!strcasecmp(y, "WHERE")) break;
        if (*y == CCOMMA) {
             if (sdslen(y) == 1) continue;
             y++;
        }
        char *nextc = y;
        while ((nextc = strchr(nextc, CCOMMA))) {
            sds z = sdsnewlen(y, nextc - y);
            nextc++;
            TABLE_CHECK_OR_REPLY(z,0)
            y      = nextc;
            sdsfree(z);
        }
        if (*y) {
            TABLE_CHECK_OR_REPLY(y,0)
        }
    }
    CHECK_WHERE_CLAUSE_REPLY(argn,0,sop)
    ARGN_OVERFLOW(0)

    sds    icl   = sdsempty();
    uchar  wtype = 0;
    robj  *jind1 = NULL;
    robj  *jind2 = NULL;
    while (argn < c->argc) {
        bool fin = 0;
        jind1    = NULL;
        jind2    = NULL;
        wtype    = checkSQLWhereJoinReply(c, &jind1, &jind2, rng, &argn, inl);
        if (wtype) {
            sds jp1       = jind1 ? jind1->ptr : NULL;
            sds jp2       = jind2 ? jind2->ptr : NULL;
            sds cnvrt_rng = 0;
            if (jind1 && !strchr(jp1, CPERIOD)) cnvrt_rng = jp1;
            if (jind2 && !strchr(jp2, CPERIOD)) cnvrt_rng = jp2;
            if (cnvrt_rng) { /* TODO this has to go, replace w/ flags */
                if (rng) {
                    addReply(c, shared.join_on_multi_col);
                    goto join_cmd_err;
                }
                convertFkValueToRange(cnvrt_rng, rng);
            } else {
                if (jind1) {
                    if (sdslen(icl) && !strstr(icl, jp1)) {
                        addReply(c, shared.join_on_multi_col);
                        goto join_cmd_err;
                    }
                    if (!strstr(icl, jp1)) {
                        if (sdslen(icl)) icl = sdscatlen(icl, COMMA, 1); 
                        icl = sdscatlen(icl, jp1, sdslen(jp1));
                    }
                }
                if (jind2) {
                    if (!strstr(icl, jp2)) {
                        if (sdslen(icl)) icl = sdscatlen(icl, COMMA, 1); 
                        icl = sdscatlen(icl, jp2, sdslen(jp2));
                    }
                }
            }

            if (argn < (c->argc - 1)) { /* Parse Next AND-Tuplet */
                ARGN_OVERFLOW(0)
                bool check_sto = 1;
                if (!strcasecmp(c->argv[argn]->ptr, "AND")) {
                    ARGN_OVERFLOW(0)
                } else {
                    if (!strcasecmp(c->argv[argn]->ptr, "ORDER")) {
                        ARGN_OVERFLOW(0)
                        int oba = -1;
                        if (!parseOrderBy(c, &argn, &oba, asc, lim))
                            goto join_cmd_err;

                        char *tbl = c->argv[oba]->ptr;
                        if (!parseJoinTable(c, tbl, obt, obc))
                            goto join_cmd_err;

                        bool hit = 0;
                        for (int i = 0; i < *qcols; i++) {
                            if (j_tbls[i] == *obt) {
                                hit = 1;
                                break;
                            }
                        }
                        if (!hit) {
                            addReply(c, shared.join_table_not_in_query);
                            goto join_cmd_err;
                        }

                        check_sto = 0;
                        if (argn != (c->argc - 1)) {
                            ARGN_OVERFLOW(0)
                            check_sto = 1;
                        }
                        fin = 1;
                    }
                    if (check_sto) {
                        if (!strcasecmp(c->argv[argn]->ptr, STORE)) {
                            ARGN_OVERFLOW(0)
                            CHECK_STORE_TYPE_OR_REPLY(c->argv[argn]->ptr,*sto,0)
                            ARGN_OVERFLOW(0)
                            *nname = c->argv[argn];
                            fin   = 1;
                        } else {
                            WHERE_CLAUSE_ERROR_REPLY(0);
                        }
                    }
                }
            } else {
                fin = 1;
            }
        }

        if (jind1) decrRefCount(jind1);
        jind1 = NULL;
        if (jind2) decrRefCount(jind2);
        jind2 = NULL;
        if (fin)   break;
        if (!wtype) goto join_cmd_err;
    }

    *n_ind = parseIndexedColumnListOrReply(c, icl, j_indxs);
    if (!n_ind) {
        addReply(c, shared.joinindexedcolumnlisterror);
        goto join_cmd_err;
    }
    if (!*rng && !*inl) {
        addReply(c, shared.join_requires_range);
        goto join_cmd_err;
    }

    ret = 1;
join_cmd_err:
    sdsfree(icl);
    if (jind1) decrRefCount(jind1);
    if (jind2) decrRefCount(jind2);
    return ret;
}

void joinReply(redisClient *c, sds clist, int argn) {
    int   j_indxs[MAX_JOIN_INDXS];
    int   j_tbls [MAX_JOIN_INDXS];
    int   j_cols [MAX_JOIN_INDXS];
    int   n_ind, qcols, sto;
    int   obt    = -1;   /* ORDER BY tbl */
    int   obc    = -1;   /* ORDER BY col */
    bool  asc    = 1;
    int   lim    = -1;
    robj *rng    = NULL; /* BETWEEN range */
    robj *nname  = NULL; /* NewName - jstore */
    list *inl    = NULL; /* IN() list */
    bool  cntstr = 0;

    bool ret = joinParseReply(c, clist, argn, j_indxs, j_tbls, j_cols,
                              &qcols, &sto, &nname, &rng, &n_ind,
                              &obt, &obc, &asc, &lim, &inl, &cntstr);

    if (ret) {
        char *range = rng ? rng->ptr : NULL;
        robj *low   = NULL;
        robj *high  = NULL;
        if (range && !range_check_or_reply(c, range, &low, &high)) return;

        if (nname) {
            jstoreCommit(c, sto, low, high, nname,
                         j_indxs, j_tbls, j_cols, n_ind, qcols,
                         obt, obc, asc, lim, inl);
            /* write back in "$" for AOF and Slaves */
            sds l_argv = sdscatprintf(sdsempty(), "%s$", 
                                             (char *)c->argv[c->argc - 1]->ptr);
            sdsfree(c->argv[c->argc - 1]->ptr);
            c->argv[c->argc - 1]->ptr = l_argv;
        } else {
            if (cntstr) { /* get PK per column for "COUNT(*)" */
               qcols = 0;
               for (int i = 0; i < n_ind; i++) {
                   j_tbls[i] = Index[server.dbid][j_indxs[i]].table;
                   j_cols[i] = 0; /* PK */
                   qcols++;
               }
            }
            joinGeneric(c, NULL, j_indxs, j_tbls, j_cols, n_ind, qcols,
                        low, high, -1, 0 , 0, NULL,
                        obt, obc, asc, lim, inl, cntstr);
        }
        if (low)  decrRefCount(low);
        if (high) decrRefCount(high);
    }
    if (inl)  listRelease(inl);
    if (rng)  decrRefCount(rng);
}

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

#include "redis.h"
#include "common.h"
#include "join.h"
#include "store.h"
#include "alsosql.h"
#include "index.h"
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

extern int Num_tbls[MAX_NUM_TABLES];
extern robj  *Tbl_col_name [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern uchar  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];

extern stor_cmd StorageCommands[NUM_STORAGE_TYPES];

char *Col_keywords_to_ignore[] = {"PRIMARY", "CONSTRAINT", "UNIQUE",
                                  "KEY", "FOREIGN" };
uint  Num_col_keywords_to_ignore = 5;

static void convertFkValueToRange(sds fk_ptr, robj **range) {
    *range        = createStringObject(fk_ptr, sdslen(fk_ptr));
    (*range)->ptr = sdscatprintf((*range)->ptr, "-%s", (char *)fk_ptr);
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

static void parseUntilNextCommaNotInParans(redisClient *c, int *argn) {
    bool got_open_parn = 0;
    for (; *argn < c->argc; *argn = *argn + 1) {
        sds x = c->argv[*argn]->ptr;
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

    for (uint i = 0; i < Num_col_keywords_to_ignore; i++) {
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
        if (p_cname) {
            int   len  = sdslen(token);
            char *tkn  = token; /* convert to "char *" to edit in raw form */
            bool  cont = 0;
            bool  ok = parseCr8TblCName(c, &argn, tkn, len, &p_cname, &cont,
                                        cnames, *ccount);
            if (!ok)  return 0;
            if (cont) continue;
        } else {
            if (trailer) { /* when last token ended with "int,x" */
                int  len  = strlen(trailer);
                bool cont = 0;
                bool ok   = parseCr8TblCName(c, &argn, trailer, len,
                                             &p_cname, &cont, cnames, *ccount);
                if (!ok)  return 0;
                if (cont) continue;
            }
            trailer = NULL;

            int c_argn = argn;
            parseUntilNextCommaNotInParans(c, &argn);

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

            /* in 2nd word search for INT (but not BIGINT) */
            int ntbls = Num_tbls[server.curr_db_id];
            if (strcasestr(tkn, "INT") && !strcasestr(tkn, "BIGINT")) {
                Tbl_col_type[ntbls][*ccount] = COL_TYPE_INT;
            } else {
                Tbl_col_type[ntbls][*ccount] = COL_TYPE_STRING;
            }
            *ccount = *ccount + 1;
        }
    }
    return 1;
}


#define PARGN_OVERFLOW()                  \
    *pargn = *pargn + 1;                  \
    if (*pargn == c->argc) {              \
        CHECK_WHERE_CLAUSE_ERROR_REPLY(0) \
    }

static unsigned char parseRangeReply(redisClient  *c,
                                     char         *x,
                                     int          *pargn,
                                     robj        **range,
                                     int           which,
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
        if (just_parse) return 2;
        *range = createStringObject(c->argv[start]->ptr,
                                    sdslen(c->argv[start]->ptr));
        (*range)->ptr  = sdscatprintf((*range)->ptr, "-%s",
                                      (char *)c->argv[finish]->ptr);
        //RL4 "RANGEQUERY: %s", (*range)->ptr);
        return 2;
    }
    if      (which == 0) addReply(c, shared.selectsyntax_noequals);
    else if (which == 1) addReply(c, shared.deletesyntax_noequals);
    else if (which == 2) addReply(c, shared.updatesyntax_noequals);
    else                 addReply(c, shared.scanselectsyntax_noequals);
    return 0;
}

#define CHECK_WHERE_CLAUSE_REPLY(X,Y)                                    \
    if (strcasecmp(c->argv[X]->ptr, "WHERE")) {                        \
        if      (which == 0) addReply(c, shared.selectsyntax_nowhere); \
        else if (which == 1) addReply(c, shared.deletesyntax_nowhere); \
        else                 addReply(c, shared.updatesyntax_nowhere); \
        return Y;                                                      \
    }

unsigned char checkSQLWhereClauseOrReply(redisClient  *c,
                                         robj       **key,
                                         robj       **range,
                                         int         *imatch,
                                         int         *cmatch,
                                         int         *pargn,
                                         int          tmatch,
                                         bool         which,
                                         bool         just_parse) {
    CHECK_WHERE_CLAUSE_REPLY(*pargn,0)
    PARGN_OVERFLOW()

    bool got_eq = 0;
    sds  token  = c->argv[*pargn]->ptr;
    sds  eq     = strchr(token, CEQUALS); /* pk=X */
    if (eq) {
        if (token[sdslen(token) - 1] == CEQUALS) {
            token[sdslen(token) - 1] = '\0';
            got_eq = 1;
        } else { // pk=X
            if (cmatch) {
                *eq     = '\0';
                *cmatch = find_column(tmatch, token);
            }
            if (just_parse) return 1;
            *key = createStringObject(eq + 1, sdslen(token) - (eq - token) - 1);
            return 1;
        }
    }

    bool  is_fk      = 0;
    int   tok_cmatch = find_column(tmatch, token);
    int   im         = find_index( tmatch, tok_cmatch); 
    if (imatch) *imatch = im;
    if (strcasecmp(token, Tbl_col_name[tmatch][0]->ptr)) { /* not PK */
        if (im != -1) { /* FK query */
            is_fk = 1;
        } else {
            if (cmatch) {
                *cmatch = tok_cmatch;
            } else {
                if      (which == 0) addReply(c, shared.selectsyntax_notpk);
                else if (which == 1) addReply(c, shared.deletesyntax_notpk);
                else                 addReply(c, shared.updatesyntax_notpk);
                return 0;
            }
        }
    }

    PARGN_OVERFLOW()
    token = c->argv[*pargn]->ptr;
    if (got_eq) {
        if (just_parse) return 1;
        *key = createStringObject(token, sdslen(token));
    } else {
        if (token[0] == CEQUALS) {
            if (just_parse) return 1;
            if (sdslen(token) == 1) {
                PARGN_OVERFLOW()
                token = c->argv[*pargn]->ptr;
                *key = createStringObject(token, sdslen(token));
            } else {
                char *k = token + 1;
                *key = createStringObject(k, sdslen(token) - 1);
            }
        } else {
            return parseRangeReply(c, token, pargn, range, which, just_parse);
        }
    }

    if (is_fk) { /* single FK lookup is HACKED into a range-query of length 1 */
        if (just_parse) return 1;
        convertFkValueToRange((*key)->ptr, range);
        return 2;
    }
    else {
        if (just_parse) return 1;
        return 1;
    }
}

static unsigned char checkSQLWhereJoinReply(redisClient  *c,
                                            robj        **jind1,
                                            robj        **jind2,
                                            robj        **range,
                                            int          *pargn) {
    int  which  = 1;
    bool got_eq = 0;
    sds  token  = c->argv[*pargn]->ptr;
    sds  eq     = strchr(token, CEQUALS); /* pk=X */
    if (eq) {
        *jind1 = createStringObject(token, (eq - token));
        if (token[sdslen(token) - 1] == CEQUALS) {
            token[sdslen(token) - 1] = '\0';
            sdsupdatelen(token);
            got_eq = 1;
        } else { // pk=X
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
                token = c->argv[*pargn]->ptr;
                *jind2 = createStringObject(token, sdslen(token));
            } else {
                char *k = token + 1;
                *jind2 = createStringObject(k, sdslen(token) - 1);
            }
        } else {
            return parseRangeReply(c, token, pargn, range, which, 0);
        }
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
                    robj        **newname,
                    robj        **range,
                    int          *n_ind) {
    bool ret   = 0;
    int  which = 1;
    *qcols = multiColCheckOrReply(c, clist, j_tbls, j_cols);
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
    CHECK_WHERE_CLAUSE_REPLY(argn,0)
    ARGN_OVERFLOW(0)

    sds            icl     = sdsempty();
    unsigned char  where   = 0;
    robj           *jind1  = NULL, *jind2 = NULL;
    while (argn < c->argc) {
        bool done = 0;
        where     = checkSQLWhereJoinReply(c, &jind1, &jind2, range, &argn);
        if (where) {
            sds jp1       = jind1 ? jind1->ptr : NULL;
            sds jp2       = jind2 ? jind2->ptr : NULL;
            sds cnvrt_rng = 0;
            if (jind1 && !strchr(jp1, CPERIOD)) cnvrt_rng = jp1;
            if (jind2 && !strchr(jp2, CPERIOD)) cnvrt_rng = jp2;
            if (cnvrt_rng) {
                if (range) {
                    addReply(c, shared.join_on_multi_col);
                    goto join_cmd_err;
                }
                convertFkValueToRange(cnvrt_rng, range);
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
                if (!strcasecmp(c->argv[argn]->ptr, "AND")) {
                    ARGN_OVERFLOW(0)
                }
                if (!strcasecmp(c->argv[argn]->ptr, STORE)) {
                    ARGN_OVERFLOW(0)
                    CHECK_STORE_TYPE_OR_REPLY(c->argv[argn]->ptr,*sto,0)
                    ARGN_OVERFLOW(0)
                    *newname = c->argv[argn];
                    done = 1;
                }
            } else {
                done = 1;
            }
        }

        if (jind1) decrRefCount(jind1);
        jind1 = NULL;
        if (jind2) decrRefCount(jind2);
        jind2 = NULL;
        if (done)   break;
        if (!where) goto join_cmd_err;
    }

    *n_ind = parseIndexedColumnListOrReply(c, icl, j_indxs);
    if (!n_ind) {
        addReply(c, shared.joinindexedcolumnlisterror);
        goto join_cmd_err;
    }
    if (!range) {
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
    robj *range   = NULL, *newname = NULL;

    bool ret = joinParseReply(c, clist, argn, j_indxs, j_tbls, j_cols,
                              &qcols, &sto, &newname, &range, &n_ind);
    if (ret) {
        if (newname) {
            jstoreCommit(c, sto, range, newname,
                         j_indxs, j_tbls, j_cols, n_ind, qcols);
        } else {
            RANGE_CHECK_OR_REPLY(range->ptr);
            joinGeneric(c, NULL, j_indxs, j_tbls, j_cols, n_ind, qcols,
                        low, high, -1, 0 , 0, NULL);
        }
    }
    if (range) decrRefCount(range);
}


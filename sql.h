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

#ifndef __REDISQL_SQL__H
#define __REDISQL_SQL__H

#include "redis.h"
#include "adlist.h"

#include "alsosql.h"
#include "common.h"

typedef struct join_block {
    cswc_t  w;
    int     j_indxs[MAX_JOIN_INDXS];
    int     j_tbls [MAX_JOIN_INDXS];
    int     j_cols [MAX_JOIN_INDXS];
    int     n_ind;
    int     qcols;
    robj   *nname; /* NewName - jstore */
    bool    cstar;
} jb_t;

bool parseCreateTable(redisClient *c,
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      sds           as_line);

#define SQL_ERR_LOOKUP       0 
#define SQL_SINGLE_LOOKUP    1
#define SQL_RANGE_QUERY      2
#define SQL_IN_LOOKUP        3
#define SQL_SINGLE_FK_LOOKUP 4

#define SQL_SELECT     0
#define SQL_DELETE     1
#define SQL_UPDATE     2
#define SQL_SCANSELECT 3

bool parseWCAddtlSQL(redisClient *c,
                     char        *token,
                     cswc_t      *w,
                     int          tmatch,
                     bool         reply);

uchar checkSQLWhereClauseReply(redisClient *c,
                               cswc_t      *w,
                               int          tmatch,
                               uchar        sop,
                               bool         just_parse,
                               bool         is_scan);

void init_join_block(jb_t *jb, char *wc);
void destroy_join_block(jb_t *jb);
bool parseJoinReply(redisClient *c, 
                    bool         just_parse,
                    jb_t        *jb,
                    char        *clist,
                    char        *tlist);
void joinReply(redisClient *c);

#endif /*__REDISQL_SQL__H */ 

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

#ifndef __REDISQL_WHERECLAUSE__H
#define __REDISQL_WHERECLAUSE__H

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
    robj   *nname; /* NewName - joinStore */
    bool    cstar;
} jb_t;

bool parseCreateTable(redisClient *c,
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      sds           as_line);

#define SQL_ERR_LOOKUP         0 
#define SQL_SINGLE_LOOKUP      1
#define SQL_RANGE_QUERY        2
#define SQL_IN_LOOKUP          3
#define SQL_SINGLE_FK_LOOKUP   4
#define SQL_STORE_LOOKUP_MASK  127

#define SQL_SELECT     0
#define SQL_DELETE     1
#define SQL_UPDATE     2
#define SQL_SCANSELECT 3

bool parseWCAddtlSQL(redisClient *c,
                     char        *token,
                     cswc_t      *w);

typedef bool parse_inum_func(redisClient *c,
                             int         *tmatch,
                             char        *token,
                             int          tlen,
                             int         *cmatch,
                             int         *imatch,
                             bool         is_scan);

void parseWCReply(redisClient *c, cswc_t *w, uchar sop, bool is_scan);

void init_join_block(jb_t *jb, char *wc);
void destroy_join_block(jb_t *jb);
bool parseJoinReply(redisClient *c, 
                    jb_t        *jb,
                    char        *clist,
                    char        *tlist);
void joinReply(redisClient *c);

#endif /* __REDISQL_WHERECLAUSE__H */

/*
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

#ifndef __ALSOSQL_COLPARSE__H
#define __ALSOSQL_COLPARSE__H

#include "redis.h"

#include "btreepriv.h"
#include "query.h"
#include "common.h"

typedef struct join_alias {
    sds alias;
    int tmatch;
} ja_t;

int find_table(char *tname);
int find_table_n(char *tname, int len);
sds getJoinAlias(int jan);

int find_column(int tmatch, char *column);
int find_column_n(int tmatch, char *column, int len);

//TODO turn into function
/* TABLE_CHECK_OR_REPLY(char *TBL, RET) -
     creates (int tmatch) */
#define TABLE_CHECK_OR_REPLY(TBL, RET)        \
    int tmatch = find_table(TBL);             \
    if (tmatch == -1) {                       \
        addReply(c, shared.nonexistenttable); \
        return RET;                           \
    }

//TODO turn into function
/* COLUMN_CHECK_OR_REPLY(char *cargv2ptr) -
     creates (char *cname, int cmatch)    */
#define COLUMN_CHECK_OR_REPLY(cargv2ptr, retval)   \
    char *cname  = cargv2ptr;                      \
    int   cmatch = find_column(tmatch, cname);     \
    if (cmatch == -1) {                            \
        addReply(c,shared.nonexistentcolumn);      \
        return retval;                             \
    }

int get_all_cols(int tmatch, int cmatchs[], bool lru2);
void incrOffsetVar(redisClient *c, wob_t *wb, long incr);

char *parseRowVals(sds vals,  char   **pk,        int    *pklen,
                   int ncols, twoint   cofsts[],  int     tmatch,
                   int pcols, int      cmatchs[]);

bool parseCommaSpaceList(cli  *c,         char *tkn,
                         bool  col_check, bool  tbl_check, bool join_check,
        /* COL or TBL */ int   tmatch,    int   cs[],
        /* JOIN */       int  *numt,      int   ts[], int jans[], jc_t js[],
                         int  *qcols,     bool *cstar);

bool parseSelectReply(redisClient *c,
                      bool         is_scan,
                      bool        *no_wc,
                      int         *tmatch,
                      int          cmatchs[MAX_COLUMN_PER_TABLE],
                      int         *qcols,
                      bool        *join,
                      bool        *cstar,
                      char        *clist,
                      char        *from,
                      char        *tlist,
                      char        *where);

int parseUpdateColListReply(redisClient  *c,
                            int           tmatch,
                            char         *vallist,
                            int           cmatchs[],
                            char         *vals   [],
                            uint32        vlens  []);

char isExpression(char *val, uint32 vlen);

uchar determineExprType(char *pred, int plen);

bool parseExpr(cli   *c,     char  e,   int    tmatch, int   cmatch,
               uchar  ctype, char *val, uint32 vlen,   ue_t *ue);

bool parseColType(cli *c, sds type, uchar *col_type);
bool parseCreateTable(cli  *c,       char  cnames[][MAX_COLUMN_NAME_SIZE],
                      int  *ccount,  sds   as_line);

#endif /*__ALSOSQL_COLPARSE__H */ 

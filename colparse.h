/*
 * This file implements the parsing of columns in SELECT and UPDATE statements
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

#ifndef __ALSOSQL_COLPARSE__H
#define __ALSOSQL_COLPARSE__H

#include "redis.h"

#include "alsosql.h"
#include "common.h"


typedef struct r_tbl {
    robj   *name;
    int     col_count;
    robj   *col_name[MAX_COLUMN_PER_TABLE];
    uchar   col_type[MAX_COLUMN_PER_TABLE];
    /* col_flags -> current usage only AUTO_INCREMENT */
    uchar   col_flags[MAX_COLUMN_PER_TABLE];
    int     virt_indx; /* TODO rename to virt_inum */
} r_tbl_t;

#define UETYPE_ERR    0
#define UETYPE_INT    1
#define UETYPE_FLOAT  2
#define UETYPE_STRING 3
typedef struct update_expression {
    bool  yes;
    char  op;
    int   c1match;
    int   type;
    char *pred;
    int   plen;
} ue_t;

int find_table(char *tname);
int find_table_n(char *tname, int len);
int find_column(int tmatch, char *column);
int find_column_n(int tmatch, char *column, int len);

/* TABLE_CHECK_OR_REPLY(char *TBL, RET) -
     creates (int tmatch) */
#define TABLE_CHECK_OR_REPLY(TBL, RET)        \
    int tmatch = find_table(TBL);             \
    if (tmatch == -1) {                       \
        addReply(c, shared.nonexistenttable); \
        return RET;                           \
    }

/* COLUMN_CHECK_OR_REPLY(char *cargv2ptr) -
     creates (char *cname, int cmatch)    */
#define COLUMN_CHECK_OR_REPLY(cargv2ptr, retval)   \
    char *cname  = cargv2ptr;                      \
    int   cmatch = find_column(tmatch, cname);     \
    if (cmatch == -1) {                            \
        addReply(c,shared.nonexistentcolumn);      \
        return retval;                             \
    }

int get_all_cols(int tmatch, int cmatchs[]);
void incrOffsetVar(redisClient *c, cswc_t *w, long incr);

char *parseRowVals(sds      vals,
                   char   **pk,
                   int     *pklen,
                   int      ncols,
                   uint32   cofsts[]);

bool parseSelectCol(int   tmatch,
                    char *cname,
                    int   clen,
                    int   cmatchs[],
                    int  *qcols,
                    bool *cstar);

bool parseJoinColsReply(redisClient *c,
                        char        *y,
                        int          len,
                        int         *numt,
                        int          tmatchs[],
                        int          j_tbls[],
                        int          j_cols[],
                        int        *qcols,
                        bool       *cstar);

bool parseCommaSpaceListReply(redisClient *c,
                              char        *y,
                              bool         col_check,
                              bool         tbl_check,
                              bool         join_check,
                              int          tmatch,    /* COL or TBL */
                              int          cmatchs[], /* COL or TBL */
                              int         *numt,      /* JOIN */
                              int          tmatchs[], /* JOIN */
                              int          j_tbls[],  /* JOIN */
                              int          j_cols[],  /* JOIN */
                              int         *qcols,
                              bool        *cstar);

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

/* UPDATE_EXPR */
char isExpression(char *val, uint32 vlen);

uchar determineExprType(char *pred, int plen);

bool parseExprReply(redisClient *c,
                    char         e,     
                    int          tmatch,
                    int          cmatch,
                    uchar        ctype, 
                    char        *val,  
                    uint32       vlen,  
                    ue_t        *ue);

#endif /*__ALSOSQL_COLPARSE__H */ 

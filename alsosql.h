/*
 * This file implements the basic SQL commands of Alsosql (single row ops)
 *  and calls the range-query and join ops
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

#ifndef __ALSOSQL__H
#define __ALSOSQL__H

#include "adlist.h"
#include "redis.h"

#include "aobj.h"
#include "common.h"

/* NOTE: SELECT STORE is implemented in LUA */
void luaIstoreCommit(redisClient *c);

typedef struct dual_lists {
    list *l1;
    list *l2;
    short num;
} d_l_t;

bool cCpyOrReply(redisClient *c, char *src, char *dest, unsigned int len);

//TODO break out into filter.h
enum OP {NONE, EQ, NE, GT, GE, LT, LE};
typedef struct filter {
    int       cmatch; /* column filter runs on                           */
    enum OP   op;     /* operation filter applies [,=,!=]                */
    sds       rhs;    /* right hand side of filter (e.g. x < 7 .. rhs=7) */
    aobj      rhsv;   /* value of rhs [sds="7",int=7,float=7.0]          */
    list     *inl;    /* WHERE ..... AND x IN (1,2,3)                    */
} f_t;
    /* WHERE fk BETWEEN 1 AND 10 AND x = 4 AND y != 5   */
    /* |-> range_query(fk, 1, 10) + list(f_t)->([x,=,4],[y,!,5]) */
void initFilter(f_t *filt);
f_t *newFilter(int cmatch, enum OP op, sds rhs);
void releaseFilter(f_t *flt);
f_t *cloneFilt(f_t *filt);
f_t *createFilter(robj *key, int cmatch, uchar ctype);
f_t *createINLFilter(list *inl, int cmatch);
void dumpFilter(f_t *filt);
void convertFilterListToAobj(list *flist, int tmatch);

typedef struct check_sql_where_clause {
    uchar   wtype;
    sds     token;
    robj   *key;
    robj   *low;                       /* BETWEEN low AND high */
    robj   *high;                      /* BETWEEN low AND high */
    list   *inl;                       /* IN (list,,,,) */
    sds     lvr;                       /* Leftover AFTER parse */
    int     imatch;
    int     tmatch;
    int     cmatch;
    int     nob;                       /* number ORDER BY columns */
    int     obc[MAX_ORDER_BY_COLS];    /* ORDER BY col */
    int     obt[MAX_ORDER_BY_COLS];    /* ORDER BY tbl -> JOINS */
    bool    asc[MAX_ORDER_BY_COLS];    /* ORDER BY ASC/DESC */
    long    lim;                       /* ORDER BY LIMIT */
    long    ofst;                      /* ORDER BY OFFSET */
    sds     ovar;                      /* OFFSET varname - used by cursors */
    list   *flist;                     /* FILTER list (nonindexed cols in WC) */
} cswc_t;

void init_check_sql_where_clause(cswc_t *w, int tmatch, sds token);
void destroy_check_sql_where_clause(cswc_t *w);


bool leftoverParsingReply(redisClient *c, char *x);

int parseUpdateOrReply(redisClient  *c,
                       int           tmatch,
                       char         *cname,
                       int           cmatchs[],
                       char         *vals   [],
                       unsigned int  vlens  []);

#define EMPTY_LEN_OBJ    \
    long  card   = 0;    \
    robj *lenobj = NULL;

#define INIT_LEN_OBJ                           \
    lenobj = createObject(REDIS_STRING, NULL); \
    addReply(c, lenobj);                       \
    decrRefCount(lenobj);

#define LEN_OBJ     \
    long  card = 0; \
    robj *lenobj;   \
    INIT_LEN_OBJ

void createTableCommitReply(redisClient *c,
                            char         cnames[][MAX_COLUMN_NAME_SIZE],
                            int          ccount,
                            char        *tname,
                            int          tlen);
void createCommand(redisClient *c);

void insertCommitReply(redisClient *c, 
                       char        *vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[],
                       bool         ret_size);

void insertCommand(redisClient *c);
void sqlSelectCommand(redisClient *c);
void updateCommand(redisClient *c);
void deleteCommand(redisClient *c);
void dumpCommand(redisClient *c);

unsigned long emptyTable(redisDb *db, int tmatch);

/* FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS */
void tscanCommand(redisClient *c);  /* scan.h does not exist */
void normCommand(redisClient *c);   /* norm.h does not exist */
void denormCommand(redisClient *c); /* denorm.h does not exist */

#endif /*__ALSOSQL__H */ 

/*
 * This file implements the basic SQL commands of Alsosql (single row ops)
 *  and calls the range-query and join ops
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __ALSOSQL__H
#define __ALSOSQL__H

#include "adlist.h"
#include "redis.h"

#include "common.h"

typedef struct r_tbl {
    robj   *name;
    int     col_count;
    robj   *col_name[MAX_COLUMN_PER_TABLE];
    uchar   col_type[MAX_COLUMN_PER_TABLE];
    /* col_flags -> current usage only AUTO_INCREMENT */
    uchar   col_flags[MAX_COLUMN_PER_TABLE];
    int     virt_indx; /* TODO is this still used? */
} r_tbl_t;

typedef struct dual_lists {
    list *l1;
    list *l2;
    short num;
} d_l_t;

typedef struct r_ind {
    robj  *obj;
    int    table;
    int    column;
    uchar  type;
    bool   virt; /* virtual - i.e. on primary key */
    bool   nrl;  /* non relational index - i.e. redis command */
} r_ind_t;

robj *cloneRobj(robj *r);
robj *convertRobj(robj *r, int type);

int find_table(char *tname);
int find_table_n(char *tname, int len);
int find_column(int tmatch, char *column);
int find_column_n(int tmatch, char *column, int len);

bool cCpyOrReply(redisClient *c, char *src, char *dest, unsigned int len);
void createTable(redisClient *c);

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

int parseColListOrReply(redisClient   *c,
                        int            tmatch,
                        char          *clist,
                        int            cmatchs[],
                        bool          *cntstr);

#define ASSIGN_UPDATE_HITS_AND_MISSES               \
    unsigned char  cmiss[MAX_COLUMN_PER_TABLE];     \
    char          *vals [MAX_COLUMN_PER_TABLE];     \
    unsigned int   vlens[MAX_COLUMN_PER_TABLE];     \
    for (int i = 0; i < ncols; i++) {               \
        unsigned char miss = 1;                     \
        for (int j = 0; j < qcols; j++) {           \
            if (i == cmatchs[j]) {                  \
                miss     = 0;                       \
                vals[i]  = mvals[j];                \
                vlens[i] = mvlens[j];               \
                break;                              \
            }                                       \
        }                                           \
        cmiss[i] = miss;                            \
    }

void parseSelectColumnList(redisClient *c, sds *clist, int *argn);

int parseUpdateOrReply(redisClient  *c,
                       int           tmatch,
                       char         *cname,
                       int           cmatchs[],
                       char         *vals   [],
                       unsigned int  vlens  []);

#define LEN_OBJ                                               \
    unsigned long  card   = 0;                                \
    robj          *lenobj = createObject(REDIS_STRING, NULL); \
    addReply(c, lenobj);                                      \
    decrRefCount(lenobj);

#define EMPTY_LEN_OBJ             \
    unsigned long  card   = 0;    \
    robj          *lenobj = NULL;

#define INIT_LEN_OBJ                           \
    lenobj = createObject(REDIS_STRING, NULL); \
    addReply(c, lenobj);                       \
    decrRefCount(lenobj);

void createTableCommitReply(redisClient *c,
                            char         col_names[][MAX_COLUMN_NAME_SIZE],
                            int          col_count,
                            char        *tname);
void insertCommitReply(redisClient *c, 
                       sds          vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[]);

unsigned long tableEmpty(redisDb *db, int tmatch);

#endif /*__ALSOSQL__H */ 

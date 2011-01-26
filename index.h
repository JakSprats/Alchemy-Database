/*
 * This file implements the indexing logic of Alsosql
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

#ifndef __INDEX__H
#define __INDEX__H

#include "redis.h"

#include "btreepriv.h"
#include "parser.h"
#include "row.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

typedef struct r_ind {
    robj  *obj;
    int    table;
    int    column;
    uchar  type;
    bool   virt; /* virtual - i.e. on primary key */
    bool   nrl;  /* non relational index - i.e. redis command */
} r_ind_t;

int find_index( int tmatch, int cmatch);
int match_index(int tmatch, int indices[]);
int match_index_name(char *iname);

/* MATCH_INDICES(tmatch)
     creates (int indices[], int matches)     */
#define MATCH_INDICES(tmatch)                      \
    int   indices[REDIS_DEFAULT_DBNUM];            \
    int   matches = match_index(tmatch, indices);

/* INDEX INDEX INDEX */
bool newIndexReply(redisClient *c,
                   sds          iname,
                   int          tmatch,
                   int          cmatch,
                   bool         virt,
                   d_l_t       *nrlind);
void createIndex(redisClient *c);

int buildIndex(bt *btr, bt_n *x, bt *ibtr, int icol, int itbl, bool nrl);


void addToIndex(redisDb *db,
                bt      *btr,
                aobj    *apk,
                char    *vals,
                uint32   cofsts[],
                int      inum);
void delFromIndex(redisDb *db,
                  bt      *btr,
                  aobj    *aopk,
                  void    *rrow,
                  int      inum,
                  int      tmatch);
void updateIndex(redisDb *db,
                 bt      *btr,
                 aobj    *aopk,
                 aobj    *anpk,
                 aobj    *newval,
                 void    *rrow,
                 int      inum,
                 int      tmatch);
void updatePKIndex(redisDb *db,
                   bt      *btr,
                   aobj    *aopk,
                   aobj    *anpk,
                   void    *rrow,
                   int      inum,
                   int      tmatch);

void emptyIndex(redisDb *db, int inum);
void dropIndex(redisClient *c);

#endif /* __INDEX__H */ 

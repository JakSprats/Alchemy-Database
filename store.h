/*
 * Implements istore and iselect
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

#ifndef __STORE__H
#define __STORE__H

#include "redis.h"
#include "adlist.h"

#include "alsosql.h"
#include "alsosql.h"
#include "common.h"

#define NUM_STORAGE_TYPES 10

unsigned char respOk(redisClient *c);
unsigned char respNotErr(redisClient *c);

bool internalCreateTable(redisClient *c,
                         redisClient *fc,
                         int          qcols, 
                         int          cmatchs[],
                         int          tmatch);

bool performStoreCmdOrReply(redisClient *c, redisClient *fc, int sto);

bool istoreAction(redisClient *c,
                  redisClient *fc,
                  int          tmatch,
                  int          cmatchs[],
                  int          qcols, 
                  int          sto,   
                  robj        *pko,  
                  robj        *row,
                  char        *nname,
                  bool         sub_pk,
                  uint32       nargc);

bool prepareToStoreReply(redisClient  *c,
                         cswc_t       *w,
                         char        **nname,
                         int          *nlen,
                         bool         *sub_pk,
                         int          *nargc,
                         char        **last,
                         int           qcols);
void istoreCommit(redisClient *c,
                  cswc_t      *w,
                  int          tmatch,
                  int          cmatchs[MAX_COLUMN_PER_TABLE],
                  int          qcols);

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols, 
                         int          j_tbls [],
                         int          j_cols[]);
#endif /* __STORE__H */ 

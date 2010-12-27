/*
 * Implements istore
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

#include "join.h"
#include "row.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

#define NUM_STORAGE_TYPES 10

bool performStoreCmdOrReply(redisClient *c,
                            redisClient *fc,
                            int          sto,
                            bool         join);

bool istoreAction(redisClient *c,
                  redisClient *fc,
                  int          tmatch,
                  int          cmatchs[],
                  int          qcols, 
                  int          sto,   
                  aobj        *apk,  
                  void        *rrow,
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
                  int          cmatchs[MAX_COLUMN_PER_TABLE],
                  int          qcols);

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
void prepare_jRowStore(jrow_reply_t *r);

bool jRowStore(jrow_reply_t *r);

#endif /* __STORE__H */ 

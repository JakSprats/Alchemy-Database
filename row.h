/*
 * This file implements the rows of Alsosql
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

#ifndef __ALSOSQL_ROW__H
#define __ALSOSQL_ROW__H

#include "redis.h"

#include "btreepriv.h"
#include "colparse.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

bool checkUIntReply(redisClient *c, long l, bool ispk);
uint32 strToFloat(redisClient *c, char *start, uint32 len, float *f);
uint32 strToInt(redisClient *c, char *start, uint32 len, uint32 *i);


void *createRow(redisClient *c,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint32       cofsts[]);
void freeRowObject(robj *o);

uint32 getRowMallocSize(uchar *stream);

void sprintfOutputFloat(char *buf, int len, float f);

aobj getRawCol(void *orow,
               int   cmatch,
               aobj *aopk,
               int   tmatch,
               flag *cflag,
               bool  force_string);

robj *outputRow(void *row,
                int   qcols,
                int   cmatchs[],
                aobj *aopk,
                int   tmatch,
                bool  quote_text_cols);

bool deleteRow(redisClient *c,
               int          tmatch,
               aobj        *apk,
               int          matches,
               int          indices[]);


bool updateRow(redisClient *c,
               bt          *btr,  
               aobj        *aopk, 
               void        *orow, 
               int          tmatch,
               int          ncols, 
               int          matches,
               int          indices[],
               char        *vals   [],
               uint32       vlens  [],
               uchar        cmisses[],
               ue_t         ue     []);

#endif /* __ALSOSQL_ROW__H */

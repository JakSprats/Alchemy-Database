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
#include "common.h"

typedef struct AlsoSqlObject {
    char   *s;
    uint32  len;
    uint32  i;
    float   f;
    uint32  s_i;
    flag    type;
    flag    enc;
    flag    sixbit;
} aobj;

robj *createRow(redisClient *c,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint32       col_ofsts[]);
void freeRowObject(robj *o);

uint32 getRowMallocSize(uchar *stream);

aobj getRawCol(robj *r, int cmatch, robj *okey, int  tmatch, flag *cflag,
               uchar ctype, bool force_string);
aobj getColStr(robj *r, int cmatch, robj *okey, int tmatch);

robj *createColObjFromRow(robj *r, int cmatch, robj *okey, int tmatch);

robj *outputRow(robj *row,
                int   qcols,
                int   cmatchs[],
                robj *okey,
                int   tmatch,
                bool  quote_text_cols);

int deleteRow(redisClient *c,
              int          tmatch,
              robj        *pko,
              int          matches,
              int          indices[]);

bool updateRow(redisClient *c,
               robj        *o,    
               robj        *okey, 
               robj        *orow, 
               int          tmatch,
               int          ncols, 
               int          matches,
               int          indices[],
               char        *vals[],
               uint32       vlens[],
               uchar        cmisses[]);

#endif /* __ALSOSQL_ROW__H */

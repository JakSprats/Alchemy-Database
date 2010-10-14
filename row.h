/*
 * This file implements the rows of Alsosql
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ALSOSQL_ROW__H
#define __ALSOSQL_ROW__H

#include "redis.h"
#include "common.h"

typedef struct AlsoSqlObject {
    char   *s;
    uint32  len;
    uint32  i;
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

aobj getRawCol(robj *r,     int   cmatch, robj *okey,        int   tmatch,
               flag *cflag, bool  icol,   bool  force_string);
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

/*
COPYRIGHT: RUSS
 */

#ifndef __ALSOSQL_ROW__H
#define __ALSOSQL_ROW__H

#include "redis.h"
#include "common.h"

typedef struct AlsoSqlObject {
    char         *s;
    uint  len;
    uint  i;
    uint  s_i;
    flag          type;
    flag          enc;
    flag          sixbit;
} aobj;

robj *createRow(redisClient *c,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint         col_ofsts[]);
void freeRowObject(robj *o);

uint getRowMallocSize(uchar *stream);

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
               uint         vlens[],
               uchar        cmisses[]);

#endif /* __ALSOSQL_ROW__H */

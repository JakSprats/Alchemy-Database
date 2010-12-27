/*
 * This file implements Alchemy's AOBJ
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

#ifndef __ALSOSQL_AOBJ__H
#define __ALSOSQL_AOBJ__H

#include "redis.h"

#include "btreepriv.h"
#include "alsosql.h"
#include "common.h"

typedef struct AlsoSqlObject { /* SIZE: 23 BYTES */
    char   *s;
    uint32  len;
    uint32  i;
    float   f;
    flag    type;
    flag    enc;               // TODO this should go, this is confusing
    flag    freeme;
} aobj;

void initAobj(aobj *a);
bool initAobjIntReply(redisClient *c, aobj *a, long l, bool ispk);
void initAobjString(aobj *a, char *s, int len);
void initAobjFloat(aobj *a, float f);
void initAobjFromString(aobj *a, char *s, int len, bool ctype);
bool initAobjFromVoid(aobj *a, redisClient *c, void *col, uchar ctype);

void releaseAobj(void *a);
void destroyAobj(void *a);

void dumpAobj(aobj *a);

aobj *cloneAobj(aobj *a);
aobj *copyRobjToAobj(robj *a, uchar ctype);
aobj *createAobjFromString(char *s, int len, bool ctype);
void convertINLtoAobj(list **inl, uchar ctype);
char *strFromAobj(aobj *a, int *len);
aobj *createStringAobjFromAobj(aobj *a); /* AAAAAAobj */
robj *createStringRobjFromAobj(aobj *a); /* RRRRRRobj */

#endif /* __ALSOSQL_AOBJ__H */

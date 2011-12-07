/*
 * This file implements ALCHEMY_DATABASE's AOBJ
 *

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of ALCHEMY_DATABASE

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __ALSOSQL_AOBJ__H
#define __ALSOSQL_AOBJ__H

#include "adlist.h"
#include "sds.h"
#include "redis.h"

#include "query.h"
#include "common.h"

void initAobj        (aobj *a);
void initAobjZeroNum (aobj *a,                   uchar ctype);
bool initAobjInt     (aobj *a, ulong l);
void initAobjLong    (aobj *a, ulong l);
void initAobjString  (aobj *a, char *s, int len);
void initAobjFloat   (aobj *a, float f);
void initAobjFromStr (aobj *a, char *s, int len, uchar ctype);
void initAobjFromLong(aobj *a, ulong l,          uchar ctype);
void releaseAobj(void *a);
void destroyAobj(void *a);

void  aobjClone( aobj *dest, aobj *src);
#define CREATE_CLONE(dest, src) \
  aobj dest; aobjClone(&dest, src);
aobj *cloneAobj( aobj *a);
void *vcloneAobj(void *a);
void  convertSdsToAobj(sds s, aobj *a, uchar ctype);
aobj *createAobjFromString(char *s, int len, uchar ctype);
aobj *createAobjFromLong  (ulong l);

void convertFilterSDStoAobj(f_t *flt);

void convertINLtoAobj(list **inl, uchar ctype);
list *cloneAobjList(list *ll);

void initStringAobjFromAobj(aobj *na, aobj *a);
sds  createSDSFromAobj(aobj *a);

sl_t outputReformat(aobj *a);
sl_t outputSL(uchar ctype, sl_t sl);

typedef bool aobj_cmp(aobj *a, aobj *b);
bool aobjEQ(aobj *a, aobj *b);
bool aobjNE(aobj *a, aobj *b);
bool aobjLT(aobj *a, aobj *b);
bool aobjLE(aobj *a, aobj *b);
bool aobjGT(aobj *a, aobj *b);
bool aobjGE(aobj *a, aobj *b);

void  incrbyAobj  (aobj *a, ulong l);
void  decrbyAobj  (aobj *a, ulong l);
ulong subtractAobj(aobj *a, aobj *b);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpAobj(printer *prn, aobj *a);

void vsdsfree(void *v); //TODO this should be somewhere else

#endif /* __ALSOSQL_AOBJ__H */

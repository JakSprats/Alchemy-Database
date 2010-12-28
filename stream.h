/*
 *
 * This file implements stream parsing for Rows
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

#ifndef __ALSOSQL_STREAM_H
#define __ALSOSQL_STREAM_H

#include "aobj.h"
#include "common.h"


uint32 skipToVal(uchar **stream);

uint32 getStreamMallocSize(uchar *stream, uchar btype);

int btStreamCmp(void *a, void *b);

char *createBTKey(const aobj *key,
                   int        ktype,
                   bool      *med,
                   uchar     *sflag,
                   uint32    *ksize);
void  destroyBTKey(char *simkey, bool  med);

void convertStream2Key(uchar *stream, aobj *key);
uchar *parseStream(uchar *stream, uchar btype);

void *createStream(bt *btr,
                   void *val,
                   char   *btkey,
                   uint32  ksize,
                   uint32 *ssize);
bool destroyStream(bt *btr, uchar *ostream);

#endif /* __ALSOSQL_STREAM_H */

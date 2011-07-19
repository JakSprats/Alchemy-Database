/*
 *
 * This file implements stream parsing for Rows
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

#ifndef __ALSOSQL_STREAM_H
#define __ALSOSQL_STREAM_H

#include "aobj.h"
#include "common.h"

void *row_malloc(bt *ibtr, int size);

uchar  getLruSflag();
int    cLRUcol(ulong l, uchar *sflag, ulong *col);
uint32 streamLRUToUInt(uchar *data);
void   overwriteLRUcol(uchar *row,    ulong icol);

int    cIcol  (ulong l, uchar *sflag, ulong *col, bool isi); /* NOTE: protected */
int    cr8Icol(ulong l, uchar *sflag, ulong *col);
int    cr8Lcol(ulong l, uchar *sflag, ulong *col);
int    cr8IcolFromStr(cli *c, char *strt, uint32 len, uchar *sflag, ulong *col);
int    cr8LcolFromStr(cli *c, char *strt, uint32 len, uchar *sflag, ulong *col);
int    cr8FColFromStr(cli *c, char *strt, uint32 len, float *col);
void   writeUIntCol (uchar **row, uchar sflag, ulong icol);
void   writeULongCol(uchar **row, uchar sflag, ulong icol);
void   writeFloatCol(uchar **row,              float fcol);
uint32 streamIntToUInt   (uchar *data, uint32 *clen);
ulong  streamLongToULong (uchar *data, uint32 *clen);
float  streamFloatToFloat(uchar *data, uint32 *clen);

uint32 getStreamMallocSize(bt *ibtr, uchar *stream);
uint32 getStreamRowSize   (bt *ibtr, uchar *stream);

/* INODE_I/L,UU,UL,LU,LL cmp */
int uintCmp (void *s1, void *s2);
int ulongCmp(void *s1, void *s2);
int uuCmp   (void *s1, void *s2);
int ulCmp   (void *s1, void *s2);
int luCmp   (void *s1, void *s2);
int llCmp   (void *s1, void *s2);

/* BT DATA cmp */
int btIntCmp  (void *a, void *b);
int btLongCmp (void *a, void *b);
int btFloatCmp(void *a, void *b);
int btTextCmp (void *a, void *b);

char *createBTKey(aobj *key, bool *med, uint32 *ksize, bt *btr);
void  destroyBTKey(char *btkey, bool  med);

void   convertStream2Key(uchar *stream, aobj *key, bt *btr);
uchar *parseStream(uchar *stream, bt *btr);

void *createStream(bt *btr, void *val, char *btkey, uint32 klen, uint32 *ssize);
bool  destroyStream(bt *btr, uchar *ostream);

#endif /* __ALSOSQL_STREAM_H */
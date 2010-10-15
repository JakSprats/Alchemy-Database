/*
 *
 * This file implementes all global #defines
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ALSOSQL_COMMON__H
#define __ALSOSQL_COMMON__H

#define uint32 unsigned int
#define uchar  unsigned char
#define ulong  unsigned long
#define ull    unsigned long long

#define bool unsigned char
#define flag unsigned char

//TODO these should be redis TYPEs no redundant defs
#define COL_TYPE_STRING       0
#define COL_TYPE_INT          1

#define TWO_POW_7         128
#define TWO_POW_14      16384
#define TWO_POW_28  268435456
#define TWO_POW_29  536870912
#define TWO_POW_32 4294967296

#define USHORT_SIZE sizeof(unsigned short)
#define UINT_SIZE   sizeof(unsigned int)
#define PTR_SIZE    sizeof(char *)

#define STORAGE_MAX_ARGC 3

#define MAX_NUM_DB             16

#define MAX_NUM_TABLES        256
#define MAX_COLUMN_PER_TABLE   64

#define MAX_NUM_INDICES       512

#define MAX_JOIN_INDXS         64
#define MAX_JOIN_COLS         128

#define INDEX_DELIM     "index"
#define INDEX_DELIM_LEN  5

#define NUM_COL_TYPES           2

//TODO make array dynamic in size
#define MAX_COLUMN_NAME_SIZE  128

#endif /* __ALSOSQL_COMMON__H */

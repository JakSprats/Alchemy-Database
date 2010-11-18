/*
 *
 * This file implementes all global #defines
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
#define COL_TYPE_FLOAT        2
#define COL_TYPE_NONE         3

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

#define NUM_COL_TYPES           3

//TODO make array dynamic in size
#define MAX_COLUMN_NAME_SIZE  128

#endif /* __ALSOSQL_COMMON__H */

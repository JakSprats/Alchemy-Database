/*
COPYRIGHT: RUSS
 */

#ifndef __ALSOSQL_COMMON__H
#define __ALSOSQL_COMMON__H

#define uint  unsigned int
#define uchar unsigned char

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

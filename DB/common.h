/*
 *
 * This file implementes all global #defines
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

#ifndef __ALSOSQL_COMMON__H
#define __ALSOSQL_COMMON__H

typedef int printer(const char *format, ...);

#define TWO_POW_32           4294967296
#define TWO_POW_64 18446744073709551615

#define bool     unsigned char
#define uchar    unsigned char
#define ushort16 unsigned short
#define uint32   unsigned int
#define ulong    unsigned long
#define lolo     long long
#define ull      unsigned long long

#define COL_TYPE_INT          0
#define COL_TYPE_LONG         1
#define COL_TYPE_STRING       2
#define COL_TYPE_FLOAT        3
#define COL_TYPE_NONE         4 //TODO should be 0 - dependency?

#define PTR_SIZE    sizeof(char *)
#define USHORT_SIZE sizeof(unsigned short)
#define UINT_SIZE   sizeof(unsigned int)
#define ULONG_SIZE  sizeof(unsigned long)

#define NUM_ACCESS_TYPES        2 /* CREATE TABLE AS [SELECT,SCAN] */

#define MAX_NUM_TABLES        256
#define MAX_COLUMN_PER_TABLE   64
#define MAX_NUM_INDICES       512
#define MAX_JOIN_INDXS         30 /* CAREFUL: tied to logic in qo.c */
#define MAX_JOIN_COLS         128
#define MAX_COLUMN_NAME_SIZE  128
#define MAX_ORDER_BY_COLS      16

#define INDEX_DELIM     "index"
#define LRUINDEX_DELIM  "lru"

#define MIN(A,B) ((A > B) ? B : A)
#define MAX(A,B) ((A > B) ? A : B)
#define INCR(x)     {x = x + 1;}
#define INCRBY(x,y) {x = x + y;}
#define DECR(x)     {x = x - 1;}
#define DECRBY(x,y) {x = x - y;}

#define C_IS_I(ctype) (ctype == COL_TYPE_INT)
#define C_IS_L(ctype) (ctype == COL_TYPE_LONG)
#define C_IS_S(ctype) (ctype == COL_TYPE_STRING)
#define C_IS_F(ctype) (ctype == COL_TYPE_FLOAT)
#define C_IS_NUM(ctype) (C_IS_I(ctype) || C_IS_L(ctype))

#define NOP 9
enum OP {NONE, EQ, NE, GT, GE, LT, LE, RQ, IN};

#define UETYPE_ERR    0
#define UETYPE_INT    1 /* also LONG */
#define UETYPE_STRING 2
#define UETYPE_FLOAT  3

#define OBY_FREE_NONE 0
#define OBY_FREE_ROBJ 1
#define OBY_FREE_AOBJ 2

#define CONSTRAINT_NONE   0
#define CONSTRAINT_UNIQUE 1
#define UNIQ(cnstr) (cnstr == CONSTRAINT_UNIQUE)

#define OREDIS OutputMode == OUTPUT_PURE_REDIS
#define EREDIS OutputMode == OUTPUT_EMBEDDED

#define FK_RQ(wtype) !(wtype == SQL_SINGLE_FK_LKP)

#define FLOAT_FMT "%.10g"

#define DEBUG_BT_TYPE(prn, btr) \
  prn("INODE: %d UU: %d UL: %d LU: %d LL: %d NORM: %d\n", \
       INODE(btr), UU(btr), UL(btr), LU(btr), LL(btr), NORM_BT(btr));

typedef struct twoint {
    int i;
    int j;
} twoint;

#define STACK_STRDUP(dest, src, len) \
  char dest[len + 1];                \
  memcpy(dest, src, len);            \
  dest[len] = '\0';

#define SERVER_BEGINNING_OF_TIME 1301419850 /* oldest LRU time possible */

#define GET_LRUC                                                      \
  uchar *lruc = NULL;                                                 \
  if (Tbl[tmatch].lrud) {                                       \
      uint32 clen; uchar rflag;                                       \
      lruc = getColData(rrow, Tbl[tmatch].lruc, &clen, &rflag); \
      if (!clen) lruc = NULL;                                         \
  }

#define P_SDS_EMT sdscatprintf(sdsempty(),

#define UPDATE_AUTO_INC(pktyp, apk)                             \
  if      (C_IS_I(pktyp) && apk.i > rt->ainc) rt->ainc = apk.i; \
  else if (C_IS_L(pktyp) && apk.l > rt->ainc) rt->ainc = apk.l;

#endif /* __ALSOSQL_COMMON__H */

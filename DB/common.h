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
#define uint128  __uint128_t
#define dbl      double

#define COL_TYPE_NONE         0
#define COL_TYPE_INT          1
#define COL_TYPE_LONG         2
#define COL_TYPE_STRING       3
#define COL_TYPE_FLOAT        4
#define COL_TYPE_U128         5
#define COL_TYPE_LUAO         6
#define COL_TYPE_FUNC         7
#define COL_TYPE_BOOL         8
#define COL_TYPE_CNAME        9
#define COL_TYPE_ERR         10

#define C_IS_N(ctype)    (ctype == COL_TYPE_NONE)
#define C_IS_I(ctype)    (ctype == COL_TYPE_INT)
#define C_IS_L(ctype)    (ctype == COL_TYPE_LONG)
#define C_IS_S(ctype)    (ctype == COL_TYPE_STRING)
#define C_IS_F(ctype)    (ctype == COL_TYPE_FLOAT)
#define C_IS_X(ctype)    (ctype == COL_TYPE_U128)
#define C_IS_P(ctype)    (ctype == COL_TYPE_FUNC)
#define C_IS_O(ctype)    (ctype == COL_TYPE_LUAO)
#define C_IS_B(ctype)    (ctype == COL_TYPE_BOOL)
#define C_IS_C(ctype)    (ctype == COL_TYPE_CNAME)
#define C_IS_E(ctype)    (ctype == COL_TYPE_ERR)
#define C_IS_NUM(ctype) (C_IS_I(ctype) || C_IS_L(ctype) || C_IS_X(ctype))

#define LUA_SEL_FUNC INT_MIN
#define IS_LSF(cmatch) (cmatch == LUA_SEL_FUNC)

#define PTR_SIZE    sizeof(char *)
#define USHORT_SIZE sizeof(unsigned short)
#define UINT_SIZE   sizeof(unsigned int)
#define ULONG_SIZE  sizeof(unsigned long)
#define U128_SIZE   sizeof(__uint128_t)

// Relational Lookup Types
#define SQL_ERR_LKP         0 
#define SQL_SINGLE_LKP      1
#define SQL_RANGE_LKP       2
#define SQL_IN_LKP          3
#define SQL_SINGLE_FK_LKP   4

#define NUM_ACCESS_TYPES 2 /* CREATE TABLE AS [SELECT,SCAN] */

#define INIT_MAX_NUM_TABLES         64
#define INIT_MAX_NUM_INDICES        64

//TODO make the next 3 MAX_* dynamic
#define MAX_JOIN_INDXS         30 /* CAREFUL: tied to logic in qo.c */
#define MAX_JOIN_COLS         128
#define MAX_ORDER_BY_COLS      16

#define INDEX_DELIM     "index"
#define LRUINDEX_DELIM  "lru"
#define LFUINDEX_DELIM  "lfu"

#define INCR(x)     {x = x + 1;}
#define INCRBY(x,y) {x = x + y;}
#define DECR(x)     {x = x - 1;}
#define DECRBY(x,y) {x = x - y;}

#define UETYPE_ERR  0
#define UETYPE_INT  1 /* also LONG */
#define UETYPE_U128 2
#define UETYPE_FLT  3

#define OBY_FREE_NONE 0
#define OBY_FREE_ROBJ 1
#define OBY_FREE_AOBJ 2

#define CONSTRAINT_NONE   0
#define CONSTRAINT_UNIQUE 1
#define UNIQ(cnstr) (cnstr == CONSTRAINT_UNIQUE)

#define OREDIS (server.alc.OutputMode == OUTPUT_PURE_REDIS)
#define EREDIS (server.alc.OutputMode == OUTPUT_EMBEDDED)
#define LREDIS (server.alc.OutputMode == OUTPUT_LUA)

#define FK_RQ(wtype) !(wtype == SQL_SINGLE_FK_LKP)

#define FLOAT_FMT "%.10g"

#define DEBUG_BT_TYPE(prn, btr)                                               \
  prn("btr: %p INODE: %d UU: %d UL: %d LU: %d LL: %d"                         \
      " UX: %d XU: %d LX: %d XL: %d XX: %d NORM: %d"                          \
      " [UP: %d LUP: %d LLP: %d XUP: %d XLP: %d XXP: %d] OBYI: %d\n",         \
       btr, INODE(btr), UU(btr), UL(btr), LU(btr), LL(btr),                   \
       UX(btr), XU(btr), LX(btr), XL(btr), XX(btr), NORM_BT(btr),             \
       UP(btr), LUP(btr), LLP(btr), XUP(btr), XLP(btr), XXP(btr), OBYI(btr));

typedef struct twoint {
    int i; int j;
} twoint;

//TODO this is dangerous
#define STACK_STRDUP(dest, src, len) /* TODO DEPRECATE */ \
  char dest[len + 1];                                     \
  memcpy(dest, src, len);                                 \
  dest[len] = '\0';

#define P_SDS_EMT sdscatprintf(sdsempty(),

#define UPDATE_AUTO_INC(pktyp, apk)                             \
  if      (C_IS_I(pktyp) && apk.i > rt->ainc) rt->ainc = apk.i; \
  else if (C_IS_L(pktyp) && apk.l > rt->ainc) rt->ainc = apk.l; \
  else if (C_IS_X(pktyp) && apk.x > rt->ainc) rt->ainc = apk.x;

#define DEL_NODE_ON_EMPTY_RELEASE_LIST(l, ln) \
    listDelNode(l, ln);                       \
    if (!l->len) { listRelease(l); l = NULL; }

#define ASSERT_OK(x) assert(x == DICT_OK)

#define VOIDINT (void *)(long)
#define INTVOID (uint32)(ulong)

#define CLIENT_BTREE_DEBUG

#define SPLICE_128(num) {                                  \
  uint128 bu = num; char *pbu = (char *)&bu; ull ubl, ubh; \
  memcpy(&ubh, pbu + 8, 8);                                \
  memcpy(&ubl, pbu,     8);

#define SPRINTF_128(dest, dsize, num)                   \
  SPLICE_128(num)                                       \
  snprintf(dest, dsize, "%llu|%llu", ubh, ubl); }

#define DEBUG_U128(prn, num)                             \
  SPLICE_128(num)                                       \
  (*prn)("DEBUG_U128: high: %llu low: %llu", ubh, ubl); }

#define CREATE_CS_LS_LIST(lsalso)               \
  list *cmatchl = listCreate();                 \
  list *ls      = lsalso ? listCreate() : NULL;

#define RELEASE_CS_LS_LIST    \
  listRelease      (cmatchl); \
  luasellistRelease(ls);

#define CLEAR_LUA_STACK                                  \
  lua_settop(server.lua, 0);

#define CURR_ERR_CREATE_OBJ \
  server.alc.CurrError = createObject(REDIS_STRING, sdscatprintf(sdsempty(),

#define ADD_REPLY_FAILED_LUA_STRING_CMD(cmd)                 \
  addReplyErrorFormat(c, "FAILED: cmd: (%s) msg: (%s).",     \
                         cmd, lua_tostring(server.lua, -1)); \
  CLEAR_LUA_STACK

#endif /* __ALSOSQL_COMMON__H */

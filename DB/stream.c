/*
 *
 * This file implements stream parsing for Rows

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <strings.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "find.h"
#include "bt.h"
#include "parser.h"
#include "colparse.h"
#include "aobj.h"
#include "common.h"
#include "stream.h"

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

#define TWO_POW_7                 128
#define TWO_POW_14              16384
#define TWO_POW_29          536870912
#define TWO_POW_44     17592186044416
#define TWO_POW_59 576460752303423488

// the value 0 here is reserved for GHOST rows
#define COL_1BYTE_INT   1
#define COL_2BYTE_INT   2
#define COL_4BYTE_INT   4
#define COL_6BYTE_INT   8
#define COL_8BYTE_INT  16
#define COL_5BYTE_INT  32 /* NOTE: INT  ONLY - not used as bitmap */
#define COL_9BYTE_INT  32 /* NOTE: LONG ONLY - not used as bitmap */

void *row_malloc(bt *btr, int size) {
    return bt_malloc(btr, ((size + 7) / 8) * 8); /* round to 8-byte boundary */
}

// FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT FLOAT
int strToFloat(redisClient *c, char *start, uint32 len, float *f) {
    if (len >= 31) { addReply(c, shared.col_float_string_too_long); return -1; }
    if (!len) return 0;
    char buf[32]; memcpy(buf, start, len); buf[len] = '\0';
    *f       = (float)atof(buf);       /* OK: DELIM: \0 */
    return 4;
}
int cr8FColFromStr(cli *c, char *start, uint32 len, float *col) {
    return strToFloat(c, start, len, col);
}
void writeFloatCol(uchar **row, bool fflag, float fcol) {
    if (!fflag) return;
    memcpy(*row, &fcol, 4);
    *row  = *row + 4;
}
float streamFloatToFloat(uchar *data, uint32 *clen) {
    float val; if (clen) *clen  = 4; memcpy(&val, data, 4); return val;
}

// LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU 
inline uchar getLruSflag() { return COL_4BYTE_INT; }
inline int cLRUcol(ulong l, uchar *sflag, ulong *col) { // updateLRU (UPDATE_1)
    *sflag = getLruSflag(); *col = (l * 8) + 4; return 4; /* COL_4BYTE_INT */
}
inline uint32 streamLRUToUInt(uchar *data) {
    ulong val = (*(uint32 *)data);
    val -= 4; val /= 8; return val;                       /* COL_4BYTE_INT */
}
inline void overwriteLRUcol(uchar *row, ulong icol) {
    icol = (icol * 8) + 4; memcpy(row, &icol, 4);         /* COL_4BYTE_INT */
}

// LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU LFU
inline uchar getLfuSflag() { return COL_8BYTE_INT; }
inline int cLFUcol(ulong l, uchar *sflag, ulong *col) { // updateLFU (UPDATE_1)
    *sflag = getLfuSflag(); *col = (l * 32) + 16; return 8; /* COL_8BYTE_INT */
}
inline ulong streamLFUToULong(uchar *data) {
    ulong val = (*(uint32 *)data);
    val -= 16; val /= 32; return val;                       /* COL_8BYTE_INT */
}
inline void overwriteLFUcol(uchar *row, ulong icol) {
    icol = (icol * 32) + 16; memcpy(row, &icol, 8);         /* COL_8BYTE_INT */
}

// INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG 
int getCSize(ulong l, bool isi) {
    if          (l < TWO_POW_7)  return 1;
    else if     (l < TWO_POW_14) return 2;
    else if     (l < TWO_POW_29) return 4;
    else {
        if      (isi)            return 5;
        else if (l < TWO_POW_44) return 6;
        else if (l < TWO_POW_59) return 8;
        else                     return 9;
    }
}


int cIcol(ulong l, uchar *sflag, ulong *col, bool isi) { // 0 -> GHOST row
    if        (l < TWO_POW_7) {
        if (sflag) *sflag = COL_1BYTE_INT; *col = (l * 2) + 1;       return 1;
    } else if (l < TWO_POW_14) {
        if (sflag) *sflag = COL_2BYTE_INT; *col = (l * 4) + 2;       return 2;
    } else if (l < TWO_POW_29) {
        if (sflag) *sflag = COL_4BYTE_INT; *col = (l * 8) + 4;       return 4;
    } else {
        if (isi) {                                                   /* INT */
            if (sflag) *sflag = COL_5BYTE_INT; *col = l;             return 5;
        } else if (l < TWO_POW_44) {                                /* LONG */
            if (sflag) *sflag = COL_6BYTE_INT; *col = (l * 16) + 8;  return 6;
        } else if (l < TWO_POW_59) {
            if (sflag) *sflag = COL_8BYTE_INT; *col = (l * 32) + 16; return 8;
        } else {
            if (sflag) *sflag = COL_9BYTE_INT; *col = l;             return 9;
        }
    }
}
static void wUCol(uchar **row, uchar sflag, ulong icol, bool isi) {
    if (!sflag) return;
    if        (sflag == COL_1BYTE_INT) {
        **row = (char)icol;     INCR(*row);
    } else if (sflag == COL_2BYTE_INT) {
        memcpy(*row, &icol, 2); INCRBY(*row, 2);
    } else if (sflag == COL_4BYTE_INT) {
        memcpy(*row, &icol, 4); INCRBY(*row, 4);
    } else {
        if (isi) {                                   /* INT */
            **row = COL_5BYTE_INT;  INCR(*row);
            memcpy(*row, &icol, 4); INCRBY(*row, 4);
        } else if (sflag == COL_6BYTE_INT) {         /* LONG */
            memcpy(*row, &icol, 6); INCRBY(*row, 6);
        } else if (sflag == COL_8BYTE_INT) {
            memcpy(*row, &icol, 8); INCRBY(*row, 8);
        } else {
            **row = COL_9BYTE_INT;  INCR(*row);
            memcpy(*row, &icol, 8); INCRBY(*row, 8);
        }
    }
}
static ulong sI2I(uchar *data, uint32 *clen, bool isi) {
    ulong  val = 0;
    uchar  b1  = *data;
    if (b1 & COL_1BYTE_INT) {
        if (clen) *clen  = 1;
        val = (*(uchar *)data);    val -= 1; val /= 2;
    } else if (b1 & COL_2BYTE_INT) {
        if (clen) *clen  = 2;
        val = (*(ushort16 *)data); val -= 2; val /= 4;
    } else if (b1 & COL_4BYTE_INT) {
        if (clen) *clen  = 4;
        val = (*(uint32 *)data);   val -= 4; val /= 8;
    } else {
        if (isi) { /* INT -> COL_5BYTE_INT */
            if (clen) *clen  = 5;
            data++; val = (*(uint32 *)data);
        } else if (b1 & COL_6BYTE_INT) {
            if (clen) *clen  = 6;
            memcpy(&val, data, 6);     val -= 8; val /= 16;
        } else if (b1 & COL_8BYTE_INT) {
            if (clen) *clen  = 8;
            val    = (*(ulong *)data); val -= 16; val /= 32;
        } else { /* LONG -> COL_9BYTE_INT */
            if (clen) *clen  = 9;
            data++; val = (*(ulong *)data);
        }
    }
    return val;
}
static bool strToULong(cli *c, char *start, uint32 len, ulong *l, bool isi) {
    if (len >= 31) { addReply(c, shared.col_uint_string_too_long); return 0; }
    char buf[32]; memcpy(buf, start, len); buf[len] = '\0';
    ulong m  = strtoul(buf, NULL, 10); /* OK: DELIM: \0 */
    if (isi && m >= TWO_POW_32) { addReply(c, shared.u2big); return 0; }
    *l       = m;
    return 1;
}
int cr8Icol(ulong l, uchar *sflag, ulong *col) {
    return cIcol(l, sflag, col, 1);
}
int cr8Lcol(ulong l, uchar *sflag, ulong *col) {
    return cIcol(l, sflag, col, 0);
}
int cr8IcolFromStr(cli   *c,     char  *start, uint32 len,
                      uchar *sflag, ulong *col) {
    if (!len) { *sflag = 0; *col = 0;       return 0; }
    if (!strToULong(c, start, len, col, 1)) return -1;
    return cr8Icol(*col, sflag, col);
}
int cr8LcolFromStr(cli *c, char  *start, uint32 len, uchar *sflag, ulong *col) {
    if (!len) { *sflag = 0; *col = 0;       return 0; }
    if (!strToULong(c, start, len, col, 0)) return -1;
    return cr8Lcol(*col, sflag, col);
}
void writeUIntCol(uchar **row,  uchar sflag, ulong icol) {
    wUCol(row, sflag, icol, 1);
}
void writeULongCol(uchar **row, uchar sflag, ulong icol) {
    wUCol(row, sflag, icol, 0);
}
uint32 streamIntToUInt(uchar *data,   uint32 *clen) {
    return (uint32)sI2I(data, clen, 1);
}
ulong  streamLongToULong(uchar *data, uint32 *clen) {
    return sI2I(data, clen, 0);
}

// STREAM_U128_COL STREAM_U128_COL STREAM_U128_COL STREAM_U128_COL
//TODO U128's can be packed as 2 StreamUlongs - probably not needed
void writeU128Col(uchar **row, uint128 xcol) {
    memcpy(*row, &xcol, 16); INCRBY(*row, 16);
}
int cr8XcolFromStr(cli *c, char *start, uint32 len, uint128 *col) {
    if (!len) { *col = 0; return 0; }
    if (!parseU128n(start, len, col)) {
        addReply(c, shared.u128_parse); return -1;
    }
    return 16;
}
uint128 streamToU128(uchar *data, uint32 *clen) {
    if (clen) *clen  = 16;
    uint128 val = (*(uint128 *)data); return val;
}
int cr8Xcol(uint128 x, uint128 *col) { *col = x; return 16; }

// LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ LUAOBJ
#define DEBUG_WRITE_LUAOBJ                                          \
  printf("writeLuaObjCol: tname: %s cname: %s Lua: (%s) apk: ", \
          rt->name, rt->col[cmatch].name, luac); dumpAobj(printf, apk);

static sds getLuaTblName(int tmatch, int cmatch) {
    r_tbl_t *rt = &Tbl[tmatch];
    return sdscatprintf(sdsempty(), "%s.%s.%s", LUA_OBJ_TABLE,
                                     rt->name, rt->col[cmatch].name);
}
void pushLuaVar(int tmatch, icol_t ic, aobj *apk) {
    r_tbl_t *rt  = &Tbl[tmatch];
    lua_getglobal (server.lua, LUA_OBJ_TABLE);
    lua_pushstring(server.lua, rt->name);
    lua_gettable  (server.lua, -2); lua_remove(server.lua, -2);
    lua_pushstring(server.lua, rt->col[ic.cmatch].name);
    lua_gettable  (server.lua, -2); lua_remove(server.lua, -2);
    pushAobjLua(apk, apk->type);
    lua_gettable  (server.lua, -2); lua_remove(server.lua, -2);
    if (ic.nlo) {
        for (uint32 i = 0; i < ic.nlo; i++) {
            printf("pushLuaVar: pushing: ic.lo[%d]: %s\n", i, ic.lo[i]);
            lua_pushstring(server.lua, ic.lo[i]);
            lua_gettable  (server.lua, -2); lua_remove    (server.lua, -2);
        }
    }
}
static bool createTableIfNonExistent(cli *c, int tmatch, int cmatch, sds tbl) {
    r_tbl_t *rt  = &Tbl[tmatch];
    bool     ret = 1;
    CLEAR_LUA_STACK
    lua_getglobal(server.lua, tbl);
    int t = lua_type(server.lua, 1);
    if (t == LUA_TNIL) { 
        CLEAR_LUA_STACK
        lua_getfield(server.lua, LUA_GLOBALSINDEX, "create_nested_table");
        lua_pushstring(server.lua, LUA_OBJ_TABLE);
        lua_pushstring(server.lua, rt->name);
        lua_pushstring(server.lua, rt->col[cmatch].name);
        int r = lua_pcall(server.lua, 3, 0, 0);
        if (r) { ret = 0;
            addReplyErrorFormat(c,
                             "Error running script (create_nested_table): %s\n",
                                lua_tostring(server.lua, -1));
        }
    } else assert (t == LUA_TTABLE);
    CLEAR_LUA_STACK
    return ret;
}
bool writeLuaObjCol(cli *c,    aobj   *apk, int tmatch, int cmatch,
                    char *val, uint32  vlen) {
    uint32  nlen;
    r_tbl_t *rt   = &Tbl[tmatch];
    char    *xcpd = new_unescaped(val, '\'', vlen, &nlen); if (!xcpd) return 1;
    sds      luac = sdsnewlen(xcpd, nlen);
    CLEAR_LUA_STACK                                          DEBUG_WRITE_LUAOBJ
    sds      tbl  = getLuaTblName(tmatch, cmatch);       // FREE 133
    bool     r    = createTableIfNonExistent(c, tmatch, cmatch, tbl);
    sdsfree(tbl); if (!r) return 0;                      // FREED 133
    CLEAR_LUA_STACK
    lua_getfield(server.lua, LUA_GLOBALSINDEX, "luaobj_assign");
    lua_pushstring(server.lua, LUA_OBJ_TABLE);
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[cmatch].name);
    pushAobjLua(apk, apk->type);
    lua_pushstring(server.lua, luac);
    int ret = lua_pcall(server.lua, 5, 0, 0);
    if (ret) {
        addReplyErrorFormat(c, "Error running script (luaobj_assign): %s\n",
                                lua_tostring(server.lua, -1)); CLEAR_LUA_STACK
    } else { //TODO this makes N lua calls, it should make 1 and Lua loops
        ci_t *ci = dictFetchValue(rt->cdict, rt->col[cmatch].name);
        assert(ci);
        printf("writeLuaObjCol: ci.ilist.len: %d\n", ci->ilist->len);
        listIter *li = listGetIterator(ci->ilist, AL_START_HEAD); listNode *ln;
        while((ln = listNext(li))) {
            int      imatch = (int)(long)ln->value; 
            r_ind_t *ri     = &Index[imatch];
            printf("ci.ilist: imatch: %d lo[0]: %s\n", imatch, ri->icol.lo[0]);
            CLEAR_LUA_STACK
            lua_getfield(server.lua, LUA_GLOBALSINDEX, "indexLORfield");
            lua_pushstring(server.lua, rt->name);
            lua_pushstring(server.lua, rt->col[cmatch].name);
            pushAobjLua(apk, apk->type);
            int argc = 3;
            for (uint32 i = 0; i < ri->icol.nlo; i++) {
                lua_pushstring(server.lua, ri->icol.lo[i]); argc++;
            }
            ret = lua_pcall(server.lua, argc, 0, 0);
            if (ret) {
                addReplyErrorFormat(c, 
                                  "Error running script (indexLORfield): %s\n",
                                lua_tostring(server.lua, -1)); break;
            }
        } listReleaseIterator(li);
        CLEAR_LUA_STACK
    }
    return ret ? 0 : 1;
}

/* COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE */
// INDEX_COMP INDEX_COMP INDEX_COMP INDEX_COMP INDEX_COMP INDEX_COMP INDEX_COMP
int ulongCmp(void *s1, void *s2) {
    ulong l1  = (ulong)s1; ulong l2  = (ulong)s2;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int uintCmp(void *s1, void *s2) { return ulongCmp(s1, s2); }
int u128Cmp(void *s1, void *s2) {
    uint128 x1, x2; memcpy(&x1, s1, 16); memcpy(&x2, s2, 16);
    return x1 == x2 ? 0 : (x1 > x2) ? 1 : -1;
}
// OTHER_BT_COMP OTHER_BT_COMP OTHER_BT_COMP OTHER_BT_COMP OTHER_BT_COMP
int uuCmp(void *s1, void *s2) { //TODO can be done w/ bit-shifting
    return (int)(((long)s1 / UINT_MAX) - ((long)s2 / UINT_MAX));
}
static inline int UCmp(void *s1, void *s2) { //struct: first arg is UINT
    ulk  *ul1 = (ulk *)s1; ulk  *ul2 = (ulk *)s2;
    long  l1  = ul1->key;  long  l2  = ul2->key;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int ulCmp(void *s1, void *s2) { return UCmp(s1, s2); }
int uxCmp(void *s1, void *s2) { return UCmp(s1, s2); }
static inline int LCmp(void *s1, void *s2) { // struct: first arg is ULONG
    luk   *lu1 = (luk *)s1; luk   *lu2 = (luk *)s2;
    ulong  l1  = lu1->key;  ulong  l2  = lu2->key;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int luCmp(void *s1, void *s2) { return LCmp(s1, s2); }
int llCmp(void *s1, void *s2) { return LCmp(s1, s2); }
int lxCmp(void *s1, void *s2) { return LCmp(s1, s2); }
static inline int XCmp(void *s1, void *s2) { // struct: first arg is U128
    xuk     *xu1 = (xuk *)s1; xuk     *xu2 = (xuk *)s2;
    uint128  x1  = xu1->key;  uint128  x2  = xu2->key;
    return x1 == x2 ? 0 : (x1 > x2) ? 1 : -1;
}
int xuCmp(void *s1, void *s2) { return XCmp(s1, s2); }
int xlCmp(void *s1, void *s2) { return XCmp(s1, s2); }
int xxCmp(void *s1, void *s2) { return XCmp(s1, s2); }

// PK_COMP PK_COMP PK_COMP PK_COMP PK_COMP PK_COMP PK_COMP PK_COMP PK_COMP
static inline uchar getSflag(uchar b1) {
    return (b1 & 1) ? 1 : 0;
}
static inline uchar *getTString(uchar *s, uint32 *slen) {
    *slen = ((uchar)*s / 2);    s++;    return s;
}
static inline uchar *getString(uchar *s, uint32 *slen) {
    *slen = *((uint32 *)s) / 2; s += 4; return s;
}

static bool cr8BTKInt(aobj *akey, uint32 *ksize, uchar *btkey) {
    uchar sflag;
    ulong l = (ulong)akey->i;
    *ksize  = cr8Icol(l, &sflag, &l);
    if (l >= TWO_POW_32) return 0;
    writeUIntCol(&btkey, sflag, l); return 1;
}
static void cr8BTKLong(aobj *akey, uint32 *ksize, uchar *btkey) {
    uchar sflag;
    ulong l = akey->l;
    *ksize  = cr8Lcol(l, &sflag, &l);
    writeULongCol(&btkey, sflag, l);
}
static void cr8BTKU128(aobj *akey, uint32 *ksize, uchar *btkey) {
    uint128 x = akey->x; *ksize = cr8Xcol(x, &x); writeU128Col(&btkey, x);
}
static void cr8BTKFloat(aobj *akey, uint32 *ksize, uchar *btkey) {
    writeFloatCol(&btkey, 1, akey->f); *ksize = 4;
}
int btIntCmp(void *a, void *b) {                        //printf("btIntCmp\n");
    uint32 key1 = streamIntToUInt(a, NULL);
    uint32 key2 = streamIntToUInt(b, NULL);
    return key1 == key2 ? 0 : (key1 > key2) ? 1 : -1;
}
int btLongCmp(void *a, void *b) {                      //printf("btLongCmp\n");
    ulong key1 = streamLongToULong(a, NULL);
    ulong key2 = streamLongToULong(b, NULL);
    return key1 == key2 ? 0 : (key1 > key2) ? 1 : -1;
}
int btU128Cmp(void *a, void *b) {                      //printf("btU128Cmp\n");
    uint128  x1 = *((uint128 *)a);
    uint128  x2 = *((uint128 *)b);
    return x1 == x2 ? 0 : (x1 > x2) ? 1 : -1;
}
int btFloatCmp(void *a, void *b) {                    //printf("btFloatCmp\n");
    float key1 = streamFloatToFloat(a, NULL);
    float key2 = streamFloatToFloat(b, NULL);
    float f    = key1 - key2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
int btTextCmp(void *a, void *b) {                      //printf("btTextCmp\n");
    uint32 slen1, slen2;
    uchar  *s1     = (uchar *)a;
    uchar  *s2     = (uchar *)b;
    s1 = (getSflag(*s1)) ? getTString(s1, &slen1) : getString( s1, &slen1);
    s2 = (getSflag(*s2)) ? getTString(s2, &slen2) : getString( s2, &slen2);
    if (slen1 == slen2) return strncmp((char *)s1, (char *)s2, slen1);
    else {
        int i   = (slen1 < slen2) ? slen1 : slen2;
        int ret = strncmp((char *)s1, (char *)s2, i); 
        return (ret == 0) ? ((slen1 < slen2) ? -1 : 1) : ret;
    }
}

#define BTK_BSIZE 2048
static uchar BTKeyBuffer[BTK_BSIZE]; /* avoid malloc()s */
static ulk UL_BTKeyPtr; static luk LU_BTKeyPtr; static llk LL_BTKeyPtr;
static xxk XX_BTKeyPtr; static xlk XL_BTKeyPtr; static lxk LX_BTKeyPtr;
static uxk UX_BTKeyPtr; static xuk XU_BTKeyPtr;

void destroyBTKey(char *btkey, bool med) { if (med) free(btkey);/* FREED 033 */}

#define OBT_CR8_BTK(btkeyptr, aobjpart)            \
  { btkeyptr.key = akey->aobjpart; return (char *)&btkeyptr; }

char *createBTKey(aobj *akey, bool *med, uint32 *ksize, bt *btr) {
    *med   = 0; *ksize = VOIDSIZE;
    if      INODE_I(btr) return (char *) (long) akey->i;
    else if INODE_L(btr) return (char *)        akey->l;
    else if INODE_X(btr) return (char *)       &akey->x;// 2 big -> pass ref
    else if UU     (btr) return (char *)((long)akey->i * UINT_MAX);
    else if UL     (btr) OBT_CR8_BTK(UL_BTKeyPtr, i)
    else if LU     (btr) OBT_CR8_BTK(LU_BTKeyPtr, l)
    else if LL     (btr) OBT_CR8_BTK(LL_BTKeyPtr, l)
    else if UX     (btr) OBT_CR8_BTK(UX_BTKeyPtr, i)
    else if XU     (btr) OBT_CR8_BTK(XU_BTKeyPtr, x)
    else if LX     (btr) OBT_CR8_BTK(LX_BTKeyPtr, l)
    else if XL     (btr) OBT_CR8_BTK(XL_BTKeyPtr, x)
    else if XX     (btr) OBT_CR8_BTK(XX_BTKeyPtr, x)
    int     ktype = btr->s.ktype;
    uchar  *btkey = BTKeyBuffer;
    if        (C_IS_S(ktype)) {
        uchar *key;
        if (akey->len < TWO_POW_7) { /* tiny STRING */
            *ksize     = akey->len + 1;
            if (*ksize >= BTK_BSIZE) { *med = 1; btkey = malloc(*ksize); }//F033
            *btkey     = (char)(uint32)(akey->len * 2 + 1); /* KLEN b(1)*/
            key        = btkey + 1;
        } else {                     /* STRING */
            *ksize     = akey->len + 4;
            if (*ksize >= BTK_BSIZE) { *med = 1; btkey = malloc(*ksize); }//F033
            uint32 len = (uint32)(akey->len * 2);           /* KLEN b(0)*/
            memcpy(btkey, &len, 4);
            key        = btkey + 4;
        }
        memcpy(key, akey->s, akey->len); /* after LEN, copy raw STRING */
    } else if (C_IS_L(ktype))        cr8BTKLong (akey, ksize, btkey);
      else if (C_IS_X(ktype))        cr8BTKU128 (akey, ksize, btkey);
      else if (C_IS_F(ktype))        cr8BTKFloat(akey, ksize, btkey);
      else if (C_IS_I(ktype)) { if (!cr8BTKInt(akey, ksize, btkey)) return NULL;
    }
    return (char *)btkey;
}
static uint32 skipToVal(uchar **stream, uchar ktype) { //printf("skipToVal\n");
    uint32  klen  = 0;
    if      (C_IS_I(ktype)) streamIntToUInt(   *stream, &klen);
    else if (C_IS_L(ktype)) streamLongToULong( *stream, &klen);
    else if (C_IS_X(ktype)) streamToU128     ( *stream, &klen);
    else if (C_IS_F(ktype)) streamFloatToFloat(*stream, &klen);
    else {
        if (getSflag(**stream)) { getTString(*stream, &klen); klen++; }
        else                    { getString (*stream, &klen); klen += 4; }
    }
    *stream += klen;
    return klen;
}

#define DEBUG_PARSE_STREAM                                                    \
if (!server.loading)                                                          \
printf("parseStream: %p btr: %p ", stream, btr); DEBUG_BT_TYPE(printf, btr);

uchar *parseStream(uchar *stream, bt *btr) {               //DEBUG_PARSE_STREAM
    if     (!stream || INODE(btr)) return NULL;
    else if UU      (btr)          return (uchar *)((long)stream % UINT_MAX);
    else if UP      (btr)          return (uchar *)(*(ulk *)(stream)).val; 
    else if LUP     (btr)          return (uchar *)(long)(*(luk *)(stream)).val;
    else if LLP     (btr)          return (uchar *)(*(llk *)(stream)).val; 
    else if XUP     (btr)          return (uchar *)(long)(*(xuk *)(stream)).val;
    else if XLP     (btr)          return (uchar *)(*(xlk *)(stream)).val; 
    else if XXP     (btr)          return (uchar *)(long)(*(xxk *)(stream)).val;
    else if OTHER_BT(btr)          return stream;
    skipToVal(&stream, btr->s.ktype);
    if      (btr->s.btype == BTREE_TABLE)   return stream;
    else if (btr->s.btype == BTREE_INODE)   return NULL;
    else                  /* BTREE_INDEX */ return *((uchar **)stream);
}
#define OBT_CONV2STREAM(t, aobjpart, cast) \
  {  key->type = key->enc = t;  key->aobjpart = (*(cast *)(stream)).key; }

void convertStream2Key(uchar *stream, aobj *key, bt *btr) {
    initAobj(key); key->empty = 0;
    if        INODE_I(btr) {
        key->type = key->enc = COL_TYPE_INT;
        key->i    = INTVOID stream;
    } else if INODE_L(btr) {
        key->type = key->enc = COL_TYPE_LONG; key->l    = (ulong)stream;
    } else if INODE_X(btr) { 
        key->type = key->enc = COL_TYPE_U128; memcpy(&key->x, stream, 16);
    } else if UU     (btr) {
        key->type = key->enc = COL_TYPE_INT;
        key->i    = (uint32)((long)stream / UINT_MAX);
    } else if UL     (btr) OBT_CONV2STREAM(COL_TYPE_INT,  i, ulk)
      else if LU     (btr) OBT_CONV2STREAM(COL_TYPE_LONG, l, luk)
      else if LL     (btr) OBT_CONV2STREAM(COL_TYPE_LONG, l, llk)
      else if UX     (btr) OBT_CONV2STREAM(COL_TYPE_INT,  i, uxk)
      else if XU     (btr) OBT_CONV2STREAM(COL_TYPE_U128, x, xuk)
      else if LX     (btr) OBT_CONV2STREAM(COL_TYPE_LONG, l, lxk)
      else if XL     (btr) OBT_CONV2STREAM(COL_TYPE_U128, x, xlk)
      else if XX     (btr) OBT_CONV2STREAM(COL_TYPE_U128, x, xxk)
      else { /* NORM_BT */
        int ktype = btr->s.ktype;
        if        (C_IS_I(ktype)) {
            key->type = key->enc = COL_TYPE_INT;
            key->i    = streamIntToUInt(stream, NULL);
        } else if (C_IS_L(ktype)) {
            key->type = key->enc = COL_TYPE_LONG;
            key->l    = streamLongToULong(stream, NULL);
        } else if (C_IS_X(ktype)) {
            key->type = key->enc = COL_TYPE_U128;
            key->x    = streamToU128(stream, NULL);
        } else if (C_IS_F(ktype)) {
            key->type = key->enc = COL_TYPE_FLOAT;
            key->f    = streamFloatToFloat(stream, NULL);
        } else { /* COL_TYPE_STRING */
            if (getSflag(*stream)) {  // tiny STRING
                key->type = key->enc = COL_TYPE_STRING;
                key->s    = (char *)getTString(stream, &key->len);
            } else {                  // STRING
                key->type = key->enc = COL_TYPE_STRING;
                key->s    = (char *)getString(stream, &key->len);
            }
        }
    }
}
#define DEBUG_CREATE_STREAM                                      \
  printf("createStream: size: %u klen: %d vlen: %d btype: %d\n", \
          *size, klen, vlen, btr->s.btype);
#define DEBUG_DESTROY_STREAM \
  printf("destroyStream: size: %u btr: %p\n", size, btr);

static uint32 getStreamVlen(bt *btr, uchar *stream) {
    if      (btr->s.btype == BTREE_TABLE)   return getRowMallocSize(stream);
    else if (btr->s.btype == BTREE_INODE)   return 0;
    else                  /* BTREE_INDEX */ return sizeof(void *);
}
uint32 getStreamMallocSize(bt *btr, uchar *stream) {
    if (INODE(btr) || OTHER_BT(btr)) return 0;
    int size  = skipToVal(&stream, btr->s.ktype) + getStreamVlen(btr, stream);
    return ((size + 7) / 8) * 8; /* round to 8-byte boundary */
}
uint32 getStreamRowSize(bt *btr, uchar *stream) { /* for rdbSaveAllRows() */
    if NORM_BT(btr) {
        return skipToVal(&stream, btr->s.ktype) + getStreamVlen(btr, stream);
    } else {
        if      INODE(btr) return 0;
        //TODO, this can be an array
        else if UU   (btr) return UU_SIZE;
        else if UL   (btr) return UL_SIZE; else if LU   (btr) return LU_SIZE;
        else if LL   (btr) return LL_SIZE;
        else if UX   (btr) return UX_SIZE; else if XU   (btr) return XU_SIZE;
        else if LX   (btr) return LX_SIZE; else if XL   (btr) return XL_SIZE;
        else if XX   (btr) return XX_SIZE;
        assert(!"getStreamRowSize ERROR"); return -1;
    }
}

static ulk UL_StreamPtr; static luk LU_StreamPtr; static llk LL_StreamPtr;
static xxk XX_StreamPtr; static xlk XL_StreamPtr; static lxk LX_StreamPtr;
static uxk UX_StreamPtr; static xuk XU_StreamPtr;
#define OBT_CR8_STRM(tcast, sptr, vcast)        \
  { tcast *ul = (tcast *)btkey;                 \
    sptr.key  = ul->key; sptr.val = (vcast)val; \
    return &sptr; }
#define XOBT_CR8_STRM(tcast, sptr)                       \
  { tcast *ux = (tcast *)btkey;                          \
    sptr.key  = ux->key; sptr.val = ((tcast *)val)->val; \
    return &sptr; }

static void *OBT_createStream(bt *btr, void *val, char *btkey) {
    //printf("OBT_createStream\n"); DEBUG_BT_TYPE(printf, btr);
    if (OBYI(btr) || MCI_UNIQ(btr)) {
        if      UU(btr) return (void *)((long)btkey + (long)val); /* merge */
        else if UL(btr) XOBT_CR8_STRM(ulk, UL_StreamPtr)
        else if LU(btr) XOBT_CR8_STRM(luk, LU_StreamPtr)
        else if LL(btr) XOBT_CR8_STRM(llk, LL_StreamPtr)
        else if UX(btr) XOBT_CR8_STRM(uxk, UX_StreamPtr)
        else if XU(btr) XOBT_CR8_STRM(xuk, XU_StreamPtr)
        else if LX(btr) XOBT_CR8_STRM(lxk, LX_StreamPtr)
        else if XL(btr) XOBT_CR8_STRM(xlk, XL_StreamPtr)
        else if XX(btr) XOBT_CR8_STRM(xxk, XX_StreamPtr)
        assert(!"OBT_createStream OBYI error"); return NULL;
    }
    if       UP(btr) OBT_CR8_STRM (ulk, UL_StreamPtr, ulong)
    else if LUP(btr) OBT_CR8_STRM (luk, LU_StreamPtr, uint32)
    else if LLP(btr) OBT_CR8_STRM (llk, LL_StreamPtr, ulong)

    else if UU(btr) return (void *)((long)btkey + (long)val); /* merge */
    else if UL(btr) XOBT_CR8_STRM(ulk, UL_StreamPtr)
    else if LU(btr) XOBT_CR8_STRM(luk, LU_StreamPtr)
    else if LL(btr) XOBT_CR8_STRM(llk, LL_StreamPtr)
    else if UX(btr) XOBT_CR8_STRM(uxk, UX_StreamPtr)
    else if XU(btr) XOBT_CR8_STRM(xuk, XU_StreamPtr)
    else if LX(btr) XOBT_CR8_STRM(lxk, LX_StreamPtr)
    else if XX(btr) XOBT_CR8_STRM(xxk, XX_StreamPtr)
    else if XL(btr) { //NOTE: XL is special it can be XXP()
        xlk *xl          = (xlk *)btkey;
        XL_StreamPtr.key = xl->key;
        XL_StreamPtr.val = XXP(btr) ? (ulong)val : ((xlk *)val)->val;
        return &XL_StreamPtr;
    } else { assert(!"OBT_createStream ERROR"); return NULL; }
}
// btkey from createBTKey() - bit-packed-num or string-w-length
// row from writeRow() - [normal|hash]_row of [bit-packed-num|string-w-length]
// NOTE: rows (but NOT btkeys) can have compressed strings
// STREAM BINARY FORMAT: [btkey|row]
void *createStream(bt *btr, void *val, char *btkey, uint32 klen, uint32 *size) {
    *size = 0;                                           // DEBUG_BT_TYPE(btr);
    if      INODE(btr)    return btkey;
    else if OTHER_BT(btr) return OBT_createStream(btr, val, btkey);
    uint32  vlen      = getStreamVlen(btr, val);
    *size             = klen + vlen;                      //DEBUG_CREATE_STREAM
    char   *bt_val    = (btr->s.btype == BTREE_TABLE) ? row_malloc(btr, *size) :
                                                        bt_malloc (btr, *size);
    char   *o_bt_val  = bt_val;
    memcpy(bt_val, btkey, klen);
    bt_val           += klen;
    uchar btype = btr->s.btype;
    if        (btype == BTREE_TABLE) {
        if (val) memcpy(bt_val, val, vlen);
        else     bzero (bt_val, sizeof(void *));
    } else if (btype != BTREE_INODE) memcpy(bt_val, &val, sizeof(void *));
    return o_bt_val; /* line above is for STRING & FLOAT INDEX */
}
bool destroyStream(bt *btr, uchar *ostream) {
    if (!ostream || INODE(btr) || OTHER_BT(btr)) return 0;
    uint32 size  = getStreamMallocSize(btr, ostream);    //DEBUG_DESTROY_STREAM
    bt_free(btr, ostream, size); /* mem-bookkeeping in ibtr */
    return 1;
}

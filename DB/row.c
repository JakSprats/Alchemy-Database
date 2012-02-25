/*
 * This file implements the rows of Alsosql
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <errno.h>
#include <fenv.h>

#include "xdb_hooks.h"

#include "redis.h"
#include "lzf.h"

#include "debug.h"
#include "hash.h"
#include "find.h"
#include "sixbit.h"
#include "lru.h"
#include "lfu.h"
#include "bt.h"
#include "colparse.h"
#include "index.h"
#include "stream.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "row.h"

/* ROW TODO LIST
    1.) pass only "aobj *" not aobj (which is now a big struct)
    2.) RawCols[] is not worth the complexity, use malloc()
    3.) in updateRow() all NUM()s are already error checked in getExprType()
    5.) break up into [write_row.c, get_row.c, output_row.c, update_row.c]
*/

bool GlobalZipSwitch = 1; // can GLOBALLY turn off [lzf] compression of rows

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

// CONSTANT GLOBALS
extern char OUTPUT_DELIM;
extern char PLUS;  extern char MINUS;
extern char MULT;  extern char DIVIDE;
extern char POWER; extern char MODULO;

#define RFLAG_1BYTE_INT   1
#define RFLAG_2BYTE_INT   2
#define RFLAG_4BYTE_INT   4
#define RFLAG_SIZE_FLAG (RFLAG_1BYTE_INT + RFLAG_2BYTE_INT + RFLAG_4BYTE_INT)

#define RFLAG_6BIT_ZIP    8
#define RFLAG_LZF_ZIP    16

#define RFLAG_HASH16_ROW 32
#define RFLAG_HASH32_ROW 64
#define RFLAG_HASH_ROW (RFLAG_HASH16_ROW + RFLAG_HASH32_ROW)
#define HASH_SIZE_COMP_ADJ        1.2
#define MIN_NUM_COLS_FOR_HASH_ROW 100

//NOTE: this may be WAY too high for fast READ ops (it favors memusage totally)
#define MIN_FILL_PERC_HASH_ROW    0.25

static char *EmptyStringCol = "''";
static char *EmptyCol       = "";

typedef robj *row_outputter(bt     *btr,  void *rrow, int qcols, 
                            icol_t *ics,  aobj *apk,  int tmatch,
                            lfca_t *lfca, bool *ost);

/* PROTOYPES */
static void initAobjCol2S(aobj *a, ulong l, uint128 x, float f, int cmatch,
                          int   ktype);
static void initIntAobjFromVal  (aobj *a, uint32 i,  bool fs, int cmatch);
static void initLongAobjFromVal (aobj *a, ulong l,   bool fs, int cmatch);
static void initU128AobjFromVal (aobj *a, uint128 x, bool fs, int cmatch);
static void initFloatAobjFromVal(aobj *a, float f,   bool fs, int cmatch);
static aobj colFromUU(ulong   key, bool fs, int cmatch);
static aobj colFromUL(ulong   key, bool fs, int cmatch);
static aobj colFromLU(ulong   key, bool fs, int cmatch);
static aobj colFromLL(ulong   key, bool fs, int cmatch);
static aobj colFromXX(uint128 key, bool fs, int cmatch);
static bool evalExpr   (cli  *c, ue_t  *ue, aobj *aval, uchar ctype);
static bool evalLuaExpr(cli  *c, icol_t *ic, uc_t *uc, aobj *apk,
                        void *orow, aobj *aval);

// PROTOTYPES: LUA_SELECT_FUNCS
static void init_LO_AobjFromCmatch(aobj *a,      aobj *apk, icol_t ic,
                                   int   tmatch, bool  fs);
static void initAobjFromLuaString(lua_State *lua, aobj *a);
static void initAobjFromLuaResponse(aobj *a, icol_t *ic, int tmatch, bool fs);

/* CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW */
typedef struct cr_t {
    int     tmatch;
    int     ncols;
    uint32  rlen;
    int     cnt;
} cr_t;
typedef struct create_row_data {
    int      mcofsts;
    char    *strs;  uint32  slens;
    ulong    icols; uchar   iflags;
    uint128  xcols;
    float    fcols; bool    fflags;
    bool     empty; // for HashRows
} crd_t;
static void init_cr(cr_t *cr, crd_t *crd, int tmatch, int ncols) {
    cr->tmatch = tmatch;
    cr->ncols  = ncols;
    cr->rlen   = 0;
    cr->cnt    = 0;
    bzero(crd, sizeof(crd_t) * ncols);
}
#define INIT_CR(tmatch, ncols)     \
  cr_t cr;                         \
  crd_t crd[ncols];                \
  init_cr(&cr, crd, tmatch, ncols);

#define CZIP_NONE     0
#define CZIP_SIX      1
#define CZIP_LZF      2
typedef struct col_zip_data {
    uchar  *sixs;
    uint32  sixl;
    void   *lzf_s; /* compressed column */
    uint32  lzf_l; /* compressed column length */
    ulong   socl;  /* stream original col-len */
    uint32  lsocl; /* LEN stream original col-len */
} czd_t;
typedef struct col_zip_ctrl {
    bool    zip;
    uchar   type;
    uint32  sixn;
    uint32  lzf_n;
} cz_t;

//TODO mv to write_row.c
static bool contains_text_col(int tmatch, int ncols) {
    for (int i = 1; i < ncols; i++) {       /* sixzip needs a string */
        if (C_IS_S(Tbl[tmatch].col[i].type)) return 1;
    }
    return 0;
}

static void init_cz(cz_t *cz, cr_t *cr) {
    cz->zip  = contains_text_col(cr->tmatch, cr->ncols);
    cz->type = CZIP_NONE;
    cz->sixn = cz->lzf_n = 0;
}
static void destroy_cz(cz_t *cz, czd_t *czd) {
    for (uint32 j = 0; j < cz->sixn;  j++) free(czd[j].sixs);  /*FREED 022 */
    for (uint32 j = 0; j < cz->lzf_n; j++) free(czd[j].lzf_s); /*FREED 034 */
    cz->sixn = cz->lzf_n = 0;
}

static char *streamLZFTextToString(uchar *s, uint32 *len) {
    uint32  clen;
    uint32  oclen = streamIntToUInt(s, &clen);
    char   *buf   = malloc(oclen);                       /* FREE ME 035 */
    int     llen  = *len - clen;
    *len          = lzf_decompress(s + clen, llen, buf, oclen);
    return buf;
}
static bool lzfZipCol(int i, crd_t *crd, cz_t *cz, czd_t *czd,
                      uint32 *tlen, uint32 *mtlen) {
    INCRBY(*tlen, crd[i].slens);
    uint32 mlen  = MAX(4, crd[i].slens + 4);
    uint32 n     = cz->lzf_n;
    czd[n].lzf_s = malloc(mlen);       /* FREE ME 034 */
    czd[n].lzf_l = lzf_compress(crd[i].strs, crd[i].slens, czd[n].lzf_s, mlen);
    if (!czd[n].lzf_l) return 0;
    czd[n].lsocl = cr8Icol(crd[i].slens, NULL, &czd[n].socl);
    INCRBY(*mtlen, (czd[cz->lzf_n].lsocl + czd[cz->lzf_n].lzf_l));
    INCR(cz->lzf_n)
    return 1;
}
static uchar *writeLzfCol(uchar *row, czd_t *czd, int k) {
    memcpy(row, &czd[k].socl, czd[k].lsocl); /* orig col_len*/
    row += czd[k].lsocl;
    memcpy(row, czd[k].lzf_s, czd[k].lzf_l); /* compressed column */
    return row + czd[k].lzf_l;
}

#define DEBUG_MCOFSTS \
  for (int i = 1; i < cr->ncols; i++) \
    printf("mcofsts[%d]: %d\n", i, crd[i].mcofsts);

static bool compression_justified(uint32 tlen, uint32 mtlen) {
    //printf("compression_justified: tlen: %u mtlen: %u\n", tlen, mtlen);
    return (mtlen < tlen); //TODO add in some intelligence
}
#define COL_LOOP_IF_TEXT \
    for (int i = 1; i < cr->ncols; i++) { \
        if (C_IS_S(Tbl[cr->tmatch].col[i].type)) {

static void zipCol(cr_t *cr, crd_t *crd, cz_t *cz, czd_t *czd) {
    cz->type = CZIP_SIX;
    COL_LOOP_IF_TEXT  /* if ANY TEXT col len > 20 -> LZF */
        if (crd[i].slens > 20) { cz->type = CZIP_LZF; break; }
    }}
    if (cz->type == CZIP_LZF) {            /* ZIP LZF */
        uint32 tlen  = 0; /* sum length TEXT_cols */
        uint32 mtlen = 0; /* sum length compressed(TEXT_cols) */
        COL_LOOP_IF_TEXT
            if (!lzfZipCol(i, crd, cz, czd, &tlen, &mtlen)) {
                cz->type = CZIP_LZF; break;
            }
        }}
        if (cz->type == CZIP_LZF && !compression_justified(tlen, mtlen)) {
            cz->type = CZIP_SIX; 
        }
    }
    if (cz->type == CZIP_SIX) {            /* ZIP SIXBIT */
        COL_LOOP_IF_TEXT
            uint32  len  = crd[i].slens;
            uchar  *dest = _createSixBit(crd[i].strs, len, &len); //FREE 022
            if (!dest) { cz->type = CZIP_NONE; return; }
            czd[cz->sixn].sixs = dest; czd[cz->sixn].sixl = len; INCR(cz->sixn)
        }}
    }
    uint32 k = 0; uint32 shrunk = 0;
    for (int i = 1; i < cr->ncols; i++) { /* MOD cofsts (zipping) */
        int diff = 0;
        if (C_IS_S(Tbl[cr->tmatch].col[i].type)) {
            if        (cz->type == CZIP_SIX) {
                diff = (crd[i].slens - czd[k].sixl);
            } else if (cz->type == CZIP_LZF) {
                diff = (crd[i].slens - (czd[k].lsocl + czd[k].lzf_l));
            }
            k++;
        }
        shrunk += diff; cr->rlen -= diff; crd[i].mcofsts -= shrunk;
    }
}
// NORMALROW BIN FRMT: [ 1B |StreamUINT |NC*(1-4B) |data ... no PK, no commas]
//                     [flag|  ncols    |  cofsts  |a,b,c,...................]
// HASHROW   BIN FRMT: [ 1B |SUINT|SUINT| ht.size  |data ... no PK, no commas]
//                     [flag|rlen |ncols| hash_tbl |a,b,c,...................]
static uchar assign_rflag(uint32 rlen, uchar ztype) {
    uchar rflag;
    if (     rlen < UCHAR_MAX) rflag = RFLAG_1BYTE_INT;
    else if (rlen < USHRT_MAX) rflag = RFLAG_2BYTE_INT;
    else                       rflag = RFLAG_4BYTE_INT;
    if (     ztype == CZIP_SIX) rflag += RFLAG_6BIT_ZIP;
    else if (ztype == CZIP_LZF) rflag += RFLAG_LZF_ZIP;
    return rflag;
}
static void rawUintWriteToRow(uchar **row, uint32 val) {
    ulong icol; uchar sflag;
    cr8Icol((ulong)val, &sflag, &icol);
    writeUIntCol(row, sflag, icol);
}
static void *createRowBlob(int ncols, uchar rflag, uint32 rlen) {
    int     rcols = ncols - 1;
    //NOTE: META_LEN: [flag + ncols              +  cofsts]
    uint32  mlen  = 1 + getCSize(rcols, 1) + (ncols * rflag);
    uchar  *orow  = malloc(mlen + rlen);                 // FREEME 023
    uchar  *row   = orow; *row = rflag; row++; // WRITE rflag
    rawUintWriteToRow(&row, (ncols - 1));      // WRITE NCOLS
    return orow;
}
static int set_col_offst(uchar *row, uchar rflag, int cofst) {
    if        (rflag & RFLAG_1BYTE_INT) {
        *row = (uchar)cofst;            return 1;
    } else if (rflag & RFLAG_2BYTE_INT) {
        ushort16 m = (ushort16)cofst;
        memcpy(row, &m, USHORT_SIZE);   return USHORT_SIZE;
    } else {        /* RFLAG_4BYTE_INT */
        memcpy(row, &cofst, UINT_SIZE); return UINT_SIZE;
    }
}

static uchar *createHash16Row(cr_t *cr, crd_t *crd, uchar *rflag,
                              uint32 *mlen, uint32 msize) {
    uchar   *orow = NULL;
    ahash16 *ht16 = alc_hash16_make(2); // start small
    for (int i = 1; i < cr->ncols; i++) {
        if (!crd[i].empty) {
            uint32 hval = crd[i].mcofsts;
            if (i) hval += crd[i - 1].mcofsts * (uint32)USHRT_MAX;
            alc_hash16_insert(i, (uint32)hval, ht16);
        }
    }
    uint32 hsize = alc_hash16_size(ht16);
    //printf("RFLAG_HASH16_ROW: msize: %d hsize: %d\n", msize, hsize);
    if (msize > hsize * HASH_SIZE_COMP_ADJ) {
        *rflag               = *rflag | RFLAG_HASH16_ROW;
        *mlen                = hsize + 1 + getCSize(cr->rlen, 1) +
                                           getCSize((cr->ncols - 1), 1);
        // SERIALIZE DICT
        orow                 = malloc(*mlen + cr->rlen);   // FREEME 095
        uchar  *row          = orow; *row = *rflag; row++;     // WRITE rflag
        rawUintWriteToRow(&row, cr->rlen);                     // WRITE rlen
        rawUintWriteToRow(&row, (cr->ncols - 1));              // WRITE NCOLS
        ahash16_entry *oents = ht16->entries;
        ht16->entries        = NULL; // ht16->entries will be simulated
        memcpy(row, ht16, sizeof(ahash16)); row += sizeof(ahash16);
        ht16->entries        = oents; // set back for alc_hash16_destroy()
        memcpy(row, oents, ht16->nentries * sizeof(ahash16_entry));
    }
    alc_hash16_destroy(ht16);
    return orow;
}
static uchar *createHash32Row(cr_t *cr, crd_t *crd, uchar *rflag,
                              uint32 *mlen, uint32 msize) {
    uchar   *orow = NULL;
    ahash32 *ht32 = alc_hash32_make(2); // start small
    for (int i = 1; i < cr->ncols; i++) {
        if (!crd[i].empty) {
            long hval = crd[i].mcofsts;
            if (i) hval += crd[i - 1].mcofsts * (long)UINT_MAX;
            alc_hash32_insert(i, (ulong)hval, ht32);
        }
    }
    uint32 hsize = alc_hash32_size(ht32);
    //printf("RFLAG_HASH32_ROW: msize: %d hsize: %d\n", msize, hsize);
    if (msize > hsize * HASH_SIZE_COMP_ADJ) {
        *rflag               = *rflag | RFLAG_HASH32_ROW;
        *mlen                = hsize + 1 + getCSize(cr->rlen, 1) +
                                           getCSize((cr->ncols - 1), 1);
        // SERIALIZE DICT
        orow                 = malloc(*mlen + cr->rlen);   // FREEME 095
        uchar  *row          = orow; *row = *rflag; row++;     // WRITE rflag
        rawUintWriteToRow(&row, cr->rlen);                     // WRITE rlen
        rawUintWriteToRow(&row, (cr->ncols - 1));              // WRITE NCOLS
        ahash32_entry *oents = ht32->entries;
        ht32->entries        = NULL; // ht32->entries will be simulated
        memcpy(row, ht32, sizeof(ahash32)); row += sizeof(ahash32);
        ht32->entries        = oents; // set back for alc_hash32_destroy()
        memcpy(row, oents, ht32->nentries * sizeof(ahash32_entry));
    }
    alc_hash32_destroy(ht32);
    return orow;
}
static uchar *createHashRow(cr_t *cr, crd_t *crd, uchar *rflag, uint32 *mlen) {
    if (cr->ncols < MIN_NUM_COLS_FOR_HASH_ROW ||
        ((double)(cr->ncols * MIN_FILL_PERC_HASH_ROW) < (double)cr->cnt)) {
        return NULL;
    }
    char    sflag = *rflag & RFLAG_SIZE_FLAG;
    bool    use16 = (!(*rflag & RFLAG_4BYTE_INT) && cr->ncols < USHRT_MAX);
    uint32  msize = cr->ncols * sflag;

    return use16 ? createHash16Row(cr, crd, rflag, mlen, msize) :
                   createHash32Row(cr, crd, rflag, mlen, msize);
}

// WRITE_LUAOBJ WRITE_LUAOBJ WRITE_LUAOBJ WRITE_LUAOBJ WRITE_LUAOBJ WRITE_LUAOBJ
#define DEBUG_WRITE_LUAOBJ                                              \
  r_tbl_t *rt   = &Tbl[tmatch];                                         \
  printf("writeLuaObjCol: tname: %s cname: %s Lua: (%s) apk: ",         \
          rt->name, rt->col[cmatch].name, json); dumpAobj(printf, apk);

static bool pushSdsToLua(sds arg) { //printf("pushSdsToLua: %s\n", arg);
    if        (is_int  (arg)) {
        long l = strtol(arg, NULL, 10);
        lua_pushinteger(server.lua, l);  return 1;
    } else if (is_float(arg)) {
        double f = strtod(arg, NULL);
        lua_pushnumber (server.lua, f);  return 1;
    } else if (is_text (arg, sdslen(arg))) {
        lua_pushstring(server.lua, arg); return 1;
    } else return 0;
}
static bool writeLOC_FromFunc(cli *c,    aobj *apk,  int   tmatch, int  cmatch,
                               sds fname, char *lprn, char *rprn) {
    r_tbl_t *rt    = &Tbl[tmatch];
    int      ret   = 0;
    sds      args  = NULL;
    CLEAR_LUA_STACK
    char *func = "__queueLuaobjAssign";
    lua_getfield  (server.lua, LUA_GLOBALSINDEX, func); // PUSH FUNC
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[cmatch].name);
    pushAobjLua   (apk, apk->type);

    int  nargs = 0;
    lua_getfield(server.lua, LUA_GLOBALSINDEX, fname); // PUSH FNAME

    lprn++; SKIP_SPACES(lprn); rprn--; REV_SKIP_SPACES(rprn);
    if (lprn < (rprn + 1)) {
        sds   args = sdsnewlen(lprn, rprn - lprn + 1); // FREE 174
        char *tkn  = args;
        while (1) { bool r;
            char *nextc = get_next_insert_value_token(tkn);
            if (!nextc) {
                sds arg = sdsnew(tkn);
                r = pushSdsToLua(arg); sdsfree(arg); if (!r) goto w_locf_end;
                nargs++; break;
            }
            sds arg = sdsnewlen(tkn, (nextc - tkn));
            r = pushSdsToLua(arg); sdsfree(arg); if (!r) goto w_locf_end;
            nextc++; tkn = nextc; SKIP_SPACES(nextc) nargs++;
        }
    }
    int lret = DXDB_lua_pcall(server.lua, nargs, 1, 0); // call FNAME
    if (lret) { ADD_REPLY_FAILED_LUA_STRING_CMD(fname) goto w_locf_end; }
    lret = DXDB_lua_pcall(server.lua, 4, 0, 0);         // call FUNC
    if (lret) { ADD_REPLY_FAILED_LUA_STRING_CMD(func)  goto w_locf_end; }
    ret = 1;

w_locf_end:
    if (args) sdsfree(args);                             // FREED 174
    CLEAR_LUA_STACK
    return ret;
}
static bool runFuncForWriteLuaObjCol(cli *c, aobj *apk, int tmatch, int cmatch,
                                     sds func, sds arg1) {
    r_tbl_t *rt = &Tbl[tmatch];
    CLEAR_LUA_STACK
    lua_getfield  (server.lua, LUA_GLOBALSINDEX, func);
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[cmatch].name);
    pushAobjLua(apk, apk->type);
    lua_pushstring(server.lua, arg1);
    int ret = DXDB_lua_pcall(server.lua, 4, 0, 0);
    if (ret) { ADD_REPLY_FAILED_LUA_STRING_CMD(func) }
    CLEAR_LUA_STACK
    return ret ? 0 : 1;
}
static bool write_LOC_NotJSON(cli *c,    aobj   *apk,  int  tmatch, int cmatch,
                              char *val, uint32  vlen) {
    char *lprn = _strnchr(val, '(', vlen);
    if (lprn) {
        char  llen = vlen - (lprn - val);
        char *rprn = _strnchr(lprn, ')', llen);
        if (rprn) {
            char *endf  = lprn - 1; REV_SKIP_SPACES(endf)
            sds   fname = sdsnewlen(val, endf - val + 1); // FREE 171
            bool  isf   = luaFuncDefined(fname); int ok = 0;
            if (isf) {
                ok = writeLOC_FromFunc(c, apk, tmatch, cmatch,
                                       fname, lprn, rprn);
            }
            sdsfree(fname);                               // FREED 171
            if      (ok ==  1) return 1;
            else if (ok == -1) return 0; // else fall thru to EVAL
        }
    }
    //NOTE: FALL THRU, EVAL in LUA
    sds   luae = sdsnewlen(val, vlen); // FREE 172
    char *func = "queueLuaobjAssignEval";
    bool  ret  = runFuncForWriteLuaObjCol(c, apk, tmatch, cmatch, func, luae);
    sdsfree(luae);                     // FREED 172
    return ret;
}

static bool writeLuaObjCol(cli *c,    aobj   *apk,  int  tmatch, int cmatch,
                           char *val, uint32  vlen) {
    if (*val != '{') {
        return write_LOC_NotJSON(c, apk, tmatch, cmatch, val, vlen);
    }
    sds   json = sdsnewlen(val, vlen);   // FREE 173
    DEBUG_WRITE_LUAOBJ
    char *func = "queueLuaobjAssignJson";
    bool  ret  = runFuncForWriteLuaObjCol(c, apk, tmatch, cmatch, func, json);
    sdsfree(json);                      // FREED 173
    return ret;
}

static uchar *writeRow(cli *c, aobj *apk, int tmatch, cr_t *cr, crd_t *crd) {
    cz_t cz; czd_t czd[cr->ncols]; init_cz(&cz, cr);
    uchar *row; uint32 mlen = 0; // compiler warning
    if (!GlobalZipSwitch) cz.zip = 0;
    if (cz.zip) zipCol(cr, crd, &cz, czd);
    uchar  rflag = assign_rflag(cr->rlen, cz.type);
    uchar *orow  = createHashRow(cr, crd, &rflag, &mlen);
    if (orow) row = orow + mlen; // HASH ROW
    else {                       // NORMAL ROW
        orow = createRowBlob(cr->ncols, rflag, cr->rlen);
        row  = orow + 1 + getCSize(cr->ncols - 1, 1); // flag + ncols
        for (int i = 1; i < cr->ncols; i++) { /* WRITE cofsts[] to row */
            row += set_col_offst(row, rflag, crd[i].mcofsts);// size+=ncols*flag
        }
    }
    uint32 k = 0;
    for (int i = 1; i < cr->ncols; i++) { /* write ROW */
        uchar ctype = Tbl[cr->tmatch].col[i].type;
        if        C_IS_I(ctype) {
            writeUIntCol(&row,  crd[i].iflags, crd[i].icols);
        } else if C_IS_L(ctype) {
            writeULongCol(&row, crd[i].iflags, crd[i].icols);
        } else if C_IS_X(ctype) {
            writeU128Col(&row, crd[i].xcols);
        } else if C_IS_F(ctype) {
            writeFloatCol(&row, crd[i].fflags, crd[i].fcols);
        } else if C_IS_O(ctype) {
            DECLARE_ICOL(ic, i)
            if (!writeLuaObjCol(c, apk, tmatch, i, crd[i].strs, crd[i].slens)) {
                return NULL;
            }
        } else if C_IS_S(ctype) {
            if        (cz.type == CZIP_SIX) {
                memcpy(row, czd[k].sixs, czd[k].sixl); row += czd[k].sixl; k++;
            } else if (cz.type == CZIP_LZF) {
                row = writeLzfCol(row, czd, k);                            k++;
            } else {
                memcpy(row, crd[i].strs, crd[i].slens); row += crd[i].slens;
            }
        } else assert(!"writeRow ERROR\n");
    }
    destroy_cz(&cz, czd);
    return orow;
}
#define DEBUG_CREATE_ROW                                                       \
  if (!crd[i].empty) {                                                         \
    printf("nclen: %d rlen: %d c[%d]: %d", nclen, cr.rlen, i, crd[i].mcofsts); \
    if C_IS_NUM(ctype) {                                                       \
        printf(" iflgs: %d c: %lu", crd[i].iflags, crd[i].icols);              \
    }                                                                          \
    printf("\n");                                                              \
  }

static void insert_LRU(cr_t *cr, crd_t *crd, int tmatch, int i) {
    int nclen       = cLRUcol(getLru(tmatch), &crd[i].iflags, &(crd[i].icols));
    cr->rlen       += nclen;
    crd[i].mcofsts  = (int)cr->rlen;
}
static void insert_LFU(cr_t *cr, crd_t *crd, int i) {
    int nclen       = cLFUcol(1, &crd[i].iflags, &(crd[i].icols));
    cr->rlen       += nclen;
    crd[i].mcofsts  = (int)cr->rlen;
}
static ulk UL_CRR; static luk LU_CRR; static llk LL_CRR; static xxk XX_CRR; 
static lxk LX_CRR; static xlk XL_CRR; static uxk UX_CRR; static xuk XU_CRR;
/*NOTE: OTHER_BT rows no malloc, 'void *' of 1st COL */
static void *OBT_createRow(bt *btr, int tmatch, char *vals, twoint cofsts[]) {
    char uubuf[64];
    r_tbl_t *rt    = &Tbl[tmatch];
    int      c1len = (cofsts[1].j - cofsts[1].i);
    memcpy(uubuf, vals + cofsts[1].i, c1len); uubuf[c1len] = '\0';
    //printf("OBT_createRow:\n"); DEBUG_BT_TYPE(printf, btr)
    if        (C_IS_I(rt->col[0].type) && C_IS_I(rt->col[1].type)) {      // UU
        return (void *)strtoul(uubuf, NULL, 10); /* OK: DELIM: \0 */
    } else if (C_IS_X(rt->col[1].type)) {                           // UX,LX,XX
        uint128 *pbu = (XX(btr) ? &XX_CRR.val :
                       (LX(btr) ? &LX_CRR.val : /* UX */  &UX_CRR.val));
        bool     r   = parseU128(uubuf, pbu);
        return r ? (XX(btr) ? (void *)&XX_CRR :
                   (LX(btr) ? (void *)&LX_CRR : (void *)&UX_CRR)) : NULL;
    } else {                                                  // UL,LU,LL,XU,XL
        ulong l = strtoul(uubuf, NULL, 10); /* OK: DELIM: \0 */
        if      UL(btr) UL_CRR.val = l;
        else if LU(btr) LU_CRR.val = l; else if LL(btr) LL_CRR.val = l;
        else if XU(btr) XU_CRR.val = l; else /* XL */   XL_CRR.val = l;
        return UL(btr) ? (void *)&UL_CRR :
               LU(btr) ? (void *)&LU_CRR :    LL(btr) ? (void *)&LL_CRR :
               XU(btr) ? (void *)&XU_CRR : /* XL */    (void *)&XL_CRR;
    }
}
void *createRow(cli *c,     aobj *apk,  bt     *btr,      int tmatch,
                int  ncols, char *vals, twoint  cofsts[]) {
    r_tbl_t *rt = &Tbl[tmatch];
    if OTHER_BT(btr) return OBT_createRow(btr, tmatch, vals, cofsts);
    int k; for (k = ncols - 1; k >= 0; k--) { if (cofsts[k].i != -1) break; }
    ncols = k + 1; // starting from the right, only write FILLED columns
    if (rt->lrud && rt->lruc >= ncols) ncols = rt->lruc + 1; // up to LRUC
    if (rt->lfu  && rt->lfuc >= ncols) ncols = rt->lfuc + 1; // up to LFUC
    INIT_CR(tmatch, ncols)
    int modi = 0;
    for (int i = 1; i < cr.ncols; i++) { /* MOD cofsts (no PK,no commas,PACK) */
        if        (rt->lrud && rt->lruc == i) { //printf("insert_LRU\n");
            insert_LRU(&cr, crd, tmatch, i); modi++;
        } else if (rt->lfu  && rt->lfuc == i) { //printf("insert_LFU\n");
            insert_LFU(&cr, crd, i); modi++;
        } else {
            uchar ctype = rt->col[i].type;
            if (cofsts[i].i == -1) { //TODO do next block's logic here
                crd[i].empty = 1; // used in writeRow 2 compute hash-row's size
                if (C_IS_S(ctype) || C_IS_O(ctype)) {
                    crd[i].strs = EmptyStringCol; crd[i].slens = 2;
                } else  {
                    crd[i].strs = EmptyCol;       crd[i].slens = 0;
                }
            } else {
                crd[i].empty  = 0; cr.cnt++; 
                char *startc  = vals + cofsts[i].i;
                char *endc    = vals + cofsts[i].j;
                crd[i].strs   = startc;
                int   clen    = endc - startc;
                SKIP_SPACES(crd[i].strs)
                char *mendc   = endc;
                if (ISBLANK(*(mendc))) REV_SKIP_SPACES(mendc)
                crd[i].slens = clen - (crd[i].strs - startc) - (endc - mendc);
            }
            int nclen = 0;
            if        C_IS_S(ctype) { // dont store \' delims
                crd[i].strs++; crd[i].slens -= 2; nclen = crd[i].slens;
            } else if C_IS_O(ctype) {
                nclen = 0; // LUAOBJ !inRow (inLua)
            } else if (C_IS_I(ctype)) {
                nclen = cr8IcolFromStr(c, crd[i].strs,    crd[i].slens,
                                          &crd[i].iflags, &crd[i].icols);
                if (nclen == -1) return NULL;
            } else if (C_IS_L(ctype)) {
                nclen = cr8LcolFromStr(c, crd[i].strs,    crd[i].slens,
                                          &crd[i].iflags, &crd[i].icols);
                //TODO cr8LcolFromStr can fail to parse, right?
            } else if (C_IS_X(ctype)) {
                nclen = cr8XcolFromStr(c, crd[i].strs,    crd[i].slens,
                                          &crd[i].xcols);
                if (nclen == -1) return NULL;
            } else if (C_IS_F(ctype)) {
                nclen = cr8FColFromStr(c, crd[i].strs,    crd[i].slens,
                                          &crd[i].fcols);
                if (nclen == -1) return NULL;
                crd[i].fflags = nclen ? 1 : 0;
            } else assert(!"createRow ERROR");
            cr.rlen        += nclen;
            crd[i].mcofsts  = (int)cr.rlen;                 //DEBUG_CREATE_ROW
        }
    }
    return writeRow(c, apk, tmatch, &cr, crd);
}

// mv to get_row.c
/* GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL */
#define DEBUG_GET_RAW_COL \
printf("getRawCol: orow: %p tmatch: %d cmatch: %d fs: %d apk: ", \
        orow, tmatch, cmatch, fs); dumpAobj(printf, apk);

#define OBT_RAWC(kt, kcast, aobjpart, cf_func)              \
  { kt key = cmatch ? ((kcast *)orow)->val : apk->aobjpart; \
    return cf_func(key, fs, cmatch); }

#define OBTXY_RAWC(vt, vcast, v_cf_func, kt, kaobjpart, k_cf_func)       \
  { if (cmatch) {                                                        \
        vt key = ((vcast *)orow)->val;                                   \
        return v_cf_func(key, fs, cmatch);                          \
    } else {                                                             \
        kt key = apk->kaobjpart; return k_cf_func(key, fs, cmatch); \
    }}

//TODO aobj *
static aobj getRC_OBT(bt *btr, void *orow, int cmatch, aobj *apk, bool fs) {
    if        UU(btr) { /* OTHER_BT values are either in PK or BT -> no ROW */
        ulong   key = cmatch ? (ulong)orow        : apk->i;
        return colFromUU(key, fs, cmatch);
    } else if UL(btr) OBT_RAWC(ulong,   ulk, i, colFromUL)
      else if LU(btr) OBT_RAWC(ulong,   luk, l, colFromLU)
      else if LL(btr) OBT_RAWC(ulong,   llk, l, colFromLL)
      else if XX(btr) OBT_RAWC(uint128, xxk, x, colFromXX)
      // NEXT 4 have different ksize,vsize
      else if UX(btr) OBTXY_RAWC(uint128, uxk, colFromXX, ulong,   i, colFromUU)
      else if XU(btr) OBTXY_RAWC(ulong,   xuk, colFromUU, uint128, x, colFromXX)
      else if LX(btr) OBTXY_RAWC(uint128, lxk, colFromXX, ulong,   l, colFromLL)
      else if XL(btr) OBTXY_RAWC(ulong,   xlk, colFromLL, uint128, x, colFromXX)
      else { assert(!"getRC_OBT: ERROR"); aobj a; return a; }
}
//TODO aobj *
static aobj getRC_LFunc(bt   *btr, uchar  *orow, int tmatch, aobj *apk,
                        bool  fs,  lfca_t *lfca) {
    aobj a; initAobj(&a); lue_t *le = lfca->l[lfca->curr]; lfca->curr++;
    lua_getglobal(server.lua, "DumpFunctionForOutput");
    lua_getglobal(server.lua, le->fname);
    printf("lua select function: fname: %s ncols: %d\n", le->fname, le->ncols);
    for (int i = 0; i < le->ncols; i++) {
        pushColumnLua(btr, orow, tmatch, le->as[i], apk);
    }
    int ret = DXDB_lua_pcall(server.lua, le->ncols + 1, 1, 0); // +1 for Wrapper
    if (ret) {
        a.err = 1; a.type = COL_TYPE_ERR;
        CURR_ERR_CREATE_OBJ
             "-ERR: Error running SELECT FUNCTION (%s): %s CARD: %ld\r\n",
               le->fname, lua_tostring(server.lua, -1), server.alc.CurrCard));
        lua_pop(server.lua, 1);
    } else {
        initAobjFromLuaResponse(&a, NULL, tmatch, fs);
    }
    return a;
}
static aobj getRC_Ipos(int cmatch, bool fs) {
    aobj a; int imatch = getImatchFromOCmatch(cmatch);
    initIntAobjFromVal(&a, Index[imatch].cipos, fs, cmatch); return a;
}

//TODO RawCols[] is not worth the complexity, use malloc()
#define RAW_COL_BUF_SIZE 256
static uchar *getHashFromRow(uchar *row) {
    uint32_t clen;               row++;       // SKIP rflag
    streamIntToUInt(row, &clen); row += clen; // SKIP rlen
    streamIntToUInt(row, &clen); row += clen; // SKIP ncols
    return row;
}
static uchar *getRowPayload(uchar  *row,   uchar  *rflag,
                            uint32 *ncols, uint32 *rlen) {
    uint32_t clen;
    uchar *o_row = row;
    *rflag       = *row;                             row++;       // GET rflag
    if (*rflag & RFLAG_HASH_ROW) {
        *rlen       = streamIntToUInt(row, &clen);   row += clen; // GET rlen
        *ncols      = streamIntToUInt(row, &clen);   row += clen; // GET ncols
        if (*rflag & RFLAG_HASH32_ROW) {
            ahash32 *ht = (ahash32 *)row;
                          /* SKIP HASH32_TABLE */    row += alc_hash32_size(ht);
        } else { // RFLAG_HASH16_ROW
            ahash16 *ht = (ahash16 *)row;
                          /* SKIP HASH16_TABLE */    row += alc_hash16_size(ht);
        }
    } else {
        char sflag = *rflag & RFLAG_SIZE_FLAG;
        *ncols     = streamIntToUInt(row, &clen);    row += clen; // GET ncols
                          /* SKIP cofsts */          row += (*ncols * sflag);
        if        (*rflag & RFLAG_1BYTE_INT) { // rlen = final cofst[FINAL]
            *rlen = (uint32)*((uchar*)row - 1);
        } else if (*rflag & RFLAG_2BYTE_INT) {
            *rlen = (uint32)(*((unsigned short *)((uchar *)(row - 2))));
        } else {         /* RFLAG_4BYTE_INT */
            *rlen = (uint32)(*((uint32 *)        ((uchar *)(row - 4))));
        }
    }
    uint32  mlen = row - o_row;
    *rlen        = *rlen + mlen;
    return row;
}
uint32 getRowMallocSize(uchar *stream) { // used in stream.c also
    if (!stream) return sizeof(void *); // NULL will be stored IN-stream
    uchar rflag; uint32 rlen; uint32 ncols;
    getRowPayload(stream, &rflag, &ncols, &rlen); return rlen;
}
uchar *getColData(uchar *orow, int cmatch, uint32 *clen, uchar *rflag) {
    uint32 rlen; uint32 ncols;
    uchar   *row     = getRowPayload(orow, rflag, &ncols, &rlen);
    if ((uint32)cmatch > ncols) { *clen = 0; return row; }
    uchar    sflag   = *rflag & RFLAG_SIZE_FLAG;
    if        (*rflag & RFLAG_HASH32_ROW) {
        ahash32 *ht    = (ahash32 *)getHashFromRow(orow);
        long     val   = alc_hash32_fetch(cmatch, ht);
        uint32   start = val / UINT_MAX,  next  = val % UINT_MAX;;
        *clen          = next - start;
        return row + start;
    } else if (*rflag & RFLAG_HASH16_ROW) {
        ahash16 *ht    = (ahash16 *)getHashFromRow(orow);
        uint32   val   = alc_hash16_fetch(cmatch, ht);
        ushort16 start = val / USHRT_MAX, next  = val % USHRT_MAX;;
        *clen          = next - start;
        return row + start;
    } else {
        uint32   start   = 0, next;
        int      mcmatch = cmatch - 1; // key NOT stored -> one less column
        uchar   *cofst   = orow + 1 + getCSize(ncols - 1, 1);
        if        (*rflag & RFLAG_1BYTE_INT) {
            if (mcmatch) start = *(uchar *)((cofst + mcmatch - 1));
            next = *(uchar *)((cofst + mcmatch));
        } else if (*rflag & RFLAG_2BYTE_INT) {
            if (mcmatch) {
                start = *(ushort16 *)(uchar *)(cofst + ((mcmatch - 1) * sflag));
            }
            next = *(ushort16 *)(char *)(cofst + (mcmatch * sflag));
        } else {         /* RFLAG_4BYTE_INT */
            if (mcmatch) {
                start = *(uint32 *)(uchar *)(cofst + ((mcmatch - 1) * sflag));
            }
            next = *(uint32 *)(uchar *)(cofst + (mcmatch * sflag));
        }
        *clen = next - start;
        return row + start;
    }
}
aobj getRawCol(bt  *btr,    uchar *orow, icol_t  ic,  aobj *apk,
               int  tmatch, bool  fs,    lfca_t *lfca) {
    int cmatch = ic.cmatch;
    if      (IS_LSF(cmatch)) return getRC_LFunc(btr, orow, tmatch, apk, fs,
                                                lfca);
    else if (cmatch < -1)    return getRC_Ipos (cmatch, fs);
    else if (OTHER_BT(btr))  return getRC_OBT  (btr, orow, cmatch, apk, fs);
    aobj a; initAobj(&a); //DEBUG_GET_RAW_COL
    if (cmatch == -1)  return a; // NOTE: used for HASHABILITY miss
    r_tbl_t *rt    = &Tbl[tmatch];
    uchar    ctype = rt->col[cmatch].type;
    if (!cmatch) { /* PK stored ONLY in KEY not in ROW, echo it */
        if (!C_IS_S(ctype) && !fs) return *apk;
        else                     {
            initStringAobjFromAobj(&a, apk); a.type = apk->type; return  a; 
        }
    }
    uint32 clen; uchar rflag;
    uchar *data  = getColData(orow, cmatch, &clen, &rflag);
    if (!clen && !C_IS_O(ctype)) a.empty = 1;
    else {
        if        C_IS_I(ctype) {
            uint32  i  = streamIntToUInt(data, &clen);
            initIntAobjFromVal(&a, i, fs, cmatch);
        } else if C_IS_L(ctype) {
            ulong   l  = streamLongToULong(data, &clen);
            initLongAobjFromVal (&a, l,   fs, cmatch);
        } else if C_IS_X(ctype) {
            uint128 x  = streamToU128(data, &clen);
            initU128AobjFromVal (&a, x,   fs, cmatch);
        } else if C_IS_F(ctype) {
            float   f  = streamFloatToFloat(data, &clen);
            initFloatAobjFromVal(&a, f,   fs, cmatch);
        } else if C_IS_O(ctype) {
            init_LO_AobjFromCmatch(&a, apk, ic, tmatch, fs);
        } else if C_IS_S(ctype) {
            a.type     = a.enc = COL_TYPE_STRING; a.empty = 0;
            if        (rflag & RFLAG_6BIT_ZIP) {
                a.s    = (char *)unpackSixBit(data, &clen);  a.freeme = 1;
            } else if (rflag & RFLAG_LZF_ZIP) {            // \/FREED 035
                a.s    = streamLZFTextToString(data, &clen); a.freeme = 1;
            } else a.s = (char *)data; /* NO ZIP -> uncompressed text */
            a.len  = clen;
        } else assert(!"getRawCol ERROR");
    }
    return a;
}
inline aobj getCol(bt     *btr, uchar *rrow, icol_t ic, aobj *apk, int tmatch,
                   lfca_t *lfca) {
    return getRawCol(btr, rrow, ic, apk, tmatch, 0, lfca);
}
inline aobj getSCol(bt     *btr, uchar *rrow, icol_t ic, aobj *apk, int tmatch,
                    lfca_t *lfca) {
    return getRawCol(btr, rrow, ic, apk, tmatch, 1, lfca);
}

//TODO RawCols[] is too complicated -> use malloc()
static char RawCols[RAW_COL_BUF_SIZE][64]; /* NOTE: avoid malloc's */
static void initAobjCol2S(aobj *a, ulong l, uint128 x, float f, int cmatch,
                          int   ktype) {
    char *dest; char *fmt = C_IS_F(ktype) ? FLOAT_FMT : "%lu";
    if (cmatch >= 0 && cmatch < RAW_COL_BUF_SIZE) dest = RawCols[cmatch];
    else                                   { dest = malloc(64); a->freeme = 1; }
    if      C_IS_X(ktype) SPRINTF_128(dest, 64,      x)
    else if C_IS_F(ktype) snprintf   (dest, 64, fmt, f);
    else    /* [I,L] */   snprintf   (dest, 64, fmt, l);
    a->len = strlen(dest); a->s   = dest; a->enc = COL_TYPE_STRING;
    //printf("initAobjCol2S: a: "); dumpAobj(printf, a);
}
static void initIntAobjFromVal(aobj *a, uint32 i, bool fs, int cmatch) {
    initAobj(a); a->type = COL_TYPE_INT; a->empty = 0;
    ulong l = (ulong)i;
    if (fs) initAobjCol2S(a, l, 0, 0.0, cmatch, COL_TYPE_INT);
    else         { a->i = i; a->enc = COL_TYPE_INT; }
}
static void initLongAobjFromVal(aobj *a, ulong l, bool fs, int cmatch) {
    initAobj(a); a->type = COL_TYPE_LONG; a->empty = 0;
    if (fs) initAobjCol2S(a, l, 0, 0.0, cmatch, COL_TYPE_LONG);
    else         { a->l = l; a->enc = COL_TYPE_LONG; }
}
static void initU128AobjFromVal(aobj *a, uint128 x, bool fs, int cmatch) {
    initAobj(a); a->type = COL_TYPE_U128; a->empty = 0;
    if (fs) initAobjCol2S(a, 0, x, 0.0, cmatch, COL_TYPE_U128);
    else         { a->x = x; a->enc = COL_TYPE_U128; }
}
static void initFloatAobjFromVal(aobj *a, float f, bool fs, int cmatch) {
    initAobj(a); a->type = COL_TYPE_FLOAT; a->empty = 0;
    if (fs) initAobjCol2S(a, 0, 0, f,   cmatch, COL_TYPE_FLOAT);
    else         { a->f = f; a->enc = COL_TYPE_FLOAT; }
}

static void initAobjFromLuaString(lua_State *lua, aobj *a) {
    a->empty = 0;
    int        len = lua_strlen(lua, -1);
    char      *x   = malloc(len + 1); // FREED in aobjDestroy()
    memcpy(x, (char*)lua_tostring(lua, -1), len); x[len] = '\0';
    initAobjString(a, x, len);
}

char *UnprintableLuaObject = "ERR: UNPRINTABLE_LUA_OBJECT";
int   lenUnplo             = 27;

static void initAobjFromLuaNumber(lua_State *lua, aobj *a,
                                  bool       fs,  int   tmatch, icol_t *ic) {
    ulong l = (ulong)lua_tonumber(lua, -1); initAobjLong(a, l);
    if (fs) {
        sds s = createSDSFromAobj(a);
        initAobjFromStr(a, s, sdslen(s), COL_TYPE_STRING); sdsfree(s);
    }
    int dtype  = COL_TYPE_LONG;
    if (ic) { // we can determine type[INT|LONG] from Index[].dtype
        int imatch = find_index(tmatch, *ic);
        if (imatch != -1) { //TODO support U128
            dtype = Index[imatch].dtype; if C_IS_I(dtype) a->i = a->l;
        }
    }
    a->type = a->enc = dtype;
    printf("initAobjFromLuaNumber: a: "); dumpAobj(printf, a);
}
static void initAobjFromLuaResponse(aobj *a, icol_t *ic, int tmatch, bool fs) {
    int t = lua_type(server.lua, -1);
    if        (t == LUA_TSTRING) {
        initAobjFromLuaString(server.lua, a);
    } else if (t == LUA_TNUMBER) {
        initAobjFromLuaNumber(server.lua, a, fs, tmatch, ic);
    } else if (t == LUA_TBOOLEAN) {
       initAobjBool(a, lua_toboolean(server.lua, -1));
       bool b = a->b;
       if (fs) {
           sds s = createSDSFromAobj(a);
           initAobjFromStr(a, s, sdslen(s), COL_TYPE_STRING); sdsfree(s);
       }
       a->type = a->enc = COL_TYPE_BOOL; // for LuaFunctionUPDATES as SELECT
       a->b    = b;
    } else {
        initAobjString(a, UnprintableLuaObject, lenUnplo);
    }
    lua_pop(server.lua, 1);
}
static void init_LO_AobjFromCmatch(aobj *a,      aobj *apk, icol_t ic,
                                   int   tmatch, bool  fs) {
    printf("initLO_AobjFromCmatch: t: %d apk: ", tmatch); dumpAobj(printf, apk);
    lua_getglobal(server.lua, "DumpObjForOutput");
    pushLuaVar(tmatch, ic, apk);
    int t     = lua_type(server.lua, -1);
    if (t == LUA_TNIL) {
        initAobjString(a, "nil", 3);
        lua_pop(server.lua, -1); // pop off func: DumpObjForOutput()
    } else if (t == LUA_TTABLE) {
        int ret = DXDB_lua_pcall(server.lua, 1, 1, 0);
        if (ret) initAobjString(a, UnprintableLuaObject, lenUnplo);
        else     initAobjFromLuaResponse(a, &ic, tmatch, fs);
    } else {
        initAobjFromLuaResponse(a, &ic, tmatch, fs);
        lua_pop(server.lua, -1); // pop off func: DumpObjForOutput()
    }
}

static aobj colFromUU(ulong key, bool fs, int cmatch) {
    int cval = (int)((long)key % UINT_MAX);
    aobj a; initIntAobjFromVal(&a, cval, fs, cmatch); return a;
}
static aobj colFromUL(ulong key, bool fs, int cmatch) {
    aobj a; 
    if (cmatch) initLongAobjFromVal(&a, key, fs, cmatch);
    else        initIntAobjFromVal( &a, key, fs, cmatch);
    return a;
}
static aobj colFromLU(ulong key, bool fs, int cmatch) {
    aobj a;
    if (cmatch) initIntAobjFromVal(&a, key, fs, cmatch);
    else        initLongAobjFromVal(&a, key, fs, cmatch);
    return a;
}
static aobj colFromLL(ulong key, bool fs, int cmatch) {
    aobj a; initLongAobjFromVal(&a, key, fs, cmatch); return a;
}
static aobj colFromXX(uint128 key, bool fs, int cmatch) {
    aobj a; initU128AobjFromVal(&a, key, fs, cmatch); return a;
}

//TODO mv to output_row.c
/* OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT */
static void destroy_erow(erow_t *er) { //printf("destroy_embedded_row\n");
    for (int i = 0; i < er->ncols; i++) destroyAobj(er->cols[i]);
    free(er->cols); free(er);
}
robj *cloneRobjErow(robj *r) { // NOTE Used in cloneRobj()
    if (!r) return NULL;
    erow_t *er  = (erow_t *)r->ptr;
    robj   *n   = createObject(REDIS_STRING, NULL);
    erow_t *ner = malloc(sizeof(erow_t));
    n->ptr      = ner;
    ner->ncols  = er->ncols;
    ner->cols   = malloc(sizeof(aobj *) * ner->ncols);
    for (int i = 0; i < ner->ncols; i++) {
        ner->cols[i] = cloneAobj(er->cols[i]);
    }
    return n;
}
void decrRefCountErow(robj *r) { // NOTE Used in cloneRobj()
    if (!r) return;
    erow_t *er  = (erow_t *)r->ptr;
    if (er) destroy_erow(er); /* destroy here (avoids deep redis integration) */
    r->ptr      = NULL;       /* already destroyed */
    decrRefCount(r);
}
// OUTPUT_ROW OUTPUT_ROW OUTPUT_ROW OUTPUT_ROW OUTPUT_ROW OUTPUT_ROW
static robj *orow_embedded(bt     *btr,  void *rrow, int qcols, 
                           icol_t *ics,  aobj *apk,  int tmatch,
                           lfca_t *lfca, bool *ost) {
    ost        = NULL; // compiler warning
    robj   *r  = createObject(REDIS_STRING, NULL);
    erow_t *er = malloc(sizeof(erow_t));
    er->ncols  = qcols;
    er->cols   = malloc(sizeof(aobj *) * er->ncols);
    for (int i = 0; i < er->ncols; i++) {
        aobj  acol  = getCol(btr, rrow, ics[i], apk, tmatch, lfca);
        er->cols[i] = copyAobj(&acol); //NOTE: do NOT releaseAobj()
    }
    r->ptr = er; return r;
}

#define OBUFF_SIZE 4096
static char OutBuff[OBUFF_SIZE]; /*avoid malloc()s */

#define QUOTE_COL \
  if (!OREDIS && C_IS_S(outs[i].type) && outs[i].len) \
      { obuf[slot] = '\''; slot++; }
#define FINAL_COMMA \
  if (!OREDIS && i != (qcols - 1)) { obuf[slot] = OUTPUT_DELIM; slot++; }

int output_start(char *buf, uint32 blen, int qcols) {
    buf[0]          = '*';
    size_t intlen   = ll2string(buf + 1, blen - 1, (lolo)qcols);
    buf[intlen + 1] = '\r';
    buf[intlen + 2] = '\n';
    return intlen + 3;
}
/* NOTE: big obufs could be alloca'd, but stack overflow scares me */
robj *write_output_row(int   qcols,   uint32  prelen, char *pbuf,
                       uint32 totlen, sl_t   *outs) {
    char   *obuf = (totlen >= OBUFF_SIZE) ? malloc(totlen) : OutBuff; //FREE 072
    if (prelen) memcpy(obuf, pbuf, prelen);
    uint32  slot = prelen;
    for (int i = 0; i < qcols; i++) {
        QUOTE_COL
        memcpy(obuf + slot, outs[i].s, outs[i].len);
        slot += outs[i].len; release_sl(outs[i]);
        QUOTE_COL FINAL_COMMA
    }
    robj *r = createStringObject(obuf, totlen);
    if (obuf != OutBuff) free(obuf);                     /* FREED 072 */
    return r;
}
static robj *orow_redis(bt     *btr,  void *rrow, int qcols, 
                        icol_t *ics,  aobj *apk,  int tmatch,
                        lfca_t *lfca, bool *ost) {
    char pbuf[128]; sl_t outs[qcols]; int faili   = 0;
    uint32 prelen   = output_start(pbuf, 128, qcols);
    uint32 totlen   = prelen;
    bool   allbools = 1; bool bool_ok = 0; // allbools means dont print row
    for (int i = 0; i < qcols; i++) {
        aobj  acol  = getCol(btr, rrow, ics[i], apk, tmatch, lfca);
        outs[i]     = outputReformat(&acol);
        releaseAobj(&acol);
        if C_IS_E(acol.type) { faili = i;               goto orowr_err; }
        if C_IS_B(acol.type) { if (acol.b) bool_ok = 1; continue; }
        allbools        = 0;
        totlen     += outs[i].len;
    }
    if (allbools) { *ost = bool_ok ? OR_ALLB_OK : OR_ALLB_NO; return NULL; }
    return write_output_row(qcols, prelen, pbuf, totlen, outs);

orowr_err:
    for (int i = 0; i <= faili; i++) release_sl(outs[i]);
    *ost = OR_LUA_FAIL; return NULL;
}
static robj *orow_normal(bt     *btr, void *rrow, int qcols, 
                        icol_t *ics,  aobj *apk,  int tmatch,
                        lfca_t *lfca, bool *ost) {
    sl_t   outs[qcols];
    uint32 totlen   = 0; int  faili   = 0;
    bool   allbools = 1; bool bool_ok = 0; // allbools means dont print row
    for (int i = 0; i < qcols; i++) {
        aobj  acol       = getSCol(btr, rrow, ics[i], apk, tmatch, lfca);
        outs[i].freeme  = acol.freeme;
        outs[i].s       = acol.s;
        outs[i].len     = acol.len;
        outs[i].type    = acol.type;
        if C_IS_E(acol.type) { faili = i;               goto orown_err; }
        if C_IS_B(acol.type) { if (acol.b) bool_ok = 1; continue; }
//NOTE: return [STRING,BOOL,STRING,BOOL,STRING] -> SEGV
        allbools        = 0;
        totlen         += acol.len;
        if (C_IS_S(outs[i].type) && outs[i].len) totlen += 2;/* 2 \'s per col */
    }
    if (allbools) { *ost = bool_ok ? OR_ALLB_OK : OR_ALLB_NO; return NULL; }
    totlen += (uint32)qcols - 1; /* one comma per COL, except final */
    return write_output_row(qcols, 0, NULL, totlen, outs);

orown_err:
    for (int i = 0; i <= faili; i++) release_sl(outs[i]);
    *ost = OR_LUA_FAIL; return NULL;
}
static robj *orow_lua(bt   *btr, void *rrow,   int     qcols, icol_t *ics,
                      aobj *apk, int   tmatch, lfca_t *lfca,  bool   *ost) {
    CLEAR_LUA_STACK lua_getglobal(server.lua, server.alc.OutputLuaFunc_Row);
    int  faili = 0; bool allbools = 1; bool bool_ok = 0;
    for (int i = 0; i < qcols; i++) {
        aobj  acol       = getSCol(btr, rrow, ics[i], apk, tmatch, lfca);
        if C_IS_E(acol.type) { faili = i;               goto orowl_err; }
        if C_IS_B(acol.type) { if (acol.b) bool_ok = 1; continue; }
        allbools        = 0;
        //TODO dont push as arg, but add to table, pass single table
        pushAobjLua(&acol, COL_TYPE_STRING);
    }
    if (allbools) { *ost = bool_ok ? OR_ALLB_OK : OR_ALLB_NO; return NULL; }
    int ret = DXDB_lua_pcall(server.lua, qcols, 1, 0);
    sds s   = ret ? sdsempty()                                    :
                    sdsnewlen((char*)lua_tostring(server.lua, -1),
                              lua_strlen(server.lua, -1));
    CLEAR_LUA_STACK
    return createObject(REDIS_STRING, s);

orowl_err:
    CLEAR_LUA_STACK *ost = OR_LUA_FAIL; return NULL;
}
robj *outputRow(bt  *btr, void *rrow,   int     qcols, icol_t *ics, 
               aobj *apk, int   tmatch, lfca_t *lfca,  bool   *ost) {
    if (lfca) lfca->curr = 0; //RESET queue
    row_outputter *rop = (EREDIS ? orow_embedded :
                          OREDIS ? orow_redis    :
                          LREDIS ? orow_lua      : orow_normal);
    return (*rop)(btr, rrow, qcols, ics, apk, tmatch, lfca, ost);
}
// ADD_REPLY_ROW ADD_REPLY_ROW ADD_REPLY_ROW ADD_REPLY_ROW ADD_REPLY_ROW
bool addReplyRow(cli   *c,    robj *r,    int    tmatch, aobj *apk,
                 uchar *lruc, bool  lrud, uchar *lfuc,   bool  lfu) {
    updateLru(c, tmatch, apk, lruc, lrud); /* NOTE: updateLRU (RQ_SELECT) */
    updateLfu(c, tmatch, apk, lfuc, lfu);  /* NOTE: updateLFU (RQ_SELECT) */
    if      (EREDIS) {
        erow_t *er  = (erow_t *)r->ptr;
        bool    ret = c->scb ? (*c->scb)(er) : 1;
        destroy_erow(er);   /* destroy here (avoids deep redis integration) */
        r->ptr      = NULL; /* already destroyed */
        return ret;
    } else if (OREDIS || LREDIS) addReply(c,     r);
      else                       addReplyBulk(c, r);
    return 1;
}

// DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW
#define DEBUG_DELETE_ROW                                                   \
  printf("deleteRow: miss: %d rrow: %p gost: %d\n", dwm.miss, rrow, gost);
#define DEBUG_DELETE_LUAOBJ                                                \
  printf("LO: tname: %s cname: %s apk: ", rt->name, rt->col[cmatch].name); \
  dumpAobj(printf, apk);

void deleteLuaObj(int tmatch, int cmatch, aobj *apk) {
    r_tbl_t *rt = &Tbl[tmatch];                           //DEBUG_DELETE_LUAOBJ
    CLEAR_LUA_STACK lua_getfield(server.lua, LUA_GLOBALSINDEX, "delete_luaobj");
    lua_pushstring(server.lua, rt->name);
    lua_pushstring(server.lua, rt->col[cmatch].name);
    pushAobjLua(apk, apk->type);
    DXDB_lua_pcall(server.lua, 3, 0, 0); CLEAR_LUA_STACK
}
//NOTE deleteRow() can NOT fail due to CONSTRAINT VIOLATIONS
int deleteRow(int tmatch, aobj *apk, int matches, int inds[]) {
    //printf("\n\nSTART: deleteRow: key: "); dumpAobj(printf, apk);
    bt    *btr  = getBtr(tmatch);
    dwm_t  dwm  = btFindD(btr, apk);
    if (dwm.miss) return -1;
    void  *rrow = dwm.k;
    if (!rrow)    return 0;
    bool   wgost = btGetDR(btr, apk); // Indexes need to know WillGhost
    bool   gost  = IS_GHOST(btr, rrow) && wgost;             //DEBUG_DELETE_ROW
    if (gost)     return 0; // GHOST -> ECASE:6
    if (!dwm.miss) {
        runDeleteIndexes(btr, apk, rrow, matches, inds, wgost);
        if (Tbl[tmatch].haslo) {
            r_tbl_t *rt = &Tbl[tmatch];
            for (int i = 0; i < rt->col_count; i++) {
                if C_IS_O(rt->col[i].type) deleteLuaObj(tmatch, i, apk);
            }}
    }
    btDelete(btr, apk); server.dirty++; 
    //printf("END: deleteRow\n\n\n"); fflush(NULL);
    return dwm.miss ? -1 : 1;
}

//TODO mv to update_row.c
// UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
static void init_lue(lue_t *le)    { bzero(le, sizeof(lue_t)); }
static void release_lue(lue_t *le) {
    if (le->as) free(le->as); // FREED 096
}
void init_uc(uc_t  *uc,     bt     *btr, 
             int    tmatch, int     ncols,   int    matches, int  inds[],
             char  *vals[], uint32  vlens[], icol_t chit[],  ue_t ue[],
             lue_t *le) {
    uc->btr    =   btr;
    uc->tmatch = tmatch; uc->ncols = ncols; uc->matches = matches;
    uc->inds   = inds;   uc->vals  = vals;  uc->vlens   = vlens;
    uc->chit   = chit;   uc->ue    = ue;    uc->le      = le;
    init_lue(uc->le); //TODO this should be a separate call
}
void release_uc(uc_t *uc) {
    if (uc->le) release_lue(uc->le); //TODO this should be a separate call
}

#define INIT_COL_AVALS                                      \
  aobj avs[uc->ncols];                                      \
  for (int i = 0; i < uc->ncols; i++) initAobj(&avs[i]);
#define DESTROY_COL_AVALS                                   \
  for (int i = 0; i < uc->ncols; i++) releaseAobj(&avs[i]);

static bool upEffectedFailableIndexes(cli    *c,       bt *btr,
                                      aobj   *opk,     void *orow,
                                      aobj   *npk,     void *nrow,
                                      int     matches, int inds[],
                                      icol_t  chit[])              {
    bool hasup = 0;
    bool upit[matches];
    for (int i = 0; i < matches; i++) { /* Redo ALL EFFECTED indexes */
        bool up = 0; upit[i] = 0;
        r_ind_t *ri = &Index[inds[i]];
        if      (ri->virt || ri->luat) continue; // LUATRIGGERS done later
        else if (ri->lru || ri->lfu) up = 1;     // ALWAYS updated
        //TODO OBI must update on OBY_Column also
        else if (ri->clist) {
            for (int i = 0; i < ri->nclist; i++) {
                if (chit[ri->bclist[i].cmatch].cmatch != -1) { up = 1; break; }
            }
        } else { up = (chit[ri->icol.cmatch].cmatch != -1); }
        if (up) { hasup = 1; upit[i] = 1; }
    }
    if (!hasup) return 1;
    for (int i = 0; i < matches; i++) {
        if (!upit[i]) continue;
        if (!addToIndex(c, btr, npk, nrow, inds[i])) {
            for (int j = 0; j < i; j++) { // ROLLBACK previous ADD-INDEXes
                if (!upit[j]) continue;
                delFromIndex(btr, npk, nrow, inds[j], 0);
            }
            return 0;
        }
        delFromIndex(btr, opk, orow, inds[i], 0);
    }
    return 1;
}

static uint32 getNumKeyLen(aobj *a) {
    if C_IS_X(a->type) return 16;
    uchar sflag; ulong col, l = C_IS_I(a->type) ? a->i : a->l;
    return cIcol(l, &sflag, &col, C_IS_I(a->type));
}
static bool aobj_sflag(aobj *a, uchar *sflag) {
    if C_IS_X(a->type)                          return !a->empty;
    else {
        ulong col, l = C_IS_I(a->type) ? a->i : a->l;
        cIcol(l, sflag, &col, C_IS_I(a->type)); return !a->empty;
    }
}
#define DEBUG_UPDATE_ROW                                                 \
  if (i) { printf ("%d: oflag: %d chit.cmatch: %d chit.nlo: %d ",        \
                    i, osflags[i], uc->chit[i].cmatch, uc->chit[i].nlo); \
    DEBUG_CREATE_ROW printf("\t\t\t\t"); dumpAobj(printf, &avs[i]); }

#define OVRWR(i, ovrwr, ctype) /*NOTE: !indexed -> upIndex uses orow & nrow */ \
  (i && ovrwr && C_IS_NUM(ctype) && !rt->col[i].indxd)

#define UP_ERR goto up_end;

/*TODO 
  UPDATE IMPROVEMENTS:
    1.) do an initial pass to determine OVWR (avoids per-col getCol() call)
        |-> OVWR detection      -> push into parsing
    2.) ERR: SET UINT to >4GB   -> push into parsing
    3.) ERR: Unparseable U128   -> push into parsing

  UPDATE_FAILURES: (problem for RANGE-UPDATEs -> would need ROLLBACKs)
    -> SOLUTION Q nrows in memory, apply when ALL have passed.
*/
#define DEBUG_RUN_UPDATE                  \
  printf("runUpdate: cq: %d\n", cq);      \
  printf("opk: "); dumpAobj(printf, opk); \
  printf("npk: "); dumpAobj(printf, npk); 
static int runUpdate(cli *c,    uc_t *uc,   aobj *opk,   void *orow,
                     aobj *npk, void *nrow, bool  lodlt, bool  cq,  int ncols) {
    r_tbl_t *rt  = &Tbl[uc->tmatch];                         //DEBUG_RUN_UPDATE
    int      ret = -1;
    if (rt->nltrgr) { // LUATRIGGER: PRE-UPDATE
        runPreUpdateLuatriggers(uc->btr, opk, orow, uc->matches, uc->inds);
    }
    if (uc->chit[0].cmatch != -1) { // PK update
        //NOTE: runFailableInsertIndexes() can NOT FAIL -> FACTORED OUT
        runFailableInsertIndexes(c, uc->btr, npk, nrow, uc->matches, uc->inds);
        runDeleteIndexes(uc->btr, opk, orow, uc->matches, uc->inds, 0);
        btDelete(uc->btr, opk);          // DELETE row w/ OLD PK
        ret = btAdd(uc->btr, npk, nrow); // ADD row w/ NEW PK
        UPDATE_AUTO_INC(rt->col[0].type, npk)
    } else { // upEffectedFailableIndexes() CAN NOT FAIL -> FACTORED OUT
        upEffectedFailableIndexes(c, uc->btr, opk, orow, npk, nrow,
                                  uc->matches, uc->inds, uc->chit);
        ret = btReplace(uc->btr, opk, nrow); // OVERWRITE w/ new row 
    }
    if (lodlt) { // Apply FULL LuaObject Updates
        lua_getglobal(server.lua, "pop_AQ");
        DXDB_lua_pcall(server.lua, 0, 0, 0);
    }
    if (cq) {    // Apply PARTIAL LuaObject Updates
        for (int i = 1; i < ncols; i++) { // 3rd loop
            uchar ctype = rt->col[i].type;
            if (C_IS_O(ctype) && uc->chit[i].nlo) {
                setLuaVar(uc->tmatch, uc->chit[i], opk);
                lua_getglobal(server.lua, "pop_CQ");
                DXDB_lua_pcall(server.lua, 0, 1, 0);
                lua_settable(server.lua, -3);
            }
        }
    }
    if (rt->nltrgr) { // LUATRIGGERS: POST-UPDATE
        runPostUpdateLuatriggers(uc->btr, npk, nrow, uc->matches, uc->inds);
    }
    server.dirty++;
    return ret;
}

// UPDATE_QUEUE UPDATE_QUEUE UPDATE_QUEUE UPDATE_QUEUE UPDATE_QUEUE UPDATE_QUEUE
typedef struct ur_t {
    aobj *opk; void *orow; aobj *npk; void *nrow;
} ur_t;
static ur_t *newUpdateRow(aobj *opk, void *orow, aobj *npk, void *nrow) {
  ur_t *ur = (ur_t *)malloc(sizeof(ur_t));
  ur->opk = opk; ur->orow = orow; ur->npk = npk; ur->nrow = nrow;
  return ur;
}
void vFreeUpdateRow(void *v) { if (v) free(v); }

static int queueUpdate(cli *c,     uc_t *uc,   aobj *opk,   void *orow,
                       aobj *npk,  void *nrow, bool  lodlt, bool  cq, 
                       int   ncols)                                  { 
    if (!c->UpdateQueue.inited) { //printf("UpdateQueue.inited\n");
        c->UpdateQueue.inited          = 1;
        c->UpdateQueue.l               = listCreate();
        c->UpdateQueue.l->free         = vFreeUpdateRow;
        c->UpdateQueue.constants.uc    = uc;
        c->UpdateQueue.constants.lodlt = lodlt;
        c->UpdateQueue.constants.cq    = cq;
        c->UpdateQueue.constants.ncols = ncols;
    }
    listAddNodeTail(c->UpdateQueue.l, newUpdateRow(opk, orow, npk, nrow));
    return getNumKeyLen(npk) + getRowMallocSize(nrow); // stream size
}
void runUpdateQueue(cli *c) { //printf("runUpdateQueue\n");
    if (!c->UpdateQueue.inited) return; //FAILED before it started
    listNode  *ln;
    uqc_t     *uqc = &c->UpdateQueue.constants;
    listIter  *li  = listGetIterator(c->UpdateQueue.l, AL_START_HEAD);
    while((ln = listNext(li))) {
        ur_t *ur = ln->value;
        runUpdate(c, uqc->uc,    ur->opk, ur->orow, ur->npk, ur->nrow,
                     uqc->lodlt, uqc->cq, uqc->ncols);
    } listReleaseIterator(li);
    abandonUpdateQueue(c);
}
void abandonUpdateQueue(cli *c) {
    if (!c->UpdateQueue.inited) return; //FAILED before it started
    listNode  *ln; uqc_t *uqc = &c->UpdateQueue.constants;
    listIter  *li = listGetIterator(c->UpdateQueue.l, AL_START_HEAD);
    while((ln = listNext(li))) {
        ur_t *ur = ln->value;
        if (NORM_BT(uqc->uc->btr)) free(ur->nrow);       // FREED 023
        destroyAobj(ur->npk);                            // FREED 168
    } listReleaseIterator(li);
    listRelease(c->UpdateQueue.l); c->UpdateQueue.l = NULL;
    c->UpdateQueue.inited = 0;
    lua_getglobal(server.lua, "reset_CQ"); DXDB_lua_pcall(server.lua, 0, 0, 0);
}

// UPDATE_OVERWRITE UPDATE_OVERWRITE UPDATE_OVERWRITE UPDATE_OVERWRITE
static int updateOverwrite(cli   *c,         uc_t *uc,  cr_t *cr, crd_t *crd,
                           uchar  osflags[], aobj *opk, void *orow) {
    r_tbl_t *rt = &Tbl[uc->tmatch];
    for (int i = 1; i < cr->ncols; i++) {
        if (osflags[i]) {
            uint32 clen; uchar rflag;
            uchar *data  = getColData(orow, i, &clen, &rflag);
            uchar  ctype = rt->col[i].type;
            if       (rt->lrud && rt->lruc == i) {// updateLRU (UPDATE_2)
                // NOTE LRU is always 4 bytes, so orow will NOT change
                updateLru(c, uc->tmatch, opk, data, rt->lrud);
            } else if (rt->lfu && rt->lfuc == i) { // updateLFU (UPDATE_2)
                // NOTE LFU is always 8 bytes, so orow will NOT change
                updateLfu(c, uc->tmatch, opk, data, rt->lfu);
            } else if C_IS_I(ctype) {
                writeUIntCol(&data,  crd[i].iflags, crd[i].icols);
            } else if C_IS_L(ctype) {
                writeULongCol(&data, crd[i].iflags, crd[i].icols);
            } else if C_IS_X(ctype) {
                writeU128Col(&data, crd[i].xcols);
            } else assert(!"updateRow OVWR ERROR");
        }
    }
    return getNumKeyLen(opk) + getRowMallocSize(orow);
}
//NOTE updateRow() can NOT fail due to CONSTRAINT VIOLATIONS
//     therefore NON-OVERWRITE updates are Qed & Replayed when ALL pass
//     & OVERWRITE updates do NOT FAIL
int updateRow(cli *c, uc_t *uc, aobj *opk, void *orow, bool isr) {
    //printf("START: updateRow: orow: %p\n", orow);
    r_tbl_t *rt     = &Tbl[uc->tmatch];
    INIT_CR(uc->tmatch, uc->ncols) /* holds values written to new ROW */
    INIT_COL_AVALS      /* merges values in update_string and vals from ROW */
    uchar osflags[uc->ncols]; bzero(osflags, uc->ncols);
    sds      tkn    = NULL;
    uchar   *nrow   = NULL; /* B4 GOTO */
    //TODO LUATRIGGER tables can do OVWR w/ split up add/delIndexes
    bool     ovrwr  = NORM_BT(uc->btr) && !rt->nltrgr;
    bool     lodlt  = 0, cq = 0;
    int      ret    = -1;    /* presume failure */
    for (int i = 0; i < cr.ncols; i++) { /* 1st LOOP Perform Ops */
        uchar ctype = rt->col[i].type;
        DECLARE_ICOL(ic, i)
        if        (rt->lrud && rt->lruc == i) { // ALWAYS updated
            avs[i] = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
            if (avs[i].empty) ovrwr      = 0; // makes updateLRU not recurse
            else              osflags[i] = getLruSflag();/* Overwrite LRU */
        } else if (rt->lfu  && rt->lfuc == i) { // ALWAYS updated
            avs[i] = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
            if (avs[i].empty) ovrwr      = 0; // makes updateLFU not recurse
            else              osflags[i] = getLfuSflag();/* Overwrite LFU */
        } else if (!uc->ue[i].yes && !uc->le[i].yes &&
                    uc->chit[i].cmatch != -1) { // from UPDATE_VALUE_LIST
            //printf("%d: NORMAL\n", i);
            if C_IS_O(ctype) lodlt = 1;
            if OVRWR(i, ovrwr, ctype) {
                aobj a = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
                if (!aobj_sflag(&a, &osflags[i])) ovrwr = 0;
            } else ovrwr = 0;
            char *endptr  = NULL;
            if (!C_IS_S(ctype) && !C_IS_O(ctype)) {
                tkn = sdsnewlen(uc->vals[i], uc->vlens[i]);  // FREE 137
            }
            if        C_IS_I(ctype) {
                ulong l = strtoul(tkn, &endptr, 10); // OK: DELIM:[\ ,=,\0]
                if (l >= TWO_POW_32) { addReply(c, shared.u2big);       UP_ERR }
                initAobjInt(&avs[i], l);
            } else if C_IS_L(ctype) {
                ulong l = strtoul(tkn, &endptr, 10); // OK: DELIM:[\ ,=,\0]
                initAobjLong(&avs[i], l);
            } else if C_IS_X(ctype) {
                uint128 x;
                if (!parseU128(tkn, &x)) {           // invalid U128
                    addReply(c, shared.updatesyntax);                   UP_ERR
                } else initAobjU128(&avs[i], x);
            } else if C_IS_F(ctype) {
                float f = atof(tkn);                 // OK: DELIM: [\ ,=,\0]
                initAobjFloat(&avs[i], f);
            } else if C_IS_S(ctype) { // ignore \' delims
                initAobjString(&avs[i], uc->vals[i] + 1, uc->vlens[i] - 2);
            } else if C_IS_O(ctype) {
                initAobjString(&avs[i], uc->vals[i],     uc->vlens[i]);
            } else assert(!"updateRow parse ERROR");
            sdsfree(tkn); tkn = NULL;                    // FREED 137
        } else if (uc->ue[i].yes) { /* SIMPLE UPDATE EXPR */
            //printf("%d: UE\n", i);
            avs[i] = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
            if (avs[i].empty) { addReply(c, shared.up_on_mt_col);       UP_ERR }
            if (!OVRWR(i, ovrwr, ctype) ||
                !aobj_sflag(&avs[i], &osflags[i])) ovrwr = 0;
            if (!evalExpr(c, &uc->ue[i], &avs[i], ctype))               UP_ERR
        } else if (uc->le[i].yes) { /* LUA UPDATE EXPR */
            //printf("%d: LUE\n", i);
            if (uc->chit[i].nlo) { cq = 1;
                if (!evalLuaExpr(c, &uc->chit[i], uc, opk, orow,
                                 &avs[i]))                              UP_ERR
            } else { // NEXT 3 lines are ONLY to set "ovrwr"
                if C_IS_O(ctype) lodlt = 1;
                avs[i] = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
                if (avs[i].empty) { addReply(c, shared.up_on_mt_col);   UP_ERR }
                if (!OVRWR(i, ovrwr, ctype) ||
                    !aobj_sflag(&avs[i], &osflags[i])) ovrwr = 0;
                if (!evalLuaExpr(c, &uc->chit[i], uc, opk, orow,
                                 &avs[i]))                              UP_ERR
            }
        } else { // Not In UPDATE_SET_LIST -> MISS
            //printf("%d: MISS\n", i);
            avs[i] = getCol(uc->btr, orow, ic, opk, uc->tmatch, NULL);
        }
    }
    for (int i = 0; i < cr.ncols; i++) { /* 2nd Write Columns to Aobjs[] */
        uchar ctype = rt->col[i].type;
        int  nclen  = 0;
        if (i) { /* NOT PK, populate cr values (PK not stored in row)*/
            if (rt->lrud && rt->lruc == i) { // NOTE: updateLRU (UPDATE_1)
                nclen = cLRUcol(getLru(uc->tmatch), &crd[i].iflags,
                                &(crd[i].icols));
            } else if (rt->lfu && rt->lfuc == i) { //NOTE: updateLFU (UPDATE_1)
                avs[i].l = getLfu(avs[i].l);
                nclen = cLFUcol(avs[i].l, &crd[i].iflags, &(crd[i].icols));
            } else if C_IS_I(ctype) { //TODO push empty logic into cr8*()
                if (avs[i].empty) { crd[i].iflags = 0; nclen = 0; }
                else nclen = cr8Icol(avs[i].i, &crd[i].iflags, &(crd[i].icols));
            } else if C_IS_L(ctype) {
                if (avs[i].empty) { crd[i].iflags = 0; nclen = 0; }
                else nclen = cr8Lcol(avs[i].l, &crd[i].iflags, &(crd[i].icols));
            } else if C_IS_X(ctype) {
                if (avs[i].empty) nclen = 0;
                else              nclen = cr8Xcol(avs[i].x, &(crd[i].xcols));
            } else if C_IS_F(ctype) {
                if (avs[i].empty) nclen = 0;
                else              { crd[i].fcols = avs[i].f; nclen = 4; }
                crd[i].fflags = nclen ? 1 : 0;
            } else if (C_IS_S(ctype)) {
                crd[i].strs  = avs[i].s; nclen = avs[i].len;
                crd[i].slens = nclen;
            } else if (C_IS_O(ctype)) {
                if (uc->chit[i].nlo) {
                    if (!avs[i].empty) { cq = 1;
                        lua_getglobal(server.lua, "queue_CQ_Json");
                        pushAobjLua(&avs[i], avs[i].type);
                        int ret = DXDB_lua_pcall(server.lua, 1, 0, 0);
                        if (ret) {
                            ADD_REPLY_FAILED_LUA_STRING_CMD("queue_CQ_Json")
                            UP_ERR
                        }
                    }
                } else {
                    crd[i].strs  = avs[i].s; nclen = avs[i].len;
                    crd[i].slens = nclen;
                } 
            } else assert(!"updateRow create-column ERROR");
        }
        crd[i].empty    = nclen ? 0 : 1;
        if C_IS_O(ctype) nclen = 0; // LUAOBJs are saved inLua, not inRow
        crd[i].mcofsts  = cr.rlen + nclen;
        //printf("empty: %d mcofst: %d\n", crd[i].empty, crd[i].mcofsts);
        cr.rlen        += nclen;                             //DEBUG_UPDATE_ROW
    }

    if (ovrwr) if (lodlt || cq) ovrwr = 0; //TODO fix this dependency

    if (ovrwr) { /* only OVERWRITE if all OLD and NEW sflags match */
        for (int i = 1; i < cr.ncols; i++) {
            if (osflags[i] && osflags[i] != crd[i].iflags) { ovrwr = 0; break; }
    }}
    if (ovrwr) { /* just OVERWRITE INTS & LONGS -> CAN NOT FAIL */
        ret = updateOverwrite(c, uc, &cr, crd, osflags, opk, orow);
    } else {                                      //printf("NEW ROW UPDATE\n");
        int oncols = cr.ncols;
        int k; for (k = cr.ncols - 1; k >= 0; k--) { if (!crd[k].empty) break; }
        cr.ncols = k + 1; // starting from the right, only write FILLED columns
        if (rt->lrud && rt->lruc >= cr.ncols) cr.ncols = rt->lruc + 1; // 2 LRUC
        if (rt->lfu  && rt->lfuc >= cr.ncols) cr.ncols = rt->lfuc + 1; // 2 LFUC
        if (XKEY(uc->btr)) { XX_CRR.val = avs[1].x; nrow = (void *)&XX_CRR; }
        else {
            nrow = UKEY(uc->btr) ? VOIDINT  avs[1].i :
                   LKEY(uc->btr) ? (uchar *)avs[1].l : 
                   writeRow(c, opk, uc->tmatch, &cr, crd);
            if (!nrow)                                                   UP_ERR
        }
        aobj    *npk = cloneAobj(&avs[0]);                  // FREE 168
        ret = isr ?
                queueUpdate(c, uc, opk, orow, npk, nrow, lodlt, cq, oncols) :
                runUpdate  (c, uc, opk, orow, npk, nrow, lodlt, cq, oncols);
    }

up_end:
    if (tkn) sdsfree(tkn);                               // FREED 137
    DESTROY_COL_AVALS
    return ret;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
void pushColumnLua(bt *btr, uchar *orow, int tmatch, aobj *a, aobj *apk) {
    aobj acol; initAobj(&acol); int ctype = COL_TYPE_NONE;
    printf("pushColumnLua: type: %d a: ", a->type); dumpAobj(printf, a);
    if        C_IS_C(a->type) {
        int cmatch = a->ic ? a->ic->cmatch : (int)a->i;
        assert(tmatch != -1 && cmatch != -1);
        ctype = Tbl[tmatch].col[cmatch].type;
        if C_IS_O(ctype) {
            if (a->ic) pushLuaVar(tmatch, *a->ic, apk);
            else { 
                DECLARE_ICOL(ic, a->i); pushLuaVar(tmatch, ic, apk);
            }
            return;
        }
        DECLARE_ICOL(ic, a->i);
        acol  = getCol(btr, orow, ic, apk, tmatch, NULL);
    } else if C_IS_O(a->type) {
        sds vname = sdsnewlen(a->s, a->len);
        printf("pushColumnLua: VARIABLE: C_IS_O: vname: %s\n", vname);
        lua_getglobal(server.lua, vname); sdsfree(vname); return;
    } else if C_IS_L(a->type) {
        ctype = COL_TYPE_LONG;   initAobjLong (&acol, a->l);
    } else if C_IS_F(a->type) {
        ctype = COL_TYPE_FLOAT;  initAobjFloat(&acol, a->f);
    } else if C_IS_S(a->type) {
        ctype = COL_TYPE_STRING; initAobjFromStr(&acol, a->s, a->len,
                                                           COL_TYPE_STRING);
    } else assert(!"pushColumnLua UNDEFINED TYPE");
printf("pushColumnLua: acol: "); dumpAobj(printf, &acol);
    pushAobjLua(&acol, ctype); releaseAobj(&acol);
}

static bool evalLuaExpr(cli *c,    icol_t *ic, uc_t *uc, aobj *apk,
                       void *orow, aobj *aval) {
    r_tbl_t *rt = &Tbl   [uc->tmatch];
    lue_t   *le = &uc->le[ic->cmatch];
    int ctype = rt->col[ic->cmatch].type;
    CLEAR_LUA_STACK
    int nargs = le->ncols; int retargs = 1;
    if C_IS_O(ctype) { // runs LuaExpr and stores it in Lua:ColumnQueue[]
        lua_getglobal(server.lua, "queue_CQ_Function"); nargs++; retargs = 0;
    }
    lua_getglobal(server.lua, le->fname);
    for (int i = 0; i < le->ncols; i++) {
        pushColumnLua(uc->btr, orow, uc->tmatch, le->as[i], apk);
    }
printf("evalLuaExpr: fname: %s ncols: %d ctype: %d nargs: %d retargs: %d\n", le->fname, le->ncols, ctype, nargs, retargs);
    int ret = DXDB_lua_pcall(server.lua, nargs, retargs, 0);
    if (ret) { ADD_REPLY_FAILED_LUA_STRING_CMD(le->fname) return 0; }
    if (retargs) {
        //NOTE: C_IS_X() disallowed
        if        C_IS_I(ctype) aval->i = (uint32)lua_tointeger(server.lua, -1);
          else if C_IS_L(ctype) aval->l = (ulong) lua_tointeger(server.lua, -1);
          else if C_IS_F(ctype) aval->f = (float) lua_tonumber (server.lua, -1);
          else if C_IS_S(ctype) {
            size_t len;
            char *s   = (char *)lua_tolstring(server.lua, -1, &len);
            aval->len = (uint32)len; 
            if (s) { aval->s = _strdup(s); aval->freeme = 1; }
        } else if C_IS_O(ctype) {
            initAobjFromLuaResponse(aval, ic, uc->tmatch, 0);
            printf("evalLuaExpr: C_IS_0\n"); dumpAobj(printf, aval);
        } else assert(!"evalLuaExpr ERROR 2"); //TODO LOG
    }
    CLEAR_LUA_STACK //printf("evalLuaExpr: a: "); dumpAobj(printf, aval);
    return 1;
}

static bool evalExpr(cli *c, ue_t *ue, aobj *aval, uchar ctype) {
    //printf("evalExpr: aval: "); dumpAobj(printf, aval);
    //NOTE: C_IS_X() disallowed
    if (C_IS_NUM(ctype)) { /* INT & LONG */
        ulong l      = 0; double f = 0.0;
        bool  is_f   = (ue->type == UETYPE_FLT);
        if (is_f) f  = atof(ue->pred);              /* OK: DELIM: [\ ,\,,\0] */
        else      l  = strtoul(ue->pred, NULL, 10); /* OK: DELIM: [\ ,\,,\0] */
        if ((ue->op == DIVIDE || ue->op == MODULO)) {
            if (is_f ? (f == 0.0) : (l == 0)) {
                addReply(c, shared.update_expr_div_0);          return 0;
            }
        }
        if (is_f && f < 0.0) { addReply(c, shared.neg_on_uint); return 0; }
        ulong m   = C_IS_I(ctype) ? aval->i : aval->l;
        ulong dlt = (is_f ? (ulong)f : l);
        if        (ue->op == PLUS)   {
            if ((C_IS_L(ctype) && (ULONG_MAX - dlt) < m) ||
                (C_IS_I(ctype) && (UINT_MAX  - dlt) < m)) {
                    addReply(c, shared.overflow);               return 0;
            }
            m += dlt;
        } else if (ue->op == MINUS)  {
            if (dlt > m) { addReply(c, shared.overflow);        return 0; }
            m -= dlt;
        } else if (ue->op == MODULO) {  m %= dlt;
        } else if (ue->op == DIVIDE) {
            if (ue->type == UETYPE_FLT) m = (ulong)((double)m / f);
            else                        m = m / l;
        } else if (ue->op == MULT) {
            if (ue->type == UETYPE_FLT) m = (ulong)((double)m * f);
            else {
                if (C_IS_L(ctype) && l > 0 && m > UINT_MAX / l) {
                    addReply(c, shared.u2big);                  return 0;
                }
                m = m * l;
            }
        } else {          /* POWER */
            if (ue->type == UETYPE_INT) f = (float)l;
            errno    = 0;
            double d = powf((float)m, f);
            if ((errno != 0) || (C_IS_L(ctype) && d > (double)ULONG_MAX)) {
                addReply(c, shared.u2big);                      return 0;
            }
            m = (ulong)d;
        }
        if (C_IS_I(ctype)) {
            if (m >= UINT_MAX) { addReply(c, shared.u2big);     return 0; }
            aval->i = m;
        } else aval->l = m; // LONG
    } else if (C_IS_F(ctype)) {
        double m;
        float  f = atof(ue->pred);                  /* OK: DELIM: [\ ,\,,\0] */
        if (ue->op == DIVIDE && f == (float)0.0) {
            addReply(c, shared.update_expr_div_0);              return 0;
        }
        errno = 0; /* overflow detection initialisation */
        if      (ue->op == PLUS)    m = aval->f + f;
        else if (ue->op == MINUS)   m = aval->f - f;
        else if (ue->op == MULT)    m = aval->f * f;
        else if (ue->op == DIVIDE)  m = aval->f / f;
        else            /* POWER */ m = powf(aval->f, f);
        if (errno != 0) {
            addReply(c, shared.update_expr_float_overflow);     return 0;
        }
        double d = m;
        if (d < 0.0) d *= -1;
        if (d < FLT_MIN || d > FLT_MAX) {
            addReply(c, shared.update_expr_float_overflow);     return 0;
        }
        aval->f = (float)m;
    }
    return 1;
}

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpRow(printer *prn, bt *btr, void *rrow, aobj *apk, int tmatch) {
   DECLARE_ICOL(ic, -1)
   for (int j = 0; j < Tbl[tmatch].col_count; j++) {
       ic.cmatch = j;
       aobj acol = getSCol(btr, rrow, ic, apk, tmatch, NULL);
       dumpAobj(prn, &acol);
       releaseAobj(&acol);
   }
}

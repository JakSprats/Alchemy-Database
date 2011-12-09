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

#include "redis.h"
#include "lzf.h"

#include "hash.h"
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

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

extern uchar    OutputMode; // NOTE: used by OREDIS

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

typedef robj *row_outputter(bt   *btr,       void *rrow, int   qcols,
                            int   cmatchs[], aobj *apk,  int   tmatch);

/* PROTOYPES */
static void initAobjCol2S(aobj *a, ulong l, uint128 x, float f, int cmatch,
                          int   ktype);
static void initIntAobjFromVal  (aobj *a, uint32 i,  bool force_s, int cmatch);
static void initLongAobjFromVal (aobj *a, ulong l,   bool force_s, int cmatch);
static void initU128AobjFromVal (aobj *a, uint128 x, bool force_s, int cmatch);
static void initFloatAobjFromVal(aobj *a, float f,   bool force_s, int cmatch);
static aobj colFromUU(ulong   key, bool force_s, int cmatch);
static aobj colFromUL(ulong   key, bool force_s, int cmatch);
static aobj colFromLU(ulong   key, bool force_s, int cmatch);
static aobj colFromLL(ulong   key, bool force_s, int cmatch);
static aobj colFromXX(uint128 key, bool force_s, int cmatch);
static bool evalExpr   (cli *c, ue_t  *ue, aobj *aval, uchar ctype);
static bool evalLuaExpr(cli  *c,    int cmatch, uc_t *uc, aobj *apk,
                        void *orow, aobj *aval);

/* CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW */
typedef struct create_row_ctrl {
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

static bool contains_text_col(int tmatch, int ncols) {
    for (int i = 1; i < ncols; i++) {       /* sixzip needs a string */
        if (C_IS_S(Tbl[tmatch].col[i].type)) return 1;
    }
    return 0;
}
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
static uchar *writeRow(cr_t *cr, crd_t *crd) {          //printf("writeRow\n");
    cz_t cz; czd_t czd[cr->ncols]; init_cz(&cz, cr);
    uchar *row; uint32 mlen = 0; // compiler warning
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
        } else {//C_IS_S()
            if        (cz.type == CZIP_SIX) {
                memcpy(row, czd[k].sixs, czd[k].sixl); row += czd[k].sixl; k++;
            } else if (cz.type == CZIP_LZF) {
                row = writeLzfCol(row, czd, k);                            k++;
            } else {
                memcpy(row, crd[i].strs, crd[i].slens); row += crd[i].slens;
            }
        }
    }
    destroy_cz(&cz, czd);
    return orow;
}
#define DEBUG_CREATE_ROW                                                       \
  if (!crd[i].empty) {                                                         \
    printf("nclen: %d rlen: %d c[%d]: %d", nclen, cr.rlen, i, crd[i].mcofsts); \
    if C_IS_NUM(ctype) printf(" iflgs: %d c: %u", crd[i].iflags, crd[i].icols);\
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
static xxk XX_CRR; static lxk LX_CRR; static xlk XL_CRR;
                   static uxk UX_CRR; static xuk XU_CRR;
/*NOTE: OTHER_BT rows no malloc, 'void *' of 1st COL */
static void *OBT_createRow(bt *btr, int tmatch, char *vals, twoint cofsts[]) {
    char uubuf[64];
    r_tbl_t *rt    = &Tbl[tmatch];
    int      c1len = (cofsts[1].j - cofsts[1].i);
    memcpy(uubuf, vals + cofsts[1].i, c1len); uubuf[c1len] = '\0';
    if        (C_IS_X(rt->col[1].type)) { // UX,LX,XX
        uint128 *pbu = (XX(btr) ? &XX_CRR.val :
                       (LX(btr) ? &LX_CRR.val : /* UX */  &UX_CRR.val));
        bool     r   = parseU128(uubuf, pbu);
        return r ? (XX(btr) ? &XX_CRR : (LX(btr) ? &LX_CRR : &UX_CRR)) : NULL;
    } else if (C_IS_X(rt->col[0].type)) { // XU, XL
        ulong l = strtoul(uubuf, NULL, 10); /* OK: DELIM: \0 */
        if XU(btr) XU_CRR.val = l; else /* XL */ XL_CRR.val = l;
        return XU(btr) ? &XU_CRR : /* XL */ &XL_CRR;
    } else return (void *)strtoul(uubuf, NULL, 10); /* OK: DELIM: \0 */
}
void *createRow(cli    *c,    bt     *btr,      int tmatch, int  ncols,
                char   *vals, twoint  cofsts[])                         {
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
                if (C_IS_S(ctype)) {
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
            if        (C_IS_S(ctype)) { /* dont store \' delimiters */
                crd[i].strs++; crd[i].slens -= 2; nclen = crd[i].slens;
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
            }
            cr.rlen        += nclen;
            crd[i].mcofsts  = (int)cr.rlen;                 //DEBUG_CREATE_ROW
        }
    }
    return writeRow(&cr, crd);
}

/* GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL */
#define DEBUG_GET_RAW_COL \
printf("getRawCol: orow: %p tmatch: %d cmatch: %d force_s: %d apk: ", \
        orow, tmatch, cmatch, force_s); dumpAobj(printf, apk);

#define OBT_RAWC(kt, kcast, aobjpart, cf_func)            \
  { kt key = cmatch ? ((kcast *)orow)->val : apk->aobjpart; \
    return cf_func(key, force_s, cmatch); }
#define OBTXY_RAWC(vt, vcast, v_cf_func, kt, kaobjpart, k_cf_func)        \
        if (cmatch) {                                                        \
            vt key = ((vcast *)orow)->val;                                   \
            return v_cf_func(key, force_s, cmatch);                          \
        } else {                                                             \
            kt key = apk->kaobjpart; return k_cf_func(key, force_s, cmatch); \
        }

static aobj OBT_getRawCol(bt   *btr, void *orow, int cmatch,
                          aobj *apk, bool  force_s) {
    if        UU(btr) { /* OTHER_BT values are either in PK or BT -> no ROW */
        ulong   key = cmatch ? (ulong)orow        : apk->i;
        return colFromUU(key, force_s, cmatch);
    } else if UL(btr) OBT_RAWC(ulong,   ulk, i, colFromUL)
      else if LU(btr) OBT_RAWC(ulong,   luk, l, colFromLU)
      else if LL(btr) OBT_RAWC(ulong,   llk, l, colFromLL)
      else if XX(btr) OBT_RAWC(uint128, xxk, x, colFromXX)
      else if UX(btr) OBTXY_RAWC(uint128, uxk, colFromXX, ulong,   i, colFromUU)
      else if XU(btr) OBTXY_RAWC(ulong,   xuk, colFromUU, uint128, x, colFromXX)
      else if LX(btr) OBTXY_RAWC(uint128, lxk, colFromXX, ulong,   l, colFromLL)
      else if XL(btr) OBTXY_RAWC(ulong,   xlk, colFromLL, uint128, x, colFromXX)
}

//TODO RawCols[] is not worth the complexity, use malloc()
#define RAW_COL_BUF_SIZE 256
static uchar *getHashFromRow(uchar *row) {
    uint32_t clen;
                                 row++;       // SKIP rflag
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
aobj getRawCol(bt  *btr,    uchar *orow, int cmatch, aobj *apk,
               int  tmatch, bool  force_s) {
    if (OTHER_BT(btr)) return OBT_getRawCol(btr, orow, cmatch, apk, force_s);
    aobj a; initAobj(&a); //DEBUG_GET_RAW_COL
    if (cmatch == -1) return a; // NOTE: used for HASHABILITY miss
    r_tbl_t *rt    = &Tbl[tmatch];
    uchar    ctype = rt->col[cmatch].type;
    if (!cmatch) { /* PK stored ONLY in KEY not in ROW, echo it */
        if (ctype != COL_TYPE_STRING && !force_s) return *apk;
        else { initStringAobjFromAobj(&a, apk);   return a; }
    }
    uint32 clen; uchar rflag;
    uchar *data  = getColData(orow, cmatch, &clen, &rflag);
    if (!clen) a.empty = 1;
    else {
        if        (C_IS_I(ctype)) {
            uint32  i    = streamIntToUInt(data, &clen);
            initIntAobjFromVal(&a, i, force_s, cmatch);
        } else if (C_IS_L(ctype)) {
            ulong   l    = streamLongToULong(data, &clen);
            initLongAobjFromVal(&a, l, force_s, cmatch);
        } else if (C_IS_X(ctype)) {
            uint128 x    = streamToU128(data, &clen);
            initU128AobjFromVal(&a, x, force_s, cmatch);
        } else if (C_IS_F(ctype)) {
            float   f    = streamFloatToFloat(data, &clen);
            initFloatAobjFromVal(&a, f, force_s, cmatch);
        } else {/* COL_TYPE_STRING */
            a.type       = a.enc = COL_TYPE_STRING;
            if       (rflag & RFLAG_6BIT_ZIP) {
                a.s      = (char *)unpackSixBit(data, &clen);
                a.freeme = 1;
            } else if (rflag & RFLAG_LZF_ZIP) {
                a.s      = streamLZFTextToString(data, &clen);
                a.freeme = 1;                                /* FREED 035 */
            } else a.s   = (char *)data; /* NO ZIP -> uncompressed text */
            a.len  = clen;
        }
    }
    return a;
}
inline aobj getCol(bt *btr, uchar *rrow, int cmatch, aobj *apk, int tmatch) {
    return getRawCol(btr, rrow, cmatch, apk, tmatch, 0);
}
inline aobj getSCol(bt *btr, uchar *rrow, int cmatch, aobj *apk, int tmatch) {
    return getRawCol(btr, rrow, cmatch, apk, tmatch, 1);
}

static char RawCols[RAW_COL_BUF_SIZE][64]; /* NOTE: avoid malloc's */
static void initAobjCol2S(aobj *a, ulong l, uint128 x, float f, int cmatch,
                          int   ktype) {
    char *dest; char *fmt = C_IS_F(ktype) ? FLOAT_FMT : "%lu";
    if (cmatch < RAW_COL_BUF_SIZE) dest = RawCols[cmatch];
    else                           { dest = malloc(64); a->freeme = 1; }
    if      C_IS_X(ktype) SPRINTF_128(dest, 64,      x)
    else if C_IS_F(ktype) snprintf   (dest, 64, fmt, f);
    else    /* [I,L] */   snprintf   (dest, 64, fmt, l);
    a->len = strlen(dest); a->s   = dest; a->enc = COL_TYPE_STRING;
    //printf("initAobjCol2S: a: "); dumpAobj(printf, a);
}
static void initIntAobjFromVal(aobj *a, uint32 i, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_INT;
    ulong l = (ulong)i;
    if (force_s) initAobjCol2S(a, l, 0, 0.0, cmatch, COL_TYPE_INT);
    else         { a->i = i; a->enc = COL_TYPE_INT; }
}
static void initLongAobjFromVal(aobj *a, ulong l, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_LONG;
    if (force_s) initAobjCol2S(a, l, 0, 0.0, cmatch, COL_TYPE_LONG);
    else         { a->l = l; a->enc = COL_TYPE_LONG; }
}
static void initU128AobjFromVal(aobj *a, uint128 x, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_U128;
    if (force_s) initAobjCol2S(a, 0, x, 0.0, cmatch, COL_TYPE_U128);
    else         { a->x = x; a->enc = COL_TYPE_U128; }
}
static void initFloatAobjFromVal(aobj *a, float f, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_FLOAT;
    if (force_s) initAobjCol2S(a, 0, 0, f,   cmatch, COL_TYPE_FLOAT);
    else         { a->f = f; a->enc = COL_TYPE_FLOAT; }
}
static aobj colFromUU(ulong key, bool force_s, int cmatch) {
    int cval = (int)((long)key % UINT_MAX);
    aobj a; initIntAobjFromVal(&a, cval, force_s, cmatch); return a;
}
static aobj colFromUL(ulong key, bool force_s, int cmatch) {
    aobj a; 
    if (cmatch) initLongAobjFromVal(&a, key, force_s, cmatch);
    else        initIntAobjFromVal( &a, key, force_s, cmatch);
    return a;
}
static aobj colFromLU(ulong key, bool force_s, int cmatch) {
    aobj a;
    if (cmatch) initIntAobjFromVal(&a, key, force_s, cmatch);
    else        initLongAobjFromVal(&a, key, force_s, cmatch);
    return a;
}
static aobj colFromLL(ulong key, bool force_s, int cmatch) {
    aobj a; initLongAobjFromVal(&a, key, force_s, cmatch); return a;
}
static aobj colFromXX(uint128 key, bool force_s, int cmatch) {
    aobj a; initU128AobjFromVal(&a, key, force_s, cmatch); return a;
}

/* OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT */
static void destroy_erow(erow_t *er) { //printf("destroy_embedded_row\n");
    for (int i = 0; i < er->ncols; i++) destroyAobj(er->cols[i]);
    free(er->cols);
    free(er);
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
    } else if (OREDIS) addReply(c,     r);
      else             addReplyBulk(c, r);
    return 1;
}

robj *EmbeddedRobj = NULL;
static robj *orow_embedded(bt   *btr,       void *rrow, int   qcols,
                           int   cmatchs[], aobj *apk,  int   tmatch) {
    robj *r = EmbeddedRobj;
    if      (!r)           r           = createObject(REDIS_STRING, NULL);
    else if (!r->refcount) r->refcount = 1;
    erow_t *er = malloc(sizeof(erow_t));
    er->ncols  = qcols;
    er->cols   = malloc(sizeof(aobj *) * er->ncols);
    for (int i = 0; i < er->ncols; i++) {
        aobj  acol  = getCol(btr, rrow, cmatchs[i], apk, tmatch);
        er->cols[i] = cloneAobj(&acol); releaseAobj(&acol);
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
static robj *orow_redis(bt   *btr,       void *rrow, int   qcols,
                        int   cmatchs[], aobj *apk,  int   tmatch) {
    char pbuf[128];
    sl_t outs[qcols];
    uint32 prelen = output_start(pbuf, 128, qcols);
    uint32 totlen = prelen;
    for (int i = 0; i < qcols; i++) {
        aobj  acol  = getCol(btr, rrow, cmatchs[i], apk, tmatch);
        outs[i]     = outputReformat(&acol);
        releaseAobj(&acol);
        totlen     += outs[i].len;
    }
    return write_output_row(qcols, prelen, pbuf, totlen, outs);
}
static robj *orow_normal(bt   *btr,       void *rrow, int   qcols,
                         int   cmatchs[], aobj *apk,  int   tmatch) {
    sl_t   outs[qcols];
    uint32 totlen = 0;
    for (int i = 0; i < qcols; i++) {
        aobj  col       = getSCol(btr, rrow, cmatchs[i], apk, tmatch);
        outs[i].s       = col.s;
        outs[i].len     = col.len;
        outs[i].freeme  = col.freeme;
        outs[i].type    = (cmatchs[i] == -1) ? COL_TYPE_NONE :
                                               Tbl[tmatch].col[cmatchs[i]].type;
        totlen         += col.len;
        if (C_IS_S(outs[i].type) && outs[i].len) totlen += 2;/* 2 \'s per col */
    }
    totlen  += (uint32)qcols - 1; /* one comma per COL, except final */
    return write_output_row(qcols, 0, NULL, totlen, outs);
}
robj *outputRow(bt   *btr,       void *rrow, int qcols,
                int   cmatchs[], aobj *apk,  int tmatch) {
    row_outputter *rop =  (EREDIS) ? orow_embedded :
                         ((OREDIS) ? orow_redis    :
                                     orow_normal);
    return (*rop)(btr, rrow, qcols, cmatchs, apk, tmatch);
}

/* DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW DELETE_ROW */
bool deleteRow(int tmatch, aobj *apk, int matches, int inds[]) {
    bt   *btr  = getBtr(tmatch);
    void *rrow = btFind(btr, apk);
    if (!rrow) return 0;
    if (matches) { /* delete indexes */
        for (int i = 0; i < matches; i++) delFromIndex(btr, apk, rrow, inds[i]);
    }
    btDelete(btr, apk); server.dirty++; return 1;
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
static void init_lue(lue_t *le)    { bzero(le, sizeof(lue_t)); }
static void release_lue(lue_t *le) {
    if (le->cmatchs) free(le->cmatchs); // FREED 096
}
void init_uc(uc_t  *uc,     bt     *btr, 
             int    tmatch, int     ncols,   int    matches, int   inds[],
             char  *vals[], uint32  vlens[], uchar  cmiss[], ue_t  ue[],
             lue_t *le) {
    uc->btr    =   btr;
    uc->tmatch = tmatch; uc->ncols = ncols; uc->matches = matches;
    uc->inds   = inds;   uc->vals  = vals;  uc->vlens   = vlens;
    uc->cmiss  = cmiss;  uc->ue    = ue;    uc->le      = le;
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

static bool upEffctdInds(cli  *c,     bt   *btr,  aobj *apk,     void *orow,
                         aobj  avs[], void *nrow, int   matches, int   inds[],
                         uchar cmiss[]) {
    for (int i = 0; i < matches; i++) { /* Redo ALL EFFECTED indexes */
        bool up = 0;
        r_ind_t *ri = &Index[inds[i]];
        if      (ri->lru) up = 1; /* ALWAYS update LRU Index */
        else if (ri->lfu) up = 1; /* ALWAYS update LFU Index */
        else if (ri->clist) {
            for (int i = 0; i < ri->nclist; i++) {
                if (!cmiss[ri->bclist[i]]) { up = 1; break; }
            }
        } else { up = ri->luat ? 1 : !cmiss[ri->column]; }
        if (up) {
            if (!updateIndex(c, btr, apk, orow, &avs[0], nrow, inds[i])) {
                return 0;
            }
        }
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
#define DEBUG_UPDATE_ROW                                                     \
  if (i) { printf ("%d: oflag: %d cmiss: %d ", i, osflags[i], uc->cmiss[i]); \
    DEBUG_CREATE_ROW printf("\t\t\t\t"); dumpAobj(printf, &avs[i]); }

#define OVRWR(i, ovrwr, ctype) /*NOTE: !indexed -> upIndex uses orow & nrow */ \
  (i && ovrwr && C_IS_NUM(ctype) && !rt->col[i].indxd)

#define UP_ERR goto up_end;

int updateRow(cli *c, uc_t *uc, aobj *apk, void *orow) {
    r_tbl_t *rt     = &Tbl[uc->tmatch];
    INIT_CR(uc->tmatch, uc->ncols) /* holds values written to new ROW */
    INIT_COL_AVALS      /* merges values in update_string and vals from ROW */
    uchar osflags[uc->ncols]; bzero(osflags, uc->ncols);
    uchar   *nrow   = NULL; /* B4 GOTO */
    bool     ovrwr  = NORM_BT(uc->btr);
    int      ret    = -1;    /* presume failure */
    for (int i = 0; i < cr.ncols; i++) { /* 1st loop UPDATE columns -> cr */
        uchar ctype = rt->col[i].type;
        if        (rt->lrud && rt->lruc == i) {
            avs[i] = getCol(uc->btr, orow, i, apk, uc->tmatch);
            if (avs[i].empty) ovrwr      = 0; // makes updateLRU not recurse
            else              osflags[i] = getLruSflag();/* Overwrite LRU */
        } else if (rt->lfu  && rt->lfuc == i) {
            avs[i] = getCol(uc->btr, orow, i, apk, uc->tmatch);
            if (avs[i].empty) ovrwr      = 0; // makes updateLFU not recurse
            else              osflags[i] = getLfuSflag();/* Overwrite LFU */
        } else if (uc->cmiss[i]) {
            avs[i] = getCol(uc->btr, orow, i, apk, uc->tmatch);
        } else if (uc->ue[i].yes) { // SIMPLE UPDATE EXPR
            avs[i] = getCol(uc->btr, orow, i, apk, uc->tmatch);
            if (avs[i].empty) { addReply(c, shared.up_on_mt_col);       UP_ERR }
            if (!OVRWR(i, ovrwr, ctype) ||
                !aobj_sflag(&avs[i], &osflags[i])) ovrwr = 0;
            if (!evalExpr(c, &uc->ue[i], &avs[i], ctype))               UP_ERR
        } else if (uc->le[i].yes) { // LUA UPDATE EXPR
            avs[i] = getCol(uc->btr, orow, i, apk, uc->tmatch);
            if (avs[i].empty) { addReply(c, shared.up_on_mt_col);       UP_ERR }
            if (!OVRWR(i, ovrwr, ctype) ||
                !aobj_sflag(&avs[i], &osflags[i])) ovrwr = 0;
            if (!evalLuaExpr(c, i, uc, apk, orow, &avs[i]))             UP_ERR
        } else { /* comes from UPDATE VALUE LIST (no expression) */
            if OVRWR(i, ovrwr, ctype) {
                aobj a = getCol(uc->btr, orow, i, apk, uc->tmatch);
                if (!aobj_sflag(&a, &osflags[i])) ovrwr = 0;
            } else ovrwr = 0;
            char *endptr  = NULL;
            //TODO all NUM()s are already error checked in getExprType()
            if        C_IS_I(ctype) {
                ulong l = strtoul(uc->vals[i], &endptr, 10);//OK:DELIM:[\ ,=,\0]
                if (endptr && !*endptr) { // valid ULONG
                    if (l >= TWO_POW_32) { addReply(c, shared.u2big);   UP_ERR }
                    initAobjInt(&avs[i], l);
                } else { addReply(c, shared.updatesyntax);              UP_ERR }
            } else if C_IS_L(ctype) {
                ulong l = strtoul(uc->vals[i], &endptr, 10);//OK:DELIM:[\ ,=,\0]
                if (endptr && !*endptr) initAobjLong(&avs[i], l); // valid ULONG
                else                { addReply(c, shared.updatesyntax); UP_ERR }
            } else if C_IS_X(ctype) {
                uint128 x;
                if (!parseU128(uc->vals[i], &x)) { // invalid U128
                    addReply(c, shared.updatesyntax);                   UP_ERR
                } else initAobjU128(&avs[i], x);
            } else if C_IS_F(ctype) {
                float f = atof(uc->vals[i]);              //OK: DELIM: [\ ,=,\0]
                initAobjFloat(&avs[i], f);
            } else {// S_IS_S()           /* ignore \' delimiters */
                initAobjString(&avs[i], uc->vals[i] + 1, uc->vlens[i] - 2);
            }
        }
        int nclen = 0;
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
            } else {/* COL_TYPE_STRING*/
                crd[i].strs  = avs[i].s; nclen = avs[i].len;
                crd[i].slens = nclen;
            } 
        }
        crd[i].empty    = nclen ? 0 : 1;
        crd[i].mcofsts  = cr.rlen + nclen;
        cr.rlen        += nclen;                             //DEBUG_UPDATE_ROW
    }
    if (ovrwr) { /* only OVERWRITE if all OLD and NEW sflags match */
        for (int i = 1; i < cr.ncols; i++) {
            if (osflags[i] && osflags[i] != crd[i].iflags) { ovrwr = 0; break; }
    }}
    if (ovrwr) { /* just OVERWRITE INTS & LONGS */ //printf("OVRWR\n");
        for (int i = 1; i < cr.ncols; i++) {
            if (osflags[i]) {
                uint32 clen; uchar rflag;
                uchar *data  = getColData(orow, i, &clen, &rflag);
                uchar  ctype = rt->col[i].type;
                if       (rt->lrud && rt->lruc == i) {// updateLRU (UPDATE_2)
                    // NOTE LRU is always 4 bytes, so orow will NOT change
                    updateLru(c, uc->tmatch, apk, data, rt->lrud);
                } else if (rt->lfu && rt->lfuc == i) { // updateLFU (UPDATE_2)
                    // NOTE LFU is always 8 bytes, so orow will NOT change
                    updateLfu(c, uc->tmatch, apk, data, rt->lfu);
                } else if C_IS_I(ctype) {
                    writeUIntCol(&data,  crd[i].iflags, crd[i].icols);
                } else if C_IS_L(ctype) {
                    writeULongCol(&data, crd[i].iflags, crd[i].icols);
                } else if C_IS_X(ctype) {
                    writeU128Col(&data, crd[i].xcols);
                }
            }
        }
        if (rt->nltrgr) { /* OVERWRITE still needs to run LUATRIGGERs */
            for (int i = 0; i < uc->matches; i++) {
                if (!Index[uc->inds[i]].luat) continue;
                if (!updateIndex(c, uc->btr, apk, orow, &avs[0],
                                 nrow, uc->inds[i]))                    UP_ERR
            }
        }
        ret = getNumKeyLen(apk) + getRowMallocSize(orow);
    } else {                                      //printf("NEW ROW UPDATE\n");
        int k; for (k = cr.ncols - 1; k >= 0; k--) { if (!crd[k].empty) break; }
        cr.ncols = k + 1; // starting from the right, only write FILLED columns
        if (rt->lrud && rt->lruc >= cr.ncols) cr.ncols = rt->lruc + 1; // 2 LRUC
        if (rt->lfu  && rt->lfuc >= cr.ncols) cr.ncols = rt->lfuc + 1; // 2 LFUC

        if (XKEY(uc->btr)) { XX_CRR.val = avs[1].x; nrow = &XX_CRR; }
        else {
            nrow = UKEY(uc->btr) ? VOIDINT  avs[1].i :
                   LKEY(uc->btr) ? (uchar *)avs[1].l : writeRow(&cr, crd);
        }
        if (!uc->cmiss[0]) { /* PK update */
            for (int i = 0; i < uc->matches; i++) { /* Redo ALL inds */
                if (!updateIndex(c, uc->btr, apk, orow, &avs[0],
                                 nrow, uc->inds[i]))                    UP_ERR
            }
            btDelete(uc->btr, apk);              /* DELETE row w/ OLD PK */
            ret = btAdd(uc->btr, &avs[0], nrow); /* ADD row w/ NEW PK */
            UPDATE_AUTO_INC(rt->col[0].type, avs[0])
        } else {
            if (!upEffctdInds(c, uc->btr, apk, orow, avs, nrow,
                              uc->matches, uc->inds, uc->cmiss))        UP_ERR
            ret = btReplace(uc->btr, apk, nrow); /* overwrite w/ new row */
        }
    }
    server.dirty++;

up_end:
    if (nrow && NORM_BT(uc->btr)) free(nrow);            /* FREED 023 */
    DESTROY_COL_AVALS
    return ret;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
static bool evalLuaExpr(cli *c,    int   cmatch, uc_t *uc, aobj *apk,
                       void *orow, aobj *aval) {
    r_tbl_t *rt = &Tbl[uc->tmatch];
    lue_t   *le = &uc->le[cmatch];
    lua_getglobal(server.lua, le->fname);
    for (int i = 0; i < le->ncols; i++) {
        int  cm    = le->cmatchs[i];
        aobj acol  = getCol(uc->btr, orow, cm, apk, uc->tmatch);
        int  ctype = rt->col[cm].type;
        //NOTE: C_IS_X() disallowed
        if        C_IS_I(ctype) {
            if (acol.empty) lua_pushnil    (server.lua);
            else            lua_pushinteger(server.lua, acol.i);
        } else if C_IS_L(ctype) {
            if (acol.empty) lua_pushnil    (server.lua);
            else            lua_pushinteger(server.lua, acol.l);
        } else if C_IS_F(ctype) {
            if (acol.empty) lua_pushnil   (server.lua);
            else            lua_pushnumber(server.lua, acol.f);
        } else {/* COL_TYPE_STRING */
            if (acol.empty) lua_pushnil   (server.lua);
            else            lua_pushlstring(server.lua, acol.s, acol.len);
        }
        releaseAobj(&acol);
    }
    int ret = lua_pcall(server.lua, le->ncols, 1, 0);
    if (ret) {
        sds err = sdscatprintf(sdsempty(),
                               "Error running LUA UPDATE (%s): %s\n",
                                le->fname, lua_tostring(server.lua, -1));
        addReplyErrorFormat(c, "%s", err);
        lua_pop(server.lua, 1);
        return 0;
    }
    int  ctype = rt->col[cmatch].type;
    //NOTE: C_IS_X() disallowed
    if      (C_IS_I(ctype)) aval->i = (uint32)lua_tointeger(server.lua, -1);
    else if (C_IS_L(ctype)) aval->l = (ulong) lua_tointeger(server.lua, -1);
    else if (C_IS_F(ctype)) aval->f = (float) lua_tonumber (server.lua, -1);
    else {// C_IS_S()
        size_t len;
        char *s   = (char *)lua_tolstring(server.lua, -1, &len);
        aval->len = (uint32)len; 
        if (s) { aval->s = _strdup(s); aval->freeme = 1; }
    } //printf("evalLuaExpr: aval: "); dumpAobj(printf, aval);
    lua_pop(server.lua, 1);
    return 1;
}

static bool evalExpr(cli *c, ue_t *ue, aobj *aval, uchar ctype) {
    //NOTE: C_IS_X() disallowed
    if (C_IS_NUM(ctype)) { /* INT & LONG */
        ulong l      = 0; double f = 0.0;
        bool  is_f   = (ue->type == UETYPE_FLT);
        if (is_f) f  = atof(ue->pred);              /* OK: DELIM: [\ ,\,,\0] */
        else      l  = strtoul(ue->pred, NULL, 10); /* OK: DELIM: [\ ,\,,\0] */
        if ((ue->op == DIVIDE || ue->op == MODULO)) {
            if (is_f ? (f == 0.0) : (l == 0)) {
                addReply(c, shared.update_expr_div_0); return 0;
            }
        }
        if (is_f && f < 0.0) { addReply(c, shared.neg_on_uint); return 0; }
        ulong m = C_IS_I(ctype) ? aval->i : aval->l;
        //TODO OVERFLOW and UNDERFLOW CHECKING
        if      (ue->op == PLUS)        m += (is_f ? (ulong)f : l);
        else if (ue->op == MINUS)       m -= (is_f ? (ulong)f : l);
        else if (ue->op == MODULO)      m %= (is_f ? (ulong)f : l);
        else if (ue->op == DIVIDE) {
            if (ue->type == UETYPE_FLT) m = (ulong)((double)m / f);
            else                        m = m / l;
        } else if (ue->op == MULT) {
            if (ue->type == UETYPE_FLT) m = (ulong)((double)m * f);
            else {
                if (C_IS_L(ctype) && l > 0 && m > (ulong)TWO_POW_64 / l) {
                    addReply(c, shared.u2big); return 0;
                }
                                        m = m * l;
            }
        } else {          /* POWER */
            if (ue->type == UETYPE_INT) f = (float)l;
            errno    = 0;
            double d = powf((float)m, f);
            if (errno != 0) { addReply(c, shared.u2big); return 0; }
            if (C_IS_L(ctype) && d > (double)TWO_POW_64) {
                addReply(c, shared.u2big); return 0;
            }
                                        m = (ulong)d;
        }
        if (C_IS_I(ctype)) {
            if (m >= TWO_POW_32) { addReply(c, shared.u2big); return 0; }
                            aval->i = m;
        } else { /* LONG */ aval->l = m; }
    } else if (C_IS_F(ctype)) {
        double m;
        float  f = atof(ue->pred);                  /* OK: DELIM: [\ ,\,,\0] */
        if (ue->op == DIVIDE && f == (float)0.0) {
            addReply(c, shared.update_expr_div_0); return 0;
        }
        errno = 0; /* overflow detection initialisation */
        if      (ue->op == PLUS)    m = aval->f + f;
        else if (ue->op == MINUS)   m = aval->f - f;
        else if (ue->op == MULT)    m = aval->f * f;
        else if (ue->op == DIVIDE)  m = aval->f / f;
        else            /* POWER */ m = powf(aval->f, f);
        if (errno != 0) {
            addReply(c, shared.update_expr_float_overflow); return 0;
        }
        double d = m;
        if (d < 0.0) d *= -1;
        if (d < FLT_MIN || d > FLT_MAX) {
            addReply(c, shared.update_expr_float_overflow); return 0;
        }
        aval->f = (float)m;
    }
    return 1;
}

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpRow(printer *prn, bt *btr, void *rrow, aobj *apk, int tmatch) {
   for (int j = 0; j < Tbl[tmatch].col_count; j++) {
       aobj acol = getSCol(btr, rrow, j, apk, tmatch);
       dumpAobj(prn, &acol);
       releaseAobj(&acol);
   }
}

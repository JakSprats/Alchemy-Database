/*
 * This file implements the rows of Alsosql
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

#include "sixbit.h"
#include "bt.h"
#include "index.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "row.h"

// FROM redis.c
extern struct redisServer server;

extern char    *OUTPUT_DELIM;
extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];

extern char PLUS;
extern char MINUS;
extern char MULT;
extern char DIVIDE;
extern char POWER;
extern char MODULO;
extern char STRCAT;

static uchar ucdum; /* dummy variable */
static flag  fdum;  /* dummy variable */

#define RFLAG_1BYTE_INT 1
#define RFLAG_2BYTE_INT 2
#define RFLAG_4BYTE_INT 4
#define RFLAG_SIZE_FLAG (RFLAG_1BYTE_INT+RFLAG_2BYTE_INT+RFLAG_4BYTE_INT)

#define RFLAG_6BIT_ZIP     8
#define RFLAG_LZF_ZIP     16

#define COL_1BYTE_INT 1
#define COL_2BYTE_INT 2
#define COL_4BYTE_INT 4
#define COL_5BYTE_INT 8

static bool evalExpr(redisClient *c, ue_t *ue, aobj *aval, uchar ctype);

#define INCR(x)     {x = x + 1;}
#define INCRBY(x,y) {x = x + y;}

//TODO break out next 10 funcs into formatcol.c
void sprintfOutputFloat(char *buf, int len, float f) {
    snprintf(buf, len, "%.10g", f);
    buf[len - 1] = '\0';
}

bool checkUIntReply(redisClient *c, long l, bool ispk) {
    if (l >= TWO_POW_32) {
        if (ispk) addReply(c, shared.uint_pk_too_big);
        else      addReply(c, shared.col_uint_too_big);
        return 0;
    } else if (l < 0) {
        if (ispk) addReply(c, shared.uint_no_negative_values);
        else      addReply(c, shared.col_uint_no_negative_values);
        return 0;
    }
    return 1;
}
static uint32 _cr8Icol(uint32 i, flag *sflag, uint32 *col) {
    if (i < TWO_POW_7) {
        *sflag = COL_1BYTE_INT;
        *col   = (i * 2) + 1;
        return 1;
    } else if (i < TWO_POW_14) {
        *sflag = COL_2BYTE_INT;
        *col   = (i * 4) + 2;
        return 2;
    } else if (i < TWO_POW_29) {
        *sflag = COL_4BYTE_INT;
        *col   = (i * 8) + 4;
        return 4;
    } else {
        *sflag = COL_5BYTE_INT;
        *col   = i;
        return 5;
    }
}
uint32 strToInt(redisClient *c, char *start, uint32 len, uint32 *i) {
    char buf[32];
    if (len >= 31) {
        addReply(c, shared.col_uint_string_too_long);
        return 0;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    long l   = atol(buf);
    if (!checkUIntReply(c, l, 0)) return 0;
    *i       = (int)l;
    return 4;
}
static uint32 cr8Icol(redisClient *c,
                      char        *start,
                      uint32       len,
                      flag        *sflag,
                      uint32      *col) {
    if (!strToInt(c, start, len, col)) return 0; /* sets col twice, but ok */
    return _cr8Icol(*col, sflag, col);           /* resets col from *col */
}
uint32 strToFloat(redisClient *c, char *start, uint32 len, float *f) {
    char buf[32];
    if (len >= 31) {
        addReply(c, shared.col_float_string_too_long);
        return 0;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    *f       = (float)atof(buf);
    return 4;
}
static uint32 createFCol(redisClient *c,
                         char        *start,
                         uint32       len,
                         float       *col) {
    return strToFloat(c, start, len, col);
}
static void writeUIntCol(uchar **row, flag sflag, uint32 icol) {
    if (sflag == COL_1BYTE_INT) {
        **row = (char)icol;
        *row  = *row + 1;
    } else if (sflag == COL_2BYTE_INT) {
        unsigned short m = (unsigned short)icol;
        memcpy(*row, &m, 2);
        *row  = *row + 2;
    } else if (sflag == COL_4BYTE_INT) {
        memcpy(*row, &icol, 4);
        *row  = *row + 4;
    } else {
        **row = COL_5BYTE_INT;
        *row  = *row + 1;
        memcpy(*row, &icol, 4);
        *row  = *row + 4;
    }
}
static void writeFloatCol(uchar **row, float fcol) {
    memcpy(*row, &fcol, 4);
    *row  = *row + 4;
}
static uint32 streamIntToUInt(uchar *data, uint32 *clen, flag *sflag) {
    uint32 val;
    uchar  b1 = *data;
    if (b1 & COL_1BYTE_INT) {
        if (sflag) *sflag = COL_1BYTE_INT;
        *clen  = 1;
        val    = ((uint32)b1);
        val   -= 1;
        val   /= 2;
    } else if (b1 & COL_2BYTE_INT) {
        unsigned short m;
        if (sflag) *sflag = COL_2BYTE_INT;
        *clen  = 2;
        memcpy(&m, data, 2);
        val    = (uint32)m;
        val   -= 2;
        val   /= 4;
    } else if (b1 & COL_4BYTE_INT) {
        if (sflag) *sflag = COL_4BYTE_INT;
        *clen  = 4;
        memcpy(&val, data, 4);
        val   -= 4;
        val   /= 8;
    } else {
        if (sflag) *sflag = COL_5BYTE_INT;
        *clen  = 5;
        data++;
        memcpy(&val, data, 4);
    }
    return val;
}
static float streamFloatToFloat(uchar *data, uint32 *clen) {
    float val;
    *clen  = 4;
    memcpy(&val, data, 4);
    return val;
}

/* CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW */
typedef struct create_row_ctrl {
    int     tmatch;
    int     ncols;
    uint32  rlen;
    uint32  mcofsts[MAX_COLUMN_PER_TABLE];
    char   *strs   [MAX_COLUMN_PER_TABLE];
    uint32  slens  [MAX_COLUMN_PER_TABLE];
    flag    iflags [MAX_COLUMN_PER_TABLE];
    uint32  icols  [MAX_COLUMN_PER_TABLE];
    float   fcols  [MAX_COLUMN_PER_TABLE];
} cr_t;
static void init_cr(cr_t *cr, int tmatch, int ncols) {
    cr->tmatch   = tmatch;
    cr->ncols    = ncols;
    cr->rlen     = 0;
    cr->strs[0]  = NULL;
    cr->slens[0] = 0;
}
#define INIT_CR cr_t cr; init_cr(&cr, tmatch, ncols);

static bool contains_text_col(int tmatch, int ncols) {
    for (int i = 1; i < ncols; i++) {       /* sixzip needs a string */
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) return 1;
    }
    return 0;
}
#define CZIP_NONE     0
#define CZIP_SIX      1
#define CZIP_LZF      2
typedef struct col_zip_ctrl {
    bool    zip;
    uchar   type;
    uint32  sixn;
    uchar  *sixs [MAX_COLUMN_PER_TABLE];
    uint32  sixl [MAX_COLUMN_PER_TABLE];
    uint32  lzf_n;
    void   *lzf_s[MAX_COLUMN_PER_TABLE]; /* compressed column */
    uint32  lzf_l[MAX_COLUMN_PER_TABLE]; /* compressed column length */
    uint32  socl [MAX_COLUMN_PER_TABLE]; /* stream original col-len */
    uint32  lsocl[MAX_COLUMN_PER_TABLE]; /* LEN stream original col-len */
} cz_t;
static void init_cz(cz_t *cz, cr_t *cr) {
    cz->zip  = contains_text_col(cr->tmatch, cr->ncols);
    cz->type = CZIP_NONE;
    cz->sixn = cz->lzf_n = 0;
}
#define INIT_CZIP cz_t cz; init_cz(&cz, cr);
static void destroy_cz(cz_t *cz) {
    for (uint32 j = 0; j < cz->sixn;  j++) free(cz->sixs[j]);  /*FREED 022 */
    for (uint32 j = 0; j < cz->lzf_n; j++) free(cz->lzf_s[j]); /*FREED 034 */
    cz->sixn = cz->lzf_n = 0;
}
#define DESTROY_CZIP destroy_cz(&cz);

static char *streamLZFTextToString(uchar *s, uint32 *len) {
    uint32  clen;
    uint32  oclen = streamIntToUInt(s, &clen, &fdum);
    char   *buf   = malloc(oclen);                       /* FREE ME 035 */
    int     llen  = *len - clen;
    *len          = lzf_decompress(s + clen, llen, buf, oclen);
    return buf;
}
static bool lzfZipCol(int i, cr_t *cr, cz_t *cz, uint32 *tlen, uint32 *mtlen) {
    INCRBY(*tlen, cr->slens[i]);
    uint32 mlen  = MAX(4, cr->slens[i] + 4);
    uint32 n     = cz->lzf_n;
    cz->lzf_s[n] = malloc(mlen);       /* FREE ME 034 */
    cz->lzf_l[n] = lzf_compress(cr->strs[i], cr->slens[i], cz->lzf_s[n], mlen);
    if (!cz->lzf_l[n]) return 0;
    cz->lsocl[n] = _cr8Icol(cr->slens[i], &ucdum, &cz->socl[n]);
    INCRBY(*mtlen, (cz->lsocl[cz->lzf_n] + cz->lzf_l[cz->lzf_n]));
    INCR(cz->lzf_n)
    return 1;
}
static uchar *writeLzoCol(uchar *row, cz_t *cz, int k) {
    memcpy(row, &cz->socl[k], cz->lsocl[k]); /* orig col_len*/
    row += cz->lsocl[k];
    memcpy(row, cz->lzf_s[k], cz->lzf_l[k]); /* compressed column */
    return row + cz->lzf_l[k];
}

static bool compression_justified(uint32 tlen, uint32 mtlen) {
    return (mtlen < tlen); //TODO add in some intelligence
}
#define COL_LOOP_IF_TEXT \
    for (int i = 1; i < cr->ncols; i++) { \
        if (Tbl[server.dbid][cr->tmatch].col_type[i] == COL_TYPE_STRING) {
static void zipCol(cr_t *cr, cz_t *cz) {
    //return; /* this would disable compression */
    cz->type = CZIP_SIX;
    COL_LOOP_IF_TEXT  /* if ANY TEXT col len > 20 -> LZF */
        if (cr->slens[i] > 20) { cz->type = CZIP_LZF; break; }
    }}
    if (cz->type == CZIP_LZF) {            /* ZIP LZF */
        uint32 tlen  = 0; /* sum length TEXT_cols */
        uint32 mtlen = 0; /* sum length compressed(TEXT_cols) */
        COL_LOOP_IF_TEXT
            if (!lzfZipCol(i, cr, cz, &tlen, &mtlen)) {
                cz->type = CZIP_LZF;
                break;
            }
        }}
        if (cz->type == CZIP_LZF && !compression_justified(tlen, mtlen))
            cz->type = CZIP_SIX; 
    }
    if (cz->type == CZIP_SIX) {            /* ZIP SIXBIT */
        COL_LOOP_IF_TEXT
            uint32  len  = cr->slens[i];
            uchar  *dest = _createSixBit(cr->strs[i], len, &len); //FREE 022
            if (!dest) { cz->type = CZIP_NONE; return; }
            cz->sixs[cz->sixn] = dest;
            cz->sixl[cz->sixn] = len;
            INCR(cz->sixn)
        }}
    }

    uint32 k      = 0;
    uint32 shrunk = 0;
    for (int i = 1; i < cr->ncols; i++) { /* MOD cofsts (zipping) */
        int   diff  = 0;
        if (Tbl[server.dbid][cr->tmatch].col_type[i] == COL_TYPE_STRING) {
            if (       cz->type == CZIP_SIX) {
                diff = (cr->slens[i] - cz->sixl[k]);
            } else if (cz->type == CZIP_LZF) {
                diff = (cr->slens[i] - (cz->lsocl[k] + cz->lzf_l[k]));
            }
            k++;
        }
        shrunk         += diff;
        cr->rlen       -= diff;
        cr->mcofsts[i] -= shrunk;
    }
}
/*  ROW BINARY FORMAT: [ 1B | 1B  |NC*(1-4B)|data ....no PK, no commas] */
/*                     [flag|ncols|cofsts|a,b,c,....................] */
static uchar assign_rflag(uint32 rlen, uchar ztype) {
    uchar rflag;
    if (     rlen < UCHAR_MAX) rflag = RFLAG_1BYTE_INT;
    else if (rlen < USHRT_MAX) rflag = RFLAG_2BYTE_INT;
    else                       rflag = RFLAG_4BYTE_INT;
    if (     ztype == CZIP_SIX) rflag += RFLAG_6BIT_ZIP;
    else if (ztype == CZIP_LZF) rflag += RFLAG_LZF_ZIP;
    return rflag;
}
#define ROW_META_SIZE 2
static void *createRowBlob(int ncols, uchar rflag, uint32 rlen) {
    uint32  meta_len    = ROW_META_SIZE + (ncols * rflag); /*flag,ncols,cofsts*/
    uint32  new_row_len = meta_len + rlen;
    uchar  *orow        = malloc(new_row_len);           /* FREE ME 023 */
    uchar  *row         = orow;
    *row                = rflag;               /* SET flag      (size++) */
    row++;
    *row                = ncols - 1;           /* SET ncols     (size++) */ 
    row++;
    return orow;
}
static int set_col_offst(uchar *row, int i, uchar rflag, uint32 c_ofst[]) {
    if (rflag        & RFLAG_1BYTE_INT) {
        *row = (uchar)c_ofst[i];
        return 1;
    } else if (rflag & RFLAG_2BYTE_INT) {
        unsigned short m = (unsigned short)c_ofst[i];
        memcpy(row, &m, USHORT_SIZE);
        return USHORT_SIZE;
    } else {        /* RFLAG_4BYTE_INT */
        memcpy(row, &(c_ofst[i]), UINT_SIZE);
        return UINT_SIZE;
    }
}
static uchar *writeRow(cr_t *cr) {
    INIT_CZIP
    if (cz.zip) zipCol(cr, &cz);

    uchar  rflag = assign_rflag(cr->rlen, cz.type);
    uchar *orow  = createRowBlob(cr->ncols, rflag, cr->rlen);
    uchar *row   = orow + ROW_META_SIZE;
    for (int i = 1; i < cr->ncols; i++) { /* WRITE cofsts[] to row */
        row += set_col_offst(row, i, rflag, cr->mcofsts); /* size+=ncols*flag */
    }

    uint32 k = 0;
    for (int i = 1; i < cr->ncols; i++) { /* write ROW */
        if (Tbl[server.dbid][cr->tmatch].col_type[i]        == COL_TYPE_INT) {
            writeUIntCol(&row, cr->iflags[i], cr->icols[i]);
        } else if (Tbl[server.dbid][cr->tmatch].col_type[i] == COL_TYPE_FLOAT) {
            writeFloatCol(&row, cr->fcols[i]);
        } else {                                        /* COL_TYPE_STRING */
            if (       cz.type == CZIP_SIX) {
                memcpy(row, cz.sixs[k], cz.sixl[k]);
                row += cz.sixl[k];
                k++;
            } else if (cz.type == CZIP_LZF) {
                row = writeLzoCol(row, &cz, k);
                k++;
            } else {
                memcpy(row, cr->strs[i], cr->slens[i]);
                row += cr->slens[i];
           }
        }
    }
    DESTROY_CZIP
    return orow;
}
char UUbuf[32];
void *createRow(redisClient *c,
                bt          *btr,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint32       cofsts[]) {
    if UU(btr) { /* UU rows are not real rows, 'void *' of the col INT */
        int c1len = (cofsts[1] - cofsts[0]);
        memcpy(UUbuf, vals + cofsts[0], c1len);
        return (void *)atol(UUbuf);
    }
    INIT_CR /*rlen = (row_tot_len     -  PK_len    -  commas) */
    cr.rlen       = cofsts[ncols - 1] - (cofsts[0] + ncols - 1);
    uint32 len    = 0;
    uint32 shrunk = 0;
    for (int i = 1; i < cr.ncols; i++) { /* MOD cofsts (no PK,no commas,PACK) */
        cr.strs[i]     = vals + cofsts[i - 1];
        cr.slens[i]    = cofsts[i] - cofsts[i - 1] - 1;
        cr.mcofsts[i]  = cr.slens[i] + len;
        len           += cr.slens[i];
        int diff       = 0;
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
            uint32 clen = cr8Icol(c, cr.strs[i], cr.slens[i],
                                  &cr.iflags[i], &cr.icols[i]);
            if (!clen) return NULL;
            diff        = (cr.slens[i] - clen);
        } else if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_FLOAT) {
            uint32 clen = createFCol(c, cr.strs[i], cr.slens[i], &cr.fcols[i]);
            if (!clen) return NULL;
            diff        = (cr.slens[i] - clen);
        }
        shrunk        += diff;
        cr.mcofsts[i] -= shrunk;

    }
    uchar *orow = writeRow(&cr);
    server.dirty++;
    return orow;
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
#define INIT_COL_AVALS \
    aobj avals [MAX_COLUMN_PER_TABLE]; \
    for (int i = 0; i < ncols; i++) initAobj(&avals[i]);
#define DESTROY_COL_AVALS \
    for (int i = 0; i < ncols; i++) releaseAobj(&avals[i]);
//TODO simultaneous PK and normal update
//TODO too many arguments for a per-row-OP, merge into a struct
bool updateRow(redisClient *c,
               bt          *btr,
               aobj        *aopk,
               void        *rrow,
               int          tmatch,
               int          ncols,
               int          matches,
               int          indices[],
               char        *vals   [],
               uint32       vlens  [],
               uchar        cmiss  [],
               ue_t         ue     []) {
    INIT_COL_AVALS   /* merges values in update_string and vals from ROW */
    INIT_CR          /* holds values written to new ROW */
    int ret  = 0; /* presume failure */
    for (int i = 0; i < cr.ncols; i++) {
        uchar ctype = Tbl[server.dbid][tmatch].col_type[i];
        if (cmiss[i] || ue[i].yes) {
            uchar dum;
            avals[i] = getRawCol(btr, rrow, i, aopk, tmatch, &dum, 0);
            if (ue[i].yes && !evalExpr(c, &ue[i], &avals[i], ctype))
                goto update_row_err;
        } else {
            if (       ctype == COL_TYPE_INT) {
                long l  = atol(vals[i]);
                if (!initAobjIntReply(c, &avals[i], l, !i)) goto update_row_err;
                if UU(btr) avals[i].i = (int)((long)avals[i].i % UINT_MAX);
            } else if (ctype == COL_TYPE_FLOAT) {
                float f = atof(vals[i]);
                initAobjFloat(&avals[i], f);
            } else {         /* COL_TYPE_STRING*/
                initAobjString(&avals[i], vals[i], vlens[i]);
            }
        }
        int clen = 0;
        if (i) { /* NOT PK, populate cr values (PK not stored in row)*/
            if (       Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
                clen = _cr8Icol(avals[i].i, &cr.iflags[i], &(cr.icols[i]));
            } else if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_FLOAT) {
                cr.fcols[i]  = avals[i].f;
                clen         = 4;
            } else {                                        /* COL_TYPE_STRING*/
                cr.strs[i]   = avals[i].s;
                cr.slens[i]  = avals[i].len;
                clen         = avals[i].len;
            }
        }
        cr.mcofsts[i]  = cr.rlen + clen;
        cr.rlen       += clen;
    }

    uchar *nrow = UU(btr) ? (uchar *)(long)avals[1].i : writeRow(&cr);

    if (!cmiss[0]) { /* PK update */
        for (int i = 0; i < matches; i++) { /* Redo ALL indices */
            updatePKIndex(c->db, btr, aopk, &avals[0],
                          rrow, indices[i], tmatch);
        }
        btDelete(btr, aopk);          /* DELETE old */
        btAdd(btr, &avals[0], nrow);  /* then ADD new */
    } else {
        if (matches) {
            for (int i = 0; i < matches; i++) { /* Redo ALL EFFECTED indices */
                int cmatch = Index[server.dbid][indices[i]].column;
                if (!cmiss[cmatch]) {
                    updateIndex(c->db, btr, aopk, &avals[0], &avals[cmatch],
                                rrow, indices[i], tmatch);
                }
            }
        }
        btReplace(btr, aopk, nrow); /* overwrite w/ new row */
    }
    if (!UU(btr)) free(nrow);                            /* FREED 023 */
    server.dirty++;
    ret = 1;

update_row_err:
    DESTROY_COL_AVALS
    return ret;
}

/* GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL */
static uchar *getRowPayload(uchar  *row,
                            uchar  *rflag,
                            uint32 *ncols,
                            uint32 *rlen) {
    uchar *o_row     = row;
    *rflag           = *row;                          /* GET flag */
    row++;
    char   sflag     = *rflag & RFLAG_SIZE_FLAG;
    *ncols           = *row;                          /* GET ncols */
    row++;
    row             += (*ncols * sflag);              /* SKIP cofsts */
    uint32 meta_len  = row - o_row;
    if (*rflag        & RFLAG_1BYTE_INT) {            /* rlen is final cofst */
        uchar *x = (uchar*)row - 1;
        *rlen    = (uint32)*x;
    } else if (*rflag & RFLAG_2BYTE_INT) {
        uchar *x = row - 2;
        *rlen    = (uint32)(*((unsigned short *)x));
    } else {         /* RFLAG_4BYTE_INT */
        uchar *x = row - 4;
        *rlen    = *((uint32 *)x);
    }
    *rlen = *rlen + meta_len;
    return row;
}
uint32 getRowMallocSize(uchar *stream) {
    uchar   rflag;
    uint32  rlen, ncols;
    getRowPayload(stream, &rflag, &ncols, &rlen);
    return rlen;
}

char RawCols[MAX_COLUMN_PER_TABLE][32];
static void initIntAobjFromVal(aobj *a, int val, bool raw, int cmatch) {
    a->type = COL_TYPE_INT;
    if (raw) {
        a->i   = val;
        a->enc = COL_TYPE_INT;
    } else {
        snprintf(RawCols[cmatch], 32, "%u", val);
        a->len = strlen(RawCols[cmatch]);
        a->s   = RawCols[cmatch];
        a->enc = COL_TYPE_STRING;
    }
}
static void initFloatAobjFromVal(aobj *a, float f, bool raw, int cmatch) {
    a->type       = COL_TYPE_FLOAT;
    if (raw) {
        a->f   = f;
        a->enc = COL_TYPE_FLOAT;
    } else {
        sprintfOutputFloat(RawCols[cmatch], 32, f);
        a->len = strlen(RawCols[cmatch]);
        a->s   = RawCols[cmatch];
        a->enc = COL_TYPE_STRING;
    }
}
#define AOBJ_FROM_UU(uu, force_s) {                \
    int cval = (int)((long)uu % UINT_MAX);          \
    initIntAobjFromVal(&a, cval, !force_s, cmatch); \
    return a; }

aobj getRawCol(bt    *btr,
               void  *orow,
               int    cmatch,
               aobj  *aopk,
               int    tmatch,
               flag  *iflag,
               bool   force_s) {
    aobj a;
    initAobj(&a);
    if (!cmatch) { /* PK stored ONLY in KEY not in ROW, echo it */
        if       UU(btr)   AOBJ_FROM_UU(aopk->i, force_s)
        else if (!force_s) return *aopk;
        else { initStringAobjFromAobj(&a, aopk); return a; }
    }
    if UU(btr) AOBJ_FROM_UU(orow, force_s);
    uchar    rflag;
    uint32   rlen, ncols;
    uchar   *meta     = (uchar *)orow;
    uchar   *row      = getRowPayload(meta, &rflag, &ncols, &rlen);
    uchar    sflag    = rflag & RFLAG_SIZE_FLAG;
    uchar   *cofst    = meta + ROW_META_SIZE;
    int      m_cmatch = cmatch - 1; /* key was not stored -> one less column */
    uint32   start    = 0;
    uint32   next;
    if (rflag        & RFLAG_1BYTE_INT) {
        if (m_cmatch) start = (uint32)*(cofst + m_cmatch - 1);
        next = (uint32)*(cofst + m_cmatch);
    } else if (rflag & RFLAG_2BYTE_INT) {
        unsigned short *m;
        if (m_cmatch) {
            m     = (ushort *)(char *)(cofst + ((m_cmatch - 1) * sflag));
            start = (uint32)*m;
        }
        m    = (ushort *)(char *)(cofst + (m_cmatch * sflag));
        next = (uint32)*m;
    } else {        /* RFLAG_4BYTE_INT */
        uint32 *i;
        if (m_cmatch) {
            i     = (uint32 *)(char *)(cofst + ((m_cmatch - 1) * sflag));
            start = *i;
        }
        i    = (uint32 *)(char *)(cofst + (m_cmatch * sflag));
        next = *i;
    }

    uint32  clen;
    uchar ctype = Tbl[server.dbid][tmatch].col_type[cmatch];
    if (Tbl[server.dbid][tmatch].col_type[cmatch]        == COL_TYPE_INT) {
        uchar  *data = row + start;
        uint32  val  = streamIntToUInt(data, &clen, iflag);
        bool    fs   = !(!force_s && ctype == COL_TYPE_INT);
        initIntAobjFromVal(&a, val, !fs, cmatch);
    } else if (Tbl[server.dbid][tmatch].col_type[cmatch] == COL_TYPE_FLOAT) {
        uchar  *data = row + start;
        float   f    = streamFloatToFloat(data, &clen);
        bool    raw  = (!force_s && ctype == COL_TYPE_FLOAT);
        initFloatAobjFromVal(&a, f, raw, cmatch);
    } else {                                             /* COL_TYPE_STRING */
        a.type       = COL_TYPE_STRING;
        a.enc        = COL_TYPE_STRING;
        uchar *s     = (uchar *)(row + start);
        clen         = next - start;
        if (      rflag & RFLAG_6BIT_ZIP) {
            a.s      = (char *)unpackSixBit(s, &clen);
            a.freeme = 1;
        } else if (rflag & RFLAG_LZF_ZIP) {
            a.s      = streamLZFTextToString(s, &clen);
            a.freeme = 1;                                /* FREED 035 */
        } else {        /* NO ZIP -> uncompressed text */
            a.s    = (char *)s;
        }
        a.len  = clen;
    }
    return a;
}

#define QUOTE_COL if (quote_text_cols) { memcpy(s + slot, "'", 1); slot++; }
#define OUPUT_BUFFER_SIZE 4096
static char OutputBuffer[OUPUT_BUFFER_SIZE]; /*avoid malloc()s */
robj *outputRow(bt   *btr,
                void *rrow,
                int   qcols,
                int   cmatchs[],
                aobj *aopk,
                int   tmatch,
                bool  quote_text_cols) {
    aobj   cols[MAX_COLUMN_PER_TABLE];
    uint32 totlen = 0;
    for (int i = 0; i < qcols; i++) {
        cols[i]  = getRawCol(btr, rrow, cmatchs[i], aopk, tmatch, NULL, 1);
        totlen  += cols[i].len;
    }
    totlen += (uint32)qcols - 1; /* -1 no final comma */
    if (quote_text_cols) totlen += 2 * (uint32)qcols; /* 2 quotes per col */
    char *s = (totlen >= OUPUT_BUFFER_SIZE) ? malloc(totlen) : /* FREE ME 014 */
                                              OutputBuffer;
    uint32 slot = 0;
    for (int i = 0; i < qcols; i++) {
        QUOTE_COL
        memcpy(s + slot, cols[i].s, cols[i].len);
        slot += cols[i].len;
        QUOTE_COL
        if (i != (qcols - 1)) {
            memcpy(s + slot, OUTPUT_DELIM, 1);
            slot++;
        }
        releaseAobj(&cols[i]);
    }
    robj *r = createStringObject(s, totlen);
    if (totlen >= OUPUT_BUFFER_SIZE) free(s);                  /* FREED 014 */
    return r;
}

bool deleteRow(redisClient *c,
               int          tmatch,
               aobj        *apk,
               int          matches,
               int          indices[]) {
    robj  *btt    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt    *btr    = (bt *)btt->ptr;
    void  *rrow   = btFindVal(btr, apk);
    if (!rrow) return 0;
    if (matches) {
        for (int i = 0; i < matches; i++) { /* delete indices */
            delFromIndex(c->db, btr, apk, rrow, indices[i], tmatch);
        }
    }
    btDelete(btr, apk);
    server.dirty++;
    return 1;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
static bool evalExpr(redisClient *c, ue_t *ue, aobj *aval, uchar ctype) {
    if (ctype        == COL_TYPE_INT) {
        long   m;
        long   l    = 0;
        double f    = 0.0;
        bool   is_f = (ue->type == UETYPE_FLOAT);
        if (is_f) f = atof(ue->pred);
        else      l = atol(ue->pred);

        if ((ue->op == DIVIDE || ue->op == MODULO) && 
            ((is_f && f == 0.0) || (!is_f && l == 0))) {
            addReply(c, shared.update_expr_div_0);
            return 0;
        }
        if      (ue->op == PLUS)   m = aval->i + (is_f ? (long)f : l);
        else if (ue->op == MINUS)  m = aval->i - (is_f ? (long)f : l);
        else if (ue->op == MODULO) m = aval->i % (is_f ? (long)f : l);
        else if (ue->op == DIVIDE) {
            if (ue->type == UETYPE_FLOAT) {
                m = (long)((double)aval->i / f);
            } else {
                m = aval->i / l;
            }
        } else if (ue->op == MULT) {
            if (ue->type == UETYPE_FLOAT) {
                m = (long)((double)aval->i * f);
            } else {
                m = aval->i * l;
            }
        } else if (ue->op == POWER)  {
            if (ue->type == UETYPE_INT) f = (float)l;
            errno    = 0;
            double d = powf((float)aval->i, f);
            if (errno != 0) {
                addReply(c, shared.col_uint_too_big);
                return 0;
            }
            m        = (long) d;
        }
        if (!checkUIntReply(c, m, 0)) return 0;
        aval->i = m;
    } else if (ctype == COL_TYPE_FLOAT) {
        double m;
        float  f = atof(ue->pred);

        if (ue->op == DIVIDE && f == (float)0.0) {
            addReply(c, shared.update_expr_div_0);
            return 0;
        }
        errno = 0; /* overflow detection initialisation */
        if      (ue->op == PLUS)   m = aval->f + f;
        else if (ue->op == MINUS)  m = aval->f - f;
        else if (ue->op == MULT)   m = aval->f * f;
        else if (ue->op == DIVIDE) m = aval->f / f;
        else if (ue->op == POWER)  m = powf(aval->f, f);
        if (errno != 0) {
            addReply(c, shared.update_expr_float_overflow);
            return 0;
        }
        double d = m;
        if (d < 0.0) d *= -1;
        if (d < FLT_MIN || d > FLT_MAX) {
            addReply(c, shared.update_expr_float_overflow);
            return 0;
        }
        aval->f = (float)m;
    } else {         /* COL_TYPE_STRING*/
        int len      = aval->len + ue->plen;
        char *s      = malloc(len + 1);
        memcpy(s,             aval->s,  aval->len);
        memcpy(s + aval->len, ue->pred, ue->plen);
        s[len]       = '\0';
        free(aval->s);
        aval->freeme = 1; /* this new string must be freed later */
        aval->s      = s;
        aval->len    = len;
    }
    return 1;
}

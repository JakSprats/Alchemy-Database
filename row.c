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

#include "sixbit.h"
#include "redis.h"
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

#define RFLAG_1BYTE_INT        1
#define RFLAG_2BYTE_INT        2
#define RFLAG_4BYTE_INT        4
#define RFLAG_SIZE_FLAG        (RFLAG_1BYTE_INT+RFLAG_2BYTE_INT+RFLAG_4BYTE_INT)
#define RFLAG_SIX_BIT_STRINGS  8

#define COL_1BYTE_INT 1
#define COL_2BYTE_INT 2
#define COL_4BYTE_INT 4
#define COL_5BYTE_INT 8

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
static uint32 _createICol(uint32 i, flag *sflag, uint32 *col) {
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
static uint32 createICol(redisClient *c,
                         char        *start,
                         uint32       len,
                         flag        *sflag,
                         uint32      *col) {
    if (!strToInt(c, start, len, col)) return 0; /* sets col twice, but ok */
    return _createICol(*col, sflag, col);        /* resets col from *col */
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

/* ROW_INTERNALS ROW_INTERNALS ROW_INTERNALS ROW_INTERNALS ROW_INTERNALS */
static uchar assign_rflag(bool can_six, uint32 rlen) {
    uchar rflag;
    if (rlen < UCHAR_MAX) {
        rflag = RFLAG_1BYTE_INT;
    } else if (rlen < USHRT_MAX) {
        rflag = RFLAG_2BYTE_INT;
    } else {
        rflag = RFLAG_4BYTE_INT;
    }
    if (can_six) rflag += RFLAG_SIX_BIT_STRINGS;
    return rflag;
}

#define ROW_META_SIZE 2
static void *createRowBlob(int ncols, uchar rflag, uint32 rlen) {
    uint32  meta_len    = 2 + (ncols * rflag); /*flag,ncols+cofsts*/ 
    uint32  new_row_len = meta_len + rlen;
    uchar  *orow        = malloc(new_row_len);
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

//  [ 1B | 1B  |NC*(1-4B)|data ....no PK, no commas]
//  [flag|ncols|cofsts|a,b,c,....................]
void *createRow(redisClient *c,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint32       cofsts[]) {
    uint32   m_cofsts [MAX_COLUMN_PER_TABLE];
    uint32   icols    [MAX_COLUMN_PER_TABLE];
    float    fcols    [MAX_COLUMN_PER_TABLE];
    uchar    sflags   [MAX_COLUMN_PER_TABLE];
    uchar   *sixbitstr[MAX_COLUMN_PER_TABLE];
    uint32   sixbitlen[MAX_COLUMN_PER_TABLE];

    uint32 rlen  = cofsts[ncols - 1];     // total length
    rlen        -= cofsts[0] + ncols - 1; // minus key, minus commas

    uint32 len = 0;
    for (int i = 1; i < ncols; i++) { // mod cofsts (no key, no commas)
        uint32 diff  = cofsts[i] - cofsts[i - 1] - 1;
        m_cofsts[i]  = diff + len;
        len         += diff;
    }

    uint32 n_6b_s  = 0;
    bool   can_six = 1;
    for (int i = 1; i < ncols; i++) {       // check can_six, create sixbitstr
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
            uint32  s_len;
            uint32  len   = cofsts[i] - cofsts[i - 1] - 1;
            char   *start = vals + cofsts[i - 1];
            uchar  *dest  = _createSixBitString(start, len, &s_len);
            if (!dest) {
                can_six = 0;
                break;
            } else {
                sixbitstr[n_6b_s] = dest;
                sixbitlen[n_6b_s] = s_len;
                n_6b_s++;
            }
        }
    }

    uint32 k      = 0;
    uint32 shrunk = 0;
    for (int i = 1; i < ncols; i++) { // modify cofsts (INTs are binary streams)
        int   len   = cofsts[i] - cofsts[i - 1] - 1;
        int   diff  = 0;
        if (Tbl[server.dbid][tmatch].col_type[i]        == COL_TYPE_INT) {
            char   *start = vals + cofsts[i - 1];
            uint32  clen  = createICol(c, start, len, &sflags[i], &icols[i]);
            if (!clen) return NULL;
            diff          = (len - clen);  
        } else if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_FLOAT) {
            char   *start = vals + cofsts[i - 1];
            uint32  clen  = createFCol(c, start, len, &fcols[i]);
            if (!clen) return NULL;
            diff          = (len - clen);  
        } else if (can_six) {                           /* COL_TYPE_STRING */
            diff          = (len - sixbitlen[k]);  
            k++;
        }
        shrunk      += diff;
        rlen        -= diff;
        m_cofsts[i] -= shrunk;
    }

    uchar  rflag = assign_rflag(can_six, rlen);
    uchar *orow  = createRowBlob(ncols, rflag, rlen);
    uchar *row   = orow + ROW_META_SIZE;
    for (int i = 1; i < ncols; i++) { // SET cofsts (size+=ncols*flag)
        row += set_col_offst(row, i, rflag, m_cofsts);
    }

    k = 0;
    for (int i = 1; i < ncols; i++) { // SET data      (size+=rlen)
        if (Tbl[server.dbid][tmatch].col_type[i]        == COL_TYPE_INT) {
            writeUIntCol(&row, sflags[i], icols[i]);
        } else if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_FLOAT) {
            writeFloatCol(&row, fcols[i]);
        } else {                                        /* COL_TYPE_STRING */
            if (can_six) {
                memcpy(row, sixbitstr[k], sixbitlen[k]);
                row += sixbitlen[k];
                k++;
            } else {
                uint32  len = cofsts[i] - cofsts[i - 1] - 1;
                char   *col = vals + cofsts[i - 1];
                memcpy(row, col, len);
                row        += len;
           }
        }
    }

    for (uint32 j = 0; j < n_6b_s; j++) free(sixbitstr[j]);

    server.dirty++;
    return orow;
}

/* IMPORTANT: do not modify buffer(row) here [READ-OP] */
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


void sprintfOutputFloat(char *buf, int len, float f) {
    //snprintf(buf, len, "%10.10f", f);
    snprintf(buf, len, "%.10g", f);
    buf[len - 1] = '\0';
}

char RawCols[MAX_COLUMN_PER_TABLE][32];

//IMPORTANT: do not modify buffer(meta) here [READ-OP]
aobj getRawCol(void  *orow,
               int    cmatch,
               aobj  *aopk,
               int    tmatch,
               flag  *cflag,
               bool   force_string) {
    aobj a;
    initAobj(&a);
    if (!cmatch) { /* PK stored ONLY in KEY not in ROW, echo it */
        if (force_string) {
            initStringAobjFromAobj(&a, aopk);
            return a;
        } else {
            return *aopk;
        }
    }

    uchar    rflag;
    uint32   rlen, ncols;
    uchar   *meta     = (uchar *)orow;
    uchar   *row      = getRowPayload(meta, &rflag, &ncols, &rlen);
    uchar    sflag    = rflag & RFLAG_SIZE_FLAG;
    uchar   *cofst    = meta + 2; // 2 {flag,ncols}
    int      cmatch_m = cmatch;

    cmatch_m--; // key was not stored
    uint32 start = 0;
    uint32 next;
    if (rflag        & RFLAG_1BYTE_INT) {
        if (cmatch_m) start = (uint32)*(cofst + cmatch_m - 1);
        next = (uint32)*(cofst + cmatch_m);
    } else if (rflag & RFLAG_2BYTE_INT) {
        unsigned short *m;
        if (cmatch_m) {
            m     = (ushort *)(char *)(cofst + ((cmatch_m - 1) * sflag));
            start = (uint32)*m;
        }
        m    = (ushort *)(char *)(cofst + (cmatch_m * sflag));
        next = (uint32)*m;
    } else {        /* RFLAG_4BYTE_INT */
        uint32 *i;
        if (cmatch_m) {
            i     = (uint32 *)(char *)(cofst + ((cmatch_m - 1) * sflag));
            start = *i;
        }
        i    = (uint32 *)(char *)(cofst + (cmatch_m * sflag));
        next = *i;
    }

    uchar ctype = Tbl[server.dbid][tmatch].col_type[cmatch];
    if (Tbl[server.dbid][tmatch].col_type[cmatch]        == COL_TYPE_INT) {
        uint32  clen;
        a.type       = COL_TYPE_INT;
        uchar  *data = row + start;
        uint32  val  = streamIntToUInt(data, &clen, cflag);
        if (!force_string && ctype == COL_TYPE_INT) { /* request is for INT */
            a.i    = val;
            a.enc  = COL_TYPE_INT;
            a.len  = clen;
        } else {
            snprintf(RawCols[cmatch], 32, "%u", val);
            a.len = strlen(RawCols[cmatch]);
            a.s   = RawCols[cmatch];
            a.enc  = COL_TYPE_STRING;
        }
    } else if (Tbl[server.dbid][tmatch].col_type[cmatch] == COL_TYPE_FLOAT) {
        uint32  clen;
        a.type       = COL_TYPE_FLOAT;
        uchar  *data = row + start;
        float   f    = streamFloatToFloat(data, &clen);
        if (!force_string && ctype == COL_TYPE_FLOAT) { /* request for FLOAT */
            a.f    = f;
            a.enc  = COL_TYPE_FLOAT;
            a.len  = clen;
        } else {
            sprintfOutputFloat(RawCols[cmatch], 32, f);
            a.len = strlen(RawCols[cmatch]);
            a.s   = RawCols[cmatch];
            a.enc  = COL_TYPE_STRING;
        }
    } else {                                               /* COL_TYPE_STRING */
        a.type     = COL_TYPE_STRING;
        a.enc      = COL_TYPE_STRING;
        uint32 len = next - start;
        if (rflag & RFLAG_SIX_BIT_STRINGS) {
            uchar *s = (uchar *)(row + start);
            a.s      = (char *)unpackSixBitString(s, &len);
            a.freeme = 1;
        } else {
            a.s    = (char *)row + start;
        }
        a.len  = len;
    }
    return a;
}



#define OUPUT_BUFFER_SIZE 4096
static char OutputBuffer[OUPUT_BUFFER_SIZE]; /*avoid malloc()s */
#define QUOTE_COL if (quote_text_cols) { memcpy(s + slot, "'", 1); slot++; }

robj *outputRow(void *rrow,
                int   qcols,
                int   cmatchs[],
                aobj *aopk,
                int   tmatch,
                bool  quote_text_cols) {
    aobj   cols[MAX_COLUMN_PER_TABLE];
    uint32 totlen = 0;
    for (int i = 0; i < qcols; i++) {
        cols[i]  = getRawCol(rrow, cmatchs[i], aopk, tmatch, NULL, 1);
        totlen  += cols[i].len;
    }
    totlen += (uint32)qcols - 1; // -1 no final comma
    if (quote_text_cols) totlen += 2 * (uint32)qcols;
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
    uchar  pktype = Tbl[server.dbid][tmatch].col_type[0];
    robj  *btt    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt    *btr    = (bt *)btt->ptr;
    void  *rrow   = btFindVal(btr, apk, pktype);
    if (!rrow) return 0;
    if (matches) { // indices
        for (int i = 0; i < matches; i++) {         // delete indices
            delFromIndex(c->db, apk, rrow, indices[i], tmatch);
        }
    }
    btDelete(btr, apk, pktype);
    server.dirty++;
    return 1;
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
static bool evalExpr(redisClient *c,
                     ue_t        *ue,
                     aobj        *aval,
                     uchar        ctype) {
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

static bool find_six(int ncols, int tmatch) {
    for (int i = 1; i < ncols; i++) {       /* can_six needs a string */
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
            return 1;
        }
    }
    return 0;
}

static void *rewriteRow(int    ncols,
                        int    tmatch,
                        uint32 rlen,
                        aobj   avals[],
                        flag   sflags[],
                        uint32 icols[],
                        uint32 cofsts[]) {
    uchar  *sixbitstr[MAX_COLUMN_PER_TABLE];
    int     sixbitlen[MAX_COLUMN_PER_TABLE];
    uint32  n_6b_s  = 0;
    bool    can_six = find_six(ncols, tmatch);

    if (can_six) {
        for (int i = 1; i < ncols; i++) {  /* check can_six, create sixbitstr */
            if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
                uint32  s_len;
                uint32  len   = avals[i].len;
                char   *start = avals[i].s;
                uchar  *dest  = _createSixBitString(start, len, &s_len);
                if (!dest) {
                    can_six = 0;
                    break;
                } else {
                    sixbitstr[n_6b_s] = dest;
                    sixbitlen[n_6b_s] = s_len;
                    n_6b_s++;
                }
            }
        }
    }

    if (can_six) {
        uint32 k      = 0;
        uint32 shrunk = 0;
        for (int i = 1; i < ncols; i++) { // modify cofsts for SixBitStrings
            uint32 len    = avals[i].len;
            uint32 diff   = 0;
            if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
                diff      = (len - sixbitlen[k]);
                k++;
            }
            shrunk       += diff;
            rlen         -= diff;
            cofsts[i] -= shrunk;
        }
    }

    uchar  rflag = assign_rflag(can_six, rlen);
    uchar *orow  = createRowBlob(ncols, rflag, rlen);
    uchar *row   = orow + ROW_META_SIZE;
    for (int i = 1; i < ncols; i++) {       // SET cofsts
        row += set_col_offst(row, i, rflag, cofsts);
    }

    uint32 k = 0;
    for (int i = 1; i < ncols; i++) {       // SET data
        if (Tbl[server.dbid][tmatch].col_type[i]        == COL_TYPE_INT) {
            writeUIntCol(&row, sflags[i], icols[i]);
        } else if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_FLOAT) {
            writeFloatCol(&row, avals[i].f);
        } else {                                        /* COL_TYPE_STRING */
           if (can_six) {
                memcpy(row, sixbitstr[k], sixbitlen[k]);
                row += sixbitlen[k];
                k++;
            } else {
                memcpy(row, avals[i].s, avals[i].len);
                row += avals[i].len;
            }
        }
    }

    for (uint32 j = 0; j < n_6b_s; j++) free(sixbitstr[j]);
    return orow;
}


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
    uint32 cofsts[MAX_COLUMN_PER_TABLE];
    aobj   avals    [MAX_COLUMN_PER_TABLE];
    flag   sflags   [MAX_COLUMN_PER_TABLE];
    uint32 icols    [MAX_COLUMN_PER_TABLE];
    uchar  pktype = Tbl[server.dbid][tmatch].col_type[0];
    uint32 rlen   = 0;
    int    ret    = 0;
    for (int i = 1; i < ncols; i++) {  /* necessary in case of update_row_err */
            avals[i].freeme = 0;
    }
    for (int i = 0; i < ncols; i++) {
        uchar ctype = Tbl[server.dbid][tmatch].col_type[i];
        avals[i].len = 0;
        if (cmiss[i] || ue[i].yes) {
            avals[i] = getRawCol(rrow, i, aopk, tmatch, &sflags[i], 0);
            if (ue[i].yes) { /* Update value is Expression */
                if (!evalExpr(c, &ue[i], &avals[i], ctype))
                    goto update_row_err;
            }
        } else {
            avals[i].freeme = 0;
            if (ctype        == COL_TYPE_INT) {
                long l  = atol(vals[i]);
                if (!initAobjIntReply(c, &avals[i], l, !i)) goto update_row_err;
            } else if (ctype == COL_TYPE_FLOAT) {
                float f = atof(vals[i]);
                initAobjFloat(&avals[i], f);
            } else {         /* COL_TYPE_STRING*/
                initAobjString(&avals[i], vals[i], vlens[i]);
            }
        }
        if (i && Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) { /*!PK*/
            avals[i].len = _createICol(avals[i].i, &sflags[i], &(icols[i]));
        }
        cofsts[i]  = rlen + avals[i].len;
        if (i) rlen  += avals[i].len; /* dont store PK */
    }

    void  *nrow = rewriteRow(ncols, tmatch, rlen, avals, sflags, icols, cofsts);

    if (!cmiss[0]) { //pk update
        for (int i = 0; i < matches; i++) { //redo ALL indices
            updatePKIndex(c->db, aopk, &avals[0], rrow, indices[i], tmatch);
        }
        // delete then add
        btDelete(btr, aopk, pktype);
        btAdd(btr, &avals[0], nrow, pktype);
    } else {
        if (matches) {
            for (int i = 0; i < matches; i++) { //redo ALL affected indices
                int inum   = indices[i];
                int cmatch = Index[server.dbid][inum].column;
                if (!cmiss[cmatch]) {
                    updateIndex(c->db, aopk, &avals[0], &avals[cmatch], rrow,
                                inum, tmatch);
                }
            }
        }
        // overwrite w/ new row
        btReplace(btr, aopk, nrow, pktype);
    }
    free(nrow);
    server.dirty++;
    ret = 1;

update_row_err:
    for (int i = 1; i < ncols; i++) {
        releaseAobj(&avals[i]);
    }
    return ret;
}

/*
 * This file implements the rows of Alsosql
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

#include "sixbit.h"
#include "redis.h"
#include "bt.h"
#include "index.h"
#include "common.h"
#include "alsosql.h"
#include "row.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer server;

extern char    *OUTPUT_DELIM;
extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];

#define RFLAG_1BYTE_INT        1
#define RFLAG_2BYTE_INT        2
#define RFLAG_4BYTE_INT        4
#define RFLAG_SIZE_FLAG        (RFLAG_1BYTE_INT+RFLAG_2BYTE_INT+RFLAG_4BYTE_INT)
#define RFLAG_SIX_BIT_STRINGS  8

#define COL_1BYTE_INT 1
#define COL_2BYTE_INT 2
#define COL_4BYTE_INT 4
#define COL_5BYTE_INT 8

// ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS
// ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS ROW_COMMANDS

uint _createICol(uint i, flag *sflag, uint *col) {
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
uint createICol(redisClient *c,
                char        *start,
                uint         len,
                flag        *sflag,
                uint        *col) {
    char buf[32];
    memcpy(buf, start, len);
    buf[len] = '\0';
    long l   = atol(buf);
    if (l >= TWO_POW_32) {
        addReply(c, shared.col_uint_too_big);
        return 0;
    } else if (l < 0) {
        addReply(c, shared.col_uint_no_negative_values);
        return 0;
    }
    return _createICol(l, sflag, col);
}
static void writeUIntCol(uchar **row, flag sflag, uint icol) {
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

static uint streamIntToUInt(uchar *data,
                                    uint  *clen,
                                    flag          *sflag) {
    uint  val;
    uchar b1 = *data;
    if (b1 & COL_1BYTE_INT) {
        if (sflag) *sflag = COL_1BYTE_INT;
        *clen  = 1;
        val    = ((uint)b1);
        val   -= 1;
        val   /= 2;
    } else if (b1 & COL_2BYTE_INT) {
        unsigned short m;
        if (sflag) *sflag = COL_2BYTE_INT;
        *clen  = 2;
        memcpy(&m, data, 2);
        val    = (uint)m;
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

#define ASSIGN_RFLAG(can_six)                    \
    if (rlen < UCHAR_MAX) {                      \
        rflag = RFLAG_1BYTE_INT;                 \
    } else if (rlen < USHRT_MAX) {               \
        rflag = RFLAG_2BYTE_INT;                 \
    } else {                                     \
        rflag = RFLAG_4BYTE_INT;                 \
    }                                            \
    if (can_six) rflag += RFLAG_SIX_BIT_STRINGS;

#define SET_FLAG_NCOLS                                                       \
    robj *r                   = createObject(REDIS_ROW, NULL);               \
    uint   meta_len    = 2 + (ncols * rflag); /*flag,ncols+cofsts*/  \
    uint   new_row_len = meta_len + rlen;                            \
    uchar *row_start   = malloc(new_row_len);                        \
    uchar *row         = row_start;                                  \
    r->ptr = row_start;                                                  \
    *row   = rflag;                         /* SET flag      (size++) */ \
    row++;                                                               \
    *row   = ncols - 1;                     /* SET ncols     (size++) */ \
    row++;

#define SET_COL_OFFSET(row, c_ofst)                        \
    if (rflag & RFLAG_1BYTE_INT) {                         \
        *row = (uchar)c_ofst[i];                   \
        row++;                                             \
    } else if (rflag & RFLAG_2BYTE_INT) {                  \
        unsigned short m = (unsigned short)c_ofst[i];      \
        memcpy(row, &m, USHORT_SIZE);                      \
        row += USHORT_SIZE;                                \
    } else {                                               \
        memcpy(row, &(c_ofst[i]), UINT_SIZE);              \
        row += UINT_SIZE;                                  \
    }

//  [ 1B | 1B  |NC*(1-4B)|data ....no key, no commas]
//  [flag|ncols|col_ofsts|a,b,c,....................]
robj *createRow(redisClient *c,
                int          tmatch,
                int          ncols,
                char        *vals,
                uint         col_ofsts[]) {
    uint   m_cofst  [MAX_COLUMN_PER_TABLE];
    uint   icols    [MAX_COLUMN_PER_TABLE];
    uchar  sflags   [MAX_COLUMN_PER_TABLE];
    uchar *sixbitstr[MAX_COLUMN_PER_TABLE];
    uint   sixbitlen[MAX_COLUMN_PER_TABLE];

    uint rlen  = col_ofsts[ncols - 1];     // total length
    rlen              -= col_ofsts[0] + ncols - 1; // minus key, minus commas

    uint len = 0;
    for (int i = 1; i < ncols; i++) { // mod cofsts (no key, no commas)
        uint diff  = col_ofsts[i] - col_ofsts[i - 1] - 1;
        m_cofst[i]         = diff + len;
        len               += diff;
    }

    uint n_6b_s  = 0;
    bool         can_six = 1;
    for (int i = 1; i < ncols; i++) {       // check can_six, create sixbitstr
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
            uint   s_len;
            uint   len   = col_ofsts[i] - col_ofsts[i - 1] - 1;
            char          *start = vals + col_ofsts[i - 1];
            uchar *dest  = _createSixBitString(start, len, &s_len);
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

    uint k      = 0;
    uint shrunk = 0;
    for (int i = 1; i < ncols; i++) { // modify cofsts (INTs are binary streams)
        int   len   = col_ofsts[i] - col_ofsts[i - 1] - 1;
        int   diff  = 0;
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
            char         *start = vals + col_ofsts[i - 1];
            uint  clen  = createICol(c, start, len, &sflags[i], &icols[i]);
            if (!clen) return NULL;
            diff                = (len - clen);  
        } else if (can_six) {       /* COL_TYPE_STRING */
            diff        = (len - sixbitlen[k]);  
            k++;
        }
        shrunk     += diff;
        rlen       -= diff;
        m_cofst[i] -= shrunk;
    }

    uchar rflag;
    ASSIGN_RFLAG(can_six)
    SET_FLAG_NCOLS
    for (int i = 1; i < ncols; i++) { // SET cofsts (size+=ncols*flag)
        SET_COL_OFFSET(row, m_cofst)
    }

    k = 0;
    for (int i = 1; i < ncols; i++) { // SET data      (size+=rlen)
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
            writeUIntCol(&row, sflags[i], icols[i]);
        } else {
            if (can_six) {
                memcpy(row, sixbitstr[k], sixbitlen[k]);
                row += sixbitlen[k];
                k++;
            } else {
                uint  len = col_ofsts[i] - col_ofsts[i - 1] - 1;
                char         *col = vals + col_ofsts[i - 1];
                memcpy(row, col, len);
                row      += len;
           }
        }
    }

    for (uint j = 0; j < n_6b_s; j++) free(sixbitstr[j]);

    server.dirty++;
    return r;
}

void freeRowObject(robj *o) {
//RL4 "freeRowObject %p", o->ptr);
    free(o->ptr);
}

//IMPORTANT: do not modify buffer(row) here [READ-OP]
static uchar *getRowPayload(uchar *row,
                                    uchar *rflag,
                                    uint  *ncols,
                                    uint  *rlen) {
    uchar *o_row     = row;
    *rflag                   = *row;                          // GET flag
    row++;
    char sflag               = *rflag & RFLAG_SIZE_FLAG;
    *ncols                   = *row;                          // GET ncols
    row++;
    row                     += (*ncols * sflag);              // SKIP col_ofsts
    uint   meta_len  = row - o_row;

    if (*rflag & RFLAG_1BYTE_INT) {                  // rlen is final cofst
        uchar *x = (uchar*)row - 1;
        *rlen            = (uint)*x;
    } else if (*rflag & RFLAG_2BYTE_INT) {
        uchar *x = row - 2;
        *rlen            = (uint)(*((unsigned short *)x));
    } else {
        uchar *x = row - 4;
        *rlen            = *((uint *)x);
    }
    *rlen = *rlen + meta_len;

    return row;
}

uint getRowMallocSize(uchar *stream) {
    uchar rflag;
    uint  rlen, ncols;
    getRowPayload(stream, &rflag, &ncols, &rlen);
    return rlen;
}

char RawColPKIntBuf[32];
static aobj getPk(robj *okey, bool icol, bool force_string) {
    aobj a;
    if (okey->encoding == REDIS_ENCODING_INT) {
        a.type = COL_TYPE_INT;
        if (!force_string && icol) {
            a.enc  = COL_TYPE_INT;
            a.i    = (uint)(long)okey->ptr; // PK NOT encoded 4 lookups
        } else {
            a.enc  = COL_TYPE_STRING;
            uint u = (uint)(unsigned long)okey->ptr;
            sprintf(RawColPKIntBuf, "%u", u);
            a.s    = RawColPKIntBuf;
            a.len  = strlen(RawColPKIntBuf);
        }
    } else {
        a.type = COL_TYPE_STRING;
        a.enc  = COL_TYPE_STRING;
        a.s    = okey->ptr;
        a.len  = sdslen(okey->ptr);
    }
    a.sixbit = 0;
    return a;
}

char RawCols[MAX_COLUMN_PER_TABLE][32];

//IMPORTANT: do not modify buffer(meta) here [READ-OP]
aobj getRawCol(robj *r,
               int   cmatch,
               robj *okey,
               int   tmatch,
               flag *cflag,
               bool  icol,
               bool  force_string) {
    if (!cmatch) { // PK is stored ONLY in KEY not in ROW -> echo it
        return getPk(okey, icol, force_string);
    }
    aobj a;
    bzero(&a, sizeof(aobj)); /* avoid compiler warning */

    uchar  rflag;
    uint   rlen, ncols;
    uchar *meta     = r->ptr;
    uchar *row      = getRowPayload(meta, &rflag, &ncols, &rlen);
    uchar  sflag    = rflag & RFLAG_SIZE_FLAG;
    uchar *cofst    = meta + 2; // 2 {flag,ncols}
    int            o_cmatch = cmatch;

    cmatch--; // key was not stored
    uint start = 0;
    uint next;
    if (rflag & RFLAG_1BYTE_INT) {
        if (cmatch) start = (uint)*(cofst + cmatch - 1);
        next  = (uint)*(cofst + cmatch);
    } else if (rflag & RFLAG_2BYTE_INT) {
        unsigned short *m;
        if (cmatch) {
            m = (unsigned short *)(char *)(cofst + ((cmatch - 1) * sflag));
            start = (uint)*m;
        }
        m = (unsigned short *)(char *)(cofst + (cmatch * sflag));
        next = (uint)*m;
    } else {
        uint *i;
        if (cmatch) {
            i = (uint *)(char *)(cofst + ((cmatch - 1) * sflag));
            start = *i;
        }
        i = (uint *)(char *)(cofst + (cmatch * sflag));
        next = *i;
    }

    if (Tbl[server.dbid][tmatch].col_type[o_cmatch] == COL_TYPE_INT) {
        uint clen;
        a.type              = COL_TYPE_INT;
        uchar *data = row + start;
        uint   val  = streamIntToUInt(data, &clen, cflag);
        if (!force_string && icol) {
            a.i    = val;
            a.enc  = COL_TYPE_INT;
            a.len  = clen;
        } else {
            sprintf(RawCols[o_cmatch], "%u", val);
            a.len = strlen(RawCols[o_cmatch]);
            a.s   = RawCols[o_cmatch];
            a.enc  = COL_TYPE_STRING;
        }
    } else {
        a.type  = COL_TYPE_STRING;
        a.enc   = COL_TYPE_STRING;
        uint len = next - start;
        if (rflag & RFLAG_SIX_BIT_STRINGS) {
            uchar *s = (uchar *)(row + start);
            a.s      = (char *)unpackSixBitString(s, &len);
            a.sixbit = 1;
        } else {
            a.s    = (char *)row + start;
        }
        a.len  = len;
    }
    return a;
}

aobj getColStr(robj *r, int cmatch, robj *okey, int tmatch) {
    bool  icol   = (Tbl[server.dbid][tmatch].col_type[cmatch] == COL_TYPE_INT);
    return getRawCol(r, cmatch, okey, tmatch, NULL, icol, 1);
}

// creates robj (write-able)
robj *createColObjFromRow(robj *r, int cmatch, robj *okey, int tmatch) {
    aobj  rcol = getColStr(r, cmatch, okey, tmatch);
    robj *o    = createStringObject(rcol.s, rcol.len); // copies data
    if (rcol.sixbit) free(rcol.s);
    return o;
}

#define OUPUT_BUFFER_SIZE 4096
static char OutputBuffer[OUPUT_BUFFER_SIZE]; /*avoid malloc()s */
#define QUOTE_COL if (quote_text_cols) { memcpy(s + slot, "'", 1); slot++; }
robj *outputRow(robj *row,
                int   qcols,
                int   cmatchs[],
                robj *okey,
                int   tmatch,
                bool  quote_text_cols) {
    aobj         cols[MAX_COLUMN_PER_TABLE];
    uint totlen = 0;

    for (int i = 0; i < qcols; i++) {
        cols[i]  = getColStr(row, cmatchs[i], okey, tmatch);
        totlen  += cols[i].len;
    }
    totlen += (uint)qcols - 1; // -1 no final comma
    if (quote_text_cols) totlen += 2 * (uint)qcols;

    char *s;
    if (totlen >= OUPUT_BUFFER_SIZE) {
        s  = malloc(totlen);                  /* FREE me soon */
    } else {
        s = OutputBuffer;
    }
    uint slot = 0;
    for (int i = 0; i < qcols; i++) {
        QUOTE_COL
        memcpy(s + slot, cols[i].s, cols[i].len);
        slot += cols[i].len;
        QUOTE_COL
        if (i != (qcols - 1)) {
            memcpy(s + slot, OUTPUT_DELIM, 1);
            slot++;
        }
        if (cols[i].sixbit) free(cols[i].s);
    }
    robj *r = createStringObject(s, totlen);
    if (totlen >= OUPUT_BUFFER_SIZE) free(s); /* freeD */
    return r;
}

int deleteRow(redisClient *c,
              int          tmatch,
              robj        *pko,
              int          matches,
              int          indices[]) {
    robj *o   = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    robj *row = btFindVal(o, pko, Tbl[server.dbid][tmatch].col_type[0]);
    if (!row) return 0;
    if (matches) { // indices
        for (int i = 0; i < matches; i++) {         // delete indices
            delFromIndex(c->db, pko, row, indices[i], tmatch);
        }
    }
    btDelete(o, pko, Tbl[server.dbid][tmatch].col_type[0]);
    server.dirty++;
    return 1;
}

static robj *createStringObjectFromAobj(aobj *a) {
    if (a->type == COL_TYPE_STRING && a->enc == COL_TYPE_STRING) {
        return createStringObject(a->s, a->len);
    }
    robj *r = createObject(REDIS_STRING, NULL);
    int   i;
    if (a->enc == COL_TYPE_STRING) {
        i = atoi(a->s);
    } else {
        i = a->i;
    }
    r->ptr      = (void *)(long)i;
    r->encoding = REDIS_ENCODING_INT;
    return r;
}

//TODO simultaneous PK and normal update
bool updateRow(redisClient *c,
               robj        *o,
               robj        *okey,
               robj        *orow,
               int          tmatch,
               int          ncols,
               int          matches,
               int          indices[],
               char        *vals[],
               uint         vlens[],
               uchar        cmiss[]) {
    uint col_ofsts[MAX_COLUMN_PER_TABLE];
    aobj         avals    [MAX_COLUMN_PER_TABLE];
    flag         sflags   [MAX_COLUMN_PER_TABLE];
    uint rlen = 0;
    for (int i = 0; i < ncols; i++) {
        avals[i].len = 0;
        bool icol = (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT);
        if (cmiss[i]) {
            avals[i] = getRawCol(orow, i, okey, tmatch, &sflags[i], icol, 0);
        } else {
            avals[i].sixbit = 0;
            if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
                long l   = atol(vals[i]);
                if (l >= TWO_POW_32) {
                    addReply(c, shared.col_uint_too_big);
                    return 0;
                } else if (l < 0) {
                    addReply(c, shared.col_uint_no_negative_values);
                    return 0;
                }
                avals[i].i    = l;
                avals[i].type = COL_TYPE_INT;
                avals[i].enc  = COL_TYPE_INT;
            } else {
                avals[i].s    = vals[i];
                avals[i].len  = vlens[i];
                avals[i].type = COL_TYPE_STRING;
                avals[i].enc  = COL_TYPE_STRING;
            }
        }
        if (i && Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
            avals[i].len = _createICol(avals[i].i, &sflags[i], &(avals[i].s_i));
        }
        col_ofsts[i]  = rlen + avals[i].len;
        if (i) rlen  += avals[i].len; // dont store pk
    }

    uchar *sixbitstr[MAX_COLUMN_PER_TABLE];
    int            sixbitlen[MAX_COLUMN_PER_TABLE];
    uint  n_6b_s  = 0;
    bool can_six = 1;
    for (int i = 1; i < ncols; i++) {       // check can_six, create sixbitstr
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
            uint   s_len;
            uint   len   = avals[i].len;
            char          *start = avals[i].s;
            uchar *dest  = _createSixBitString(start, len, &s_len);
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

    // free SixBitStr from getRawCol()
    for (int i = 1; i < ncols; i++) if (avals[i].sixbit) free(avals[i].s);

    if (can_six) {
        uint k      = 0;
        uint shrunk = 0;
        for (int i = 1; i < ncols; i++) { // modify cofsts for SixBitStrings
            uint len  = avals[i].len;
            uint diff = 0;
            if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_STRING) {
                diff     = (len - sixbitlen[k]);
                k++;
            }
            shrunk       += diff;
            rlen         -= diff;
            col_ofsts[i] -= shrunk;
        }
    }

    uchar rflag;
    ASSIGN_RFLAG(can_six)
    SET_FLAG_NCOLS
    for (int i = 1; i < ncols; i++) {       // SET col_ofsts
        SET_COL_OFFSET(row, col_ofsts)
    }

    uint k = 0;
    for (int i = 1; i < ncols; i++) {       // SET data
        if (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT) {
            writeUIntCol(&row, sflags[i], avals[i].s_i);
        } else {
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

    for (uint j = 0; j < n_6b_s; j++) free(sixbitstr[j]);

    if (!cmiss[0]) { //pk update
        robj *npk = createStringObjectFromAobj(&avals[0]);
        for (int i = 0; i < matches; i++) { //redo ALL indices
            updateIndex(c->db, okey, npk, NULL, orow, indices[i], 1, tmatch);
        }
        // delete then add
        btDelete(o, okey,    Tbl[server.dbid][tmatch].col_type[0]);
        btAdd(   o, npk,  r, Tbl[server.dbid][tmatch].col_type[0]);
        decrRefCount(npk);
    } else if (matches) {
        robj *npk = createStringObjectFromAobj(&avals[0]);
        for (int i = 0; i < matches; i++) { //redo ALL affected indices
            int inum   = indices[i];
            int cmatch = Index[server.dbid][inum].column;
            if (!cmiss[cmatch]) {
                robj *new_val = createStringObjectFromAobj(&avals[cmatch]);
                updateIndex(c->db, okey, npk, new_val, orow, inum, 0, tmatch);
                decrRefCount(new_val);
            }
        }
        decrRefCount(npk);
        // overwrite w/ new row
        btReplace(o, okey, r, Tbl[server.dbid][tmatch].col_type[0]);
    }
    decrRefCount(r);
    server.dirty++;
    return 1;
}

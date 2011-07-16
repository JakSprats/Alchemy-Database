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

#include "sixbit.h"
#include "bt.h"
#include "index.h"
#include "lru.h"
#include "stream.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "row.h"

extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_INDICES];

extern uchar    OutputMode; // NOTE: used by OREDIS

// CONSTANT GLOBALS
extern char OUTPUT_DELIM;
extern char PLUS;  extern char MINUS;
extern char MULT;  extern char DIVIDE;
extern char POWER; extern char MODULO;
extern char STRCAT;

#define RFLAG_1BYTE_INT 1
#define RFLAG_2BYTE_INT 2
#define RFLAG_4BYTE_INT 4
#define RFLAG_SIZE_FLAG (RFLAG_1BYTE_INT + RFLAG_2BYTE_INT + RFLAG_4BYTE_INT)

#define RFLAG_6BIT_ZIP     8
#define RFLAG_LZF_ZIP     16

/* PROTOYPES */
static bool evalExpr(redisClient *c, ue_t *ue, aobj *aval, uchar ctype);

/* CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW CREATE_ROW */
typedef struct create_row_ctrl {
    int     tmatch;
    int     ncols;
    uint32  rlen;
    int     mcofsts[MAX_COLUMN_PER_TABLE]; //TODO stack alloc -> [ncols + lru]
    char   *strs   [MAX_COLUMN_PER_TABLE]; // break the []s out
    uint32  slens  [MAX_COLUMN_PER_TABLE];
    uchar   iflags [MAX_COLUMN_PER_TABLE];
    ulong   icols  [MAX_COLUMN_PER_TABLE]; /* for INT & LONG */
    float   fcols  [MAX_COLUMN_PER_TABLE];
} cr_t;
static void init_cr(cr_t *cr, int tmatch, int ncols) {
    cr->tmatch   = tmatch;
    cr->ncols    = ncols;
    cr->rlen     = 0;
    cr->strs [0] = NULL;
    cr->slens[0] = 0;
}
#define INIT_CR cr_t cr; init_cr(&cr, tmatch, ncols);

static bool contains_text_col(int tmatch, int ncols) {
    for (int i = 1; i < ncols; i++) {       /* sixzip needs a string */
        if (C_IS_S(Tbl[tmatch].col_type[i])) return 1;
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
    ulong   socl [MAX_COLUMN_PER_TABLE]; /* stream original col-len */
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
    uint32  oclen = streamIntToUInt(s, &clen);
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
    cz->lsocl[n] = cr8Icol(cr->slens[i], NULL, &cz->socl[n]);
    INCRBY(*mtlen, (cz->lsocl[cz->lzf_n] + cz->lzf_l[cz->lzf_n]));
    INCR(cz->lzf_n)
    return 1;
}
static uchar *writeLzfCol(uchar *row, cz_t *cz, int k) {
    memcpy(row, &cz->socl[k], cz->lsocl[k]); /* orig col_len*/
    row += cz->lsocl[k];
    memcpy(row, cz->lzf_s[k], cz->lzf_l[k]); /* compressed column */
    return row + cz->lzf_l[k];
}

#define DEBUG_MCOFSTS \
  for (int i = 1; i < cr->ncols; i++) \
    printf("mcofsts[%d]: %d\n", i, cr->mcofsts[i]);

static bool compression_justified(uint32 tlen, uint32 mtlen) {
    //printf("compression_justified: tlen: %u mtlen: %u\n", tlen, mtlen);
    return (mtlen < tlen); //TODO add in some intelligence
}
#define COL_LOOP_IF_TEXT \
    for (int i = 1; i < cr->ncols; i++) { \
        if (C_IS_S(Tbl[cr->tmatch].col_type[i])) {

static void zipCol(cr_t *cr, cz_t *cz) {/* NOTE: return here -> no compression*/
    cz->type = CZIP_SIX;
    COL_LOOP_IF_TEXT  /* if ANY TEXT col len > 20 -> LZF */
        if (cr->slens[i] > 20) { cz->type = CZIP_LZF; break; }
    }}
    if (cz->type == CZIP_LZF) {            /* ZIP LZF */
        uint32 tlen  = 0; /* sum length TEXT_cols */
        uint32 mtlen = 0; /* sum length compressed(TEXT_cols) */
        COL_LOOP_IF_TEXT
            if (!lzfZipCol(i, cr, cz, &tlen, &mtlen)) {
                cz->type = CZIP_LZF; break;
            }
        }}
        if (cz->type == CZIP_LZF && !compression_justified(tlen, mtlen)) {
            cz->type = CZIP_SIX; 
        }
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
    uint32 k = 0; uint32 shrunk = 0;
    for (int i = 1; i < cr->ncols; i++) { /* MOD cofsts (zipping) */
        int diff = 0;
        if (C_IS_S(Tbl[cr->tmatch].col_type[i])) {
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
static int set_col_offst(uchar *row, int i, uchar rflag, int cofst[]) {
    if        (rflag & RFLAG_1BYTE_INT) {
        *row = (uchar)cofst[i];
        return 1;
    } else if (rflag & RFLAG_2BYTE_INT) {
        ushort16 m = (ushort16)cofst[i];
        memcpy(row, &m, USHORT_SIZE);
        return USHORT_SIZE;
    } else {        /* RFLAG_4BYTE_INT */
        memcpy(row, &cofst[i], UINT_SIZE);
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
        uchar ctype = Tbl[cr->tmatch].col_type[i];
        if        C_IS_I(ctype) {
            writeUIntCol(&row,  cr->iflags[i], cr->icols[i]);
        } else if C_IS_L(ctype) {
            writeULongCol(&row, cr->iflags[i], cr->icols[i]);
        } else if C_IS_F(ctype) {
            writeFloatCol(&row, cr->fcols[i]);
        } else {/* COL_TYPE_STRING */
            if (       cz.type == CZIP_SIX) {
                memcpy(row, cz.sixs[k], cz.sixl[k]);
                row += cz.sixl[k];
                k++;
            } else if (cz.type == CZIP_LZF) {
                row = writeLzfCol(row, &cz, k);
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
#define DEBUG_CREATE_ROW \
  printf("nclen: %d rlen: %d c[%d]: %d", nclen, cr.rlen, i, cr.mcofsts[i]);    \
  if C_IS_NUM(ctype) printf(" iflags: %d col: %u", cr.iflags[i], cr.icols[i]); \
  printf("\n");

char *EmptyStringCol = "''";
char *EmptyCol       = "";
void *createRow(cli    *c,    bt     *btr,      int tmatch, int  ncols,
                char   *vals, twoint  cofsts[]) {
    if OTHER_BT(btr) { /* UU,UL,LU,LL rows no malloc, 'void *' of 1st COL */
        char uubuf[32];
        int c1len = (cofsts[1].j - cofsts[1].i);
        memcpy(uubuf, vals + cofsts[1].i, c1len); uubuf[c1len] = '\0';
        return (void *)strtoul(uubuf, NULL, 10); /* OK: DELIM: \0 */
    }
    INIT_CR
    for (int i = 1; i < cr.ncols; i++) { /* MOD cofsts (no PK,no commas,PACK) */
        uchar ctype  = Tbl[tmatch].col_type[i];
        if (cofsts[i].i == -1) { //TODO next block's logic should be done here
            if (C_IS_S(ctype)) { cr.strs[i] = EmptyStringCol; cr.slens[i] = 2; }
            else               { cr.strs[i] = EmptyCol;       cr.slens[i] = 0; }
        } else {
            char *startc = vals + cofsts[i].i;
            char *endc   = vals + cofsts[i].j;
            cr.strs[i]   = startc;
            int   clen   = endc - startc;
            SKIP_SPACES(cr.strs[i])
            char *mendc  = endc;
            if (ISBLANK(*(mendc))) REV_SKIP_SPACES(mendc)
            cr.slens[i]  = clen - (cr.strs[i] - startc) - (endc - mendc);
        }
        int nclen    = 0;
        if        (C_IS_S(ctype)) { /* dont store \' delimiters */
            cr.strs[i]++; cr.slens[i] -= 2; nclen = cr.slens[i];
        } else if (C_IS_I(ctype)) {
            nclen = cr8IcolFromStr(c, cr.strs[i],    cr.slens[i],
                                      &cr.iflags[i], &cr.icols[i]);
            if (nclen == -1) return NULL;
        } else if (C_IS_L(ctype)) {
            nclen = cr8LcolFromStr(c, cr.strs[i],    cr.slens[i],
                                      &cr.iflags[i], &cr.icols[i]);
        } else if (C_IS_F(ctype)) {
            nclen = cr8FColFromStr(c, cr.strs[i], cr.slens[i], &cr.fcols[i]);
            if (nclen == -1) return NULL;
        }
        cr.rlen       += nclen;
        cr.mcofsts[i]  = (int)cr.rlen;                       //DEBUG_CREATE_ROW
    }
    if (Tbl[tmatch].lrud) { /* NOTE: updateLRU (INSERT) */
        int    i       = cr.ncols;
        int    nclen   = cLRUcol(getLru(tmatch), &cr.iflags[i], &(cr.icols[i]));
        cr.rlen       += nclen;
        cr.mcofsts[i]  = (int)cr.rlen;
        cr.ncols++;
    }
    return writeRow(&cr);
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
#define INIT_COL_AVALS \
  aobj avs[MAX_COLUMN_PER_TABLE]; \
  for (int i = 0; i < ncols; i++) initAobj(&avs[i]);
#define DESTROY_COL_AVALS \
  for (int i = 0; i < ncols; i++) releaseAobj(&avs[i]);

static bool upEffctdInds(cli  *c,     bt   *btr,  aobj *apk,     void *orow,
                         aobj  avs[], void *nrow, int   matches, int   inds[],
                         uchar cmiss[]) {
    for (int i = 0; i < matches; i++) { /* Redo ALL EFFECTED indexes */
        bool up = 0;
        r_ind_t *ri = &Index[inds[i]];
        if      (ri->lru) up = 1; /* ALWAYS update LRU Index */
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
static uint32 getIorLKeyLen(aobj *a) {
    uchar sflag; ulong col, l = C_IS_I(a->type) ? a->i : a->l;
    return cIcol(l, &sflag, &col, C_IS_I(a->type));
}
static bool aobj_sflag(aobj *a, uchar *sflag) {
    ulong col, l = C_IS_I(a->type) ? a->i : a->l;
    cIcol(l, sflag, &col, C_IS_I(a->type));
    return !a->empty;
}
#define DEBUG_UPDATE_ROW                                                 \
  if (i) { printf ("%d: oflag: %d cmiss: %d ", i, osflags[i], cmiss[i]); \
    DEBUG_CREATE_ROW printf("\t\t\t\t"); dumpAobj(printf, &avs[i]); }

#define OVRWR(i, ovrwr, ctype) /*NOTE: !indexed -> upIndex uses orow & nrow */ \
  (i && ovrwr && C_IS_NUM(ctype) && !Tbl[tmatch].col_indxd[i])

//TODO too many arguments for a per-row-OP, merge into a struct
int updateRow(cli  *c,      bt      *btr,    aobj  *apk,     void *orow,
              int   tmatch, int     ncols,   int    matches, int   inds[],
              char *vals[], uint32  vlens[], uchar  cmiss[], ue_t  ue[])   {
    INIT_COL_AVALS      /* merges values in update_string and vals from ROW */
    INIT_CR             /* holds values written to new ROW */
    uchar osflags[MAX_COLUMN_PER_TABLE]; bzero(osflags, MAX_COLUMN_PER_TABLE);
    uchar   *nrow  = NULL; /* B4 GOTO */
    r_tbl_t *rt    = &Tbl[tmatch];
    bool     ovrwr = NORM_BT(btr);
    int      ret   = -1;    /* presume failure */
    for (int i = 0; i < cr.ncols; i++) { /* 1st loop UPDATE columns -> cr */
        uchar ctype = rt->col_type[i];
        if (cmiss[i]) {
            avs[i] = getCol(btr, orow, i, apk, tmatch);
            if (rt->lrud && rt->lruc == i) {
                if (avs[i].empty) ovrwr      = 0;
                else              osflags[i] = getLruSflag();/* Overwrite LRU */
            }
        } else if (ue[i].yes) {
            avs[i] = getCol(btr, orow, i, apk, tmatch);
            if (avs[i].empty) { addReply(c, shared.up_on_mt_col); goto up_end; }
            if (!OVRWR(i, ovrwr, ctype) ||
                !aobj_sflag(&avs[i], &osflags[i])) ovrwr = 0;
            if (!evalExpr(c, &ue[i], &avs[i], ctype))             goto up_end;
        } else { /* comes from UPDATE VALUE LIST (no expression) */
            if OVRWR(i, ovrwr, ctype) {
                aobj a = getCol(btr, orow, i, apk, tmatch);
                if (!aobj_sflag(&a, &osflags[i])) ovrwr = 0;
            } else ovrwr = 0;
            if        C_IS_I(ctype) {
                ulong l = strtoul(vals[i], NULL, 10); /* OK: DELIM: [\ ,=,\0] */
                if (l >= TWO_POW_32) { addReply(c, shared.u2big); goto up_end; }
                initAobjInt(&avs[i], l);
            } else if C_IS_L(ctype) {
                ulong l = strtoul(vals[i], NULL, 10); /* OK: DELIM: [\ ,=,\0] */
                initAobjLong(&avs[i], l);
            } else if C_IS_F(ctype) {
                float f = atof(vals[i]);              /* OK: DELIM: [\ ,=,\0] */
                initAobjFloat(&avs[i], f);
            } else {/* COL_TYPE_STRING*/  /* ignore \' delimiters */
                initAobjString(&avs[i], vals[i] + 1, vlens[i] - 2);
            }
        }
        int nclen = 0;
        if (i) { /* NOT PK, populate cr values (PK not stored in row)*/
            if (rt->lrud && rt->lruc == i) { /* NOTE: updateLRU (UPDATE)1 */
                nclen = cLRUcol(getLru(tmatch), &cr.iflags[i], &(cr.icols[i]));
            } else if C_IS_I(ctype) { //TODO push empty logic into cr8*()
                if (avs[i].empty) { cr.iflags[i] = 0; nclen = 0; }
                else nclen = cr8Icol(avs[i].i, &cr.iflags[i], &(cr.icols[i]));
            } else if C_IS_L(ctype) {
                if (avs[i].empty) { cr.iflags[i] = 0; nclen = 0; }
                else nclen = cr8Lcol(avs[i].l, &cr.iflags[i], &(cr.icols[i]));
            } else if C_IS_F(ctype) {
                if (avs[i].empty) nclen = 0;
                else              { cr.fcols[i] = avs[i].f; nclen = 4; }
            } else {/* COL_TYPE_STRING*/
                cr.strs[i]  = avs[i].s; nclen = avs[i].len; cr.slens[i] = nclen;
            } 
        }
        cr.mcofsts[i]  = cr.rlen + nclen;
        cr.rlen       += nclen;                              //DEBUG_UPDATE_ROW
    }
    if (ovrwr) { /* only OVERWRITE if all OLD and NEW sflags match */
        for (int i = 1; i < cr.ncols; i++) {
            if (osflags[i] && osflags[i] != cr.iflags[i]) { ovrwr = 0; break; }
    }}
    if (ovrwr) { /* just OVERWRITE INTS & LONGS */
        for (int i = 1; i < cr.ncols; i++) {
            if (osflags[i]) {
                uint32 clen; uchar rflag;
                uchar *row   = getColData(orow, i, &clen, &rflag);
                uchar  ctype = rt->col_type[i];
                if       (rt->lrud && rt->lruc == i) {
                    updateLru(c, tmatch, apk, row);/*NOTE: updateLRU (UPDATE)2*/
                } else if C_IS_I(ctype) {
                    writeUIntCol(&row,  cr.iflags[i], cr.icols[i]);
                } else if C_IS_L(ctype) {
                    writeULongCol(&row, cr.iflags[i], cr.icols[i]);
                }
            }
        }
        if (rt->nltrgr) { /* OVERWRITE still needs to run LUATRIGGERs */
            for (int i = 0; i < matches; i++) {
                if (!Index[inds[i]].luat) continue;
                if (!updateIndex(c, btr, apk, orow, &avs[0], nrow, inds[i])) {
                                                                  goto up_end;
            }}
        }
        ret = getIorLKeyLen(apk) + getRowMallocSize(orow);
    } else {
        nrow = (UU(btr) || LU(btr)) ? (uchar *)(long)avs[1].i :
               (UL(btr) || LL(btr)) ? (uchar *)      avs[1].l : writeRow(&cr);
        if (!cmiss[0]) { /* PK update */
            for (int i = 0; i < matches; i++) { /* Redo ALL inds */
                if (!updateIndex(c, btr, apk, orow, &avs[0], nrow, inds[i])) {
                                                                  goto up_end;
            }}
            btDelete(btr, apk);              /* DELETE row w/ OLD PK */
            ret = btAdd(btr, &avs[0], nrow); /* ADD row w/ NEW PK */
            UPDATE_AUTO_INC(rt->col_type[0], avs[0])
        } else {
            if (!upEffctdInds(c, btr, apk, orow, avs, nrow,
                              matches, inds, cmiss))              goto up_end;
            ret = btReplace(btr, apk, nrow); /* overwrite w/ new row */
        }
    }
    server.dirty++;

up_end:
    if (nrow && NORM_BT(btr)) free(nrow);                /* FREED 023 */
    DESTROY_COL_AVALS
    return ret;
}

/* GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL GET_COL */
#define DEBUG_GET_RAW_COL \
printf("getRawCol: tmatch: %d cmatch: %d force_s: %d apk: ", \
        tmatch, cmatch, force_s); dumpAobj(printf, apk);

static uchar *getRowPayload(uchar  *row,   uchar  *rflag,
                            uint32 *ncols, uint32 *rlen) {
    uchar *o_row     = row;
    *rflag           = *row;                          /* GET flag */
    row++;
    char   sflag     = *rflag & RFLAG_SIZE_FLAG;
    *ncols           = *row;                          /* GET ncols */
    row++;
    row             += (*ncols * sflag);              /* SKIP cofsts */
    uint32 meta_len  = row - o_row;
    if        (*rflag & RFLAG_1BYTE_INT) {            /* rlen is final cofst */
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
    uchar rflag; uint32 rlen; uint32 ncols;
    getRowPayload(stream, &rflag, &ncols, &rlen);
    return rlen;
}

static char RawCols[MAX_COLUMN_PER_TABLE][32]; /* NOTE: avoid malloc's */
static void initAobjCol2S(aobj *a, ulong l, float f, int cmatch, int ktype) {
    char *fmt = C_IS_F(ktype) ? FLOAT_FMT : "%lu";
    C_IS_F(ktype) ? snprintf(RawCols[cmatch], 32, fmt, f) :
                    snprintf(RawCols[cmatch], 32, fmt, l);
    a->len = strlen(RawCols[cmatch]);
    a->s   = RawCols[cmatch];
    a->enc = COL_TYPE_STRING; //dumpAobj(printf, a);
}
static void initLongAobjFromVal(aobj *a, ulong l, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_LONG;
    if (force_s) initAobjCol2S(a, l, 0.0, cmatch, COL_TYPE_LONG);
    else         { a->l = l; a->enc = COL_TYPE_LONG; }
}
static void initIntAobjFromVal(aobj *a, uint32 i, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_INT;
    ulong l = (ulong)i;
    if (force_s) initAobjCol2S(a, l, 0.0, cmatch, COL_TYPE_INT);
    else         { a->i = i; a->enc = COL_TYPE_INT; }
}
static void initFloatAobjFromVal(aobj *a, float f, bool force_s, int cmatch) {
    initAobj(a); a->type = COL_TYPE_FLOAT;
    if (force_s) initAobjCol2S(a, 0, f,   cmatch, COL_TYPE_FLOAT);
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
uchar *getColData(void *orow, int cmatch, uint32 *clen, uchar *rflag) {
    uint32 rlen; uint32 ncols;
    uchar   *meta     = (uchar *)orow;
    uchar   *row      = getRowPayload(meta, rflag, &ncols, &rlen);
    if ((uint32)cmatch > ncols) { *clen = 0; return row; }
    uchar    sflag    = *rflag & RFLAG_SIZE_FLAG;
    uchar   *cofst    = meta + ROW_META_SIZE;
    int      mcmatch = cmatch - 1; /* key was not stored -> one less column */
    uint32   start    = 0;
    uint32   next;
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
    *clen  = next - start;
    return row + start;
}
aobj getRawCol(bt  *btr,    void *orow, int cmatch, aobj *apk,
               int  tmatch, bool force_s) {
    if        UU(btr) { /* OTHER_BT values are either in PK or BT - no ROW */
        ulong key = cmatch ? (ulong)orow        : apk->i;
        return colFromUU(key, force_s, cmatch);
    } else if UL(btr) {
        ulong key = cmatch ? ((ulk *)orow)->val : apk->i;
        return colFromUL(key, force_s, cmatch);
    } else if LU(btr) {
        ulong key = cmatch ? ((luk *)orow)->val : apk->l;
        return colFromLU(key, force_s, cmatch);
    } else if LL(btr) {
        ulong key = cmatch ? ((llk *)orow)->val : apk->l;
        return colFromLL(key, force_s, cmatch);
    }
    aobj a; initAobj(&a); //DEBUG_GET_RAW_COL
    r_tbl_t *rt    = &Tbl[tmatch];
    uchar    ctype = rt->col_type[cmatch];
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
inline aobj getCol(bt *btr, void *rrow, int cmatch, aobj *apk, int tmatch) {
    return getRawCol(btr, rrow, cmatch, apk, tmatch, 0);
}
inline aobj getSCol(bt *btr, void *rrow, int cmatch, aobj *apk, int tmatch) {
    return getRawCol(btr, rrow, cmatch, apk, tmatch, 1);
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
        slot += outs[i].len;
        release_sl(outs[i]);
        QUOTE_COL
        FINAL_COMMA
    }
    robj *r = createStringObject(obuf, totlen);
    if (obuf != OutBuff) free(obuf);                     /* FREED 072 */
    return r;
}
static robj *orow_redis(bt   *btr,       void *rrow, int   qcols,
                        int   cmatchs[], aobj *apk,  int   tmatch) {
    char pbuf[128];
    sl_t outs[MAX_COLUMN_PER_TABLE];
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
    sl_t   outs[MAX_COLUMN_PER_TABLE];
    uint32 totlen = 0;
    for (int i = 0; i < qcols; i++) {
        aobj  col       = getSCol(btr, rrow, cmatchs[i], apk, tmatch);
        outs[i].s       = col.s;
        outs[i].len     = col.len;
        outs[i].freeme  = col.freeme;
        outs[i].type    = Tbl[tmatch].col_type[cmatchs[i]];
        totlen         += col.len;
        if (C_IS_S(outs[i].type) && outs[i].len) totlen += 2;/* 2 \'s per col */
    }
    totlen  += (uint32)qcols - 1; /* one comma per COL, except final */
    return write_output_row(qcols, 0, NULL, totlen, outs);
}
robj *outputRow(bt   *btr,       void *rrow, int qcols,
                int   cmatchs[], aobj *apk,  int tmatch) {
    return OREDIS ? orow_redis (btr, rrow, qcols, cmatchs, apk, tmatch) :
                    orow_normal(btr, rrow, qcols, cmatchs, apk, tmatch);
}

bool deleteRow(int tmatch, aobj *apk, int matches, int inds[]) {
    bt   *btr  = getBtr(tmatch);
    void *rrow = btFind(btr, apk);
    if (!rrow) return 0;
    if (matches) { /* delete indexes */
        for (int i = 0; i < matches; i++) delFromIndex(btr, apk, rrow, inds[i]);
    }
    btDelete(btr, apk);
    server.dirty++;
    return 1;
}

/* UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR UPDATE_EXPR */
static bool evalExpr(redisClient *c, ue_t *ue, aobj *aval, uchar ctype) {
    if (C_IS_NUM(ctype)) { /* INT & LONG */
        ulong l      = 0; double f = 0.0;
        bool  is_f   = (ue->type == UETYPE_FLOAT);
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
        if      (ue->op == PLUS)   m += (is_f ? (ulong)f : l);
        else if (ue->op == MINUS)  m -= (is_f ? (ulong)f : l);
        else if (ue->op == MODULO) m %= (is_f ? (ulong)f : l);
        else if (ue->op == DIVIDE) {
            if (ue->type == UETYPE_FLOAT) m = (ulong)((double)m / f);
            else                          m = m / l;
        } else if (ue->op == MULT) {
            if (ue->type == UETYPE_FLOAT) m = (ulong)((double)m * f);
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
            m        = (ulong)d;
        }
        if (C_IS_I(ctype)) {
            if (m >= TWO_POW_32) { addReply(c, shared.u2big); return 0; }
            aval->i = m;
        } else { /* LONG */
            aval->l = m;
        }
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
    } else { /* COL_TYPE_STRING*/
        int len      = aval->len + ue->plen;
        char *s      = malloc(len + 1);
        memcpy(s,             aval->s,  aval->len);
        memcpy(s + aval->len, ue->pred, ue->plen);
        s[len]       = '\0';
        //TODO MEMLEAK
        //free(aval->s);
        aval->freeme = 1; /* this new string must be freed later */
        aval->s      = s;
        aval->len    = len;
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

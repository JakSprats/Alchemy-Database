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

#include "bt.h"
#include "aobj.h"
#include "common.h"
#include "stream.h"

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
/* LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU LRU */
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
/* INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG INT+LONG */
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
    uchar  b1 = *data;
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
    if (!len) { *sflag = 0; *col = 0; return 0; }
    if (!strToULong(c, start, len, col, 1)) return -1;
    return cr8Icol(*col, sflag, col);
}
int cr8LcolFromStr(cli   *c,     char  *start, uint32 len,
                      uchar *sflag, ulong *col) {
    if (!len) { *sflag = 0; *col = 0; return 0; }
    if (!strToULong(c, start, len, col, 0)) return -1;
    return cr8Lcol(*col, sflag, col);
}
void writeUIntCol(uchar **row, uchar sflag, ulong icol) {
    wUCol(row, sflag, icol, 1);
}
void writeULongCol(uchar **row, uchar sflag, ulong icol) {
    wUCol(row, sflag, icol, 0);
}
uint32 streamIntToUInt(uchar *data, uint32 *clen) {
    return (uint32)sI2I(data, clen, 1);
}
ulong  streamLongToULong(uchar *data, uint32 *clen) {
    return sI2I(data, clen, 0);
}
float streamFloatToFloat(uchar *data, uint32 *clen) {
    float val;
    if (clen) *clen  = 4;
    memcpy(&val, data, 4);
    return val;
}

/* COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE COMPARE */
int ulongCmp(void *s1, void *s2) {
    ulong l1  = (ulong)s1;
    ulong l2  = (ulong)s2;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int uintCmp(void *s1, void *s2) {
    return ulongCmp(s1, s2);
}
int uuCmp(void *s1, void *s2) { //TODO can be done w/ bit-shifting
    return (int)(((long)s1 / UINT_MAX) - ((long)s2 / UINT_MAX));
}
int luCmp(void *s1, void *s2) { /* 12 bytes [8:ULONG,4:UINT] */
    luk   *lu1 = (luk *)s1;
    luk   *lu2 = (luk *)s2;
    ulong  l1  = lu1->key;
    ulong  l2  = lu2->key;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int ulCmp(void *s1, void *s2) { /* 12 bytes [4:UINT,8:ULONG] */
    ulk  *ul1 = (ulk *)s1;
    ulk  *ul2 = (ulk *)s2;
    long  l1  = ul1->key;
    long  l2  = ul2->key;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}
int llCmp(void *s1, void *s2) { /* 16 bytes [8:UINT,8:ULONG] */
    llk   *ll1 = (llk *)s1;
    llk   *ll2 = (llk *)s2;
    ulong  l1  = ll1->key;
    ulong  l2  = ll2->key;
    return l1 == l2 ? 0 : (l1 > l2) ? 1 : -1;
}

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
static void cr8BTKFloat(aobj *akey, uint32 *ksize, uchar *btkey) {
    writeFloatCol(&btkey, 1, akey->f); *ksize = 4;
}
int btIntCmp(void *a, void *b) {
    uint32 key1 = streamIntToUInt(a, NULL);
    uint32 key2 = streamIntToUInt(b, NULL);
    return key1 == key2 ? 0 : (key1 > key2) ? 1 : -1;
}
int btLongCmp(void *a, void *b) {
    ulong key1 = streamLongToULong(a, NULL);
    ulong key2 = streamLongToULong(b, NULL);
    return key1 == key2 ? 0 : (key1 > key2) ? 1 : -1;
}
int btFloatCmp(void *a, void *b) {
    float key1 = streamFloatToFloat(a, NULL);
    float key2 = streamFloatToFloat(b, NULL);
    float f    = key1 - key2;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}
int btTextCmp(void *a, void *b) {
    uint32 slen1, slen2;
    uchar *s1     = (uchar *)a;
    uchar *s2     = (uchar *)b;
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

void destroyBTKey(char *btkey, bool med) { if (med) free(btkey);/* FREED 033 */}
char *createBTKey(aobj *akey, bool *med, uint32 *ksize, bt *btr) {
    *med   = 0; *ksize = VOIDSIZE;
    if        INODE_I(btr) { return (char *) (long)akey->i;
    } else if INODE_L(btr) { return (char *) (long)akey->l;
    } else if UU     (btr) { return (char *)((long)akey->i * UINT_MAX);
    } else if UL     (btr) { /* NOTE: UP() also */
        UL_BTKeyPtr.key = akey->i; return (char *)&UL_BTKeyPtr;
    } else if LU     (btr) { /* NOTE: LUP() also */
        LU_BTKeyPtr.key = akey->l; return (char *)&LU_BTKeyPtr;
    } else if LL     (btr) { /* NOTE: LP() also */
        LL_BTKeyPtr.key = akey->l; return (char *)&LL_BTKeyPtr;
    }
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
    } else if (C_IS_L(ktype))    cr8BTKLong (akey, ksize, btkey);
      else if (C_IS_F(ktype))    cr8BTKFloat(akey, ksize, btkey);
      else if (C_IS_I(ktype) && !cr8BTKInt  (akey, ksize, btkey)) return NULL;
    return (char *)btkey;
}
static uint32 skipToVal(uchar **stream, uchar ktype) {
    uint32  klen  = 0;
    if      (C_IS_I(ktype)) streamIntToUInt(   *stream, &klen);
    else if (C_IS_L(ktype)) streamLongToULong( *stream, &klen);
    else if (C_IS_F(ktype)) streamFloatToFloat(*stream, &klen);
    else {
        if (getSflag(**stream)) { getTString(*stream, &klen); klen++; }
        else                    { getString (*stream, &klen); klen += 4; }
    }
    *stream += klen;
    return klen;
}

#define DEBUG_PARSE_STREAM \
printf("parseStream: INODE: %d UU: %d UP: %d LU: %d LP: %d OTHER: %d\n", \
                 INODE(btr), UU(btr), UP(btr), LU(btr), LP(btr), OTHER_BT(btr));

uchar *parseStream(uchar *stream, bt *btr) { //DEBUG_PARSE_STREAM
    if     (!stream || INODE(btr)) return NULL;
    else if UU      (btr)          return (uchar *)((long)stream % UINT_MAX);
    else if UP      (btr)          return (uchar *)(*(ulk *)(stream)).val; 
    else if LUP     (btr)          return (uchar *)(long)(*(luk *)(stream)).val;
    else if LP      (btr)          return (uchar *)(*(llk *)(stream)).val; 
    else if OTHER_BT(btr)          return stream;
    skipToVal(&stream, btr->s.ktype);
    if      (btr->s.btype == BTREE_TABLE)   return stream;
    else if (btr->s.btype == BTREE_INODE)   return NULL;
    else                  /* BTREE_INDEX */ return *((uchar **)stream);
}
void convertStream2Key(uchar *stream, aobj *key, bt *btr) {
    initAobj(key);
    if        INODE_I(btr) {
        key->type = key->enc = COL_TYPE_INT;
        key->i    = INTVOID stream;
    } else if INODE_L(btr) {
        key->type = key->enc = COL_TYPE_LONG;
        key->l    = (ulong)stream;
    } else if UU     (btr) {
        key->type = key->enc = COL_TYPE_INT;
        key->i    = (uint32)((long)stream / UINT_MAX);
    } else if UL     (btr) { /* NOTE: UP() also */
        key->type = key->enc = COL_TYPE_INT;
        key->i    = (*(ulk *)(stream)).key;
    } else if LU     (btr) { /* NOTE: LUP() also */
        key->type = key->enc = COL_TYPE_LONG;
        key->l    = (*(luk *)(stream)).key;
    } else if LL     (btr) { /* NOTE: LP() also */
        key->type = key->enc = COL_TYPE_LONG;
        key->l    = (*(luk *)(stream)).key;
    } else { /* NORM_BT */
        int ktype = btr->s.ktype;
        if        (C_IS_I(ktype)) {
            key->type = key->enc = COL_TYPE_INT;
            key->i    = streamIntToUInt(stream, NULL);
        } else if (C_IS_L(ktype)) {
            key->type = key->enc = COL_TYPE_LONG;
            key->l    = streamLongToULong(stream, NULL);
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
#define DEBUG_CREATE_STREAM \
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
        else if UU   (btr) return UU_SIZE;
        else if UL   (btr) return UL_SIZE; /* NOTE: UP() also */
        else if LU   (btr) return LU_SIZE; /* NOTE: LUP() also */
        else /* LL */      return LL_SIZE; /* NOTE: LP() also */
    }
}

static ulk UL_StreamPtr; static luk LU_StreamPtr; static llk LL_StreamPtr;
// btkey from createBTKey() - bit-packed-num or string-w-length
// row from writeRow() - [normal|hash]_row of [bit-packed-num|string-w-length]
// NOTE: rows (but NOT btkeys) can have compressed strings
// STREAM BINARY FORMAT: [btkey|row]
void *createStream(bt *btr, void *val, char *btkey, uint32 klen, uint32 *size) {
    *size             = 0;                               // DEBUG_BT_TYPE(btr);
    if        INODE(btr) { return btkey;
    } else if UU   (btr) { return (void *)((long)btkey + (long)val); /* merge */
    } else if UL   (btr) { /* NOTE: UP() also */
        ulk *ul          = (ulk *)btkey;
        UL_StreamPtr.key = ul->key; UL_StreamPtr.val = (ulong)val;
        return &UL_StreamPtr;
    } else if LU   (btr) { /* NOTE: LUP() also */
        luk *lu          = (luk *)btkey;
        LU_StreamPtr.key = lu->key; LU_StreamPtr.val = (uint32)(long)val;
        return &LU_StreamPtr;
    } else if LL   (btr) { /* NOTE: LP() also */
        llk *ll          = (llk *)btkey;
        LL_StreamPtr.key = ll->key; LL_StreamPtr.val = (ulong)val;
        return &LL_StreamPtr;
    }
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

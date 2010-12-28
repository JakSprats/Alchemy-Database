/*
 *
 * This file implements stream parsing for Rows

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
#include <assert.h>
#include <strings.h>

#include "bt.h"
#include "aobj.h"
#include "common.h"
#include "stream.h"


/* TODO these flags should be #defines */
//TODO this should be a one-liner, not a nested if-else
static uchar getSflag(uchar b1) {
    if      (b1 & 1)  return 1;
    else if (b1 & 2)  return 2;
    else if (b1 & 4)  return 4;
    else if (b1 & 8)  return 8;
    else if (b1 & 16) return 16;
    else if (b1 & 32) return 32;
    else {
        RL4 "b1: %d", b1);
        assert(!"getSflag programming error");
    }
}

static inline uint32 get14BitInt(uchar *s) {
    uint32 key   = (uint32)(*((unsigned short *)s));
    key         -= 2;
    key         /= 4;
    return key;
}
static inline uint32 get28BitInt(uchar *s) {
    uint32 key  = *((uint32 *)s);
    key        -= 8;
    key        /= 16;
    return key;
}
static inline uint32 getInt(uchar **s) {
    *s         = *s + 1;
    uint32 key = *((uint32 *)*s);
    return key;
}
static inline uchar *getTinyString(uchar *s, uint32 *slen) {
    *slen = ((int)*s / 2);
    s++;
    return s;
}
static inline uchar *getString(uchar *s, uint32 *slen) {
    s++;
    *slen  = *((uint32 *)s);
    s     += 4;
    return s;
}
static inline float getFloat(uchar *s) {
    s++;
    float f;
    memcpy(&f, s, 4);
    return f;
}

int btStreamCmp(void *a, void *b) {
    if (!a || !b) return -1;
    uchar *s1     = (uchar *)a;
    uchar *s2     = (uchar *)b;
    uchar  sflag1 = getSflag(*s1);
    uchar  sflag2 = getSflag(*s2);

    if (sflag1 == 1 || sflag1 == 4) { // STRING
        uint32 slen1, slen2;
        slen1 = slen2 = 0; /* compiler warning */
        if (sflag1 == 1)      s1 = getTinyString(s1, &slen1);
        else if (sflag1 == 4) s1 = getString(    s1, &slen1);
        if (sflag2 == 1)      s2 = getTinyString(s2, &slen2);
        else if (sflag2 == 4) s2 = getString(    s2, &slen2);

        if (slen1 == slen2) return strncmp((char *)s1, (char *)s2, slen1);
        else {
            int i   = (slen1 < slen2) ? slen1 : slen2;
            int ret = strncmp((char *)s1, (char *)s2, i); 
            return (ret == 0) ? ((slen1 < slen2) ? -1 : 1) : ret;
        }
    } else if (sflag1 <= 16) {        // INT
        uint32 key1, key2;
        if      (sflag1 == 2)   key1  = get14BitInt(s1);
        else if (sflag1 == 8)   key1  = get28BitInt(s1);
        else  /* sflag == 16 */ key1  = getInt(&s1); 

        if      (sflag2 == 2)   key2  = get14BitInt(s2);
        else if (sflag2 == 8)   key2  = get28BitInt(s2);
        else  /* sflag == 16 */ key2  = getInt(&s2);
        return (key1 == key2) ? 0 : ((key1 > key2) ? 1 : -1);
    } else {                          // FLOAT
        float key1 = getFloat(s1);
        float key2 = getFloat(s2);
        float f    = key1 - key2;
        return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
    }
    return 0;
}

#define BTKEY_BUFFER_SIZE 2048
static char BTKeyBuffer[BTKEY_BUFFER_SIZE]; /*avoid malloc()s */

void destroyBTKey(char *simkey, bool med) {
    if (med) free(simkey);
}

char *createBTKey(const aobj *akey,
                  int         ktype,
                  bool       *med,
                  uchar      *sflag,
                  uint32     *ksize) {
    if (ktype == COL_TYPE_STRING) assert(akey->s);

    *med           = 0;
    char   *simkey = NULL; /* compiler warning */
    uint32  data   = 0;
    if (ktype == COL_TYPE_STRING) {
        if (akey->len < TWO_POW_7) { // tiny STRING
            *sflag     = 1;
            *ksize     = akey->len + 1;
            data       = akey->len * 2 + 1;
            if (akey->len + 1 >= BTKEY_BUFFER_SIZE) {
                simkey = malloc(akey->len + 1); /* MUST be freed soon */
                *med   = 1;
            } else {
                simkey = BTKeyBuffer;
            }
            *simkey    = (char)data;
            memcpy(simkey + 1, akey->s, akey->len);
        } else {                           // STRING
            uint32 len = akey->len;
            *sflag     = 4;
            *ksize     = len + 5;
            if (len + 5 >= BTKEY_BUFFER_SIZE) {
                simkey = malloc(len + 5); /* MUST be freed soon */
                *med   = 1;
            } else {
                simkey = BTKeyBuffer;
            }
            *simkey    = 4;
            data       = len;
            memcpy(simkey + 1, &data, 4);
            memcpy(simkey + 5, akey->s, len);
        }
    } else if (ktype == COL_TYPE_INT) {
        ulong l = (ulong)akey->i;
        simkey  = BTKeyBuffer;
        if (l >= TWO_POW_32) {
            //redisLog(REDIS_WARNING, "column value > UINT_MAX");
            return NULL;
        }
        if (l < TWO_POW_14) {        // 14bit INT
            ushort m = (ushort)(l * 4 + 2);
            memcpy(simkey, &m, 2);
            *sflag    = 2;
            *ksize    = 2;
        } else if (l < TWO_POW_28) { // 28bit INT
            data      = (l * 16 + 8);
            memcpy(simkey, &data, 4);
            *sflag    = 8;
            *ksize    = 4;
        } else {                     // INT
            *simkey   = 16;
            data      = l;
            memcpy(simkey + 1, &data, 4);
            *sflag    = 16;
            *ksize    = 5;
        }
    } else if (ktype == COL_TYPE_FLOAT) {
        *sflag  = 32;
        *ksize  = 5;
        float f = akey->f;
        simkey  = BTKeyBuffer;
        *simkey = 32;
        memcpy(simkey + 1, &f, 4);
    }
    return simkey; /* MUST be freed soon */
}

uint32 skipToVal(uchar **stream) {
    uchar   sflag = getSflag(**stream);
    uint32  klen  = 0;
    uint32  slen  = 0;
    if (sflag == 1) {         // TINY STRING
        getTinyString(*stream, &slen);
        klen = slen + 1;
    } else if (sflag == 2) {  // 14bit INT
        klen = 2;
    } else if (sflag == 4) {  // STRING
        getString(*stream, &slen);
        klen = slen + 5;
    } else if (sflag == 8) {  // 28bit INT
        klen = 4;
    } else if (sflag == 16) { // INT
        klen = 5;
    } else if (sflag == 32) { // FLOAT
        klen = 5;
    }
    *stream += klen;
    return klen;
}

uchar *parseStream(uchar *stream, uchar btype) {
    if (!stream) return NULL;
    skipToVal(&stream);
    if (     btype == BTREE_TABLE)      return stream;
    else if (btype == BTREE_INDEX_NODE) return NULL;
    else           /* BTREE_INDEX */    return *((uchar **)stream);
}

void convertStream2Key(uchar *stream, aobj *key) {
    initAobj(key);
    uchar b1    = *stream;
    uchar sflag = getSflag(b1);
    if (sflag == 1) {         // tiny STRING
        key->type     = COL_TYPE_STRING;
        key->enc      = COL_TYPE_STRING;
        key->s        = (char *)getTinyString(stream, &key->len);
    } else if (sflag == 2) {  // tiny INT
        key->type     = COL_TYPE_INT;
        key->enc      = COL_TYPE_INT;
        key->i        = get14BitInt(stream);
    } else if (sflag == 4) {  // STRING
        key->type     = COL_TYPE_STRING;
        key->enc      = COL_TYPE_STRING;
        key->s        = (char *)getString(stream, &key->len);
    } else if (sflag == 8) {  // 28bit INT
        key->type     = COL_TYPE_INT;
        key->enc      = COL_TYPE_INT;
        key->i        = get28BitInt(stream);
    } else if (sflag == 16) { // INT
        key->type     = COL_TYPE_INT;
        key->enc      = COL_TYPE_INT;
        key->i        = getInt(&stream);
    } else if (sflag == 32) { // FLOAT
        key->type     = COL_TYPE_FLOAT;
        key->enc      = COL_TYPE_FLOAT;
        key->f        = getFloat(stream);
    }
}

uint32 getStreamMallocSize(uchar *stream, uchar btype) {
    uint32 vlen;
    uint32 klen = skipToVal(&stream);

    if (     btype == BTREE_TABLE)      vlen = getRowMallocSize(stream);
    else if (btype == BTREE_INDEX_NODE) vlen = 0;
    else {         /* BTREE_INDEX */
        vlen     = sizeof(void *);
        bt *btr  = (bt *)(*((char **)stream));
        vlen    += (uint32)btr->malloc_size;
    }
    return klen + vlen;
}

void *createStream(bt *btr, void *val, char *btkey,
                   uint32 ksize, uint32 *ssize) {
    uint32   vlen  = 0;;
    *ssize         = ksize;
    if (btr->btype == BTREE_TABLE) vlen = getRowMallocSize(val);
    else if (val)  /* ptr2Index*/  vlen = sizeof(void *);
    *ssize = *ssize + vlen;

    char *bt_val   = bt_malloc(*ssize, btr); /* mem bookkeeping done in BT */
    char *o_bt_val = bt_val;

    memcpy(bt_val, btkey, ksize);
    bt_val += ksize;

    if (btr->btype == BTREE_TABLE) memcpy(bt_val, val, vlen);
    else if (val)  /* ptr2Index*/  memcpy(bt_val, &val, sizeof(void *));
    //bt_val += vlen; // only needed if stream is further modified
    return o_bt_val;
}

bool destroyStream(bt *btr, uchar *ostream) {
    if (!ostream) return 0;
    uint32 ssize  = getStreamMallocSize(ostream, btr->btype);
    bt_free(ostream, btr, ssize); /* memory bookkeeping in btr */
    return 1;
}

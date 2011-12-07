/*
 * This file implements ALCHEMY_DATABASE's AOBJ
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
#include <ctype.h>
#include <assert.h>

#include "sds.h"
#include "adlist.h"
#include "redis.h"

#include "row.h"
#include "parser.h"
#include "query.h"
#include "common.h"
#include "aobj.h"

extern r_tbl_t *Tbl;

void initAobj(aobj *a) {
    bzero(a, sizeof(aobj));
    a->type = COL_TYPE_NONE;
}
void releaseAobj(void *v) {
    aobj *a = (aobj *)v;
    if (a->freeme) { free(a->s); a->s = NULL; a->freeme = 0; }
}
void destroyAobj(void *v) {
    releaseAobj(v); free(v);
}
void initAobjZeroNum(aobj *a, uchar ctype) {
    initAobj(a);
    a->i = 0; a->l = 0; a->type = a->enc = ctype;
}
bool initAobjInt(aobj *a, ulong l) {
    initAobjZeroNum(a, COL_TYPE_INT); a->i = l; return 1;
}
void initAobjLong(aobj *a, ulong l) {
    initAobj(a);
    a->l = l; a->type = a->enc = COL_TYPE_LONG;
}
void initAobjString(aobj *a, char *s, int len) {
    initAobj(a);
    a->s = s; a->len  = len; a->type = a->enc = COL_TYPE_STRING;
}
void initAobjFloat(aobj *a, float f) {
    initAobj(a);
    a->f = f; a->type = a->enc = COL_TYPE_FLOAT;
}
void initAobjFromStr(aobj *a, char *s, int len, uchar ctype) {
    initAobj(a);
    if (       C_IS_S(ctype)) {
        a->enc    = COL_TYPE_STRING; a->type   = COL_TYPE_STRING;
        a->freeme = 1;
        a->s      = malloc(len);
        memcpy(a->s, s, len);
        a->len    = len;
    } else if (C_IS_F(ctype)) {
        a->enc  = a->type = COL_TYPE_FLOAT; a->f = atof(s);  /* OK: DELIM: \0 */
    } else if (C_IS_L(ctype)) {
        a->enc  = a->type = COL_TYPE_LONG;
        a->l    = strtoul(s, NULL, 10);           /* OK: DELIM: \0 */
    } else { /* COL_TYPE_INT */
        a->enc  = a->type = COL_TYPE_INT;
        a->i    = (uint32)strtoul(s, NULL, 10);   /* OK: DELIM: \0 */
    }
}
void initAobjFromLong(aobj *a, ulong l, uchar ctype) {
    initAobj(a);
    if (C_IS_L(ctype)) {
        a->l    = l;         a->enc = a->type = COL_TYPE_LONG;
    } else if (C_IS_I(ctype)) {
        a->i    = (uint32)l; a->enc = a->type = COL_TYPE_INT;
    } else { assert(!"initAobjFromLong must be INT|LONG"); }
}
aobj *createAobjFromString(char *s, int len, uchar ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj)); initAobjFromStr(a, s, len, ctype);
    return a;
}
aobj *createAobjFromLong(ulong l) {
    aobj *a = (aobj *)malloc(sizeof(aobj)); initAobjLong(a, l); return a;
}

void aobjClone(aobj *dest, aobj *src) {
    memcpy(dest, src, sizeof(aobj));
    if (C_IS_S(src->type)) {
        dest->s      = malloc(src->len);
        memcpy(dest->s, src->s, src->len);
        dest->freeme = 1;
    }
}
aobj *cloneAobj(aobj *a) {
    aobj *na = (aobj *)malloc(sizeof(aobj)); aobjClone(na, a); return na;
}
inline void *vcloneAobj(void *v) { return cloneAobj((aobj *)v); }

void convertSdsToAobj(sds s, aobj *a, uchar ctype) {
    initAobj(a);
    if (       C_IS_S(ctype)) {
        a->enc    = a->type = COL_TYPE_STRING;
        a->freeme = 1;
        a->s      = _strdup(s);
        a->len    = sdslen(s);
    } else if (C_IS_I(ctype)) { //TODO > UINT_MAX -> ERR
        a->enc    = a->type = COL_TYPE_INT;
        a->i      = (uint32)strtoul(s, NULL, 10); /* OK: DELIM: \0 */
    } else if (C_IS_L(ctype)) {
        a->enc    = a->type = COL_TYPE_LONG;
        a->l      = strtoul(s, NULL, 10);         /* OK: DELIM: \0 */
    } else { /* COL_TYPE_FLOAT) */
        a->enc    = a->type = COL_TYPE_FLOAT;
        a->f      = atof(s);                      /* OK: DELIM: \0 */
    } 
}
static aobj *cloneSDSToAobj(sds s, uchar ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj));
    convertSdsToAobj(s, a, ctype); return a;
}

void vsdsfree(void *v) { sdsfree((sds)v); }

void convertINLtoAobj(list **inl, uchar ctype) { /* walk INL & cloneSDSToAobj */
    listNode *ln;
    list     *nl = listCreate();
    list     *ol = *inl;
    listIter *li = listGetIterator(ol, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sds   ink = ln->value;
        aobj *a   = cloneSDSToAobj(ink, ctype);
        listAddNodeTail(nl, a);
    } listReleaseIterator(li);
    ol->free     = vsdsfree;
    listRelease(ol);
    *inl         = nl;
}
void convertFilterSDStoAobj(f_t *flt) {
    uchar ctype = Tbl[flt->tmatch].col[flt->cmatch].type;
    if      (flt->key) convertSdsToAobj(flt->key, &flt->akey, ctype);
    else if (flt->low) {
        convertSdsToAobj(flt->low,  &flt->alow,  ctype);
        convertSdsToAobj(flt->high, &flt->ahigh, ctype);
    }
}

list *cloneAobjList(list *ll) {
    listNode *ln;
    list *l2     = listCreate();
    listIter *li = listGetIterator(ll, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *a = ln->value;
        listAddNodeTail(l2, cloneAobj(a));
    } listReleaseIterator(li);
    return l2;
}

static char SFA_buf[32];
static char *strFromAobj(aobj *a, int *len) {
    char *s;
    if (       C_IS_S(a->type)) {
        s         = malloc(a->len + 1);                  /* FREE ME 015 */
        memcpy(s, a->s, a->len);
        s[a->len] = '\0';
        *len      = a->len;
    } else if (C_IS_I(a->type)) {
        snprintf(SFA_buf, 32, "%u",      a->i);
        s = _strdup(SFA_buf);                            /* FREE ME 015 */
        *len = strlen(SFA_buf);
    } else if (C_IS_L(a->type)) {
        snprintf(SFA_buf, 32, "%lu",     a->l);
        s = _strdup(SFA_buf);                            /* FREE ME 015 */
        *len = strlen(SFA_buf);
    } else { /* COL_TYPE_FLOAT */
        snprintf(SFA_buf, 32, FLOAT_FMT, a->f);
        s = _strdup(SFA_buf);                            /* FREE ME 015 */
        *len = strlen(SFA_buf);
    }
    return s;
}
void initStringAobjFromAobj(aobj *a, aobj *a2) {
    initAobj(a);
    a->enc    = COL_TYPE_STRING; a->type   = COL_TYPE_STRING;
    a->freeme = 1;
    a->s      = strFromAobj(a2, (int *)&a->len);
}

sds createSDSFromAobj(aobj *a) {
    if (       C_IS_S(a->type)) {
        return sdsnewlen(a->s, a->len);
    } else if (C_IS_F(a->type)) {
        if (a->enc == COL_TYPE_STRING) return sdsnewlen(a->s, a->len);
        else {
            char buf[32]; sprintf(buf, FLOAT_FMT, a->f);
            return sdsnewlen(buf, strlen(buf));
        }
    } else if (C_IS_L(a->type)) {
        if (a->enc == COL_TYPE_STRING) return sdsnewlen(a->s, a->len);
        else {
            char buf[32]; sprintf(buf, "%lu", a->l);
            return sdsnewlen(buf, strlen(buf));
        }
    } else { /* COL_TYPE_INT */
        if (a->enc == COL_TYPE_STRING) return sdsnewlen(a->s, a->len);
        else {
            char buf[32]; sprintf(buf, "%u", a->i);
            return sdsnewlen(buf, strlen(buf));
        }
    }
}

// ROW_OUTPUT ROW_OUTPUT ROW_OUTPUT ROW_OUTPUT ROW_OUTPUT ROW_OUTPUT ROW_OUTPUT
static sds outputIntS(char *s, int len, int *rlen) {
    *rlen       = len + 3;
    char *rs    = malloc(*rlen);
    rs[0]       = ':';
    memcpy(rs + 1, s, len);
    rs[len + 1] = '\r';
    rs[len + 2] = '\n';
    return rs;
}
static char *outputInt(uint32 i, int *rlen) {
    char buf[128];  sprintf(buf, "%u", i);
    return outputIntS(buf, strlen(buf), rlen);
}
static char *outputLong(ulong l, int *rlen) {
    char buf[128];  sprintf(buf, "%lu", l);
    return outputIntS(buf, strlen(buf), rlen);
}
static char *outputFloat(float f, int *rlen) {
    char buf[ 128]; sprintf(buf,  "%.17g",          f);
    char buf2[128]; sprintf(buf2, "$%lu\r\n%s\r\n", (ulong)strlen(buf), buf);
    *rlen = strlen(buf2);
    return _strdup(buf2);
}
static char *outputS(char *s, int len, int *rlen) {
    char buf[128];
    buf[0]             = '$';
    size_t ilen        = ll2string(buf + 1, sizeof(buf) - 1, (lolo)len);
    buf[ilen + 1]      = '\r';
    buf[ilen + 2]      = '\n';
    *rlen              = ilen + 5 + len;
    char *rs           = malloc(*rlen);
    memcpy(rs,                  buf,    ilen + 3);
    memcpy(rs + ilen + 3,       s,      len);
    rs[ilen + len + 3] = '\r';
    rs[ilen + len + 4] = '\n';
    return rs;
}
static char *outputAobj(aobj *a, int *rlen) {
    if      (C_IS_I(a->type))   return outputInt(  a->i,     rlen);
    else if (C_IS_L(a->type))   return outputLong( a->l,     rlen);
    else if (C_IS_F(a->type))   return outputFloat(a->f,     rlen);
    else  /* COL_TYPE_STRING */ return outputS(a->s, a->len, rlen);
}
sl_t outputReformat(aobj *a) {
    sl_t sl;
    sl.s      = outputAobj(a, &sl.len);
    sl.freeme = 1;
    sl.type   = a->type;
    return sl;
}
sl_t outputSL(uchar ct, sl_t sl) { /* NOTE: in redis: FLOATs are strings */
    sl_t sl2;
    sl2.s = (C_IS_I(ct) || C_IS_L(ct)) ? outputIntS(sl.s, sl.len, &sl2.len) :
            /* STRING   ||  FLOAT */     outputS(   sl.s, sl.len, &sl2.len);
    sl2.freeme = 1;
    sl2.type   = ct;
    return sl2;
}
void release_sl(sl_t sl) {
    if (sl.freeme) free(sl.s);
}
void dumpSL(sl_t sl) {
    printf("START dumpSL\n");
    printf("\ts:      %s\n", sl.s);
    printf("\tlen:    %u\n", sl.len);
    printf("\tfreeme: %u\n", sl.freeme);
    printf("\ttype:   %u\n", sl.type);
    printf("END dumpSL\n");
}

/* COMPARISON COMPARISON COMPARISON COMPARISON COMPARISON COMPARISON */
static int aobjCmp(aobj *a, aobj *b) {
    if (       C_IS_S(a->type)) {
        return strncmp(a->s, b->s, a->len);
    } else if (C_IS_F(a->type)) {
        float f    = a->f - b->f;
        return (f == 0.0)     ? 0 : ((f > 0.0)     ? 1 : -1);
    } else if (C_IS_L(a->type)) {
        return (a->l == b->l) ? 0 : ((a->l > b->l) ? 1 : -1);
    } else { /* COL_TYPE_INT */
        return (long)(a->i - b->i);
    }
}
bool aobjEQ(aobj *a, aobj *b) {
    return !aobjCmp(a, b);
}
bool aobjNE(aobj *a, aobj *b) {
    return aobjCmp(a, b);
}
bool aobjLT(aobj *a, aobj *b) {
    return (aobjCmp(a, b) < 0);
}
bool aobjLE(aobj *a, aobj *b) {
    return (aobjCmp(a, b) <= 0);
}
bool aobjGT(aobj *a, aobj *b) {
    return (aobjCmp(a, b) > 0);
}
bool aobjGE(aobj *a, aobj *b) {
    return (aobjCmp(a, b) >= 0);
}
void incrbyAobj(aobj *a, ulong l) {
    if      (C_IS_L(a->type)) a->l += l;
    else if (C_IS_I(a->type)) a->i += (uint32)l;
    else                      assert(!"incrbyAobj must be INT|LONG");
}
void decrbyAobj(aobj *a, ulong l) {
    if      (C_IS_L(a->type)) a->l -= l;
    else if (C_IS_I(a->type)) a->i -= (uint32)l;
    else                      assert(!"decrbyAobj must be INT|LONG");
}
ulong subtractAobj(aobj *a, aobj *b) {
    if      (C_IS_L(a->type) && C_IS_L(b->type)) return a->l - b->l;
    else if (C_IS_I(a->type) && C_IS_I(b->type)) return a->i - b->i;
    else                      assert(!"subtractAobj must be INT|LONG");
}

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
static char DumpBuf[1024];

static void memcpyAobjStoDumpBuf(aobj *a) {
    int len      = MIN(a->len, 1023);
    if (a->s) memcpy(DumpBuf, a->s, len);
    DumpBuf[len] = '\0';
}
void dumpAobj(printer *prn, aobj *a) {
    if (       C_IS_S(a->type)) {
        memcpyAobjStoDumpBuf(a);
        (*prn)("\tSTRING aobj: mt: %d len: %d -> (%s)\n",
                a->empty, a->len, DumpBuf);
    } else if (C_IS_I(a->type)) {
        if (a->enc == COL_TYPE_INT) (*prn)("\tINT aobj: mt: %d val: %u\n",
                                            a->empty, a->i);
        else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\tINT(S) aobj: mt: %d val: %s\n", a->empty, DumpBuf);
        }
    } else if (C_IS_L(a->type)) {
        if (a->enc == COL_TYPE_LONG) (*prn)("\tLONG aobj: mt: %d val: %lu\n",
                                             a->empty, a->l);
        else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\tLONG(S) aobj: mt: %d val: %s\n", a->empty, DumpBuf);
        }
    } else if (C_IS_F(a->type)) {
        if (a->enc == COL_TYPE_INT) (*prn)("\tFLOAT aobj: mt: %d val: %f\n",
                                            a->empty, a->f);
        else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\tFLOAT(S) aobj: mt: %d val: %s\n", a->empty, DumpBuf);
        }
    } else {
        (*prn)("\tEMPTY aobj mt: %d\n", a->empty);
    }
}

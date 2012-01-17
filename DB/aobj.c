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

#include "debug.h"
#include "colparse.h"
#include "filter.h"
#include "row.h"
#include "parser.h"
#include "query.h"
#include "common.h"
#include "aobj.h"

extern r_tbl_t *Tbl;

void initAobj(aobj *a) {
    bzero(a, sizeof(aobj)); a->type = COL_TYPE_NONE;              a->empty = 1;
}
void releaseAobj(void *v) {
    aobj *a = (aobj *)v; a->type = COL_TYPE_NONE;                 a->empty = 1;
    if (a->freeme) { free(a->s); a->s = NULL; a->freeme = 0; }
}
void destroyAobj(void *v) { releaseAobj(v); free(v); }

void initAobjZeroNum(aobj *a, uchar ctype) {
    initAobj(a);
    a->i = 0; a->l = 0; a->x = 0; a->type = a->enc = ctype;       a->empty = 0;
}
bool initAobjInt(aobj *a, ulong l) {
    initAobjZeroNum(a, COL_TYPE_INT); a->i = l; return 1;
}
void initAobjLong(aobj *a, ulong l) {
    initAobj(a); a->l = l; a->type = a->enc = COL_TYPE_LONG;      a->empty = 0;
}
void initAobjString(aobj *a, char *s, int len) {
    initAobj(a);
    a->s     = s;               a->len  = len;
    a->type  = a->enc = COL_TYPE_STRING;                          a->empty = 0;
}
void initAobjU128(aobj *a, uint128 x) {
    a->enc  = COL_TYPE_U128;   a->type = COL_TYPE_U128; a->x = x; a->empty = 0;
}
void initAobjFloat(aobj *a, float f) {
    initAobj(a); a->f = f; a->type = a->enc = COL_TYPE_FLOAT;     a->empty = 0;
}
void initAobjBool(aobj *a, bool b) {
    initAobj(a); a->b = b; a->type = a->enc = COL_TYPE_BOOL;;     a->empty = 0;
}
void initAobjDetermineType(aobj *a, char *s, int len, bool fs) {  a->empty = 0;
    if        (is_int(s)) {
        a->type = a->enc = COL_TYPE_LONG;
        if (fs) {a->len = len; a->s = s; }
        else     a->l = strtoul(s, NULL, 10); // OK: DELIM: \0
    } else if (is_float(s)) {
        a->type = a->enc = COL_TYPE_FLOAT;
        if (fs) {a->len = len; a->s = s; }
        else    a->f = atof(s);               // OK: DELIM: \0
    } else if (!strcmp(s, "true")) {
        a->b = 1; a->type = a->enc = COL_TYPE_BOOL;
    } else if (!strcmp(s, "false")) {
        a->b = 0; a->type = a->enc = COL_TYPE_BOOL;
    } else {
        a->len = len; a->s = s; a->type = a->enc = COL_TYPE_LUAO;
    } printf("initAobjDetermineType: a: "); dumpAobj(printf, a); 
} 
//NOTE: does not throw errors
void initAobjFromStr(aobj *a, char *s, int len, uchar ctype) {
    initAobj(a);                                                  a->empty = 0;
    if         C_IS_S(ctype) {
        a->enc    = a->type   = COL_TYPE_STRING;
        a->freeme = 1;
        a->s      = malloc(len); memcpy(a->s, s, len);
        a->len    = len;
    } else if C_IS_F(ctype) {
        a->enc    = a->type = COL_TYPE_FLOAT; a->f = atof(s); // OK: DELIM: \0 
    } else if C_IS_L(ctype) {
        a->enc    = a->type = COL_TYPE_LONG;
        a->l      = strtoul(s, NULL, 10);                     // OK: DELIM: \0
    } else if C_IS_I(ctype) {
        a->enc    = a->type = COL_TYPE_INT;
        a->i      = (uint32)strtoul(s, NULL, 10);             // OK: DELIM: \0
    } else if C_IS_X(ctype) {
        a->enc    = COL_TYPE_U128;     a->type = COL_TYPE_U128;
        parseU128(s, &a->x);
    } else if C_IS_P(ctype) {
        a->enc    = COL_TYPE_FUNC;      a->type = COL_TYPE_FUNC;
        a->i      = (uint32)strtoul(s, NULL, 10);             // OK: DELIM: \0
    } else if C_IS_O(ctype) {
        a->enc    = COL_TYPE_LUAO;      a->type = COL_TYPE_LUAO;
        sds s2 = sdsnewlen(s, len);
        initAobjDetermineType(a, s2, len, 0); sdsfree(s2);
    } else assert(!"initAobjFromStr ERROR\n");
    
}
void initAobjFromLong(aobj *a, ulong l, uchar ctype) {
    initAobj(a);                                                  a->empty = 0;
    if        C_IS_I(ctype) {
        a->i    = (uint32)l; a->enc = a->type = COL_TYPE_INT;
    } else if C_IS_L(ctype) {
        a->l    = l;         a->enc = a->type = COL_TYPE_LONG;
    } else if C_IS_X(ctype) {
        a->x    = l;         a->enc = a->type = COL_TYPE_U128;
    } else { assert(!"initAobjFromLong must be INT|LONG"); }
}
aobj *createEmptyAobj() {
    aobj *a = (aobj *)malloc(sizeof(aobj)); initAobj(a); return a;
}
aobj *createAobjFromString(char *s, int len, uchar ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj));
    initAobjFromStr(a, s, len, ctype); return a;
}
aobj *createAobjFromLong(ulong l) {
    aobj *a = (aobj *)malloc(sizeof(aobj)); initAobjLong(a, l); return a;
}
aobj *createAobjFromInt(uint32 i) {
    aobj *a = (aobj *)malloc(sizeof(aobj)); initAobjInt(a, i);  return a;
}

// COPY/CLONE COPY/CLONE COPY/CLONE COPY/CLONE COPY/CLONE COPY/CLONE
void aobjClone(aobj *dest, aobj *src) {
    memcpy(dest, src, sizeof(aobj));
    if (src->freeme) {
        dest->s      = malloc(src->len);
        memcpy(dest->s, src->s, src->len);
        dest->freeme = 1;
    }
}
aobj *cloneAobj(aobj *a) {
    aobj *na = (aobj *)malloc(sizeof(aobj)); aobjClone(na, a); return na;
}
aobj *copyAobj (aobj *a) { //WARNING: do not double-free a->s
    aobj *na = (aobj *)malloc(sizeof(aobj));
    memcpy(na, a, sizeof(aobj)); return na;
}

// CONVERSION CONVERSION CONVERSION CONVERSION CONVERSION CONVERSION
void convertSdsToAobj(sds s, aobj *a, uchar ctype) {//NOTE: NO thrown errors
    initAobj(a);                                                  a->empty = 0;
    if        (C_IS_S(ctype)) {
        a->enc    = a->type   = COL_TYPE_STRING;
        a->freeme = 1;
        a->s      = _strdup(s);      a->len    = sdslen(s);
    } else if (C_IS_I(ctype)) {
        a->enc    = a->type   = COL_TYPE_INT;
        a->i      = (uint32)strtoul(s, NULL, 10); /* OK: DELIM: \0 */
    } else if (C_IS_L(ctype)) {
        a->enc    = a->type   = COL_TYPE_LONG;
        a->l      = strtoul(s, NULL, 10);         /* OK: DELIM: \0 */
    } else if (C_IS_F(ctype)) {
        a->enc    = a->type   = COL_TYPE_FLOAT;
        a->f      = atof(s);                      /* OK: DELIM: \0 */
    } else if (C_IS_X(ctype)) {
        a->enc    = COL_TYPE_U128;   a->type   = COL_TYPE_U128; 
        parseU128(s, &a->x);
    } else if (C_IS_P(ctype)) {
        a->enc    = COL_TYPE_FUNC;    a->type   = COL_TYPE_FUNC;
        a->i      = (uint32)strtoul(s, NULL, 10); /* OK: DELIM: \0 */
    } else assert(!"convertSdsToAobj ERROR");
}
static aobj *cloneSDSToAobj(sds s, uchar ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj));
    convertSdsToAobj(s, a, ctype); return a;
}

void convertFilterSDStoAobj(f_t *flt) {
    uchar ctype    = CTYPE_FROM_FLT(flt)
    flt->akey.type = ctype;
    if         (flt->key && flt->akey.empty) {
        convertSdsToAobj(flt->key, &flt->akey, ctype);
     } else if (flt->low && flt->alow.empty) {
        convertSdsToAobj(flt->low,  &flt->alow,  ctype);
        convertSdsToAobj(flt->high, &flt->ahigh, ctype);
    }
}

static void vsdsfree(void *v) { sdsfree((sds)v); }
void convertINLtoAobj(list **inl, uchar ctype) { /* walk INL & cloneSDSToAobj */
    listNode *ln;
    list     *nl  = listCreate();
    list     *ol  = *inl;
    listIter *li  = listGetIterator(ol, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sds   ink = ln->value;
        aobj *a   = cloneSDSToAobj(ink, ctype);
        listAddNodeTail(nl, a);
    } listReleaseIterator(li);
    ol->free      = vsdsfree; listRelease(ol); *inl = nl;
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

static char SFA_buf[64];
static char *strFromAobj(aobj *a, int *len) {
    char *s;
    if        (C_IS_S(a->type)) {
        s         = malloc(a->len + 1);                  /* FREE ME 015 */
        memcpy(s, a->s, a->len);
        s[a->len] = '\0';
        *len      = a->len;
        return s;
    } else if (C_IS_I(a->type))   snprintf   (SFA_buf, 64, "%u",      a->i);
      else if (C_IS_P(a->type))   snprintf   (SFA_buf, 64, "%u",      a->i);
      else if (C_IS_L(a->type))   snprintf   (SFA_buf, 64, "%lu",     a->l);
      else if (C_IS_F(a->type))   snprintf   (SFA_buf, 64, FLOAT_FMT, a->f);
      else if (C_IS_X(a->type)) { SPRINTF_128(SFA_buf, 64,            a->x); }
      else                        assert(!"strFromAobj ERROR");
    s    = _strdup(SFA_buf);                    /* FREE ME 015 */
    *len = strlen(SFA_buf);
    return s;
}
void initStringAobjFromAobj(aobj *a, aobj *a2) {
    initAobj(a);                                                 a->empty  = 0;
    a->enc    = COL_TYPE_STRING; a->type   = COL_TYPE_STRING;
    a->freeme = 1;
    a->s      = strFromAobj(a2, (int *)&a->len);
}

sds createSDSFromAobj(aobj *a) {
    if (C_IS_S(a->type) || a->enc == COL_TYPE_STRING) {
        return sdsnewlen(a->s, a->len);
    } else {
        char buf[64];
        if      (C_IS_F(a->type))   snprintf   (buf, 64, FLOAT_FMT, a->f);
        else if (C_IS_L(a->type))   snprintf   (buf, 64, "%lu",     a->l);
        else if (C_IS_I(a->type))   snprintf   (buf, 64, "%u",      a->i);     
        else if (C_IS_P(a->type))   snprintf   (buf, 64, "%u",      a->i);     
        else if (C_IS_X(a->type)) { SPRINTF_128(buf, 64,            a->x); }
        else                        assert(!"createSDSFromAobj ERROR");
        return sdsnewlen(buf, strlen(buf));
    }
}

// PURE_REDIS_OUTPUT PURE_REDIS_OUTPUT PURE_REDIS_OUTPUT PURE_REDIS_OUTPUT
static char *outputIntS(char *s, int len, int *rlen) {
    *rlen       = len + 3;
    char *rs    = malloc(*rlen);
    rs[0]       = ':';  memcpy(rs + 1, s, len);
    rs[len + 1] = '\r'; rs[len + 2] = '\n';
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
static char *outputX(uint128 x, int *rlen) { //TODO test
    char buf [128]; SPRINTF_128(buf, 64, x);
    char buf2[128]; sprintf(buf2, "$%lu\r\n%s\r\n", (ulong)strlen(buf), buf);
    *rlen = strlen(buf2); return _strdup(buf2);
}
static char *outputFloat(float f, int *rlen) {
    char buf [128]; sprintf(buf,  "%.17g",          f);
    char buf2[128]; sprintf(buf2, "$%lu\r\n%s\r\n", (ulong)strlen(buf), buf);
    *rlen = strlen(buf2); return _strdup(buf2);
}
static char *outputS(char *s, int len, int *rlen) {
    char buf[128];
    buf[0]             = '$';
    size_t ilen        = ll2string(buf + 1, sizeof(buf) - 1, (lolo)len);
    buf[ilen + 1]      = '\r'; buf[ilen + 2]      = '\n';
    *rlen              = ilen + 5 + len;
    char *rs           = malloc(*rlen);
    memcpy(rs,                  buf,    ilen + 3);
    memcpy(rs + ilen + 3,       s,      len);
    rs[ilen + len + 3] = '\r'; rs[ilen + len + 4] = '\n';
    return rs;
}
static char *outputNil(int *rlen) {
    static char *snil = ":-1\r\n"; static uint32 lnil = 5; *rlen = lnil;
    char *rs = malloc(lnil); memcpy(rs, snil, lnil); return rs;
}
static char *outputAobj(aobj *a, int *rlen) {
    if      (C_IS_I(a->type)) return outputInt  (a->i,         rlen);
    else if (C_IS_P(a->type)) return outputInt  (a->i,         rlen);
    else if (C_IS_L(a->type)) return outputLong (a->l,         rlen);
    else if (C_IS_F(a->type)) return outputFloat(a->f,         rlen);
    else if (C_IS_S(a->type)) return outputS    (a->s, a->len, rlen);
    else if (C_IS_X(a->type)) return outputX    (a->x,         rlen);
    else if (C_IS_O(a->type)) return outputS    (a->s, a->len, rlen);
    else if (C_IS_N(a->type)) return outputNil  (              rlen);
    else                      assert(!"outputAobj ERROR");
}
sl_t outputReformat(aobj *a) { //NOTE: used by orow_redis()
    sl_t sl; sl.s = outputAobj(a, &sl.len); sl.freeme = 1; sl.type = a->type;
    return sl;
}
sl_t outputSL(uchar ct, sl_t sl) { /* NOTE: in redis: FLOATs are strings */
    aobj a; initAobjFromStr(&a, sl.s, sl.len, COL_TYPE_STRING);
    sl_t sl2; sl2.s = outputAobj(&a, &sl2.len); sl2.freeme = 1; sl2.type = ct;
    return sl2;
}
void release_sl(sl_t sl) { if (sl.freeme) free(sl.s); }
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
    if        (C_IS_S(a->type)) {
        return strncmp(a->s, b->s, a->len);
    } else if (C_IS_F(a->type)) {
        float f    = a->f - b->f;
        return (f == 0.0)     ? 0 : ((f > 0.0)     ? 1 : -1);
    } else if (C_IS_L(a->type)) {
        return (a->l == b->l) ? 0 : ((a->l > b->l) ? 1 : -1);
    } else if (C_IS_X(a->type)) {
        return (a->x == b->x) ? 0 : ((a->x > b->x) ? 1 : -1);
    } else if (C_IS_I(a->type) || C_IS_P(a->type)) {
        return (long)(a->i - b->i);
    } else assert(!"aobjCmp ERROR");
}
bool aobjEQ(aobj *a, aobj *b) { return !aobjCmp(a, b);       }
bool aobjNE(aobj *a, aobj *b) { return  aobjCmp(a, b);       }
bool aobjLT(aobj *a, aobj *b) { return (aobjCmp(a, b) <  0); }
bool aobjLE(aobj *a, aobj *b) { return (aobjCmp(a, b) <= 0); }
bool aobjGT(aobj *a, aobj *b) { return (aobjCmp(a, b) >  0); }
bool aobjGE(aobj *a, aobj *b) { return (aobjCmp(a, b) >= 0); }

int getSizeAobj(aobj *a) { //TODO support FLOAT,STRING
    if (!C_IS_NUM(a->type)) return -1; // ONLY NUM()s supported
    if (C_IS_I(a->type))    return sizeof(int);
    if (C_IS_L(a->type))    return sizeof(long);
    if (C_IS_X(a->type))    return sizeof(uint128);
    assert(!"getSizeAobj ERROR");
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
    if        (C_IS_S(a->type) || C_IS_O(a->type)) {
        if (a->empty) { (*prn)("\tSTRING aobj: EMPTY\n"); return; }
        memcpyAobjStoDumpBuf(a);
        (*prn)("\tSTRING aobj: mt: %d len: %d -> (%s) type: %d\n",
                a->empty, a->len, DumpBuf, a->type);
    } else if (C_IS_C(a->type)) {
        if (a->empty) { (*prn)("\tCNAME aobj: EMPTY\n"); return; }
        memcpyAobjStoDumpBuf(a);
        (*prn)("\tCNAME aobj: mt: %d len: %d -> (%s) cmatch: %d\n",
                a->empty, a->len, DumpBuf, a->i);
    } else if (C_IS_I(a->type) || C_IS_P(a->type)) {
        char *name = C_IS_I(a->type) ? "INT" : "FUNC";
        if (a->enc == COL_TYPE_INT || a->enc == COL_TYPE_FUNC) {
            (*prn)("\t%s aobj: mt: %d val: %u\n", name, a->empty, a->i);
        } else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\t%s(S) aobj: mt: %d val: %s\n", name, a->empty, DumpBuf);
        }
    } else if (C_IS_L(a->type)) {
        if (a->enc == COL_TYPE_LONG) (*prn)("\tLONG aobj: mt: %d val: %lu\n",
                                             a->empty, a->l);
        else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\tLONG(S) aobj: mt: %d val: %s\n", a->empty, DumpBuf);
        }
    } else if (C_IS_X(a->type)) {
        (*prn)("\tU128 aobj: mt: %d val: ", a->empty); DEBUG_U128(prn, a->x);
        (*prn)("\n");
    } else if (C_IS_F(a->type)) {
        if (a->enc == COL_TYPE_INT) (*prn)("\tFLOAT aobj: mt: %d val: %f\n",
                                            a->empty, a->f);
        else {
            memcpyAobjStoDumpBuf(a);
            (*prn)("\tFLOAT(S) aobj: mt: %d val: %s\n", a->empty, DumpBuf);
        }
    } else {
        (*prn)("\tUNINITIALISED aobj mt: %d\n", a->empty);
    }
}

/*
 * This file implements Alchemy's AOBJ
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
#include <ctype.h>

#include "adlist.h"
#include "redis.h"

#include "row.h"
#include "parser.h"
#include "common.h"
#include "aobj.h"

// FROM redis.c
#define RL4 redisLog(4,

void initAobj(aobj *a) {
    bzero(a, sizeof(aobj));
    a->type = COL_TYPE_NONE;
}
void releaseAobj(void *v) {
    aobj *a = (aobj *)v;
    if (a->freeme) {
        free(a->s);
        a->freeme = 0;
    }
}
void destroyAobj(void *v) {
    releaseAobj(v);
    free(v);
}

static char DumpBuf[1024];
void dumpAobj(aobj *a) {
    if (       a->type == COL_TYPE_STRING) {
        int len      = MIN(a->len, 1023);
        memcpy(DumpBuf, a->s, len);
        DumpBuf[len] = '\0';
        printf("\tSTRING aobj: len: %d -> (%s)\n", a->len, DumpBuf);
    } else if (a->type == COL_TYPE_INT) {
        printf("\tINT aobj: val: %d\n", a->i);
    } else if (a->type == COL_TYPE_FLOAT) {
        printf("\tFLOAT aobj: val: %f\n", a->f);
    } else {
        printf("\tEMPTY aobj\n");
    }
}

bool initAobjIntReply(redisClient *c, aobj *a, long l, bool ispk) {
    initAobj(a);
    if (!checkUIntReply(c, l, !ispk)) return 0;
    a->i    = (int)l;
    a->type = COL_TYPE_INT;
    a->enc  = COL_TYPE_INT;
    return 1;
}
void initAobjString(aobj *a, char *s, int len) {
    initAobj(a);
    a->s    = s;
    a->len  = len;
    a->type = COL_TYPE_STRING;
    a->enc  = COL_TYPE_STRING;
}
void initAobjFloat(aobj *a, float f) {
    initAobj(a);
    a->f    = f;
    a->len  = sizeof(float);
    a->type = COL_TYPE_FLOAT;
    a->enc  = COL_TYPE_FLOAT;
}

void initAobjFromString(aobj *a, char *s, int len, bool ctype) {
    initAobj(a);
    if (       ctype == COL_TYPE_STRING) {
        a->enc    = COL_TYPE_STRING;
        a->type   = COL_TYPE_STRING;
        a->freeme = 1;
        a->s      = malloc(len);
        memcpy(a->s, s, len);
        a->len    = len;
    } else if (ctype == COL_TYPE_FLOAT) {
        a->enc  = COL_TYPE_FLOAT;
        a->type = COL_TYPE_FLOAT;
        a->f    = atof(s);
    } else {         /* COL_TYPE_FLOAT */
        a->enc  = COL_TYPE_INT;
        a->type = COL_TYPE_INT;
        a->i    = atoi(s);
    }
}

void aobjClone(aobj *dest, aobj *src) {
    memcpy(dest, src, sizeof(aobj));
    if (src->type == COL_TYPE_STRING) {
        dest->s = malloc(src->len);
        memcpy(dest->s, src->s, src->len);
    }
}
aobj *cloneAobj(aobj *a) {
    aobj *na = (aobj *)malloc(sizeof(aobj));
    aobjClone(na, a);
    return na;
}

/* IN_List objects need to be converted from string to Aobj */
aobj *copyRobjToAobj(robj *r, uchar ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj));
    initAobj(a);
    if (ctype == COL_TYPE_STRING) {
        a->enc    = COL_TYPE_STRING;
        a->type   = COL_TYPE_STRING;
        a->freeme = 1;
        if (r->encoding == REDIS_ENCODING_RAW) {
            a->s   = _strdup(r->ptr);
            a->len = sdslen(r->ptr);
        } else {        /* REDIS_ENCODING_INT */
            char buf[32];
            snprintf(buf, 32, "%d", (int)(long)r->ptr);
            buf[31] = '\0'; /* paranoia */
            a->s     = _strdup(buf);
            a->len   = strlen(buf);
        }
    } else if (ctype == COL_TYPE_INT) {
        a->enc  = COL_TYPE_INT;
        a->type = COL_TYPE_INT;
        if (r->encoding == REDIS_ENCODING_RAW)   a->i = atoi(r->ptr);
        else            /* REDIS_ENCODING_INT */ a->i = (uint32)(long)r->ptr;
    } else {         /* COL_TYPE_FLOAT) */
        a->enc  = COL_TYPE_FLOAT;
        a->type = COL_TYPE_FLOAT;
        if (r->encoding == REDIS_ENCODING_RAW)   a->f = atof(r->ptr);
        else            /* REDIS_ENCODING_INT */ a->f = (float)(long)r->ptr;
    } 
    return a;
}

aobj *createAobjFromString(char *s, int len, bool ctype) {
    aobj *a = (aobj *)malloc(sizeof(aobj));
    initAobjFromString(a, s, len, ctype);
    return a;
}

/* walk IN_List and copyRobjToAobj() */
void convertINLtoAobj(list **inl, uchar ctype) {
    listNode *ln;
    list     *nl = listCreate();
    list     *ol = *inl;
    listIter *li = listGetIterator(ol, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *ink = ln->value;
        aobj *a   = copyRobjToAobj(ink, ctype);
        listAddNodeTail(nl, a);
    }
    listReleaseIterator(li);
    ol->free     = decrRefCount;
    listRelease(ol);
    *inl         = nl;
    (*inl)->free = destroyAobj;
}

list *cloneAobjList(list *ll) {
    listNode *ln;
    list *l2     = listCreate();
    listIter *li = listGetIterator(ll, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *a = ln->value;
        listAddNodeTail(l2, cloneAobj(a));
    }
    listReleaseIterator(li);
    return l2;
}

static char SFA_buf[32];
char *strFromAobj(aobj *a, int *len) {
    char *s = NULL; /* compiler warning */
    if (       a->type == COL_TYPE_STRING) {
        s         = malloc(a->len + 1);                  /* FREE ME 015 */
        memcpy(s, a->s, a->len);
        s[a->len] = '\0';
        *len      = a->len;
    } else if (a->type == COL_TYPE_INT) {
        snprintf(SFA_buf, 32, "%d", a->i);
        s = _strdup(SFA_buf);
        *len = strlen(SFA_buf);
    } else {           /* COL_TYPE_FLOAT */
        sprintfOutputFloat(SFA_buf, 32, a->f);
        s = _strdup(SFA_buf);
        *len = strlen(SFA_buf);
    }
    return s;
}

bool initAobjFromVoid(aobj *a, redisClient *c, void *col, uchar ctype) {
    if (ctype == COL_TYPE_INT) {
        if (initAobjIntReply(c, a, (long)col, 0)) return 0;
    } else if (ctype == COL_TYPE_FLOAT) {
        float f;
        memcpy(&f, col, sizeof(float));
        initAobjFloat(a, f);
    } else {
        initAobjString(a, (char *)col, strlen((char *)col));
    }
    return 1;
}

void initStringAobjFromAobj(aobj *a, aobj *a2) {
    initAobj(a);
    a->enc    = COL_TYPE_STRING;
    a->type   = COL_TYPE_STRING;
    a->freeme = 1;
    a->s      = strFromAobj(a2, (int *)&a->len);
}

robj *createStringRobjFromAobj(aobj *a) {
    if (       a->type == COL_TYPE_STRING) {
        return createStringObject(a->s, a->len);
    } else if (a->type == COL_TYPE_FLOAT) {
        char buf[32];
        sprintfOutputFloat(buf, 32, a->f);
        return createStringObject(buf, strlen(buf));
    } else {           /* COL_TYPE_INT */
        robj *r     = createObject(REDIS_STRING, NULL);
        int   i;
        if (a->enc == COL_TYPE_STRING) i = atoi(a->s);
        else                           i = a->i;
        r->ptr      = (void *)(long)i;
        r->encoding = REDIS_ENCODING_INT;
        return r;
    }
}

static int aobjCmp(aobj *a, aobj *b) {
    if (      a->type == COL_TYPE_STRING) {
        return strncmp(a->s, b->s, a->len);
    } else if (a->type == COL_TYPE_FLOAT) {
        float f    = a->f - b->f;
        return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
    } else {           /* COL_TYPE_INT */
        return a->i - b->i;
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

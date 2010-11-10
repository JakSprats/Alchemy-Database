/*
 * This file implements  parsing functions
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

#include "redis.h"
#include "zmalloc.h"

#include "parser.h"

char *_strnchr(char *s, int c, int len) {
    for (int i = 0; i < len; i++) {
        if (*s == c) return s;
        s++;
    }
    return NULL;
}

robj *cloneRobj(robj *r) { // must be decrRefCount()ed
    if (r->encoding == REDIS_ENCODING_RAW) {
        return createStringObject(r->ptr, sdslen(r->ptr));
    } else {
        robj *n     = createObject(REDIS_STRING, r->ptr);
        n->encoding = REDIS_ENCODING_INT;
        return n;
    }
}

robj *convertRobj(robj *r, int type) {
    if ((r->encoding == REDIS_ENCODING_RAW && type == COL_TYPE_STRING) ||
        (r->encoding == REDIS_ENCODING_INT && type == COL_TYPE_INT)) {
        return r;
    }
    robj *n = NULL;
    if (r->encoding == REDIS_ENCODING_RAW) { /* string -> want int */
        int i = atoi(r->ptr);
        n           = createObject(REDIS_STRING, (void *)(long)i);
        n->encoding = REDIS_ENCODING_INT;
    } else {                                /* int    -> want string */
        char buf[32];
        sprintf(buf, "%d", (int)(long)r->ptr);
        n = createStringObject(buf, strlen(buf));
    }
    decrRefCount(r);
    return n;
}



char *rem_backticks(char *token, int *len) {
    char *otoken = token;
    int   olen   = *len;
    if (*token == '`') {
        token++;
        *len = *len - 1;
    }
    if (otoken[olen - 1] == '`') {
        *len = *len - 1;
    }
    return token;
}

char *str_next_unescaped_chr(char *beg, char *s, int x) {
    char *nextc = s;
    while ((nextc = strchr(nextc, x))) {
        if (nextc - beg > 1) { /* handle backslash-escaped commas */
            if  (*(nextc - 1) == '\\') {
                char *backslash = nextc - 1;
                while (backslash >= beg) {
                    if (*backslash != '\\') break;
                    backslash--;
                }
                int num_backslash = nextc - backslash - 1;
                if (num_backslash % 2 == 1) {
                    nextc++;
                    continue;
                }
            }
        }
        return nextc;
    }
    return NULL;
}

/* This is essentially the inner-argv[] parser */
char *next_token(char *nextp) {
    char *p = strchr(nextp, ' ');
    if (!p) return NULL;
    while (isblank(*p)) p++;
    return *p ? p : NULL;
}
int get_token_len(char *tok) {
    char *x = strchr(tok, ' ');
    return x ? x - tok : (int)strlen(tok);
}
int get_token_len_delim(char *nextp, char x) {
    char *q = strchr(nextp, x);
    char *p = strchr(nextp, ' ');
    if (p && q) {
        if (p > q) return q - nextp;
        else       return p - nextp;
    } else if (q) {
        return q - nextp;
    } else if (p) {
        return p - nextp;
    } else {
        return 0;
    }
}
char *next_token_delim(char *p, char x) {
    int len  = get_token_len_delim(p, x);
    if (!len) return NULL;
    p       += len + 1;
    while (isblank(*p)) p++;
    return *p ? p : NULL;
}


char *get_next_token_nonparaned_comma(char *token) {
   bool got_open_parn = 0;
   while (token) {
       sds x = token;
       int xlen = get_token_len(x);
       if (_strnchr(x, '(', xlen)) got_open_parn = 1;
       if (got_open_parn) {
           char *y = _strnchr(x, ')', xlen);
           if (y) {
               int ylen = get_token_len(y);
               got_open_parn = 0;
               char *z = _strnchr(y, ',', ylen);
               if (z) {
                   token = z + 1;
                   break;
               }
           }
       } else {
           char *z = _strnchr(x, ',', xlen);
           if (z) {
               token = z + 1;
               break;
           }
       }
       token = next_token(token);
   }
   if (token) while (isblank(*token)) token++;
   return token;
}


robj **parseCmdToArgv(char *as_cmd, int *rargc) {
    sds   *argv  = sdssplitlen(as_cmd, strlen(as_cmd), " ", 1, rargc);
    robj **rargv = zmalloc(sizeof(robj *) * *rargc);
    for (int j = 0; j < *rargc; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    zfree(argv);
    return rargv;
}

robj **parseSelectCmdToArgv(char *as_cmd) {
    int    argc;
    robj **rargv = NULL;
    sds   *argv  = sdssplitlen(as_cmd, strlen(as_cmd), " ", 1, &argc);
    if (argc < 6)                            goto parse_sel_2argv_err;
    if (strcasecmp(argv[0], "SELECT"))       goto parse_sel_2argv_err;
    int j;
    for (j = 1; j < argc; j++) {
        if (!strcasecmp(argv[j], "FROM")) break;
    }
    if ((j == (argc -1)) || (j == 1))        goto parse_sel_2argv_err;
    int k;
    for (k = j + 1; k < argc; k++) {
        if (!strcasecmp(argv[k], "WHERE")) break;
    }
    if ((k == (argc - 1)) || (k == (j + 1))) goto parse_sel_2argv_err;

    rargv      = zmalloc(sizeof(robj *) * 6);
    rargv[0]   = createStringObject("SELECT", 6);
    sds clist  = sdsempty();
    for (int i = 1; i < j; i++) {
        if (sdslen(clist)) clist = sdscatlen(clist, ",", 1);
        clist = sdscatlen(clist, argv[i], sdslen(argv[i]));
    }
    rargv[1]   = createObject(REDIS_STRING, clist);
    rargv[2]   = createStringObject("FROM",   4);
    sds tlist  = sdsempty();
    for (int i = j + 1; i < k; i++) {
        if (sdslen(tlist)) tlist = sdscatlen(tlist, ",", 1);
        tlist = sdscatlen(tlist, argv[i], sdslen(argv[i]));
    }
    rargv[3]   = createObject(REDIS_STRING, tlist);
    rargv[4]   = createStringObject("WHERE",  5);
    sds wc     = sdsempty();
    for (int i = k + 1; i < argc; i++) {
        if (sdslen(wc)) wc = sdscatlen(wc, " ", 1);
        wc = sdscatlen(wc, argv[i], sdslen(argv[i]));
    }
    rargv[5]   = createObject(REDIS_STRING, wc);

parse_sel_2argv_err:
    for (int i = 0; i < argc; i++) sdsfree(argv[i]);
    zfree(argv);
    return rargv;
}

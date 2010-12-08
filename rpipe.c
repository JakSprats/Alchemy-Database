/*
 *
 * This file implements Alchemy's Data Pipes
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

#include "redis.h"
#include "adlist.h"

#include "parser.h"
#include "rpipe.h"

// FROM redis.c
#define RL4 redisLog(4,

bool emptyNoop(redisClient *c) {
    c = NULL; /* compiler warning */
    return 1;
}

bool respOk(redisClient *rfc) {
    listNode *ln = listFirst(rfc->reply);
    if (!ln) return 0;
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    if (!strcmp(s, shared.ok->ptr)) return 1;
    else                            return 0;
}

static bool respNotErr(redisClient *rfc) {
    listNode *ln = listFirst(rfc->reply);
    if (!ln) return 1;
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    //RL4 "respNotErr: %s", s);
    if (!strncmp(s, "-ERR", 4)) return 0;
    else                        return 1;
}

bool replyIfNestedErr(redisClient *c, redisClient *rfc, char *msg) {
    if (!respNotErr(rfc)) {
        listNode *ln   = listFirst(rfc->reply);
        robj     *emsg = ln->value;
        robj     *repl = _createStringObject(msg);
        repl->ptr      = sdscatlen(repl->ptr, emsg->ptr, sdslen(emsg->ptr));
        addReply(c, repl);
        decrRefCount(repl);
        return 0;
    }
    return 1;
}

/* NOTE: this function implements a fakeClient pipe */
long fakeClientPipe(redisClient *c,
                    redisClient *rfc,
                    void        *wfc, /* can be redisClient,list,LuaState */
                    int          is_ins,
                    flag        *flg,
                    bool (* adder)
                    (redisClient *c, void *x, robj *key, long *l, int b, int n),
                    bool (* emptyer) (redisClient *c)) {
    redisCommand *cmd = lookupCommand(rfc->argv[0]->ptr);
    rsql_resetFakeClient(rfc);
    cmd->proc(rfc);

    listNode *ln;
    *flg             = PIPE_NONE_FLAG;
    int       nlines = 0;
    long      card   = 1; /* ZER0 as pk can cause problems */
    bool      fline  = 1;
    listIter  *li = listGetIterator(rfc->reply, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *o    = ln->value;
        sds   s    = o->ptr;
        bool  o_fl = fline;
        fline = 0;
        //RL4 "PIPE: %s", s);
        /* ignore protocol, we just want data */
        if (*s == '\r' && *(s + 1) == '\n') continue;
         /* TODO introduce more state -> data starting w/ '\r\n' ignored */
        if (o_fl) {
            if (*s == '-') {
                *flg = PIPE_ERR_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* error */
            }
            if (*s == '+') {
                *flg = PIPE_ONE_LINER_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* OK */
            }
            if (*s == ':') {
                robj *r;
                char *x = s + 1;
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                if (!y) { /* colon is coming in next ln->value [incrCommand] */
                    if (!(ln = listNext(li))) return -1;
                    o = ln->value;
                    x = o->ptr;
                    y = strchr(x, '\r'); /* ignore the final \r\n */
                    if (!y) y = x + sdslen(x);
                }
                r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
                break; /* single integer reply */
            }
            if (*s == '*') {
                nlines = atoi(s+1); /* some pipes need to know num_lines */
                if (nlines == 0) {
                    *flg = PIPE_EMPTY_SET_FLAG;
                    break;
                }
                continue;
            }
        }
        if (*s == '$') { /* parse doubles which are w/in this list element */
            if (*(s + 1) == '-') continue; /* $-1 -> nil */
            char   *x    = strchr(s, '\r');
            if (!x) return -1;
            uint32  llen = x - s;
            if (llen + 2 < sdslen(s)) { /* got a double */
                x += 2; /* move past \r\n */
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                if (!y) return -1;
                robj *r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
            }
            continue;
        }
        /* all ranges are single */
        if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
    }
    listReleaseIterator(li);
    if (card == 1) { /* empty response from rfc */
        if (!(*emptyer)(c)) return -1;
    }
    return card - 1; /* started at 1 */
}

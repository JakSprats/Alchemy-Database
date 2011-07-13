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

extern struct redisServer server;

// FROM redis.c
#define RL4 redisLog(4,

//TODO this can be a static redisClient (but make sure call is not nested)
redisClient *rsql_createFakeClient() {
    int          curr_db_id = server.dbid;
    redisClient *fc         = createFakeClient();
    selectDb(fc, curr_db_id);
    return fc;
}

void rsql_resetFakeClient(struct redisClient *c) {
    /* Discard the reply objects list from the fake client */
    while(listLength(c->reply))
        listDelNode(c->reply, listFirst(c->reply));

}

void rsql_freeFakeClient(struct redisClient *c) {
    freeFakeClient(c);
}

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

/* NOTE: this function depends on the implementation of addReply of
         various *Command()s ... if they change, this function must follow */
static robj *parseUpToR(listIter     *li,
                        listNode    **ln,
                        char         *x) {
    char *y = strchr(x, '\r'); /* ignore the final \r\n */
    if (!y) {
        if (strlen(x)) { /* typeCommand, substrCommand */
            y = x + sdslen(x);
        } else { /* colon is coming in next ln->value [incrCommand] */
            if (!(*ln = listNext(li))) return NULL;
            robj *o = (*ln)->value;
            x       = o->ptr;
            y       = strchr(x, '\r'); /* ignore the final \r\n */
            if (!y) y = x + sdslen(x);
        }
    }
    return createStringObject(x, y - x);
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
    *flg               = PIPE_NONE_FLAG;
    robj     *r        = NULL;
    int       nlines   = 0;
    long      card     = 1; /* ZER0 as pk can cause problems */
    bool      fline    = 1;
    bool      ldef     = 0;
    listIter *li       = listGetIterator(rfc->reply, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *o    = ln->value;
        sds   s    = o->ptr;
        bool  o_fl = fline;
        fline      = 0;
        //RL4 "PIPE: %s", s);
        if (o_fl) {
            if (*s == '-') { /* -ERR */
                *flg = PIPE_ONE_LINER_FLAG;
                r    = parseUpToR(li, &ln, s);
                if (!r) goto p_err;
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
                break;
            } else if (*s == '+') { /* +OK */
                *flg = PIPE_ONE_LINER_FLAG;
                r    = parseUpToR(li, &ln, s);
                if (!r) goto p_err;
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
                break;
            } else if (*s == ':') { /* :INT */
                *flg = PIPE_ONE_LINER_FLAG;
                r    = parseUpToR(li, &ln, s);
                if (!r) goto p_err;
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
                break;
            } else if (*s == '*') {
                nlines = atoi(s + 1); /* some pipes need to know num_lines */
                if (nlines == -1) {
                    *flg    = PIPE_EMPTY_SET_FLAG;
                    break; /* "*-1" multibulk empty */
                }
            }
            continue;
        }
        /* not first line -> 2+ */
        if (*s == '$') { /* parse length [and element] */
            if (*(s + 1) == '-') { /* $-1 -> nil */
                *flg = PIPE_EMPTY_SET_FLAG;
                /* NOTE: "-1" must be "adder()"d for Multi-NonRelIndxs */
                r    = createStringObject("-1", 2);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
                continue;
            }
            ldef         = 1;
            char   *x    = strchr(s, '\r');
            if (!x) goto p_err;
            uint32  llen = x - s;
            if (llen + 2 < sdslen(s)) { /* more on next line (past "\r\n") */
                x     += 2; /* move past \r\n */
                r     = parseUpToR(li, &ln, x);
                if (!r) goto p_err;
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
                ldef  = 0;
            }
            continue;
        }
        /* ignore empty protocol lines */
        if (!ldef && sdslen(s) == 2 && *s == '\r' && *(s + 1) == '\n') continue;

        r    = createStringObject(s, sdslen(s));
        if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) goto p_err;
        ldef = 0;
    }
    listReleaseIterator(li);

    if (card == 1) { /* rfc never got called, call empty handler */
        if (!(*emptyer)(c)) goto p_err;
    }
    return card - 1; /* started at 1 */

p_err:
    listReleaseIterator(li);
    if (r) decrRefCount(r);
    return -1;
}

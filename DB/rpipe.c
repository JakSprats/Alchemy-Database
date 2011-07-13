/*
 *
 * This file implements ALCHEMY_DATABASE's Data Pipes
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

#include "redis.h"
#include "adlist.h"

#include "parser.h"
#include "rpipe.h"

static redisClient *FakeClient = NULL;
redisClient *getFakeClient() {
    if (!FakeClient) {
        FakeClient         = createClient(-1);
        FakeClient->flags |= REDIS_LUA_CLIENT; // means no networking
    }
    return FakeClient;
}
void resetFakeClient(struct redisClient *c) {
    listRelease(c->reply); c->reply = listCreate();
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
        addReply(c, repl); decrRefCount(repl);
        return 0;
    }
    return 1;
}

static char *rp2Func(char *reply, ADDER_FUNC_DECL, void *v, long *card);

static char *rp2FuncInt(char *reply, ADDER_FUNC_DECL, void *v, long *card) {
    long long value;
    char *p = strchr(reply + 1, '\r');
    string2ll(reply + 1, p - reply - 1, &value);
    (*adder)(v, value, NULL, 0, card);
    return p + 2;
}
static char *rp2FuncBulk(char *reply, ADDER_FUNC_DECL, void *v, long *card) {
    long long bulklen;
    char *p = strchr(reply + 1, '\r');
    string2ll(reply + 1, p - reply - 1, &bulklen);
    if (bulklen == -1) { //TODO this ignores NULL DB fields
        return p + 2;
    } else {
        (*adder)(v, 0, p + 2, bulklen, card);
        return p + 2 + bulklen + 2;
    }
}
static char *rp2FuncMBulk(char *reply, ADDER_FUNC_DECL, void *v, long *card) {
    long long mbulklen;
    int   j = 0;
    char *p = strchr(reply + 1, '\r');

    string2ll(reply + 1, p - reply - 1, &mbulklen);
    p += 2;
    if (mbulklen == -1) { //TODO this ignores NULL DB fields
        return p;
    }
    for (j = 0; j < mbulklen; j++) {
        p = rp2Func(p, adder, v, card);
    }
    return p;
}
static char *rp2Func(char *reply, ADDER_FUNC_DECL, void *v, long *card) {
    char *p    = reply;
    switch(*p) {
        case ':':
            p = rp2FuncInt   (reply, adder, v, card); break;
        case '$':
            p = rp2FuncBulk  (reply, adder, v, card); break;
        case '+': case '-':
            p = strchr(reply + 1, '\r'); p += 2; break;
        case '*':
            p = rp2FuncMBulk (reply, adder, v, card); break;
    }
    return p;
}

void fakeClientPipe(cli *rfc, void *v, ADDER_FUNC_DECL) {
    rcommand *cmd = lookupCommand(rfc->argv[0]->ptr);
    resetFakeClient(rfc);
    cmd->proc(rfc);

    sds reply = sdsempty();
    if (rfc->bufpos) {
        reply = sdscatlen(reply, rfc->buf, rfc->bufpos);
        rfc->bufpos = 0;
    }
    while(listLength(rfc->reply)) {
        robj *o = listNodeValue(listFirst(rfc->reply));
        reply   = sdscatlen(reply, o->ptr, sdslen(o->ptr));
        listDelNode(rfc->reply, listFirst(rfc->reply));
    }

    long card = 0;
    rp2Func(reply, adder, v, &card);
    sdsfree(reply);
}

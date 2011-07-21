/*
 * This file implements ALCHEMY_DATABASE's advanced messaging hooks
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

#include <strings.h>

#include "hiredis.h"

#include "redis.h"
#include "dict.h"
#include "adlist.h"

#include "rpipe.h"
#include "messaging.h"

//GLOBALS
extern redisContext *context; //from redis-cli.c

//PROTOTYPES
// from redis-cli.c
void setClientParams(sds hostip, int hostport);
int cliConnect(int force);
int cliSendCommand(int argc, char **argv, int repeat);
// from networking.c
int processMultibulkBuffer(redisClient *c);
// from scripting.c
void luaPushError(lua_State *lua, char *error);

static void ignoreFCP(void *v, lolo val, char *x, lolo xlen, long *card) {
    v = NULL; val = 0; x = NULL; xlen = 0; card = NULL;
}
void messageCommand(redisClient *c) { //NOTE: this command does not reply
    redisClient  *rfc   = getFakeClient();
    rfc->reqtype        = REDIS_REQ_MULTIBULK;
    rfc->argc           = 0;
    rfc->multibulklen   = 0;
    rfc->querybuf       = c->argv[2]->ptr;
    processMultibulkBuffer(rfc);
    fakeClientPipe(rfc, NULL, ignoreFCP);
}

void rsubscribeCommand(redisClient *c) {
    sds  ip     = c->argv[1]->ptr;
    int  port   = atoi(c->argv[2]->ptr);
    { // PING the remote machine to create a fd for it
        setClientParams(ip, port);
        cliConnect(1);
        int  argc = 1;
        sds *argv = zmalloc(sizeof(sds));
        argv[0]   = sdsnew("PING");
        cliSendCommand(argc, argv, 1);
        while(argc--) sdsfree(argv[argc]); /* Free the argument vector */
        zfree(argv);
        if (!context) { addReply(c, shared.err); return; } //TODO err: no ping
    }
    cli *rc = createClient(context->fd); // use this fd for remote-subscription
    if (!rc) { addReply(c, shared.err); return; } //TODO err repeat rsubscribe
    for (int j = 3; j < c->argc; j++) {//carbon-copy of pubsubSubscribeChannel()
        struct dictEntry *de;
        list *clients = NULL;
        robj *channel = c->argv[j];
        /* Add the channel to the client -> channels hash table */
        if (dictAdd(rc->pubsub_channels, channel, NULL) == DICT_OK) {
            incrRefCount(channel);
            /* Add the client to the channel -> list of clients hash table */
            de = dictFind(server.pubsub_channels, channel);
            if (de == NULL) {
                clients = listCreate();
                dictAdd(server.pubsub_channels, channel, clients);
                incrRefCount(channel);
            } else {
                clients = dictGetEntryVal(de);
            }
            listAddNodeTail(clients, rc);
        }
    }
    addReply(c, shared.ok);
}

int luaConvertToRedisProtocolCommand(lua_State *lua) {
    int  argc  = lua_gettop(lua);
    if (argc < 1) {
        luaPushError(lua, "Lua ConvertToRedisProtocol() takes 1+ string arg");
        return 1;
    }
    sds *argv  = zmalloc(sizeof(sds) * argc);
    int  i     = argc -1;
    while(1) {
        int t = lua_type(lua, 1);
        if (t == LUA_TNIL) {
            argv[i] = sdsempty();
            lua_pop(lua, 1);
            break;
        } else if (t == LUA_TSTRING) {
            size_t len;
            char *s = (char *)lua_tolstring(lua, -1, &len);
            argv[i] = sdsnewlen(s, len);
        } else if (t == LUA_TNUMBER) {
            argv[i] = sdscatprintf(sdsempty(), "%lld",
                                               (lolo)lua_tonumber(lua, -1));
        } else break;
        lua_pop(lua, 1);
        i--;
    }
    size_t *argvlen = malloc(argc * sizeof(size_t));
    for (int j = 0; j < argc; j++) argvlen[j] = sdslen(argv[j]);
    char *cmd;
    int   len = redisFormatCommandArgv(&cmd,              argc,
                                      (const char**)argv, argvlen);
    lua_pushlstring(lua, cmd, len);
    free(cmd);
    free(argvlen);
    while(argc--) sdsfree(argv[argc]); /* Free the argument vector */
    zfree(argv);
    return 1;
}

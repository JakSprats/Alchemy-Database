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
#include <poll.h>

#include "hiredis.h"

#include "redis.h"
#include "dict.h"
#include "adlist.h"

#include "rpipe.h"
#include "xdb_hooks.h"
#include "messaging.h"

//GLOBALS
extern cli *CurrClient;

//PROTOTYPES
// from networking.c
int processMultibulkBuffer(redisClient *c);
// from scripting.c
void luaPushError(lua_State *lua, char *error);
void hashScript(char *digest, char *script, size_t len);

static void ignoreFCP(void *v, lolo val, char *x, lolo xlen, long *card) {
    v = NULL; val = 0; x = NULL; xlen = 0; card = NULL;
}
void messageCommand(redisClient *c) { //NOTE: this command does not reply
    //DEBUG_C_ARGV(c)
    redisClient  *rfc   = getFakeClient();
    rfc->reqtype        = REDIS_REQ_MULTIBULK;
    rfc->argc           = 0;
    rfc->multibulklen   = 0;
    rfc->querybuf       = c->argv[2]->ptr;
    rfc->sa             = c->sa;
    processMultibulkBuffer(rfc);
    if (!rfc->argc) return; /* PARSE ERROR */
    fakeClientPipe(rfc, NULL, ignoreFCP);
    cleanupFakeClient(rfc);
}

int remoteMessage(sds ip, int port, sds cmd, bool wait,
                  redisReply **ret_reply) {
    int fd         = -1;
    struct timeval tv; tv.tv_sec = 0; tv.tv_usec = 100000; // 100ms timeout
    redisContext *context = redisConnectWithTimeout(ip, port, tv);
    if (!context || context->err)                                    goto rmend;
    context->obuf  = cmd;
    int wdone      = 0;
    do {
        if (redisBufferWrite(context, &wdone) == REDIS_ERR)          goto rmend;
    } while (!wdone);
    context->obuf  = NULL;

    redisReply *reply = NULL;
    if (wait) { // WAIT is for PINGs (wait for PONG) validate box is up
        struct pollfd fds[1];
        int timeout_msecs = 100; // 100ms timeout, this is a PING
        fds[0].fd = context->fd;
        fds[0].events = POLLIN | POLLPRI;
        int ret = poll(fds, 1, timeout_msecs);
        if (!ret || fds[0].revents & POLLERR || fds[0].revents & POLLHUP) {
            goto rmend;
        }
        void *aux = NULL;
        do {
            if (redisBufferRead(context)               == REDIS_ERR ||
                redisGetReplyFromReader(context, &aux) == REDIS_ERR) goto rmend;
        } while (aux == NULL);
        reply = aux;
    }
    if (ret_reply)  *ret_reply = reply;
    else if (reply) freeReplyObject(reply);
    fd = context->fd;

rmend:
    if (context) {
        if (context->obuf)   sdsfree(context->obuf);
        if (context->errstr) sdsfree(context->errstr);
        if (context->reader) redisReplyReaderFree(context->reader);
    }
    return fd;
}

static void subscribeClient(cli *rc, robj **rargv, int arg_beg, int arg_end) {
    // carbon-copy of pubsubSubscribeChannel()
    for (int j = arg_beg; j < arg_end; j++) {
        struct dictEntry *de;
        list *clients = NULL;
        robj *channel = rargv[j];
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
}
int luaSubscribeFDCommand(lua_State *lua) {
    int  argc   = lua_gettop(lua);
    if (argc < 2) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua SubscribeCurrentFD(fd, channel)"); return 1;
    }
    int t = lua_type(lua, 1);
    if (t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua SubscribeCurrentFD(fd, channel)"); return 1;
    }
    size_t   len;
    robj   **rargv = zmalloc(sizeof(robj *));
    char    *s     = (char *)lua_tolstring(lua, -1, &len);
    lua_pop(lua, 1);
    rargv[0]       = createStringObject(s, len);
    t = lua_type(lua, 1);
    if (t != LUA_TNUMBER && t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua SubscribeCurrentFD(fd, channel)"); return 1;
    }
    int fd   = (t == LUA_TNUMBER) ? (int)(lolo)lua_tonumber(lua, -1) :
                                    atoi((char *)lua_tolstring(lua, -1, &len));
    lua_pop(lua, 1);
    cli *rc  = createClient(-1);                         // DESTROY ME 086
    rc->fd   = fd;                               // fd for remote-subscription
    DXDB_setClientSA(rc);
    rc->argc = 1;
    rc->argv = rargv;
    subscribeClient(rc, rc->argv, 0, 1);
    return 0;
}

int luaUnsubscribeFDCommand(lua_State *lua) {
    int  argc   = lua_gettop(lua);
    if (argc != 2) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua UnsubscribeCurrentFD(fd, channel)"); return 1;
    }
    int t = lua_type(lua, 1);
    if (t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua UnsubscribeCurrentFD(fd, channel)"); return 1;
    }
    size_t   len;
    char *s       = (char *)lua_tolstring(lua, -1, &len);
    lua_pop(lua, 1);
    robj *channel = createStringObject(s, len);
    t = lua_type(lua, 1);
    if (t != LUA_TNUMBER && t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua UnsubscribeCurrentFD(fd, channel)"); return 1;
    }
    int fd   = (t == LUA_TNUMBER) ? (int)(lolo)lua_tonumber(lua, -1) :
                                    atoi((char *)lua_tolstring(lua, -1, &len));
    lua_pop(lua, 1);

    struct dictEntry *de = dictFind(server.pubsub_channels, channel);
    if (de) {
        listNode *ln;
        listIter  li;
        list *list = dictGetEntryVal(de);
        listRewind(list, &li);
        while ((ln = listNext(&li))) {
            redisClient *c = ln->value;
            if (c->fd == fd) { freeClient(c); break; }   // DESTROYED 086
        }
    }
    return 0;
}
int luaCloseFDCommand(lua_State *lua) {
    int  argc   = lua_gettop(lua);
    if (argc != 1) {
        LUA_POP_WHOLE_STACK luaPushError(lua, "Lua CloseFD(fd)"); return 1;
    }
    int t = lua_type(lua, 1);
    if (t != LUA_TNUMBER && t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK luaPushError(lua, "Lua CloseFD(fd)"); return 1;
    }
    size_t len;
    int    fd = (t == LUA_TNUMBER) ? (int)(lolo)lua_tonumber(lua, -1) :
                                     atoi((char *)lua_tolstring(lua, -1, &len));
    lua_pop(lua, 1);
    int ret = close(fd); //printf("CloseFDCommand: fd: %d ret: %d\n", fd, ret);
    lua_pushnumber(lua, (lua_Number)ret);
    return 1;
}

static int luaRemoteRPC(lua_State *lua, bool closer) {//printf("luaRemoeRPC\n");
    int  argc   = lua_gettop(lua);
    if (argc != 3) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua RemoteMessage(ip, port, msg)"); return 1;
    }
    int t       = lua_type(lua, 1);
    if (t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua RemoteMessage(ip, port, msg)"); return 1;
    }
    size_t  len;
    char   *s   = (char *)lua_tolstring(lua, -1, &len);
    sds     cmd = sdsnewlen(s, len);              // DESTROYED IN remoteMessage
    lua_pop(lua, 1);
    t           = lua_type(lua, 1);
    if (t != LUA_TNUMBER && t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua RemoteMessage(ip, port, msg)"); return 1;
    }
    int port = (t == LUA_TNUMBER) ? (int)(lolo)lua_tonumber(lua, -1) :
                                    atoi((char *)lua_tolstring(lua, -1, &len));
    lua_pop(lua, 1);
    t           = lua_type(lua, 1);
    if (t != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua RemoteMessage(ip, port, msg)"); return 1;
    }
    s           = (char *)lua_tolstring(lua, -1, &len);
    sds     ip  = sdsnewlen(s, len);                     // DESTROY ME 085
    lua_pop(lua, 1);
    int     fd = remoteMessage(ip, port, cmd, 0, NULL); // NON-blocking CMD
    sdsfree(ip);                                         // DESTROYED 085
    if (closer) {                                  // NOT Pipe -> FIRE & FORGET
        if (fd != -1) { close(fd); }         return 0;
    } else {
        lua_pushnumber(lua, (lua_Number)fd); return 1;
    }
}
int luaRemoteMessageCommand(lua_State *lua) {
  return luaRemoteRPC(lua, 1);
}
int luaRemotePipeCommand(lua_State *lua) {
  return luaRemoteRPC(lua, 0);
}

int luaSQLCommand(lua_State *lua) { //printf("SQL\n");
    int  argc  = lua_gettop(lua);
    if (argc != 1 || lua_type(lua, 1) != LUA_TSTRING) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua SQL() takes 1 string arg"); return 1;
    }
    size_t len;
    char *s = (char *)lua_tolstring(lua, -1, &len);
    lua_pop(lua, 1);
    sds sql = sdsnewlen(s, len);
    int  sargc;
    sds *sargv = sdssplitargs(sql, &sargc);
    DXDB_cliSendCommand(&sargc, sargv);
    for (int j = 0; j < sargc; j++) {
        lua_pushlstring(lua, sargv[j], sdslen(sargv[j]));
    }
    zfree(sargv);
    return luaRedisCommand(lua);
}

int luaConvertToRedisProtocolCommand(lua_State *lua) { //printf("Redisify()\n");
    int  argc  = lua_gettop(lua);
    if (argc < 1) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua Redisify() takes 1+ string arg"); return 1;
    }
    sds *argv  = zmalloc(sizeof(sds) * argc);
    int  i     = argc - 1;
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

int luaSha1Command(lua_State *lua) { //printf("SHA1\n");
    int  argc  = lua_gettop(lua);
    if (argc < 1) {
        LUA_POP_WHOLE_STACK
        luaPushError(lua, "Lua SHA1() takes 1+ string arg"); return 1;
    }
    sds tok = sdsempty();                                // DESTROY ME 083
    while(1) {
        int t = lua_type(lua, 1);
        if (t == LUA_TNIL) {
            lua_pop(lua, 1);
            break;
        } else if (t == LUA_TSTRING) {
            size_t len;
            char *s = (char *)lua_tolstring(lua, -1, &len);
            tok = sdscatlen(tok, s, len);
        } else if (t == LUA_TNUMBER) {
            char buf[32];
            snprintf(buf, 32, "%lld", (lolo)lua_tonumber(lua, -1));
            buf[31] = '\0';
            tok = sdscatlen(tok, buf, strlen(buf));
        } else break;
        lua_pop(lua, 1);
    }
    char sha1v[41];
    hashScript(sha1v, tok, sdslen(tok));
    lua_pushlstring(lua, sha1v, strlen(sha1v));
    sdsfree(tok);                                        // DESTROYED 083
    return 1;
}

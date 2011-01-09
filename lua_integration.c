/*
 * This file implements Lua c bindings for lua function "redis"
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
#include "zmalloc.h"

#include "rpipe.h"
#include "parser.h"
#include "lua_integration.h"

extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern lua_State   *Lua;

extern redisClient *LuaClient;
extern flag         LuaFlag;

// FROM redis.c
#define RL4 redisLog(4,

void luaCommand(redisClient *c) {
    //printf("LUA: %s\n", c->argv[1]->ptr);
    LuaFlag   = PIPE_NONE_FLAG;
    LuaClient = c;             /* used in func redisLua */
    int s     = luaL_dostring(Lua, c->argv[1]->ptr);
    if (s) {
        const char *x = lua_tostring(Lua, -1);
        lua_pop(Lua, 1);
        addReplySds(c, sdscatprintf(sdsempty(), "-ERR Lua error: %s \r\n", x));
        return;
    }

    int lret = lua_gettop(Lua);
    //printf("LuaFlag: %d lret: %d\n", LuaFlag, lret);
    if (lua_istable(Lua, -1)) {
        const int len = lua_objlen(Lua, -1 );
        addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", len));
        for (int i = 1; i <= len; ++i ) {
            lua_pushinteger(Lua, i);
            lua_gettable(Lua, -2);
            char *x = (char *)lua_tostring(Lua, -1);
            robj *r = _createStringObject(x);
            addReplyBulk(c, r);
            decrRefCount(r);
            lua_pop(Lua, 1);
        }
        lua_pop(Lua, 1);
    } else if (LuaFlag == PIPE_EMPTY_SET_FLAG) {
        addReply(c, shared.emptymultibulk);
        lua_pop(Lua, 1); /* pop because Pipe adds "-1" for Multi-NonRelIndxs */
    } else if (!lret) {
        addReply(c, shared.nullbulk);
    } else {
        char *x = (char *)lua_tostring(Lua, -1);
        if (!x) {
            addReply(c, shared.nullbulk);
        } else { 
            /* NOTE: if "client() is called in a lua func and the lua func
                     then returns "+OK" it will 'correctly' returned */
            if (LuaFlag == PIPE_ONE_LINER_FLAG &&
                (*x == '-' || *x == '+' || *x == ':')) {
                addReplySds(c, sdscatprintf(sdsempty(), "%s\r\n", x));
            } else {
                robj *r = _createStringObject(x);
                addReplyBulk(c, r);
                decrRefCount(r);
            }
        }
        lua_pop(Lua, 1);
    }
    lua_gc(Lua, LUA_GCCOLLECT, 0);
}

/* This gets called when the function "raw_write()" gets called in Lua */
int luaCall_raw_write(lua_State *L) {
    char       *buf  = "-ERR raw_write() needs arguments\r\n";
    const char *arg1 = lua_tostring(L, 1);
    if (!arg1) arg1 = buf;
    lua_pushstring(L, arg1);
    LuaFlag          = PIPE_ONE_LINER_FLAG;
    return 1;
}

/* This function stores the results from the
    LUA call "client()" (which is actually the C func redisLua()
    into a lua table, which mimics client-server I/O but is 100% in-server
   
   Function luaLine() called for each result row
    from the fakeClientPipe() call in redisLua() */
static bool luaLine(redisClient *c,
                    void        *x,
                    robj        *key,
                    long        *card,
                    int          i,
                    int          n) {
    c = NULL; i = 0; n = 0; /* compiler warning */
    char *kp = key->ptr;
    //RL4 "luaLine: %s", key->ptr);
    lua_State *L = (lua_State *)x;
    if (*card == 1) {
        if (*kp == '-' || *kp == '+' || *kp == ':') { /* single line response */
            lua_pushstring(L, kp);
        } else {
            lua_newtable(L);
            lua_pushnumber(L, *card);
            lua_pushstring(L, kp);
            lua_settable(L, -3);
        }
    } else {
        lua_pushnumber(L, *card);
        lua_pushstring(L, kp);
        lua_settable(L, -3);
    }
    decrRefCount(key);
    *card = *card + 1;
    return 1;
}

static int redisLuaArityErr(lua_State *L, char *name) {
    char buf[64];
    snprintf(buf, 63, "-ERR wrong number of arguments for '%s' command\r\n",
              name);
    buf[63] = '\0';
    lua_pushstring(L, buf);
    LuaFlag = PIPE_ONE_LINER_FLAG;
    return 1;
}

/* This gets called when the function "client)" gets called in Lua */
int luaCall_client(lua_State *L) {
    LuaFlag            = PIPE_NONE_FLAG;
    int           argc = lua_gettop(L);
    const char   *arg1 = lua_tostring(L, 1);
    if (!arg1) {
        return redisLuaArityErr(L, NULL);
    }
    redisCommand *cmd  = lookupCommand((char *)arg1);

    if (!cmd) {
        char buf[64];
        snprintf(buf, 63, "-ERR Unknown command '%s'\r\n", arg1);
        buf[63] = '\0';
        lua_pushstring(L, buf);
        LuaFlag = PIPE_ONE_LINER_FLAG;
        return 1;
    } else if ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity)) {
        return redisLuaArityErr(L, cmd->name);
    }

    if (server.maxmemory && (cmd->flags & REDIS_CMD_DENYOOM) &&
        (zmalloc_used_memory() > server.maxmemory)) {
        LuaFlag = PIPE_ONE_LINER_FLAG;
        lua_pushstring(L,
                 "-ERR command not allowed when used memory > 'maxmemory'\r\n");
        return 1;
    }

    if (server.vm_enabled && server.vm_max_threads > 0 &&
        blockClientOnSwappedKeys(LuaClient, cmd)) return 1;

    long          ok    = 0; /* must come before first GOTO */
    redisClient  *rfc   = rsql_createFakeClient();
    robj        **rargv = zmalloc(sizeof(robj *) * argc);
    for (int i = 0; i < argc; i++) {
        char *arg = (char *)lua_tostring(L, i + 1);
        if (!arg) {
            char *lbuf = "args must be strings";
            luaL_argerror (L, i, lbuf);
            LuaFlag    = PIPE_ONE_LINER_FLAG;
            ok         = 1;
            goto redis_lua_end;
        }
        rargv[i] = _createStringObject(arg);
    }
    rfc->argv = rargv;
    rfc->argc = argc;

    ok = fakeClientPipe(LuaClient, rfc, L, 0, &LuaFlag, luaLine, emptyNoop);

redis_lua_end:
    for (int i = 0; i < argc; i++) decrRefCount(rargv[i]);
    zfree(rargv);
    rsql_freeFakeClient(rfc);
    return (ok > 0) ? 1 : 0;
}

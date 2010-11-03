/*
 * This file implements Lua c bindings for lua function "redis"
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

#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

#include "redis.h"
#include "zmalloc.h"

#include "denorm.h"
#include "lua_integration.h"

extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern redisClient *LuaClient;
extern flag         LuaFlag;

// FROM redis.c
#define RL4 redisLog(4,

// TODO use Lua tables
static bool luaLine(redisClient *c,
                    void        *x,
                    robj        *key,
                    long        *card,
                    int          i,
                    int          n) {
    c = NULL; i = 0; /* compiler warning */
    lua_State *L = (lua_State *)x;
    //RL4 "luaLine: %s", key->ptr);
    if (n > 1) {
        if (*card == 1) lua_newtable(L);
        lua_pushnumber(L, *card);
        lua_pushstring(L, key->ptr);
        lua_settable(L, -3);
    } else {
        lua_pushstring(L, key->ptr);
    }
    *card = *card + 1;
    return 1;
}

static int redisLuaArityErr(lua_State *L, char *name) {
    char buf[64];
    sprintf(buf, "-ERR wrong number of arguments for '%s' command\r\n", name);
    lua_pushstring(L, buf);
    LuaFlag = PIPE_ERR_FLAG;
    return 1;
}

int redisLua(lua_State *L) {
    LuaFlag            = PIPE_NONE_FLAG;
    int           argc = lua_gettop(L);
    const char   *arg1 = lua_tostring(L, 1);
    if (!arg1) {
        return redisLuaArityErr(L, NULL);
    }
    redisCommand *cmd  = lookupCommand((char *)arg1);

    if (!cmd) {
        char buf[64];
        sprintf(buf, "-ERR: Unknown command '%s'\r\n", arg1);
        lua_pushstring(L, buf);
        LuaFlag = PIPE_ERR_FLAG;
        return 1;
    } else if ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity)) {
        return redisLuaArityErr(L, cmd->name);
    }

    if (server.maxmemory && (cmd->flags & REDIS_CMD_DENYOOM) &&
        zmalloc_used_memory() > server.maxmemory) {
        char *buf =
                  "-ERR command not allowed when used memory > 'maxmemory'\r\n";
        lua_pushstring(L, buf);
        LuaFlag = PIPE_ERR_FLAG;
        return 1;
    }

    if (server.vm_enabled         &&
        server.vm_max_threads > 0 &&
        blockClientOnSwappedKeys(LuaClient, cmd)) return 1;

    long ok = 0; /* must come before first goto */
    redisClient *rfc = rsql_createFakeClient();
    robj **rargv     = zmalloc(sizeof(robj *) * argc);
    rfc->argv        = rargv;
    for (int i = 0; i < argc; i++) {
        if (!lua_isstring(L, i + 1)) {
            char *lbuf = "args must be strings";
            luaL_argerror (L, i, lbuf);
            LuaFlag    = PIPE_ERR_FLAG;
            ok         = 1;
            goto redis_lua_err;
        }
        char *arg    = (char *)lua_tostring(L, i + 1);
        rfc->argv[i] = createStringObject(arg, strlen(arg));
    }
    rfc->argc = argc;

    ok = fakeClientPipe(LuaClient, rfc, L, 0, &LuaFlag, luaLine, emptyNoop);

redis_lua_err:
    // TODO free argv[]'s elements???
    zfree(rargv);
    rsql_freeFakeClient(rfc);
    return ok ? 1 : 0;
}

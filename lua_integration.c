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

#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"

#include "redis.h"
#include "zmalloc.h"

#include "denorm.h"
#include "lua_integration.h"

extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern lua_State   *Lua;

extern redisClient *LuaClient;
extern flag         LuaFlag;

// FROM redis.c
#define RL4 redisLog(4,

void luaCommand(redisClient *c) {
    LuaClient = c;             /* used in func redisLua */
    LuaFlag   = PIPE_NONE_FLAG;
    //RL4 "LUA: %s", c->argv[1]->ptr);
    int s     = luaL_dostring(Lua, c->argv[1]->ptr);
    if (s) {
        const char *x = lua_tostring(Lua, -1);
        lua_pop(Lua, 1);
        addReplySds(c, sdscatprintf(sdsempty(), "-ERR: Lua error: %s \r\n", x));
        return;
    }

    RL4 "LuaFlag: %d", LuaFlag);
    int lret = lua_gettop(Lua);
    if (lua_istable(Lua, -1)) {
        const int len = lua_objlen(Lua, -1 );
        addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", len));
        for (int i = 1; i <= len; ++i ) {
            lua_pushinteger(Lua, i);
            lua_gettable(Lua, -2);
            char *x = (char *)lua_tostring(Lua, -1);
            robj *r = createStringObject(x, strlen(x));
            addReplyBulk(c, r);
            decrRefCount(r);
            lua_pop(Lua, 1);
        }
        lua_pop(Lua, 1);
    } else if (LuaFlag == PIPE_EMPTY_SET_FLAG) {
        addReply(c, shared.emptymultibulk);
    } else if (LuaFlag == PIPE_ONE_LINER_FLAG || LuaFlag == PIPE_ERR_FLAG) {
        char *x = (char *)lua_tostring(Lua, -1);
        if (!x) {
            addReply(c, shared.nullbulk);
        } else if (!strncmp(x, "+OK", 3) || !strncmp(x, "-ERR", 4)) {
            addReplySds(c, sdsnewlen(x, strlen(x)));
            addReply(c,shared.crlf);
        } else { /* PIPE_ONE_LINER_FLAG passed to a lua_func() */
            robj *r = createStringObject(x, strlen(x));
            addReplyBulk(c, r);
            decrRefCount(r);
        }
        lua_pop(Lua, 1);
    } else if (!lret) {
        addReply(c, shared.nullbulk);
    } else {
        char *x = (char *)lua_tostring(Lua, -1);
        lua_pop(Lua, 1);
        robj *r = createStringObject(x, strlen(x));
        addReplyBulk(c, r);
        decrRefCount(r);
    }
}

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

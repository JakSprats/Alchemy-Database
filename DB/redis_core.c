/*
 * This file implements ALCHEMY_DATABASE's redis-server interface
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

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "redis.h"
#include "sds.h"

#include "lua_integration.h"
#include "common.h"
#include "redis_core.h"

extern int        Num_tbls;
extern r_tbl_t    Tbl[MAX_NUM_TABLES];
extern int        Num_indx;
extern r_ind_t    Index[MAX_NUM_INDICES];

char        *Basedir         = "./";
char        *LuaIncludeFile  = NULL;
char        *WhiteListLua    = NULL;
#define LUA_INTERNAL_FILE "internal.lua"

/* PROTOTYPES */
void scriptingInit();

void init_Tbl_and_Index() {
    Num_tbls = 0;
    Num_indx = 0;
    bzero(&Tbl,      sizeof(r_tbl_t) * MAX_NUM_TABLES);
    bzero(&Index,    sizeof(r_ind_t) * MAX_NUM_INDICES);
}

static bool loadLuaHelperFile(cli *c, char *fname) {
    sds fwpath = sdscatprintf(sdsempty(), "%s%s", Basedir, fname);
    //printf("loadLuaHelperFile: %s\n", fwpath);
    if (luaL_loadfile(server.lua, fwpath) || lua_pcall(server.lua, 0, 0, 0)) {
        const char *lerr = lua_tostring(server.lua, -1);
        if (c) addReplySds(c, sdscatprintf(sdsempty(),
                           "-ERR luaL_loadfile: %s err: %s\r\n", fwpath, lerr));
        else fprintf(stderr, "loadLuaHelperFile: err: %s\r\n", lerr);
        lua_pop(server.lua, 1); /* pop error from stack */
        return 0;
    }
    sdsfree(fwpath);
    return 1;
}
bool initLua(cli *c) {
    if                    (!loadLuaHelperFile(c, LUA_INTERNAL_FILE)) return 0;
    if (WhiteListLua    && !loadLuaHelperFile(c, WhiteListLua))      return 0;
    if (LuaIncludeFile  && !loadLuaHelperFile(c, LuaIncludeFile))    return 0;
    else                                                             return 1;
}
bool reloadLua(cli *c) {
    lua_close(server.lua);
    scriptingInit();
    return initLua(c);
}

void configAddCommand(redisClient *c) {
    robj *o = getDecodedObject(c->argv[3]);
    if (!strcasecmp(c->argv[2]->ptr, "lua")) {
        if (!loadLuaHelperFile(c, o->ptr)) {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR problem adding lua helper file: %s\r\n", (char *)o->ptr));
            decrRefCount(o);
            return;
        }
    }
    decrRefCount(o);
    addReply(c,shared.ok);
}

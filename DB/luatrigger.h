/*
 * This file implements Lua c bindings for lua function "redis"
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

#ifndef __LUA_INTEGRATION__H
#define __LUA_INTEGRATION__H

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "redis.h"

#include "bt.h"
#include "common.h"

// LUA_CRON
/* this calls lua routines every second from a server cron -> an event */
int luaCronTimeProc(struct aeEventLoop *eventLoop, lolo id, void *clientData);

// LUATRIGGER
luat_t *init_lua_trigger();
void    destroy_lua_trigger(luat_t *luat) ;

void createLuaTrigger(cli *c);
void dropLuaTrigger  (cli *c);

void luatAdd       (bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow);
void luatDel       (bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow);
void luatPreUpdate (bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow);
void luatPostUpdate(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow);

// DESC
sds getLUATlist(ltc_t *ltc, int tmatch);

// INTERPRET_LUA
void interpretLua    (redisClient *c);
void interpretLuaFile(redisClient *c);

#endif /* __LUA_INTEGRATION__H */ 

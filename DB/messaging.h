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

#ifndef DXDB_MESSAGING_H
#define DXDB_MESSAGING_H

#include "redis.h"
#include "sds.h"

#include "common.h"

void messageCommand   (redisClient *c);

int luaConvertToRedisProtocolCommand(lua_State *lua);
int luaSha1Command                  (lua_State *lua);
int luaSQLCommand                   (lua_State *lua);
int luaIsConnectedToMaster          (lua_State *lua);

int remoteMessage(sds ip, int port, sds cmd, bool wait,
                  redisReply **ret_reply);
int luaRemoteMessageCommand         (lua_State *lua);
int luaRemotePipeCommand            (lua_State *lua);

int luaSubscribeFDCommand           (lua_State *lua);
int luaGetFDForChannelCommand       (lua_State *lua);
int luaUnsubscribeFDCommand         (lua_State *lua);
int luaCloseFDCommand               (lua_State *lua);

#define LUA_POP_WHOLE_STACK \
  while(lua_type(lua, 1) != LUA_TNONE) { lua_pop(lua, 1); }

#endif /* DXDB_MESSAGING_H */

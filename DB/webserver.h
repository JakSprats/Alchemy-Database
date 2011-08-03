/*
 * This file implements ALCHEMY_DATABASE's ShortStack WebServer
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

#ifndef DXDB_WEBSERVER_H
#define DXDB_WEBSERVER_H

#include "redis.h"

#include "common.h"

#define HTTP_MODE_OFF      0
#define HTTP_MODE_ON       1
#define HTTP_MODE_POSTBODY 2

bool luafunc_call(cli *c, int argc, robj **argv);

int start_http_session   (cli *c);
int continue_http_session(cli *c);
void end_http_session    (cli *c);

int luaSetHttpResponseHeaderCommand(lua_State *lua);
int luaSetHttpRedirectCommand      (lua_State *lua);
int luaSetHttp304Command           (lua_State *lua);

#endif /* DXDB_WEBSERVER_H */

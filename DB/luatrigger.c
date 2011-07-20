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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/time.h>
#include <ctype.h>

#include "zmalloc.h"
#include "redis.h"

#include "index.h"
#include "colparse.h"
#include "rpipe.h"
#include "parser.h"
#include "alsosql.h"
#include "luatrigger.h"

extern r_tbl_t Tbl[MAX_NUM_TABLES];
extern r_ind_t Index[MAX_NUM_INDICES];

// GLOBALS
ulong  Operations  = 0;
char  *LuaCronFunc = NULL;

#define MIN_MSEC_SIGNIFICANT 2
/* NOTE: this calls lua routines every second from a server cron -> an event */
/*  luacronfunc(Operations) -> use Operations to estimate current load */
int luaCronTimeProc(struct aeEventLoop *eventLoop, lolo id, void *clientData) {
    eventLoop = NULL; id = 0; clientData = 0; /* compiler warnings */
    if (LuaCronFunc) {
        lua_getfield(server.lua, LUA_GLOBALSINDEX, LuaCronFunc); /* func2call */
        lua_pushinteger(server.lua, Operations);             /* prepare param */
        struct timeval t1, t2; gettimeofday(&t1, NULL);
        lua_call(server.lua, 1, 0);
        gettimeofday(&t2, NULL);
#if 0
        long diff = ((t2.tv_sec  - t1.tv_sec)  * 1000) + 
                    ((t2.tv_usec - t1.tv_usec) / 1000);
        if (diff > MIN_MSEC_SIGNIFICANT) printf("lua_cron: ms: %ld\n", diff);
#endif
    }
    Operations = 0;
    return 1000; /* 1000ms -> 1second */
}

/* LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER */
luat_t *init_lua_trigger() {
    luat_t * luat = malloc(sizeof(luat_t));
    bzero(luat, sizeof(luat_t));
    return luat;
}
static void destroy_lua_trigger(luat_t *luat) {//printf("destroy_luatrigger\n");
    free(luat);                                          /* FREED 079 */
}

//TODO "lfunc(col1, col2, col3) -> OK
//TODO "lfunc(1, 45, 'xyz', col1, col2, col3) -> NOT OK
static bool pLTCerr(cli *c) {
    addReply(c, shared.luat_c_decl); return 0;
}
static bool parseLuatCmd(cli *c, sds cmd, ltc_t *ltc, int tmatch) {
    printf("parseLuatCmd: cmd: %s\n", cmd);
    if (!isalpha(*cmd))                      return pLTCerr(c);
    if (cmd[sdslen(cmd) -1] != ')')          return pLTCerr(c);
    char *end  = strchr(cmd, '('); if (!end) return pLTCerr(c);
    ltc->fname = sdsnewlen(cmd, end - cmd);
    uint32 len = sdslen(cmd) - 2 - sdslen(ltc->fname); /* minus "fname()" */
    STACK_STRDUP(clist, (end + 1), len)
    bool ok = parseCommaSpaceList(c, clist, 1, 0, 0, tmatch, ltc->cmatchs,
                                  0, NULL, NULL, NULL, &ltc->ncols, NULL);
    if (!ok) return 0;
    return 1;
}
void luaTAdd(cli *c, sds trname, sds tname, sds acmd, sds dcmd) {
    printf("luaTAdd; trname: %s tname: %s acmd: %s dcmd: %s\n",
           trname, tname, acmd, dcmd);
    int     tmatch = find_table(tname);
    if (tmatch == -1) { addReply(c, shared.nonexistenttable);    return; }
    luat_t *luat   = init_lua_trigger();                   /* FREEME 079 */
    if (!parseLuatCmd(c, acmd, &luat->add, tmatch)) {
        addReply(c, shared.luat_decl_fmt);     goto luatadd_err;
    }
    if (dcmd) {
        if (!parseLuatCmd(c, dcmd, &luat->del, tmatch)) {
            addReply(c, shared.luat_decl_fmt); goto luatadd_err;
        }
    }
    if (!newIndex(c, trname, tmatch, -1, NULL, 0, 0, 0, luat)) {
                                               goto luatadd_err;
    }
    addReply(c, shared.ok);
    return;

luatadd_err:
    destroy_lua_trigger(luat);
}
void createLuaTrigger(cli *c) {
    if (c->argc < 6) { addReply(c, shared.luatrigger_wrong_nargs); return; }
    if (strcasecmp(c->argv[3]->ptr, "ON")) { 
        addReply(c, shared.createsyntax); return;
    }
    sds   trname = c->argv[2]->ptr;
    char *dcmd   = (c->argc > 6) ? c->argv[6]->ptr : NULL;
    luaTAdd(c, trname, c->argv[4]->ptr, c->argv[5]->ptr, dcmd);
}
sds getLUATlist(ltc_t *ltc, int tmatch) {
    sds cmd = sdsdup(ltc->fname);
    cmd     = sdscatlen(cmd, "(", 1);
    for (int j = 0; j < ltc->ncols; j++) {
        if (j) cmd = sdscatlen(cmd, ",", 1);
        sds cname  = Tbl[tmatch].col_name[ltc->cmatchs[j]]->ptr;
        cmd        = sdscatprintf(cmd, "%s", cname);
    }
    cmd     = sdscatlen(cmd, ")", 1);
    return cmd;
}

void dropLuaTrigger(cli *c) {
    char *iname = c->argv[2]->ptr;
    int   imatch  = match_index_name(iname);
    if (imatch == -1) {
        addReply(c, shared.nullbulk);        return;
    }
    if (!Index[imatch].luat) {
        addReply(c, shared.drop_luatrigger); return;
    }
    emptyIndex(imatch);
    addReply(c, shared.cone);
}

static void luatDo(bt  *btr,    luat_t *luat, aobj *apk, 
                   int  imatch, void   *rrow, bool  add) {
    r_ind_t *ri    = &Index[imatch];
    r_tbl_t *rt    = &Tbl[ri->table];
    ltc_t   *ltc   = add ? &luat->add   : &luat->del;
    if (!ltc->ncols) return; /* e.g. no luatDel */

    lua_getfield(server.lua, LUA_GLOBALSINDEX, ltc->fname);/* function to call*/

    for (int i = 0; i < ltc->ncols; i++) {
        aobj acol = getCol(btr, rrow, ltc->cmatchs[i], apk, ri->table);
        int ctype = rt->col_type[ltc->cmatchs[i]];
        if        C_IS_I(ctype) {
            if (acol.empty) lua_pushnil    (server.lua);
            else            lua_pushinteger(server.lua, acol.i);
        } else if C_IS_L(ctype) {
            if (acol.empty) lua_pushnil    (server.lua);
            else            lua_pushinteger(server.lua, acol.l);
        } else if C_IS_F(ctype) {
            if (acol.empty) lua_pushnil   (server.lua);
            else            lua_pushnumber(server.lua, acol.f);
        } else {/* COL_TYPE_STRING */
            if (acol.empty) lua_pushnil   (server.lua);
            else            lua_pushlstring(server.lua, acol.s, acol.len);
        }
        releaseAobj(&acol);
    }
    lua_call(server.lua, ltc->ncols, 0);

}
void luatAdd(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, 1);
}
void luatDel(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, 0);
}

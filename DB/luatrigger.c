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
#include "find.h"
#include "alsosql.h"
#include "luatrigger.h"

/* LUATRIGGER TODO LIST
    1.) LuaCronFuncs should return the MS they want to be called again with
    2.) add "fk" to luatrigger (similar to arg: "table") will enable fk-checks
*/

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

extern cli    *CurrClient;

// GLOBALS
ulong  Operations  = 0;
char  *LuaCronFunc = NULL;

#define SLOW_LUA_TRIGGER //TODO TEST VALUE - slow things down


/* NOTE: this calls lua routines every second from a server cron -> an event */
/*  luacronfunc(Operations) -> use Operations to estimate current load */
int luaCronTimeProc(struct aeEventLoop *eventLoop, lolo id, void *clientData) {
    eventLoop = NULL; id = 0; clientData = 0; /* compiler warnings */
    if (LuaCronFunc) {
        lua_getfield(server.lua, LUA_GLOBALSINDEX, LuaCronFunc); /* func2call */
        lua_pushinteger(server.lua, Operations);             /* prepare param */
        struct timeval t1, t2; gettimeofday(&t1, NULL);
        CurrClient = server.lua_client;
        lua_call(server.lua, 1, 0);
        gettimeofday(&t2, NULL);
    }
    Operations = 0;
#ifdef SLOW_LUA_TRIGGER
    return 10000; /* 10000ms -> 10secs */
#else
    return 1000; /* 1000ms -> 1second */
#endif
}

/* LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER */
luat_t *init_lua_trigger() {
    luat_t * luat = malloc(sizeof(luat_t)); bzero(luat, sizeof(luat_t));
    return luat;
}
static void destroy_lua_trigger(luat_t *luat) {//printf("destroy_luatrigger\n");
    if (luat->add.cmatchs) free(luat->add.cmatchs);      // FREED 083
    if (luat->del.cmatchs) free(luat->del.cmatchs);      // FREED 083
    free(luat);                                          // FREED 079
}

static bool pLTCerr(cli *c) { addReply(c, shared.luat_c_decl); return 0; }

static bool parseLuatCmd(cli *c, sds cmd, ltc_t *ltc, int tmatch) {
    if (!ISALPHA(*cmd))                      return pLTCerr(c);
    if (cmd[sdslen(cmd) -1] != ')')          return pLTCerr(c);
    char *end  = strchr(cmd, '('); if (!end) return pLTCerr(c);
    ltc->fname = sdsnewlen(cmd, end - cmd);
    uint32 len = sdslen(cmd) - 2 - sdslen(ltc->fname); /* minus "fname()" */
    STACK_STRDUP(clist, (end + 1), len)
    char *tkn = clist;
    SKIP_SPACES(tkn)
    if (strlen(tkn) >= 5 && !strncmp(tkn, "table", 5)) {// case: 1st arg "table"
        tkn += 5;
        if (!*tkn || ISBLANK(*tkn) || *tkn == ',') {
            if (*tkn) tkn++; SKIP_SPACES(tkn); ltc->tblarg = 1;
        }
    }
    if (!strlen(tkn)) return 1; // zero args is ok
    list *cmatchl = listCreate();
    bool  ok      = parseCommaSpaceList(c, tkn, 1, 0, 0, 1, 0, tmatch, cmatchl,
                                        NULL, NULL, NULL, &ltc->ncols, NULL);
    if (ok) {
        ltc->cmatchs = malloc(sizeof(int) * ltc->ncols); // FREE ME 083
        listNode *ln;
        r_tbl_t  *rt = &Tbl[tmatch];
        int       i  = 0;
        listIter *li = listGetIterator(cmatchl, AL_START_HEAD);
        while((ln = listNext(li))) {
            int cmatch      = (int)(long)ln->value;
            //NOTE: no support for U128 or index.pos()
            if (C_IS_X(rt->col[cmatch].type) || cmatch < 0) { ok = 0; break; }
            ltc->cmatchs[i] = cmatch; i++;
        } listReleaseIterator(li);
    }
    listRelease(cmatchl);
    return ok;
}
void luaTAdd(cli *c, sds trname, sds tname, sds acmd, sds dcmd) {
    //printf("luaTAdd; trname: %s tname: %s acmd: %s dcmd: %s\n", 
    //        trname, tname, acmd, dcmd);
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
    newIndex(c, trname, tmatch, -1, NULL, 0, 0, 0, luat, -1, 0, 0); //Cant fail
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
sds getLUATlist(ltc_t *ltc, int tmatch) { // Used in DESC & AOF
    sds cmd = sdsdup(ltc->fname);
    cmd     = sdscatlen(cmd, "(", 1);
    for (int j = 0; j < ltc->ncols; j++) {
        if (j) cmd = sdscatlen(cmd, ",", 1);
        sds cname  = Tbl[tmatch].col[ltc->cmatchs[j]].name;
        cmd        = sdscatprintf(cmd, "%s", cname);
    }
    cmd     = sdscatlen(cmd, ")", 1);
    return cmd;
}

void dropLuaTrigger(cli *c) {
    sds iname  = c->argv[2]->ptr;
    int imatch = match_index_name(iname);
    if (imatch == -1)        { addReply(c, shared.nullbulk);        return; }
    if (!Index[imatch].luat) { addReply(c, shared.drop_luatrigger); return; }
    emptyIndex(imatch);
    addReply(c, shared.cone);
}

static void luatDo(bt  *btr,    luat_t *luat, aobj *apk, 
                   int  imatch, void   *rrow, bool  add) {
    r_ind_t *ri    = &Index[imatch];
    r_tbl_t *rt    = &Tbl[ri->table];
    ltc_t   *ltc   = add ? &luat->add : &luat->del;
    int      tcols = ltc->ncols + ltc->tblarg;
    if (!ltc->fname) return; /* e.g. no luatDel */

    lua_getfield(server.lua, LUA_GLOBALSINDEX, ltc->fname); // function to call
    if (ltc->tblarg) {
        lua_pushlstring(server.lua, rt->name, sdslen(rt->name));
    }

    for (int i = 0; i < ltc->ncols; i++) {
        aobj acol = getCol(btr, rrow, ltc->cmatchs[i], apk, ri->table);
        int ctype = rt->col[ltc->cmatchs[i]].type;
        //NOTE: C_IS_X is prohibited (need lua 128 bit num lib)
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
    lua_call(server.lua, tcols, 0);

}
void luatAdd(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, 1);
}
void luatDel(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, 0);
}

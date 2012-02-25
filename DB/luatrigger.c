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
#include <assert.h>

#include "xdb_hooks.h"

#include "zmalloc.h"
#include "redis.h"

#include "index.h"
#include "stream.h"
#include "colparse.h"
#include "rpipe.h"
#include "parser.h"
#include "find.h"
#include "alsosql.h"
#include "luatrigger.h"

/* LUATRIGGER TODO LIST
    1.) LuaCronFuncs should return the MS they want to be called again with
    2.) add "fk" to luatrigger (similar to arg: "table") will enable fk-checks

    CHANGE SYNTAX: CREATE LUATRIGGER lname ON tbl TYPE command
    where TYPE:[ADD,DEL,PREUPDATE,POSTUPDATE]
*/

extern r_tbl_t *Tbl;
extern r_ind_t *Index;

//#define SLOW_LUA_TRIGGER //TEST VALUE - slow things down

// LUA_CRON LUA_CRON LUA_CRON LUA_CRON LUA_CRON LUA_CRON LUA_CRON LUA_CRON
/* NOTE: this calls lua routines every second from a server cron -> an event */
/*  luacronfunc(Operations) -> use Operations to estimate current load */
int luaCronTimeProc(struct aeEventLoop *eventLoop, lolo id, void *clientData) {
    eventLoop = NULL; id = 0; clientData = 0; /* compiler warnings */
    if (server.alc.LuaCronFunc) {
        CLEAR_LUA_STACK
        lua_getfield(server.lua, LUA_GLOBALSINDEX, server.alc.LuaCronFunc);
        lua_pushinteger(server.lua, server.alc.Operations);
        struct timeval t1, t2; gettimeofday(&t1, NULL);
        server.alc.CurrClient = server.lua_client;
        int ret = DXDB_lua_pcall(server.lua, 1, 0, 0);
        if (ret) redisLog(REDIS_WARNING, "Error running luaCron: %s",
                           lua_tostring(server.lua, -1));
        CLEAR_LUA_STACK
        gettimeofday(&t2, NULL);
    }
    server.alc.Operations = 0;
#ifdef SLOW_LUA_TRIGGER
    return 10000; /* 10000ms -> 10secs */
#else
    return 1000; /* 1000ms -> 1second */
#endif
}

// LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER
luat_t *init_lua_trigger() {
    luat_t * luat = malloc(sizeof(luat_t));              // FREE 079
    bzero(luat, sizeof(luat_t)); return luat;
}
static void destroy_ltc(ltc_t *ltc) {
    if (ltc->ics)   free   (ltc->ics);                   // FREED 083
    //TODO WALK ics -> destroyIC(ltc->ics[i])
    if (ltc->fname) sdsfree(ltc->fname);                 // FREED 174
}
void destroy_lua_trigger(luat_t *luat) { //printf("destroy_luatrigger\n");
    destroy_ltc(&luat->add);   destroy_ltc(&luat->del);
    destroy_ltc(&luat->preup); destroy_ltc(&luat->postup);
    free(luat);                                          // FREED 079
}

static bool parseLuatCmd(cli *c, sds cmd, ltc_t *ltc, int tmatch) {
    char *begc = cmd; char *endc = cmd + sdslen(cmd) -1;
    SKIP_SPACES(begc) REV_SKIP_SPACES(endc)
    if (!ISALPHA(*begc)) { addReply(c, shared.luat_c_decl); return 0; }
    if (*endc != ')')    { addReply(c, shared.luat_c_decl); return 0; }
    char *fend = strchr(begc, '(');
    if (!fend)           { addReply(c, shared.luat_c_decl); return 0; }
    ltc->fname = sdsnewlen(begc, fend - begc);           // FREE 174
    fend++; endc--; // skip '(' & ')'
    if (fend == endc) return 1; // zero args is ok
    sds args = sdsnewlen(fend, (endc - fend + 1));       // FREE 175
    char *tkn = args;
    SKIP_SPACES(tkn)
    if (strlen(tkn) >= 5 && !strncasecmp(tkn, "table", 5)) {// 1st arg "table"
        tkn += 5;
        if (ISBLANK(*tkn)) SKIP_SPACES(tkn);
        if (*tkn == ',') { tkn++; SKIP_SPACES(tkn); ltc->tblarg = 1; }
        else if (!*tkn)                             ltc->tblarg = 1;
        else             { tkn -= 5; } // maybe args is: "tablenameofwhoknows"
    }
    if (!strlen(tkn)) return 1; // zero args is ok
    CREATE_CS_LS_LIST(1)
    bool  ok      = parseCSLSelect(c, tkn, 1, 0, tmatch, cmatchl,
                                   ls, &ltc->ncols, NULL);
    if (ok) {
        r_tbl_t  *rt = &Tbl[tmatch];
        ltc->ics     = malloc(sizeof(icol_t) * ltc->ncols); // FREE 083
        int       i  = 0;
        listIter *li = listGetIterator(cmatchl, AL_START_HEAD); listNode *ln;
        while((ln = listNext(li))) {
            icol_t *ic      = ln->value;
            int cm          = ic->cmatch;
            //NOTE: no support for U128 or index.pos()
            if (C_IS_X(rt->col[cm].type) || cm < 0) { ok = 0; break; }
            cloneIC(&ltc->ics[i], ic); i++;
        } listReleaseIterator(li);
    }
    RELEASE_CS_LS_LIST
    return ok;
}

static void luaTAdd(cli    *c,    sds    trname, int tmatch,
                    luat_t *luat, ltc_t *ltc,    sds cmd,    int imatch) {
    printf("luaTAdd; trname: %s tmatch: %d cmd: %s\n", trname, tmatch, cmd);
    if (!parseLuatCmd(c, cmd, ltc, tmatch)) {
        addReply(c, shared.luat_decl_fmt);     goto luatadd_err;
    }
    if (imatch == -1) {
        DECLARE_ICOL(ic, -1)
        newIndex(c, trname, tmatch, ic, NULL, 0, 0, 0, luat, ic,
                 0, 0, 0, NULL, NULL, NULL); //Cant fail
    }
    addReply(c, shared.ok);
    return;

luatadd_err:
    destroy_lua_trigger(luat);
}
//SYNTAX: CREATE LUATRIGGER lname ON tbl TYPE command
void createLuaTrigger(cli *c) {
    if (c->argc != 7) { addReply(c, shared.luatrigger_wrong_nargs);   return; }
    if (strcasecmp(c->argv[3]->ptr, "ON")) { 
        addReply(c, shared.createsyntax);                             return;
    }
    int     tmatch = find_table(c->argv[4]->ptr);
    if (tmatch == -1) { addReply(c, shared.nonexistenttable);         return; }
    sds     trname = c->argv[2]->ptr;
    int     imatch = match_index_name(trname);
    luat_t *luat   = (imatch == -1) ? init_lua_trigger() : Index[imatch].luat;
    ltc_t  *ltc; sds targtype = c->argv[5]->ptr;
    if      (!strcasecmp(targtype, "INSERT"))     ltc = &luat->add;
    else if (!strcasecmp(targtype, "DELETE"))     ltc = &luat->del;
    else if (!strcasecmp(targtype, "PREUPDATE"))  ltc = &luat->preup;
    else if (!strcasecmp(targtype, "POSTUPDATE")) ltc = &luat->postup;
    else {
        addReply(c, shared.luat_decl_fmt); destroy_lua_trigger(luat); return;
    }
    if (ltc->fname && imatch != -1) {
        addReply(c, shared.nonunique_ltname);                         return;
    }
    sds cmd = c->argv[6]->ptr;
    luaTAdd(c, trname, tmatch, luat, ltc, cmd, imatch);
}

// CALL_LUATRIGGER CALL_LUATRIGGER CALL_LUATRIGGER CALL_LUATRIGGER
#define LUAT_DO_ADD    1
#define LUAT_DO_DEL    2
#define LUAT_DO_PREUP  3
#define LUAT_DO_POSTUP 4
static void luatDo(bt  *btr,    luat_t *luat, aobj *apk, 
                   int  imatch, void   *rrow, int whom) {
    ltc_t   *ltc   = (whom == LUAT_DO_ADD)   ?  &luat->add   :
                     (whom == LUAT_DO_DEL)   ?  &luat->del   :
                     (whom == LUAT_DO_PREUP) ?  &luat->preup :
                     /*       LUAT_DO_POSTUP */ &luat->postup;
    if (!ltc->fname) return; /* e.g. no luatDel */
    r_ind_t *ri    = &Index[imatch];
    r_tbl_t *rt    = &Tbl[ri->tmatch];
    int      tcols = ltc->ncols + ltc->tblarg;
    //printf("luatDo: fname: %s tcols: %d\n", ltc->fname, tcols);
    lua_getfield(server.lua, LUA_GLOBALSINDEX, ltc->fname); // function to call
    if (ltc->tblarg) {
        lua_pushlstring(server.lua, rt->name, sdslen(rt->name));
    }
    for (int i = 0; i < ltc->ncols; i++) {
        if (ltc->ics[i].nlo) {                //printf("luatDo: pushLuaVar\n");
            pushLuaVar(ri->tmatch, ltc->ics[i], apk);
        } else {                          //printf("luatDo: rrow: %p\n", rrow);
            aobj acol = getCol(btr, rrow, ltc->ics[i], apk, ri->tmatch, NULL);
            pushAobjLua(&acol, rt->col[ltc->ics[i].cmatch].type);
            releaseAobj(&acol);
        }
    }
    int ret = DXDB_lua_pcall(server.lua, tcols, 0, 0);
    //NOTE: LUATRIGGERS can not fail TRANSACTIONS, failures are simply LOGGED
    if (ret) redisLog(REDIS_WARNING, "Error running LUATRIGGER: %s ERROR: %s",
                                      ltc->fname, lua_tostring(server.lua, -1));
}
void luatAdd(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, LUAT_DO_ADD);
}
void luatDel(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, LUAT_DO_DEL);
}
void luatPreUpdate(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, LUAT_DO_PREUP);
}
void luatPostUpdate(bt *btr, luat_t *luat, aobj *apk, int imatch, void *rrow) {
    luatDo(btr, luat, apk, imatch, rrow, LUAT_DO_POSTUP);
}

// DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP
void dropLuaTrigger(cli *c) {
    sds iname  = c->argv[2]->ptr;
    int imatch = match_index_name(iname);
    if (imatch == -1)        { addReply(c, shared.nullbulk);        return; }
    if (!Index[imatch].luat) { addReply(c, shared.drop_luatrigger); return; }
    emptyIndex(c, imatch);
    addReply(c, shared.cone);
}

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
//NOTE: Used in DESC & AOF
sds getLUATlist(ltc_t *ltc, int tmatch) {
    sds cmd = sdsdup(ltc->fname);
    cmd     = sdscatlen(cmd, "(", 1);
    if (ltc->tblarg) cmd = sdscatlen(cmd, "table, ", 7);
    for (int j = 0; j < ltc->ncols; j++) {
        if (j) cmd = sdscatlen(cmd, ", ", 2);
        int cmatch = ltc->ics[j].cmatch;                    assert(cmatch > -1);
        sds cname  = Tbl[tmatch].col[cmatch].name;
        cmd        = sdscatprintf(cmd, "%s", cname);
        if (ltc->ics[j].nlo) {
            for (uint32 k = 0; k < ltc->ics[j].nlo; k++) {
                cmd = sdscatprintf(cmd, ".%s", ltc->ics[j].lo[k]);
            }
        }
    }
    cmd     = sdscatlen(cmd, ")", 1);
    return cmd;
}

// INTERPRET_LUA INTERPRET_LUA INTERPRET_LUA INTERPRET_LUA INTERPRET_LUA
void interpretLua(redisClient *c) { //printf("interpretCommandLua\n");
    for (int i = 2; i < c->argc; i++) {
        CLEAR_LUA_STACK
        if (luaL_dostring(server.lua, c->argv[i]->ptr)) {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR problem adding lua: (%s) msg: (%s)\r\n",
               (char *)c->argv[i]->ptr, lua_tostring(server.lua, -1)));
            CLEAR_LUA_STACK return;
        }
    }
    CLEAR_LUA_STACK
    addReply(c,shared.ok);
}
void interpretLuaFile(redisClient *c) { //printf("interpretCommandLuaFile\n");
    for (int i = 2; i < c->argc; i++) {
        CLEAR_LUA_STACK
        if (!loadLuaHelperFile(c, c->argv[i]->ptr)) {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR problem adding LuaFile: (%s) msg: (%s)\r\n",
               (char *)c->argv[i]->ptr, lua_tostring(server.lua, -1)));
            CLEAR_LUA_STACK return;
        }
    }
    CLEAR_LUA_STACK
    addReply(c,shared.ok);
}

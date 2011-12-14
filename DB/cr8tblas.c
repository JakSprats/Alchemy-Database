/*
 *
 * This file implements "CREATE TABLE x AS redis_datastructure"
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

#include "redis.h"
#include "zmalloc.h"

#include "internal_commands.h"
#include "wc.h"
#include "row.h"
#include "rpipe.h"
#include "ddl.h"
#include "parser.h"
#include "find.h"
#include "alsosql.h"
#include "colparse.h"
#include "cr8tblas.h"

extern r_tbl_t *Tbl;
extern char    *Col_type_defs[];

// CONSTANT GLOBALS
stor_cmd AccessCommands[NUM_ACCESS_TYPES];

static int  AIdum[1]; /* dummy array of int */
static bool ABdum[1]; /* dummy array of bool */
static jc_t AJdum[1]; /* dummy array of bool */

#define INTERNAL_CREATE_TABLE_ERR_MSG \
  "-ERR CREATE TABLE SELECT - Automatic Table Creation failed with error: "
#define CR8TBL_SELECT_ERR_MSG \
  "-ERR CREATE TABLE SELECT - SELECT command had error: "

static void cpyColDef(sds  *cdefs,
                      int   tmatch,
                      int   cmatch,
                      int   qcols,
                      int   i,
                      bool  conflix,
                      bool  x[]) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (conflix && x[i]) { /* prepend tbl_name */
        *cdefs      = sdscatprintf(*cdefs, "%s_", rt->name);
    }
    sds   cname = rt->col[cmatch].name;
    char *ctype = Col_type_defs[rt->col[cmatch].type];
    char *finc  = (i == (qcols - 1)) ? ")" : ",";
    *cdefs      = sdscatprintf(*cdefs, "%s %s%s", cname, ctype, finc);
}
static bool internalCr8Tbl(redisClient *c,
                           redisClient *rfc,
                           int          qcols,
                           int          cmatchs[],
                           int          tmatch,
                           jc_t         js[],
                           bool         x[]) {
    sds tname = c->argv[2]->ptr;
    if (find_table(tname) > 0) return 1;
    sds cdefs = sdsnewlen("(", 1);                       /* FREE ME 038 */
    for (int i = 0; i < qcols; i++) {
        if (tmatch != -1) cpyColDef(&cdefs, tmatch, cmatchs[i], qcols, i, 0, x);
        else              cpyColDef(&cdefs, js[i].t, js[i].c, qcols, i, 1, x);
    }
    robj **rargv = zmalloc(sizeof(robj *) * 4);
    rfc->argv    = rargv;
    rfc->argc    = 4;
    rfc->argv[0] = _createStringObject("CREATE");
    rfc->argv[1] = _createStringObject("TABLE");
    rfc->argv[2] = createStringObject(tname, sdslen(tname));
    rfc->argv[3] = createStringObject(cdefs, sdslen(cdefs));
    sdsfree(cdefs);                                      /* FREED 038 */
    resetFakeClient(rfc);
    createCommand(rfc);
    cleanupFakeClient(rfc);
    if (!replyIfNestedErr(c, rfc, INTERNAL_CREATE_TABLE_ERR_MSG)) return 0;
    else                                                          return 1;
}
bool createTableFromJoin(redisClient *c,
                         redisClient *rfc,
                         int          qcols,
                         jc_t         js[]) {
    bool x[qcols];
    for (int i = 0; i < qcols; i++) { /* check for column name collisions */
        x[i] = 0;
        for (int j = 0; j < qcols; j++) {
            if (i == j) continue;
            sds cname1 = Tbl[js[i].t].col[js[i].c].name;
            sds cname2 = Tbl[js[j].t].col[js[j].c].name;
            if (sdslen(cname1) == sdslen(cname2) && !strcmp(cname1, cname2)) {
                x[i] = 1;
                break;
            }
        }
    }
    return internalCr8Tbl(c, rfc, qcols, AIdum, -1, js, x);
}

void createTableSelect(redisClient *c) {
    char *cmd = c->argv[3]->ptr;
    int axs = getAccessCommNum(cmd);
    if (axs == -1) {
        addReply(c, shared.create_table_err);
        return;
    }
    list  *cmatchl = listCreate();
    bool   cstar   =  0;
    int    qcols   =  0;
    int    tmatch  = -1;
    bool   join    =  0;
    int    rargc;
    robj **rargv   = (*AccessCommands[axs].parse)(cmd, &rargc);
    if (!rargv) { listRelease(cmatchl); return; }
    if (!strcasecmp(rargv[0]->ptr, "SCAN")) {//TODO CREATE TABLE AS SCAN
        addReply(c, shared.cr8tbl_scan);
        listRelease(cmatchl); zfree(rargv); return;
    }
    if (!parseSelect(c, 0, NULL, &tmatch, cmatchl, &qcols, &join,
                     &cstar,  rargv[1]->ptr, rargv[2]->ptr,
                     rargv[3]->ptr, rargv[4]->ptr, 1)) {
        listRelease(cmatchl); zfree(rargv); return;
    }
    if (cstar) {
        addReply(c, shared.create_table_as_count);
        listRelease(cmatchl); zfree(rargv); return;
    }
    CMATCHS_FROM_CMATCHL
    sds          tname = c->argv[2]->ptr;
    sds          clist = rargv[1]->ptr;
    sds          tlist = rargv[3]->ptr;
    sds          wc    = rargv[5]->ptr;
    bool         ok    = 0;
    char        *msg   = CR8TBL_SELECT_ERR_MSG;
    redisClient *rfc   = getFakeClient(); // frees last rfc->rargv[] + contents
    rfc->argc          = 0;
    if (join) { /* CREATE TABLE AS SELECT JOIN */
        jb_t jb;
        init_join_block(&jb);
        parseJoin(rfc, &jb, clist, tlist, wc);
        qcols = jb.qcols;
        if (replyIfNestedErr(c, rfc, msg)) {
            ok = createTableFromJoin(c, rfc, qcols, jb.js);
        }
        destroy_join_block(c, &jb);
    } else  {   /* CREATE TABLE AS SELECT RANGE QUERY */
        cswc_t w; wob_t wb;
        init_check_sql_where_clause(&w, tmatch, wc); init_wob(&wb);
        parseWCplusQO(rfc, &w, &wb, SQL_SELECT);
        if (replyIfNestedErr(c, rfc, msg)) {
            ok = internalCr8Tbl(c, rfc, qcols, cmatchs, w.wf.tmatch,
                                AJdum, ABdum);
        }
        destroy_check_sql_where_clause(&w);
    }
    resetFakeClient(rfc);
    if (ok) {
        lua_State *lua   = server.lua;
        //TODO use luafunc_call()
        lua_getfield(lua, LUA_GLOBALSINDEX, "internal_copy_table_from_select");
        lua_pushlstring(lua, tname, sdslen(tname));
        lua_pushlstring(lua, clist, sdslen(clist));
        lua_pushlstring(lua, tlist, sdslen(tlist));
        lua_pushlstring(lua, wc, sdslen(wc));
        int ret = lua_pcall(lua, 4, 1, 0);
        if (ret) {
            addReplyErrorFormat(c,"Error running script (%s): %s\n",
                "internal_copy_table_from_select", lua_tostring(lua,-1));
            lua_pop(lua,1);
        } else addReply(c, shared.ok);
    }

    listRelease(cmatchl); zfree(rargv);
}
int getAccessCommNum(char *cmd) {
    int   axs    = -1;
    for (int i = 0; i < NUM_ACCESS_TYPES; i++) {
        if (!strncasecmp(cmd, AccessCommands[i].name,
                              strlen(AccessCommands[i].name))) {
            char *x = cmd + strlen(AccessCommands[i].name);
            if (*x == ' ') { /* needs to be space delimited as well */
                axs = i; break;
            }
        }
    }
    return axs;
}

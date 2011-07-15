/*
 * This file implements ALCHEMY_DATABASE's redis-server hooks
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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <strings.h>

#include "xdb_hooks.h"

#include "redis.h"
#include "dict.h"
#include "rdb.h"

// FOR override_getKeysFromComm()
#include "wc.h"
#include "colparse.h"

#include "internal_commands.h"
#include "aof_alsosql.h"
#include "sixbit.h"
#include "range.h"
#include "index.h"
#include "rdb_alsosql.h"
#include "alsosql.h"
#include "lua_integration.h"
#include "redis_core.h"

extern int      Num_tbls;
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern int      Num_indx;
extern r_ind_t  Index[MAX_NUM_INDICES];

extern stor_cmd AccessCommands[];

extern cli     *CurrClient;
extern ulong    CurrCard;        // TODO remove

//TODO if a SELECT is bigger than MTU,
//       it may be read in separate event-loop calls
//       so any variables that may be in separate event-loop calls
//       MUST be in redisClient and NOT global-vars
extern bool     LruColInSelect;  // TODO move to redisClient

extern char    *Basedir;
extern char    *LuaIncludeFile;
extern char    *WhiteListLua;
extern char    *LuaCronFunc;
extern ulong    Operations;

extern uchar    OutputMode;
extern int      WebServerMode;
extern char    *WebServerIndexFunc = NULL;

extern int      NumJTAlias;      // TODO move to redisClient
extern int      LastJTAmatch;    // TODO move to redisClient

extern bool     InternalRequest; // TODO move to redisClient

bool            HTTP_Mode = 0;    // TODO: move to redisClient
robj           *HTTPFile = NULL; // TODO: move to redisClient
bool            HTTP_KA   = 0;    // TODO: move to redisClient

struct in_addr  WS_WL_Addr;
struct in_addr  WS_WL_Mask;
unsigned int    WS_WL_Broadcast = 0;
unsigned int    WS_WL_Subnet    = 0;

/* PROTOTYPES */
int  yesnotoi(char *s);
sds override_getKeysFromComm(rcommand *cmd, robj **argv, int argc, bool *err);
int *getKeysUsingCommandTable(rcommand *cmd, robj **argv, int argc,
                              int *numkeys);
void luaReplyToRedisReply(redisClient *c, lua_State *lua); // from scripting.c
static bool luafunc_call(redisClient *c, int argc, robj **argv);

void showCommand     (redisClient *c);

void createCommand   (redisClient *c);
void dropCommand     (redisClient *c);
void descCommand     (redisClient *c);
void alterCommand    (redisClient *c);
void sqlDumpCommand  (redisClient *c);

void insertCommand   (redisClient *c);
void replaceCommand  (redisClient *c);
void sqlSelectCommand(redisClient *c);
void updateCommand   (redisClient *c);
void deleteCommand   (redisClient *c);
void tscanCommand    (redisClient *c);

void luafuncCommand  (redisClient *c);

void explainCommand  (redisClient *c);

void btreeCommand    (redisClient *c);

#ifdef REDIS3
  #define CMD_END       NULL,1,1,1,0,0
  #define GLOB_FUNC_END NULL,0,0,0,0,0
#else
  #define CMD_END NULL,0,0,0
#endif

struct redisCommand DXDBCommandTable[] = {
    {"select",  sqlSelectCommand, -2, 0,                   CMD_END},
    {"insert",  insertCommand,    -5, REDIS_CMD_DENYOOM,   CMD_END},
    {"update",  updateCommand,     6, REDIS_CMD_DENYOOM,   CMD_END},
    {"delete",  deleteCommand,     5, 0,                   CMD_END},
    {"replace", replaceCommand,   -5, 0|REDIS_CMD_DENYOOM, CMD_END},
    {"scan",    tscanCommand,     -4, 0,                   CMD_END},

    {"lua",     luafuncCommand,   -2, 0,                   GLOB_FUNC_END},

    {"create",  createCommand,    -4, REDIS_CMD_DENYOOM,   GLOB_FUNC_END},
    {"drop",    dropCommand,       3, 0,                   GLOB_FUNC_END},
    {"desc",    descCommand,       2, 0,                   GLOB_FUNC_END},
    {"dump",    sqlDumpCommand,   -2, 0,                   GLOB_FUNC_END},

    {"explain", explainCommand,    7, 0,                   GLOB_FUNC_END},
    {"alter",   alterCommand,     -6, 0,                   GLOB_FUNC_END},
    {"show",    showCommand,       2, 0,                   GLOB_FUNC_END},
    {"btree",   btreeCommand,      2, 0,                   GLOB_FUNC_END},
};


int *DXDB_getKeysFromCommand(rcommand *cmd, robj **argv, int argc, int *numkeys,
                             int flags,     sds *override_key, bool *err) {
    //printf("DXDB_getKeysFromCommand\n");
    if (cmd->proc == sqlSelectCommand || cmd->proc == insertCommand    || 
        cmd->proc == updateCommand    || cmd->proc == deleteCommand    || 
        cmd->proc == replaceCommand   || cmd->proc == tscanCommand) {
            *numkeys      = 0;
            *override_key = override_getKeysFromComm(cmd, argv, argc, err);
            return NULL;
    }
    if (cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd, argv, argc, numkeys, flags);
    } else {
        return getKeysUsingCommandTable(cmd, argv, argc, numkeys);
    }
}

void DXDB_populateCommandTable(dict *server_commands) {
    //printf("addDXDBfunctions: %p\n", server_commands);
    int numcommands = sizeof(DXDBCommandTable)/sizeof(struct redisCommand);
    for (int j = 0; j < numcommands; j++) {
        struct redisCommand *c = DXDBCommandTable + j;
        int retval = dictAdd(server_commands, sdsnew(c->name), c);
        assert(retval == DICT_OK);
    }
}

void DXDB_initServerConfig() { //printf("DXDB_initServerConfig\n");
    LuaIncludeFile     = NULL;
    WhiteListLua       = NULL;
    LuaCronFunc        = NULL;
    Basedir            = zstrdup("./");
    WebServerMode      = -1;
    WebServerIndexFunc = NULL;
    InternalRequest    =  0;

    WS_WL_Broadcast    =  0;
    WS_WL_Subnet       =  0;
    bzero(&WS_WL_Addr, sizeof(struct in_addr));
    bzero(&WS_WL_Mask, sizeof(struct in_addr));
}

void DXDB_initServer() { //printf("DXDB_initServer\n");
    aeCreateTimeEvent(server.el, 1, luaCronTimeProc, NULL, NULL);
    initX_DB_Range();
    initAccessCommands();
    init_six_bit_strings();
    init_Tbl_and_Index();
    if (!initLua(NULL)) exit(-1);
    CurrClient     = NULL;
    CurrCard       = 0;
    LruColInSelect = 0;
    Operations     = 0;
}

void DXDB_emptyDb() { //printf("DXDB_emptyDb\n");
    for (int k = 0; k < Num_tbls; k++) emptyTable(k); /* deletes indices also */
    init_Tbl_and_Index();
}

rcommand *DXDB_lookupCommand(sds name) {
    struct redisCommand *cmd = dictFetchValue(server.commands, name);
    //printf("DXDB_lookupCommand: %p\n", cmd);
    if (WebServerMode > 0) { //printf("InternalRequest: %d\n", InternalRequest);
        if (!InternalRequest) {
            if (WS_WL_Broadcast) { // check WHITELISTED IPs
                unsigned int saddr     = CurrClient->sa.sin_addr.s_addr;
                unsigned int b_masked  = saddr | WS_WL_Broadcast;
                unsigned int sn_masked = saddr & WS_WL_Subnet;
                if (b_masked  == WS_WL_Broadcast &&
                    sn_masked == WS_WL_Subnet) return cmd;
            }
            return cmd ? (cmd->proc == luafuncCommand) ? cmd : NULL : NULL;
        }
    }
    return cmd;
}

static void computeWS_WL_MinMax() {
    if (!WS_WL_Broadcast && WS_WL_Mask.s_addr && WS_WL_Addr.s_addr) {
        WS_WL_Subnet    = WS_WL_Addr.s_addr & WS_WL_Mask.s_addr;
        WS_WL_Broadcast = WS_WL_Addr.s_addr | ~WS_WL_Mask.s_addr;
    }
}
int DXDB_loadServerConfig(int argc, sds *argv) {
    //printf("DXDB_loadServerConfig: 0: %s\n", argv[0]);
    if        (!strcasecmp(argv[0], "include_lua")   && argc == 2) {
        if (LuaIncludeFile) zfree(LuaIncludeFile);
        LuaIncludeFile = zstrdup(argv[1]);
        return 0;
    } else if (!strcasecmp(argv[0], "whitelist_lua") && argc == 2) {
        if (WhiteListLua) zfree(WhiteListLua);
        WhiteListLua = zstrdup(argv[1]);
        return 0;
    } else if (!strcasecmp(argv[0], "luacronfunc")   && argc == 2) {
        if (LuaCronFunc) zfree(LuaCronFunc);
        LuaCronFunc = zstrdup(argv[1]);
        return 0;
    } else if (!strcasecmp(argv[0], "basedir")       && argc == 2) {
        if (Basedir) zfree(Basedir);
        Basedir = zstrdup(argv[1]);
        return 0;
    } else if (!strcasecmp(argv[0], "outputmode")    && argc == 2) {
        if (!strcasecmp(argv[1], "pure_redis")) {
            OutputMode = OUTPUT_PURE_REDIS;
        } else if (!strcasecmp(argv[1],"normal")) {
            OutputMode = OUTPUT_NORMAL;
        } else {
            char *err = "argument must be 'pure_redis' or 'normal'";
            fprintf(stderr, "%s\n", err);
            return -1;
        }
        return 0;
    } else if (!strcasecmp(argv[0], "webserver_mode") && argc == 2) {
        if ((WebServerMode = yesnotoi(argv[1])) == -1) {
            char *err = "argument must be 'yes' or 'no'";
            fprintf(stderr, "%s\n", err);
            return -1;
        }
        return 0;
    } else if (!strcasecmp(argv[0], "webserver_index_function") && argc == 2) {
        if (WebServerIndexFunc) zfree(WebServerIndexFunc);
        WebServerIndexFunc = zstrdup(argv[1]);
        return 0;
    } else if (!strcasecmp(argv[0], "webserver_whitelist_address") &&
                argc == 2) {
        if (!inet_aton(argv[1], &WS_WL_Addr)) {
            fprintf(stderr, "ERR: webserver_whitelist_address: %s\n", argv[1]);
            return -1;
        }
        computeWS_WL_MinMax();
        return 0;
    } else if (!strcasecmp(argv[0], "webserver_whitelist_netmask") &&
                argc == 2) {
        if (!inet_aton(argv[1], &WS_WL_Mask)) {
            fprintf(stderr, "ERR: webserver_whitelist_netmask: %s\n", argv[1]);
            return -1;
        }
        computeWS_WL_MinMax();
        return 0;
    }
    return 1;
}

int   DXDB_processCommand(redisClient *c) { //printf("DXDB_processCommand\n");
    if (HTTP_Mode) {
        if (!strcasecmp(c->argv[0]->ptr, "Connection:")) {
            if (!strcasecmp(c->argv[1]->ptr, "Keep-Alive")) HTTP_KA = 1;
            if (!strcasecmp(c->argv[1]->ptr, "Close"))      HTTP_KA = 0;
        }
        return 1; /* NOTE: this effectively skips HTTP headers */
    }
    Operations++;
    CurrCard       =  0;
    CurrClient     = c;
    NumJTAlias     =  0; // TODO: move to redisClient
    LastJTAmatch   = -1; // TODO: move to redisClient
    LruColInSelect =  0; // TODO: move to redisClient
    HTTP_Mode      =  0; // TODO: move to redisClient
    HTTP_KA        =  0; // TODO: move to redisClient
    sds arg2       = c->argc > 2 ? c->argv[2]->ptr : NULL;
    if (c->argc == 3 && !strcasecmp(c->argv[0]->ptr, "GET") && // HTTP REQ
        (!strcasecmp(arg2, "HTTP/1.0") || !strcasecmp(arg2, "HTTP/1.1"))) {
        if (!strcasecmp(arg2, "HTTP/1.1")) HTTP_KA = 1; /* Default: ON in 1.1 */
        HTTP_Mode    = 1; // TODO: move to redisClient
        char *fname = c->argv[1]->ptr;
        int   fnlen = sdslen(c->argv[1]->ptr);
        if (*fname == '/') { fname++; fnlen--; }
        HTTPFile   = createStringObject(fname, fnlen);// TODO: move2redisClient
        return 1;
    } else return 0;
}

static robj *luaReplyToHTTPReply(lua_State *lua) {
    robj *r = createObject(REDIS_STRING, NULL);
    int   t = lua_type(lua,1);
    switch(t) {
    case LUA_TSTRING:
        r->ptr = sdsnewlen((char*)lua_tostring(lua, 1), lua_strlen(lua, 1));
        break;
    case LUA_TBOOLEAN:
        r->ptr = sdsnewlen(lua_toboolean(lua, 1) ? "1" : "0", 1);
        break;
    case LUA_TNUMBER:
        r->ptr = sdscatprintf(sdsempty(), "%lld", (lolo)lua_tonumber(lua,1));
        break;
    case LUA_TTABLE:
        lua_pushstring(lua, "err"); // check for error
        lua_gettable(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TSTRING) {
            r->ptr = sdscatprintf(sdsempty(), "%s",
                                  (char*)lua_tostring(lua, -1));
            lua_pop(lua,2);
        } else { // check for ok
            lua_pop(lua, 1); lua_pushstring(lua, "ok"); lua_gettable(lua, -2);
            t = lua_type(lua, -1);
            if (t == LUA_TSTRING) {
                r->ptr = sdscatprintf(sdsempty(), "%s\r\n",
                                      (char*)lua_tostring(lua, -1));
                lua_pop(lua, 1);
            } else { // this is a real table
                int j = 1, mbulklen = 0;
                lua_pop(lua, 1); /* Discard the 'ok' field value we popped */
                while(1) {
                    lua_pushnumber(lua, j++); lua_gettable(lua, -2);
                    t = lua_type(lua, -1);
                    if (t == LUA_TNIL) { lua_pop(lua,1); break;
                    } else if (t == LUA_TSTRING) {
                        size_t len;
                        char *s = (char *)lua_tolstring(lua, -1, &len);
                        r->ptr = r->ptr ? sdscatlen(r->ptr, s, len) :
                                           sdsnewlen(s, len);
                        mbulklen++;
                    } else if (t == LUA_TNUMBER) {
                        sds s = r->ptr ? r->ptr : sdsempty();
                        r->ptr = sdscatprintf(s, "%lld",
                                                     (lolo)lua_tonumber(lua,1));
                        mbulklen++;
                    }
                    lua_pop(lua, 1);
                }
            }
            break;
        default:
            r->ptr = sdsempty();
        }
    }
    lua_pop(lua, 1);
    return r;
}

#define SEND_REPLY_FROM_STRING(s)                 \
  { robj *r = createStringObject(s, sdslen(s));   \
    addReply(c, r);                               \
    decrRefCount(r); }

#define SEND_404                                  \
  { sds s = sdsnew("HTTP/1.0 404 Not Found\r\n"); \
  SEND_REPLY_FROM_STRING(s) }

sds create_http_reponse_header(robj *resp) {
    return sdscatprintf(sdsempty(), 
                        "HTTP/1.0 200 OK\r\nContent-length: %ld\r\n%s\r\n",
                         sdslen(resp->ptr), 
                         HTTP_KA ? "Connection: Keep-Alive\r\n": "");
}

bool  DXDB_processInputBuffer_ZeroArgs(redisClient *c) {//HTTP Request End-Delim
    //printf("DXDB_procInputBufr_ZeroArgs: qb_len: %d\n", sdslen(c->querybuf));
    bool ret = 0;
    if (HTTP_Mode) {
        if (!strncasecmp(HTTPFile->ptr, "STATIC/", 7)) {
            robj *o;
            sds f = sdscatlen(sdsempty(), HTTPFile->ptr, sdslen(HTTPFile->ptr));
            sdsfree(HTTPFile->ptr); HTTPFile->ptr = f;
            if      ((o = lookupKeyRead(c->db, HTTPFile)) == NULL) SEND_404
            else if (o->type != REDIS_STRING)                      SEND_404
            else {
                sds s = create_http_reponse_header(o);
                SEND_REPLY_FROM_STRING(s)
                addReply(c, o);
            }
        } else {
            int argc; robj **rargv = NULL;
            sds  file = HTTPFile->ptr;
            if (!sdslen(file)) {
                argc      = 2; //NOTE: rargv[0] is ignored
                rargv     = zmalloc(sizeof(robj *) * argc);
                rargv[1]  = _createStringObject(WebServerIndexFunc);
            } else {
                sds *argv = sdssplitlen(file, strlen(file), "/", 1, &argc);
                if (argc < 1)                                      SEND_404
                rargv     = zmalloc(sizeof(robj *) * (argc + 1));
                for (int i = 0; i < argc; i++) {
                    rargv[i + 1] = createStringObject(argv[i], sdslen(argv[i]));
                }
                argc++; //NOTE: rargv[0] is ignored
            }
            luafunc_call(c, argc, rargv);
            robj *resp = luaReplyToHTTPReply(server.lua);
            sds   s    = create_http_reponse_header(resp);
            SEND_REPLY_FROM_STRING(s)
            for (int i = 1; i < argc; i++) decrRefCount(rargv[i]);
            zfree(rargv);
            addReply(c, resp); decrRefCount(resp);
        }
        decrRefCount(HTTPFile); HTTPFile = NULL;
        if (!HTTP_KA && !sdslen(c->querybuf)) { // not KeepAlive, not Pipelined
            c->flags |= REDIS_CLOSE_AFTER_REPLY;
        }
        ret = 1;
    }
    HTTP_Mode = 0;
    return ret;
}

//TODO webserver_mode, webserver_whitelist_address, webserver_whitelist_netmask, webserver_index_function
int DXDB_configSetCommand(cli *c, robj *o) {
    if (!strcasecmp(c->argv[2]->ptr, "include_lua")) {
        if (LuaIncludeFile) zfree(LuaIncludeFile);
        LuaIncludeFile = zstrdup(o->ptr);
        if (!reloadLua(c)) {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR problem loading lua helper file: %s\r\n", (char *)o->ptr));
            decrRefCount(o);
            return -1;
        }
        return 0;
    } else if (!strcasecmp(c->argv[2]->ptr,"whitelist_lua")) {
        zfree(WhiteListLua);
        WhiteListLua = zstrdup(o->ptr);
        if (!reloadLua()) {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR problem loading lua helper file: %s\r\n", (char *)o->ptr));
            decrRefCount(o);
            return -1;
        }
        return 0;
    } else if (!strcasecmp(c->argv[2]->ptr,"luacronfunc")) {
        zfree(LuaCronFunc);
        LuaCronFunc = zstrdup(o->ptr);
        return 0;
    } else if (!strcasecmp(c->argv[2]->ptr, "outputmode")) {
        if (!strcasecmp(o->ptr, "pure_redis")) {
            OutputMode = OUTPUT_PURE_REDIS;
        } else if (!strcasecmp(o->ptr, "normal")) {
            OutputMode = OUTPUT_NORMAL;
        } else {
            addReplySds(c,sdscatprintf(sdsempty(),
               "-ERR OUTPUTMODE can be [PURE_REDIS|NORMAL] not: %s\r\n",
                (char *)o->ptr));
            decrRefCount(o);
            return -1;
        }
        return 0;
    }
    return 1;
}
unsigned char DXDB_configCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr, "ADD")) {
        if (c->argc != 4) return -1;
        configAddCommand(c);
        return 0;
    }
    return 1;
}
void DXDB_configGetCommand(redisClient *c, char *pattern, int *matches) {
    if (stringmatch(pattern, "include_lua", 0)) {
        addReplyBulkCString(c, "include_lua");
        addReplyBulkCString(c, LuaIncludeFile);
        *matches = *matches + 1;
    }
    if (stringmatch(pattern, "whitelist_lua", 0)) {
        addReplyBulkCString(c, "whitelist_lua");
        addReplyBulkCString(c, WhiteListLua);
        *matches = *matches + 1;
    }
    if (stringmatch(pattern, "luacronfunc", 0)) {
        addReplyBulkCString(c, "luafronfunc");
        addReplyBulkCString(c, LuaCronFunc);
        *matches = *matches + 1;
    }
    if (stringmatch(pattern, "outputmode", 0)) {
        addReplyBulkCString(c, "outputmode");
        if (OREDIS) addReplyBulkCString(c, "pure_redis");
        else        addReplyBulkCString(c, "normal");
        *matches = *matches + 1;
    }
}

int DXDB_rdbSave(FILE *fp) { //printf("DXDB_rdbSave\n");
    for (int tmatch = 0; tmatch < Num_tbls; tmatch++) {
        r_tbl_t *rt = &Tbl[tmatch];
        if (rdbSaveType        (fp, REDIS_BTREE) == -1) return -1;
        if (rdbSaveBT          (fp, rt->btr)     == -1) return -1;
        MATCH_INDICES(tmatch)
        if (matches) {
            for (int i = 0; i < matches; i++) {
                r_ind_t *ri = &Index[inds[i]];
                if (ri->luat) {
                    if (rdbSaveType    (fp, REDIS_LUA_TRIGGER) == -1) return -1;
                    if (rdbSaveLuaTrigger(fp, ri)              == -1) return -1;
                } else {
                    if (rdbSaveType        (fp, REDIS_BTREE) == -1) return -1;
                    if (rdbSaveBT          (fp, ri->btr)     == -1) return -1;
                }
            }
        }
    }
    if (rdbSaveType(fp, REDIS_EOF) == -1) return -1; /* SQL delim REDIS_EOF */
    return 0;
}

int DXDB_rdbLoad(FILE *fp) { //printf("DXDB_rdbLoad\n");
    while (1) {
        int type;
        if ((type = rdbLoadType(fp)) == -1) return -1;
        if (type == REDIS_EOF)              break;    /* SQL delim REDIS_EOF */
        if        (type == REDIS_BTREE) {
            if (!rdbLoadBT(fp))             return -1;
        } else if (type == REDIS_LUA_TRIGGER) {
            if (!rdbLoadLuaTrigger(fp))     return -1;
        }
    }
    rdbLoadFinished();
    return 0;
}

int DXDB_rewriteAppendOnlyFile(FILE *fp) {
    //printf("DXDB_rewriteAppendOnlyFile fp: %p\n", fp);
    for (int tmatch = 0; tmatch < Num_tbls; tmatch++) {
        r_tbl_t *rt = &Tbl[tmatch];
        if (!rt->btr) continue; /* virtual indices have NULLs */
        if (rt->btr->s.btype == BTREE_TABLE) { // Tables dump their indexes
            /* First dump table definition and ALL rows */
            if (!appendOnlyDumpTable(fp, rt->btr, tmatch)) return -1;
            /* then dump Table's Index definitions */
            if (!appendOnlyDumpIndices(fp, tmatch))        return -1;
        }
    }
    return 0;
}

void DXDB_flushdbCommand() {
    for (int tmatch = 0; tmatch < Num_tbls; tmatch++) emptyTable(tmatch);
    Num_tbls = Num_indx = 0;
}

void DBXD_genRedisInfoString(sds info) {
#ifdef REDIS3
    info = sdscat(info,"\r\n");
#endif
    info = sdscatprintf(info,
#ifdef REDIS3
            "# ALCHEMY\r\n"
#endif
            "luafilname:%s\r\n"
            "whitelist_lua:%s\r\n"
            "luacronfunc:%s\r\n"
            "basedir:%s\r\n"
            "outputmode:%s\r\n"
            "webserver_mode:%s\r\n"
            "webserver_index_function:%s\r\n",
             LuaIncludeFile, WhiteListLua, LuaCronFunc, Basedir,
             (OREDIS) ? "pure_redis" : "normal",
             (WebServerMode == -1) ? "no" : "yes",
             WebServerIndexFunc);
}

#define WHITELIST_ERR \
  "module \"whitelist\" must be defined in file: whitelist.lua"

static bool luafunc_call(redisClient *c, int argc, robj **argv) {
    char *fname = argv[1]->ptr;
    if (WebServerMode > 0) {
        lua_getglobal(server.lua, "whitelist");
        if (lua_type(server.lua, -1) != LUA_TTABLE) { //TODO HTTP ERROR
            addReplySds(c,sdsnew(WHITELIST_ERR)); return 1;
        }
        lua_getfield(server.lua, -1, fname);
        if (lua_type(server.lua, -1) != LUA_TFUNCTION) { //TODO HTTP ERROR
            addReplySds(c,sdsnew(WHITELIST_ERR)); return 1;
        }
        lua_replace(server.lua, -2); // removes "whitelist" table from the stack
    } else {
        lua_getglobal(server.lua, fname);
    }
    for (int i = 2; i < argc; i++) {
        sds arg = argv[i]->ptr;
        lua_pushlstring(server.lua, arg, sdslen(arg));
    }

    InternalRequest = 1;
    int ret = lua_pcall(server.lua, (argc - 2), 1, 0);
    InternalRequest = 0;
    if (ret) { //TODO HTTP VERSION
        addReplyErrorFormat(c, "Error running script (%s): %s\n",
            fname, lua_tostring(server.lua,-1));
        lua_pop(server.lua, 1);
        return 1;
    }
    return 0;
}
void luafuncCommand(redisClient *c) {
    if (luafunc_call(c, c->argc, c->argv)) return;
    selectDb(c, server.lua_client->db->id); /* set DB ID from Lua client */
    luaReplyToRedisReply(c, server.lua);
}

extern struct sockaddr_in AcceptedClientSA;
void DXDB_setClientSA(redisClient *c) { c->sa = AcceptedClientSA; }



//TODO put in seperate file ctable.c

//TODO when parsing work is done in these functions
//      it does not need to be done AGAIN (store globally)

/* RULES: for commands in cluster-mode
    1.) INSERT/REPLACE
          CLUSTERED -> OK
          SHARDED
            a.) bulk -> PROHIBITED
            b.) column declaration must include shard-key
    2.) UPDATE
          CLUSTERED -> must have indexed-column
          SHARDED   -> must have shard-key as (w.wf.akey) & be single-lookup
    3.) DELETE
          CLUSTERED -> must have indexed-column
          SHARDED   -> must have shard-key as (w.wf.akey) & be single-lookup
    4.) SIMPLE READS:
        A.) SELECT
              CLUSTERED -> OK
              SHARDED   -> must have shard-key as (w.wf.akey) & be single-lookup
        B.) DSELECT
              CLUSTERED -> OK
              SHARDED   -> must have indexed-column
        C.) SCAN
              CLUSTERED -> OK
              SHARDED   -> PROHIBITED
        D.) DSCAN
              CLUSTERED -> OK
              SHARDED   -> OK
    5.) JOINS: (same rules as #4: SIMPLE READS, w/ following definitions)
          PROHIBITED: 2+ SHARDED & not related via FKs
          SHARDED: A.) Single SHARDED
                   B.) multiple SHARDED w/ FK relation
                   C.) SHARDED + CLUSTERED
          CLUSTERED: 1+ CLUSTERED
*/

static bool Bdum;
sds override_getKeysFromComm(rcommand *cmd, robj **argv, int argc, bool *err) {
    int argt;
    cli              *c    = CurrClient;
    redisCommandProc *proc = cmd->proc;
    if      (proc == sqlSelectCommand || proc == tscanCommand     ) argt = 3;
    else if (proc == insertCommand    || proc == deleteCommand ||
             proc == replaceCommand                               ) argt = 2;
    else /* (proc == updateCommand */                               argt = 1;

    sds tname = argv[argt]->ptr;
    if (proc == sqlSelectCommand || proc == tscanCommand) {
        if (strchr(tname, ',')) { printf("JOIN\n"); //TODO
            int  ts  [MAX_JOIN_INDXS];
            int  jans[MAX_JOIN_INDXS];
            int  numt = 0;
            if (!parseCommaSpaceList(c, argv[3]->ptr, 0, 1, 0, -1, NULL, &numt,
                                     ts, jans, NULL, NULL, &Bdum)) return NULL;
            uint32 n_clstr = 0, n_shrd = 0;
            for (int i = 0; i < numt; i++) {
                r_tbl_t *rt = &Tbl[ts[i]];
                if (rt->sk == -1) n_clstr++;
                else              n_shrd++;
            }
            if (n_shrd > 1) { // check for FK relations

                //TODO parse WC, validate join-chain
                int fk_otmatch[MAX_JOIN_INDXS];
                int fk_ocmatch[MAX_JOIN_INDXS];
                int n_fk = 0;
                for (int i = 0; i < numt; i++) {
                    r_tbl_t *rt = &Tbl[ts[i]];
                    if (rt->sk != -1 && rt->fk_cmatch != -1) {
                        fk_otmatch[n_fk] = rt->fk_otmatch;
                        fk_ocmatch[n_fk] = rt->fk_ocmatch;
                        n_fk++;
                    }
                }
                for (int i = 0; i < n_fk; i++) {
printf("%d: ot: %d oc: %d\n", i, fk_otmatch[i], fk_ocmatch[i]);
                }
            }
printf("n_clstr: %d n_shrd: %d\n", n_clstr, n_shrd);
            return NULL;
        }
    }
    printf("override_getKeysFromComm: table: %s\n", tname);
    int      tmatch = find_table(tname);
    if (tmatch == -1) return NULL;
    r_tbl_t *rt     = &Tbl[tmatch];

    if (rt->sk == -1) { printf("CLUSTERED TABLE\n");
        return NULL;
    } else {                    printf("SHARDED TABLE rt->sk: %d\n", rt->sk);
        if (proc == tscanCommand) {
            *err = 1; addReply(c, shared.scan_sharded); return NULL;
        } else if (proc == insertCommand || proc == replaceCommand) {
            if (argc < 5) {
                *err = 1; addReply(c, shared.insertsyntax); return NULL;
            }
            sds key;
            bool repl = (proc == replaceCommand);
            insertParse(c, argv, repl, tmatch, 1, &key);
            //printf("insertParse: cluster-key: %s\n", *key);
            return key;
        } else if (proc == sqlSelectCommand || proc == deleteCommand ||
                   proc == updateCommand) {
            cswc_t w; wob_t wb;
            init_check_sql_where_clause(&w, tmatch, argv[5]->ptr);
            init_wob(&wb);
            parseWCReply(c, &w, &wb, SQL_SELECT);
            if (w.wtype == SQL_ERR_LKP)   return NULL;
            if (w.wtype == SQL_RANGE_LKP) return NULL;
            if (w.wtype == SQL_IN_LKP   ) return NULL; //TODO evaluate each key
            if (w.wf.cmatch != rt->sk) {
                *err = 1; addReply(c, shared.select_on_sk); return NULL;
            }
            aobj *afk = &w.wf.akey;
            sds   sk  = createSDSFromAobj(afk);
            sds   key  = sdscatprintf(sdsempty(), "%s=%s.%s",
                                         sk, tname, Tbl->col_name[rt->sk]->ptr);
            //printf("sqlSelectCommand: key: %s\n", key);
            return key;
        }
    }
    return NULL;
}


//TODO put in separate file, this is just too many lines of code here
/* SHARED_OBJECTS  SHARED_OBJECTS  SHARED_OBJECTS  SHARED_OBJECTS */
void DXDB_createSharedObjects() {
    shared.singlerow = createObject(REDIS_STRING,sdsnew("*1\r\n"));
    shared.inserted  = createObject(REDIS_STRING,sdsnew("+INSERTED\r\n"));
    shared.upd8ed    = createObject(REDIS_STRING,sdsnew("+UPDATED\r\n"));
    shared.toomanytables = createObject(REDIS_STRING,sdsnew(
        "-ERR MAX tables reached (256)\r\n"));
    shared.missingcolumntype = createObject(REDIS_STRING,sdsnew(
        "-ERR LegacyTable: Column Type Missing\r\n"));
    shared.undefinedcolumntype = createObject(REDIS_STRING,sdsnew(
        "-ERR Column Type Unknown ALCHEMY_DATABASE uses[INT,LONG,FLOAT,TEXT] and recognizes INT=[*INT],LONG=[BIGINT],FLOAT=[FLOAT,REAL,DOUBLE],TEXT=[*CHAR,TEXT,BLOB,BINARY,BYTE]\r\n"));
    shared.columnnametoobig = createObject(REDIS_STRING,sdsnew(
        "-ERR ColumnName too long MAX(64)\r\n"));
    shared.toomanycolumns = createObject(REDIS_STRING,sdsnew(
        "-ERR MAX columns reached(64)\r\n"));
    shared.toofewcolumns = createObject(REDIS_STRING,sdsnew(
        "-ERR Too few columns (min 2)\r\n"));
    shared.nonuniquecolumns = createObject(REDIS_STRING,sdsnew(
        "-ERR Column name defined more than once\r\n"));
    shared.nonuniquetablenames = createObject(REDIS_STRING,sdsnew(
        "-ERR Table name already exists\r\n"));
    shared.toomanyindices = createObject(REDIS_STRING,sdsnew(
        "-ERR MAX indices reached(512)\r\n"));
    shared.nonuniqueindexnames = createObject(REDIS_STRING,sdsnew(
        "-ERR Index name already exists\r\n"));
    shared.indextargetinvalid = createObject(REDIS_STRING,sdsnew(
        "-ERR Index on Tablename.columnname target error\r\n"));
    shared.indexedalready = createObject(REDIS_STRING,sdsnew(
        "-ERR Tablename.Columnname is ALREADY indexed)\r\n"));
    shared.index_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE INDEX' command\r\n"));
    shared.nonuniquekeyname = createObject(REDIS_STRING,sdsnew(
        "-ERR Key name already exists\r\n"));
    shared.trigger_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE TRIGGER' command\r\n"));
    shared.luatrigger_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE LUATRIGGER' command\r\n"));

    shared.nonexistenttable = createObject(REDIS_STRING,sdsnew(
        "-ERR Table does not exist\r\n"));
    shared.insertcolumn = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (1234,'abc',,,)\r\n"));
    shared.insert_ovrwrt = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT on Existing Data - use REPLACE\r\n"));

    shared.uint_pkbig = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: PK greater than UINT_MAX(4GB)\r\n"));
    shared.col_uint_string_too_long = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: UINT Column longer than 32 bytes\r\n"));
    shared.u2big = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: UINT Column greater than UINT_MAX(4GB)\r\n"));
    shared.col_float_string_too_long = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: FLOAT Column longer than 32 bytes\r\n"));

    shared.columntoolarge = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT - MAX column size is 1GB\r\n"));
    shared.nonexistentcolumn = createObject(REDIS_STRING,sdsnew(
        "-ERR Column does not exist\r\n"));
    shared.nonexistentindex = createObject(REDIS_STRING,sdsnew(
        "-ERR Index does not exist\r\n"));
    shared.drop_virtual_index = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Primary Key Indices can not be dropped\r\n"));
    shared.drop_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: LRU Indices can not be dropped\r\n"));
    shared.drop_ind_on_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Index on SHARDKEY can not be dropped\r\n"));
    shared.drop_luatrigger = createObject(REDIS_STRING,sdsnew(
        "-ERR TARGET: DROP LUATRIGGER on wrong object\r\n"));
    shared.badindexedcolumnsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: JOIN WHERE tablename.columname ...\r\n"));
    shared.index_nonrel_decl_fmt = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE TRIGGER name ON table ADD_CMD [DEL_CMD] - syntax for CMD: [INSERT,SET,GET,etc...] $col_name text = '$' is used for variable substitution of columns\r\n"));
    shared.luat_decl_fmt = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LUATRIGGER name ON table ADD_FUNC [DEL_FUNC]\r\n"));
    shared.luat_c_decl = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LUATRIGGER ... ADD_FUNC can ONLY contain column names and commas, e.g. \"luafunc(col1, col2, col3)\"\r\n"));

    shared.invalidupdatestring = createObject(REDIS_STRING,sdsnew(
        "-ERR UPDATE: string error, syntax is col1=val1,col2=val2,....\r\n"));
    shared.invalidrange = createObject(REDIS_STRING,sdsnew(
        "-ERR RANGE: Invalid range\r\n"));

    shared.toomany_nob = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY columns MAX = 16\r\n"));

    shared.mci_on_pk = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Compound Indexes can NOT be on PrimaryKey\r\n"));
    shared.UI_SC = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX must be a Compound Index - e.g. ON (fk1, fk2)\r\n"));
    shared.two_uniq_mci = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: only ONE UNIQUE INDEX per table\r\n"));
    shared.uniq_mci_notint = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX final column must be INT\r\n"));
    shared.uniq_mci_pk_notint = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX Primary Key must be INT\r\n"));

    shared.accesstypeunknown = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT ... WHERE x IN ([SELECT|SCAN])\r\n"));

    shared.createsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: \"CREATE TABLE tablename (columnname type,,,,)\" OR \"CREATE INDEX indexname ON tablename (columnname)\" OR \"CREATE LRUINDEX ON tablename\" OR \"CREATE LUATRIGGER luatriggername ON tablename ADD_RPC_CALL DEL_RPC_CALL\"\r\n"));
    shared.dropsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DROP TABLE tablename OR DROP INDEX indexname OR DROP LUATRIGGER\r\n"));
    shared.altersyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: ALTER TABLE tablename ADD [COLUMN columname columntype[INT,LONG,FLOAT,TEXT]] [SHARDKEY columname] [FOREIGN KEY (fk_name) REFERENCES othertable (other_table_indexed_column)]\r\n"));
    shared.alter_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: ALTER TABLE tablename ADD COLUMN columname columntype - CAN NOT be done on OPTIMISED 2 COLUMN TABLES\r\n"));
    shared.lru_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LRUINDEX ON tablename - CAN NOT be done on OPTIMISED 2 COLUMN TABLES\r\n"));
    shared.lru_repeat = createObject(REDIS_STRING,sdsnew(
        "-ERR LOGIC: LRUINDEX already exists on this table\r\n"));
    shared.col_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR KEYWORD: LRU is a keyword, can not be used as a columnname\r\n"));
    shared.update_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: LRU column can not be DIRECTLY UPDATED\r\n"));
    shared.insert_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: INSERT of LRU column DIRECTLY not kosher\r\n"));
    shared.insert_replace_update = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED:  REPLACE INTO tbl ... ON DUPLICATE KEY UPDATE\r\n"));

    shared.insertsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,)\r\n"));
    shared.insertsyntax_no_into = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,) - \"INTO\" keyword MISSING\r\n"));
    shared.insertsyntax_no_values = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,) - \"VALUES\" keyword MISSING\r\n"));
    shared.part_insert_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO table with 2 values, both values must be specified - these tables are optimised and stored inside the BTREE and MUST have ALL values defined\r\n"));

    shared.key_query_mustbe_eq = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT WHERE fk != 4 - primary key or index lookup must use EQUALS (=) - OR use \"SCAN\"\r\n"));

    shared.whereclause_in_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col IN (...) - \"IN\" requires () delimited list\r\n"));
    shared.where_in_select = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col IN ($SELECT col ....) INNER SELECT SYNTAX ERROR \r\n"));
    shared.whereclause_between = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col BETWEEN x AND y\r\n"));

    shared.wc_orderby_no_by = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col - \"BY\" MISSING\r\n"));
    shared.order_by_col_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: ORDER BY columname - column does not exist\r\n"));
    shared.oby_lim_needs_num = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col [DESC] LIMIT N = \"N\" MISSING\r\n"));
    shared.oby_ofst_needs_num = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col [DESC] LIMIT N OFFSET M = \"M\" MISSING\r\n"));
    shared.orderby_count = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT COUNT(*) ... WHERE ... ORDER BY col - \"ORDER BY\" and \"COUNT(*)\" dont mix, drop the \"ORDER BY\"\r\n"));

    shared.selectsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT [col,,,,] FROM tablename WHERE [[indexed_column = val]|| [indexed_column BETWEEN x AND y] || [indexed_column IN (X,Y,Z,...)] || [indexed_column IN ($redis_statment)]] [ORDER BY [col [DESC/ASC]*,,] LIMIT n OFFSET m]\r\n"));
    shared.selectsyntax_nofrom = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT col,,,, FROM tablename WHERE indexed_column = val - \"FROM\" keyword MISSING\r\n"));
    shared.selectsyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT col,,,, FROM tablename WHERE indexed_column = val - \"WHERE\" keyword MISSING\r\n"));
    shared.rangequery_index_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - indexed_column either non-existent or not indexed\r\n"));

    shared.deletesyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DELETE FROM tablename WHERE indexed_column = val || WHERE indexed_column BETWEEN x AND y\r\n"));
    shared.deletesyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DELETE FROM tablename WHERE indexed_column = val - \"WHERE\" keyword MISSING\r\n"));

    shared.updatesyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE tablename SET col1=val1,col2=val2,,,, WHERE indexed_column = val\r\n"));
    shared.updatesyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE tablename SET col1=val1,col2=val2,,,, WHERE indexed_column = val \"WHERE\" keyword MISSING\r\n"));
    shared.update_pk_range_query = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE of PK not allowed with Range Query\r\n"));
    shared.update_pk_overwrite = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE of PK would overwrite existing row - disallowed\r\n"));
    shared.update_expr = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: parse error\r\n"));
    shared.update_expr_col = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: SYNTAX: 'columname OP value' - OP=[+-*/%||] value=[columname,integer,float]\r\n"));
    shared.update_expr_div_0 = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression evaluation resulted in divide by zero\r\n"));
    shared.update_expr_mod = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: MODULO only possible on INT columns \r\n"));
    shared.update_expr_cat = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: String Concatenation only possible on TEXT columns \r\n"));
    shared.update_expr_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: TEXT columns do not support [+-*/%^] Operations\r\n"));
    shared.update_expr_empty_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: Concatenating Empty String\r\n"));
    shared.update_expr_math_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: INT or FLOAT columns can not be set to STRINGS\r\n"));
    shared.update_expr_col_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: SYNTAX: Left Hand Side column operations not possible on other columns in table\r\n"));
    shared.update_expr_float_overflow = createObject(REDIS_STRING,sdsnew(
        "-ERR MATH: UPDATE expression: Floating point arithmetic produced overflow or underflow [FLT_MIN,FLT_MAX]\r\n"));
    shared.up_on_mt_col = createObject(REDIS_STRING,sdsnew(
        "-ERR LOGIC: UPDATE expression against an empty COLUMN - behavior undefined\r\n"));
    shared.neg_on_uint = createObject(REDIS_STRING,sdsnew(
        "-ERR MATH: UPDATE expression: NEGATIVE value against UNSIGNED [INT,LONG]\r\n"));

    shared.wc_col_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - Column does not exist\r\n"));
    shared.whereclause_col_not_indxd = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - Column must be indexed\r\n"));
    shared.whereclause_no_and = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE-CLAUSE: WHERE indexed_column BETWEEN start AND finish - \"AND\" MISSING\r\n"));

    shared.scansyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SCAN col,,,, FROM tablename [WHERE [indexed_column = val]|| [indexed_column BETWEEN x AND y] [ORDER BY col LIMIT num offset] ]\r\n"));
    shared.cr8tbl_scan = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE TABLE AS SCAN not yet supported\r\n"));

    shared.toofewindicesinjoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: Too few indexed columns in join(min=2)\r\n"));
    shared.toomanyindicesinjoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: MAX indices in JOIN reached(64)\r\n"));
    shared.joincolumnlisterror = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: error in columnlist (select columns)\r\n"));
    shared.join_order_by_tbl = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - table does not exist\r\n"));
    shared.join_order_by_col = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - column does not exist\r\n"));
    shared.join_table_not_in_query = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - table not in SELECT *\r\n"));
    shared.joinsyntax_no_tablename = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT tbl.col,,,, FROM tbl1,tbl2 WHERE tbl1.indexed_column = tbl2.indexed_column AND tbl1.indexed_column BETWEEN x AND y - MISSING table-name in WhereClause\r\n"));
    shared.join_chain = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: TABLES in WHERE statement must form a chain to be a star schema (e.g. WHERE t1.pk = 3 AND t1.fk = t2.pk AND t2.pk = t3.pk AND t3.fk = t4.pk [tables were chained {t1,t2,t3,t4 each joined to one another}]\r\n"));
    shared.fulltablejoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: Join has a full table scan in it (maybe in the middle) - this is not supported via SELECT - use \"SCAN\"\r\n"));
    shared.joindanglingfilter = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: relationship not joined (i.e. a.x = 5 and a is not joined)\r\n"));
    shared.join_noteq = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: table joins only possible via the EQUALS operator (e.g. t1.fk = t2.fk2 ... not t1.fk < t2.fk2)\r\n"));
    shared.join_coltypediff = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: column types of joined columns do not match\r\n"));
    shared.join_col_not_indexed = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: joined column IS not indexed - USE \"SCAN\"\r\n"));
    shared.join_qo_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: query optimiser could not find a join plan\r\n"));


    shared.create_table_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE TABLE tablename (col INT,,,,,,) [SELECT ....]\r\n"));

    shared.create_table_as_count = createObject(REDIS_STRING,sdsnew(
        "-ERR TYPE: CREATE TABLE tbl AS SELECT COUNT(*) - is disallowed\r\n"));

    shared.dump_syntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DUMP tablename [TO MYSQL [mysqltablename]],[TO FILE fname]\r\n"));
    shared.show_syntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SHOW [TABLES|INDEXES]\r\n"));

    shared.alter_sk_rpt = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: TABLE can only have 1 shardkey\r\n"));
    shared.alter_sk_no_i = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: must be Indexed\r\n"));
    shared.alter_sk_no_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: can not be on LRU column\r\n"));
    shared.alter_fk_not_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR ALTER TABLE ADD FOREIGN KEY: must point from this table's shard-key to the foreign table's shard-key\r\n"));
    shared.alter_fk_repeat = createObject(REDIS_STRING,sdsnew(
        "-ERR ALTER TABLE ADD FOREIGN KEY: table already has foreign key, drop foreign key first to redefine ... and caution if your data is already distributed\r\n"));

    shared.select_on_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: NOT ON SHARDKEY\r\n"));
    shared.scan_sharded = createObject(REDIS_STRING,sdsnew(
        "-ERR SCAN: PROHIBITED on SHARDED TABLE -> USE \"DSCAN\"\r\n"));
}

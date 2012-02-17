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
char *strcasestr(const char *haystack, const char *needle); /*compiler warning*/

#include "xdb_hooks.h"

#include "redis.h"
#include "adlist.h"

#include "debug.h"
#include "parser.h"
#include "alsosql.h"
#include "webserver.h"

// PROTOTYPES
static robj *luaReplyToHTTPReply(lua_State *lua);
// from scripting.c
void luaPushError(lua_State *lua, char *error);
void luaMaskCountHook(lua_State *lua, lua_Debug *ar);
// from networking.c
void addReplyString(redisClient *c, char *s, size_t len);


typedef struct two_sds {
  sds a;
  sds b;
} two_sds;
static two_sds *init_two_sds(sds a, sds b) {
    two_sds *ss = malloc(sizeof(two_sds));
    ss->a       = sdsdup(a);
    ss->b       = sdsdup(b);
    return ss;
}
static void free_two_sds(void *v) {
    if (!v) return;
    two_sds *ss = (two_sds *)v;
    sdsfree(ss->a); sdsfree(ss->b); free(ss);
}


static sds send_http_response_header_extended(cli *c, sds s) {
    if (c->http.resp_hdr) {
        listNode *ln;
        listIter *li = listGetIterator(c->http.resp_hdr, AL_START_HEAD);
        while((ln = listNext(li))) {// POPULATE Lua Global HTTP_HEADER[]
            two_sds *ss = ln->value;
            s = sdscatprintf(s, "%s: %s\r\n", ss->a, ss->b);
         } listReleaseIterator(li);
    }
    return sdscatlen(s, "\r\n", 2);
}
static sds send_http200_reponse_header(cli *c, long bodylen) {
    sds s = sdscatprintf(sdsempty(), 
                        "HTTP/1.%d 200 OK\r\nContent-length: %ld\r\n%s",
                         c->http.proto_1_1 ? 1 : 0, bodylen,
                         c->http.ka ? "Connection: Keep-Alive\r\n": "");
    return send_http_response_header_extended(c, s);
}
static sds send_http302_reponse_header(cli *c) {
    sds s = sdscatprintf(sdsempty(), 
               "HTTP/1.%d 302 Found\r\nContent-Length: 0\r\nLocation: %s\r\n%s",
                         c->http.proto_1_1 ? 1 : 0,
                         c->http.redir,
                         c->http.ka ? "Connection: Keep-Alive\r\n": "");
    sds s2 = send_http_response_header_extended(c, s);
    sdsfree(c->http.redir); c->http.redir = NULL; // DESTROYED 079
    return s2;
}
static sds send_http304_reponse_header(cli *c) {
    sds s = sdscatprintf(sdsempty(), 
               "HTTP/1.%d 304 Not Modified\r\nContent-Length: 0\r\n%s",
                         c->http.proto_1_1 ? 1 : 0,
                         c->http.ka ? "Connection: Keep-Alive\r\n": "");
    sds s2 = send_http_response_header_extended(c, s);
    sdsfree(c->http.redir); c->http.redir = NULL; // DESTROYED 079
    return s2;
}
static sds send_http404_reponse_header(cli *c, long bodylen) {
    sds s = sdscatprintf(sdsempty(), 
                        "HTTP/1.%d 404 Not Found\r\nContent-length: %ld\r\n%s",
                         c->http.proto_1_1 ? 1 : 0, bodylen,
                         c->http.ka ? "Connection: Keep-Alive\r\n": "");
    return send_http_response_header_extended(c, s);
}
static sds send_http_reponse_header(cli *c, long bodylen) {
  return (c->http.retcode == 200) ? send_http200_reponse_header(c, bodylen) :
         (c->http.retcode == 302) ? send_http302_reponse_header(c)          :
                          /* 304 */ send_http304_reponse_header(c);
}
#define SEND_REPLY_FROM_STRING(s)               \
  { robj *r = createStringObject(s, sdslen(s)); \
    addReply(c, r); decrRefCount(r); }
#define SEND_404                                                               \
  { sds s = send_http404_reponse_header(c, 0); SEND_REPLY_FROM_STRING(s) }
#define SEND_405                                                               \
  { sds s = c->http.proto_1_1 ?                                                \
      sdsnew("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n") : \
      sdsnew("HTTP/1.0 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n");  \
  SEND_REPLY_FROM_STRING(s) }

// HTTP_LUA_COMMANDS HTTP_LUA_COMMANDS HTTP_LUA_COMMANDS HTTP_LUA_COMMANDS
static void addHttpResponseHeader(sds name, sds value) {
    if (!server.alc.CurrClient->http.resp_hdr) {
        server.alc.CurrClient->http.resp_hdr       = listCreate(); 
        server.alc.CurrClient->http.resp_hdr->free = free_two_sds;
    }
    two_sds *ss = init_two_sds(name, value);
    // Store RESP Headers in List
    listAddNodeTail(server.alc.CurrClient->http.resp_hdr, ss);
}
int luaSetHttpResponseHeaderCommand(lua_State *lua) {
    int argc = lua_gettop(lua);
    if (argc != 2 || !lua_isstring(lua, 1) || !lua_isstring(lua, 2)) {
        luaPushError(lua, "Lua SetHttpResponseHeader() takes 2 string args");
        return 1;
    }
    sds a    = sdsnew(lua_tostring(lua, 1));
    sds b    = sdsnew(lua_tostring(lua, 2));
    addHttpResponseHeader(a, b);
    return 0;
}
int luaSetHttpRedirectCommand(lua_State *lua) {
    int argc = lua_gettop(lua);
    if (argc != 1 || !lua_isstring(lua, 1)) {
        luaPushError(lua, "Lua SetHttpRedirect() takes 1 string arg"); return 1;
    }
    server.alc.CurrClient->http.retcode = 302;
    server.alc.CurrClient->http.redir   = sdsnew(lua_tostring(lua, 1));//FREE079
    return 0;
}
int luaSetHttp304Command(lua_State *lua) {
    int argc = lua_gettop(lua);
    if (argc != 0) {
        luaPushError(lua, "Lua SetHttp304() takes ZERO args"); return 1;
    }
    server.alc.CurrClient->http.retcode = 304;
    return 0;
}

// HTTP_REQ_RESP_SESSION HTTP_REQ_RESP_SESSION HTTP_REQ_RESP_SESSION
int start_http_session(cli *c) {
    if      (!strcasecmp(c->argv[2]->ptr, "HTTP/1.1")) {
        c->http.proto_1_1 = 1;
        c->http.ka        = 1; // Default: keep-alive -> ON in HTTP 1.1
    }
    if      (!strcasecmp(c->argv[0]->ptr, "GET"))      c->http.get  = 1;
    else if (!strcasecmp(c->argv[0]->ptr, "POST"))     c->http.post = 1;
    else if (!strcasecmp(c->argv[0]->ptr, "HEAD"))     c->http.head = 1;
    else                                               { SEND_405; return 1; }
    c->http.mode     = HTTP_MODE_ON;
    if (c->http.resp_hdr) listRelease(c->http.resp_hdr);
    c->http.resp_hdr = NULL;
    if (c->http.req_hdr) listRelease(c->http.req_hdr);
    c->http.req_hdr  = listCreate(); c->http.req_hdr->free = free_two_sds;
    char *fname      = c->argv[1]->ptr;
    int   fnlen      = sdslen(c->argv[1]->ptr);
    if (*fname == '/') { fname++; fnlen--; }
    c->http.file     = createStringObject(fname, fnlen); //TODO use sds
    return 1;
}

int continue_http_session(cli *c) {
    if (c->argc < 2) return 1; // IGNORE Headers w/ no values
    if (!strcasecmp(c->argv[0]->ptr, "Connection:")) {
        if (!strcasecmp(c->argv[1]->ptr, "Keep-Alive")) c->http.ka = 1;
        if (!strcasecmp(c->argv[1]->ptr, "Close"))      c->http.ka = 0;
    }
    if (!strcasecmp(c->argv[0]->ptr, "Content-Length:")) {
      c->http.req_clen = atoi(c->argv[1]->ptr);
    }
    sds hval = sdsdup(c->argv[1]->ptr);                  // DESTROY ME 084
    for (int i = 2; i < c->argc; i++) {
        hval = sdscatlen(hval, " ", 1); //TODO this assumes single space
        hval = sdscatlen(hval, c->argv[i]->ptr, sdslen(c->argv[i]->ptr));
    }
    two_sds *ss = init_two_sds(c->argv[0]->ptr, hval);
    sdsfree(hval);                                       // DESTROYED 084
    listAddNodeTail(c->http.req_hdr, ss); // Store REQ Headers in List
    return 1;
}

static void sendStaticFileReply(cli *c) {
    robj *o;
    if ((o = lookupKeyRead(c->db, c->http.file)) == NULL) SEND_404
    else if (o->type != REDIS_STRING)                     SEND_404
    else { //NOTE: STATIC expire in 10 years (HARDCODED)
        listNode *ln;
        bool      dfl = 0;
        listIter *li  = listGetIterator(c->http.req_hdr, AL_START_HEAD);
        while((ln = listNext(li))) { // check for "deflate"
            two_sds *ss = ln->value;
            if (!strncasecmp(ss->a, "Accept-Encoding", 15)) {
                if (strcasestr(ss->b, "deflate")) { dfl = 1; break; }
            }
        } listReleaseIterator(li);
        if (dfl) {
            robj *dfile = _createStringObject("DEFLATE/");
            dfile->ptr  = sdscatlen(dfile->ptr, c->http.file->ptr,
                                                sdslen(c->http.file->ptr));
            robj *od;
            if ((od = lookupKeyRead(c->db, dfile)) && od->type == REDIS_STRING){
                o = od;
                addHttpResponseHeader(sdsnew("Content-Encoding"),
                                      sdsnew("deflate"));
            }
        }
        addHttpResponseHeader(sdsnew("Expires"),
                              sdsnew("Wed, 09 Jun 2021 10:18:14 GMT;"));
        SEND_REPLY_FROM_STRING(send_http200_reponse_header(c, sdslen(o->ptr)));
        addReply(c, o);
    }
}
static void sendLuaFuncReply(cli *c, sds file) {
    int argc; robj **rargv = NULL;
    if (!sdslen(file) || !strcmp(file, "/")) {
        argc      = 2; //NOTE: rargv[0] is ignored
        rargv     = zmalloc(sizeof(robj *) * argc);
        rargv[1]  = _createStringObject(server.alc.WebServerIndexFunc);
    } else if (c->http.post && c->http.req_clen) {
        int  urgc;
        sds *urgv = sdssplitlen(file, sdslen(file), "/", 1, &urgc);
        sds  pb   = c->http.post_body;
        sds *argv = sdssplitlen(pb, sdslen(pb), "&", 1, &argc);
        rargv     = zmalloc(sizeof(robj *) * (argc + urgc + 1));
        for (int i = 0; i < urgc; i++) {
            rargv[i + 1] = createStringObject(urgv[i], sdslen(urgv[i]));
        }
        for (int i = 0; i < argc; i++) {
            char *x = strchr(argv[i], '=');
            if (!x) continue; x++;
            rargv[i + urgc + 1] = createStringObject(x, strlen(x));
        }
        argc += (urgc + 1); //NOTE: rargv[0] is ignored
        zfree(urgv); zfree(argv);
    } else {
        sds *argv = sdssplitlen(file, sdslen(file), "/", 1, &argc);
        rargv     = zmalloc(sizeof(robj *) * (argc + 1));
        for (int i = 0; i < argc; i++) {
            rargv[i + 1] = createStringObject(argv[i], sdslen(argv[i]));
        }
        argc++; //NOTE: rargv[0] is ignored
        zfree(argv);
    }
    if (!luafunc_call(c, argc, rargv)) {
        robj *resp = luaReplyToHTTPReply(server.lua);
        SEND_REPLY_FROM_STRING(send_http_reponse_header(c, sdslen(resp->ptr)));
        if (c->http.get || c->http.post) addReply(c, resp);
        decrRefCount(resp);
    }
    for (int i = 1; i < argc; i++) decrRefCount(rargv[i]);
    zfree(rargv);
}
static bool sendRestAPIReply(cli *c, sds file) { //printf("sendRestAPIReply\n");
    int argc; bool ret = 0;
    sds pb  = c->http.post_body; //TODO cat [file,pb] is too much copying
    sds url = pb ? sdscatprintf(sdsempty(), "%s%s", file, pb) :
                   sdsdup(file);                                     //FREE 156
    sds       *argv  = sdssplitlen(url, sdslen(url), "/", 1, &argc); //FREE 157
    rcommand  *cmd   = lookupCommand(argv[0]);
    if (!cmd) goto send_rest_end;
    ret = 1;
    //printf("sendRestAPIReply: found cmd: %s\n", cmd->name);
    if ((cmd->arity > 0 && cmd->arity != argc) || (argc < -cmd->arity)) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            cmd->name);
        goto send_rest_end;
    }
    //NOTE: rest is copy of redis.c processCommand()
    if (server.maxmemory) freeMemoryIfNeeded();
    if (server.maxmemory && (cmd->flags & REDIS_CMD_DENYOOM) &&
        zmalloc_used_memory() > server.maxmemory) {
        addReplyError(c, "command not allowed when used memory > 'maxmemory'");
        goto send_rest_end;
    }
    if ((dictSize(c->pubsub_channels) > 0 || listLength(c->pubsub_patterns) > 0)
        &&
        cmd->proc != subscribeCommand && cmd->proc != unsubscribeCommand &&
        cmd->proc != psubscribeCommand && cmd->proc != punsubscribeCommand) {
        addReplyError(c, 
           "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
        goto send_rest_end;
    }
    if (server.masterhost && server.replstate != REDIS_REPL_CONNECTED &&
        server.repl_serve_stale_data == 0 &&
        cmd->proc != infoCommand && cmd->proc != slaveofCommand) {
        addReplyError(c,
            "link with MASTER is down and slave-serve-stale-data is set to no");
        goto send_rest_end;
    }
    if (server.loading && cmd->proc != infoCommand) {
        addReply(c, shared.loadingerr);
        goto send_rest_end;
    }
    if (c->flags & REDIS_MULTI &&
        cmd->proc != execCommand && cmd->proc != discardCommand &&
        cmd->proc != multiCommand && cmd->proc != watchCommand) {
        queueMultiCommand(c, cmd); addReply(c, shared.queued);
        goto send_rest_end;
    }
    listNode *ln; listIter *li; cli *restc = server.alc.RestClient;
    // 1.) call() cmd in RestClient
    { // REST CLIENT call
        robj **rargv = zmalloc(sizeof(robj *) * argc);
        for (int i = 0; i < argc; i++) {
            rargv[i] = createStringObject(argv[i], sdslen(argv[i]));
        }
        restc->argc = argc; restc->argv = rargv; call(restc, cmd);
        for (int i = 0; i < argc; i++) decrRefCount(rargv[i]);
        zfree(rargv);
    }
    // 2.) calculate Content-Length from RestClient's response
    ulong brlen = restc->bufpos; ulong trlen = brlen;
    if (restc->reply->len) {
        li = listGetIterator(restc->reply, AL_START_HEAD);
        while((ln = listNext(li))) {
            robj *r = ln->value; trlen += sdslen(r->ptr);
        } listReleaseIterator(li);
    }
    bool err = brlen && (*restc->buf == '-');
    //TODO check for "+OK" and return 201 w/ no body
    // 3.) create header w/ Content-Length
    sds   s  = err ? send_http404_reponse_header(c, trlen) :
                     send_http200_reponse_header(c, trlen);
    robj *ho = createObject(REDIS_STRING, s);
    addReply(c, ho);
    // 4.) tack on RestClient's response as HTTP Body
    if (brlen) { addReplyString(c, restc->buf, brlen); }
    if (restc->reply->len) {
        li = listGetIterator(restc->reply, AL_START_HEAD);
        while((ln = listNext(li))) {
            robj *r = ln->value; addReply(c, r);
        } listReleaseIterator(li);
    }
    // 5.) reset RestClient
    restc->bufpos = 0;
    while (restc->reply->len) {
        listDelNode(restc->reply, listFirst(restc->reply));
    }

send_rest_end:
    sdsfree(url); zfree(argv); return ret;               // FREED 156 & 157
}
void end_http_session(cli *c) {
    if (c->http.get || c->http.post || c->http.head) {
        bool ws = (server.alc.WebServerMode != -1);
        bool rs = (server.alc.RestAPIMode   != -1);
        if (!ws && !rs) addReply(c, shared.http_not_on);
        else {
            sds  file = c->http.file->ptr;
            bool done = rs ? sendRestAPIReply(c, file) : 0;
            if (!done) {
                if (!strncasecmp(file, "STATIC/", 7)) sendStaticFileReply(c);
                else                                  sendLuaFuncReply(c, file);
            }
        }
        if (c->http.post_body) sdsfree(c->http.post_body);
    }
    if (c->http.file) { decrRefCount(c->http.file); c->http.file = NULL; }
    if (!c->http.ka && !sdslen(c->querybuf)) { // not KeepAlive, not Pipelined
        c->flags |= REDIS_CLOSE_AFTER_REPLY;
    }
}

// LUA_SCRIPT_TO_HTTP_RESPONSE LUA_SCRIPT_TO_HTTP_RESPONSE
static robj *luaReplyToHTTPReply(lua_State *lua) {
    robj *r = createObject(REDIS_STRING, NULL);
    int   t = lua_type(lua, 1);
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

#define LUA_FUNCTION_REDIS_ERR \
  addReplySds(c, sdsnew("-ERR LUA function not defined\r\n"));

bool luafunc_call(redisClient *c, int argc, robj **argv) {
    sds fname;
    if (server.alc.WebServerMode == -1) {
        fname = sdsdup(argv[1]->ptr);
    } else {
        if (isWhiteListedIp(c)) {
            if (c->http.mode == HTTP_MODE_ON) {
                fname = sdscatprintf(sdsempty(), "WL_%s", (char *)argv[1]->ptr);
            } else {
                fname = sdsdup(argv[1]->ptr);
            }
        } else {
            fname = sdscatprintf(sdsempty(), "WL_%s", (char *)argv[1]->ptr);
        }
    } //printf("luafunc_call: fname: %s\n", fname);

    lua_getglobal(server.lua, fname);
    sdsfree(fname);
    int type = lua_type(server.lua, -1);
    if (type != LUA_TFUNCTION) {
        lua_pop(server.lua, 1);
        if (c->http.mode == HTTP_MODE_ON) SEND_404
        else                              LUA_FUNCTION_REDIS_ERR
        return 1;
    }
    for (int i = 2; i < argc; i++) {
        sds arg = argv[i]->ptr; lua_pushlstring(server.lua, arg, sdslen(arg));
    }

    // POPULATE Lua Global HTTP Vars
    if (server.alc.WebServerMode > 0 && c->http.req_hdr) {
        listNode *ln;
        bool      hascook = 0;
        lua_newtable(server.lua);
        int       top  = lua_gettop(server.lua);
        listIter *li   = listGetIterator(c->http.req_hdr, AL_START_HEAD);
        while((ln = listNext(li))) {// POPULATE Lua Global HTTP_HEADER[]
            two_sds *ss = ln->value;
            char *cln = strchr(ss->a, ':');
            if (cln) { // no colon -> IGNORE, dont include colon
                *cln = '\0';
                if (!strcasecmp(ss->a, "cookie")) hascook = 1;
                lua_pushstring(server.lua, ss->a);
                lua_pushstring(server.lua, ss->b);
                lua_settable(server.lua, top);
            }
        } listReleaseIterator(li);
        lua_setglobal(server.lua, "HTTP_HEADER");
        lua_newtable(server.lua);
        if (hascook) { // POPULATE Lua Global COOKIE[]
            top   = lua_gettop(server.lua);
            li    = listGetIterator(c->http.req_hdr, AL_START_HEAD);
            while((ln = listNext(li))) {
                two_sds *ss = ln->value;
                if (!strcasecmp(ss->a, "cookie")) {
                    char *eq, *cookie = ss->b;
                    while ((eq = strchr(cookie, '='))) {
                        *eq = '\0'; eq++;       
                        lua_pushstring(server.lua, cookie);
                        char *cln = strchr(eq, ';');
                        if (cln) *cln = '\0';
                        lua_pushstring(server.lua, eq);
                        lua_settable(server.lua, top);
                        if (!cln) break;
                        cookie = cln + 1; SKIP_SPACES(cookie);
                    }
                }
            } listReleaseIterator(li);
        }
        lua_setglobal(server.lua, "COOKIE");
    } else { // Make empty tables, to not break lua scripts
        lua_newtable(server.lua); lua_setglobal(server.lua, "HTTP_HEADER");
        lua_newtable(server.lua); lua_setglobal(server.lua, "COOKIE");
    }

    // Set hook to stop script execution if it takes too long
    // NOTE: hooks degrade performance
    if (server.lua_time_limit > 0) {
        lua_sethook(server.lua, luaMaskCountHook, LUA_MASKCOUNT, 100000);
        server.lua_time_start = ustime() / 1000;
    } else {
        lua_sethook(server.lua,luaMaskCountHook, 0, 0);
    }

    /* Select the right DB in the context of the Lua client */
    selectDb(server.lua_client, c->db->id);
    c->InternalRequest = 1;
    int ret = DXDB_lua_pcall(server.lua, (argc - 2), 1, 0);
    c->InternalRequest = 0;
    selectDb(c, server.lua_client->db->id); /* set DB ID from Lua client */
    if (ret) {
        sds err = sdscatprintf(sdsempty(), "Error running script (%s): %s\n",
                                           fname, lua_tostring(server.lua, -1));
        if (c->http.mode == HTTP_MODE_ON) {
            SEND_REPLY_FROM_STRING(send_http200_reponse_header(c, sdslen(err)));
            SEND_REPLY_FROM_STRING(err); sdsfree(err);
        } else {
            addReplyErrorFormat(c, "%s", err);
        }
    }
    CLEAR_LUA_STACK return ret ? 1 : 0;
}

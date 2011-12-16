/*
 * This file implements the API for embedding ALCHEMY_DATABASE
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

#include "xdb_client_hooks.h"
#include "xdb_hooks.h"
#include "query.h"
#include "find.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "embed.h"

#include "hiredis.h"
#include "sds.h"
#include "redis.h"

extern r_tbl_t *Tbl; extern r_ind_t *Index;
extern uchar    OutputMode;

// PROTOTYPES
redisClient *createClient(int fd);              // from networking.c
int initEmbedded();                             // from redis.c (-DNO_MAIN)
redisContext *redisContextInit();               // from hiredis.c
void __redisCreateReplyReader(redisContext *c); // from hiredis.c

// GLOBALS
char        *ConfigFile      = NULL;
redisClient *EmbeddedCli     = NULL;
eresp_t     *CurrEresp       = NULL;
static int   NumSelectedCols = 0;
static sds  *SelectedCols    = NULL;

void SetConfig(char *file) { ConfigFile = file; }

static inline void initEmbeddedClient() {
    if (!EmbeddedCli) { //printf("initEmbeddedClient\n");
        EmbeddedCli         = createClient(-1);  /* no fd -> embedded */
        EmbeddedCli->flags |= REDIS_LUA_CLIENT; /* do not write to socket */
    }
}
static inline void initEmbeddedResponse() {
    if (!CurrEresp) {
        eresp_t *ersp = malloc(sizeof(eresp_t)); bzero(ersp, sizeof(eresp_t));
        CurrEresp     =  ersp;
    }
}
static inline void resetEmbeddedResponse() {
    eresp_t *ersp = CurrEresp;
    if (ersp->objs) {
        for (int i = 0; i < ersp->nobj; i++) destroyAobj(ersp->objs[i]);
        free(ersp->objs); ersp->objs = NULL;
    }
    ersp->nobj = 0;
}
static void resetEmbeddedAlchemy() {
    resetEmbeddedResponse();
    cli *c          = EmbeddedCli;
    if (c->reply->len) { listRelease(c->reply); c->reply = listCreate(); }
    c->bufpos       = 0;
    if (SelectedCols) {
        for (int i = 0; i < NumSelectedCols; i++) sdsfree(SelectedCols[i]);
        free(SelectedCols);
    }
}

static bool embeddedInited = 0;
void initEmbeddedAlchemy() {
    OutputMode = OUTPUT_EMBEDDED;
    if (!embeddedInited) {
        initEmbedded(); // defined in redis.c -DNO_MAIN
        initEmbeddedClient(); initEmbeddedResponse(); embeddedInited  = 1;
    } else {
        resetEmbeddedAlchemy();
    }
    NumSelectedCols   = 0; SelectedCols      = NULL;
    CurrEresp->ncols  = 0; CurrEresp->cnames = NULL;
    initClient(EmbeddedCli);
}

void init_ereq(ereq_t *ereq) { bzero(ereq, sizeof(ereq_t)); }
void release_ereq(ereq_t *ereq) {
    if (ereq->tablelist)           sdsfree(ereq->tablelist);
    if (ereq->insert_value_string) sdsfree(ereq->insert_value_string);
    if (ereq->select_column_list)  sdsfree(ereq->select_column_list);
    if (ereq->where_clause)        sdsfree(ereq->where_clause);

}

static uint32 numElementsReply(redisReply *r) {
    uint32 count = 0;
    switch (r->type) {
      case REDIS_REPLY_NIL:                              return 0;
      case REDIS_REPLY_ERROR:  case REDIS_REPLY_STATUS:
      case REDIS_REPLY_STRING: case REDIS_REPLY_INTEGER: return 1;
      case REDIS_REPLY_ARRAY:
        for (size_t i = 0; i < r->elements; i++) {
            count += numElementsReply(r->element[i]);
        }
        return count;
      default:
        fprintf(stderr, "Unknown reply type: %d\n", r->type); exit(1);
    }
}
static void createEmbedRespFromReply(eresp_t *ersp, redisReply *r, int *cnt) {
    switch (r->type) {
      case REDIS_REPLY_NIL:
        /* Nothing... */ break;
      case REDIS_REPLY_ERROR:
        ersp->retcode = REDIS_ERR;
      case REDIS_REPLY_STATUS:
        ersp->retcode = (!strncmp(r->str, "ERR", 3)) ? REDIS_ERR : REDIS_OK;
      case REDIS_REPLY_STRING:
        ersp->objs[*cnt] = createAobjFromString(r->str, r->len,
                                                COL_TYPE_STRING);
        *cnt = *cnt + 1; break;
      case REDIS_REPLY_INTEGER:
        ersp->objs[*cnt] = createAobjFromLong(r->integer);
        *cnt = *cnt + 1; break;
      case REDIS_REPLY_ARRAY:
        for (size_t i = 0; i < r->elements; i++) {
            createEmbedRespFromReply(ersp, r->element[i], cnt);
        }
        break;
    }
}

void printEmbedResp(eresp_t *ersp) {
   printf("\tERESP.retcode: %d\n", ersp->retcode);
   for (int i = 0; i < ersp->nobj; i++) {
       printf("\t\tERESP[%d] :", i); dumpAobj(printf, ersp->objs[i]);
   }
}

static void redisReplyToEmbedResp(cli *c, eresp_t *ersp) {
    sds out = sdsempty();
    while(c->bufpos > 0 || listLength(c->reply)) {// copy of sendReplyToClient()
        if (c->bufpos) {
            out = sdscatlen(out, c->buf, c->bufpos);
            c->bufpos = 0;
        } else {
            robj *o = listNodeValue(listFirst(c->reply));
            if (!o || !o->ptr) break;
            int objlen = sdslen(o->ptr);
            if (objlen) { out = sdscatlen(out, o->ptr, objlen); }
            listDelNode(c->reply,listFirst(c->reply));
        }
    }

    if (sdslen(out)) {
        void *_reply;
        redisContext *context = redisContextInit();
        __redisCreateReplyReader(context);
        redisReplyReaderFeed(context->reader, out, sdslen(out));
        if (redisReplyReaderGetReply(context->reader, &_reply) == REDIS_ERR) {
            printf("redisReplyReaderGetReply: error\n"); return;
        }
        if (_reply) {
            redisReply *reply = (redisReply*)_reply;
            ersp->nobj        = numElementsReply(reply);
            ersp->objs        = malloc(sizeof(aobj) * ersp->nobj);
            int         cnt   = 0;
            createEmbedRespFromReply(ersp, reply, &cnt);
            freeReplyObject(reply);
        }
        redisFree(context);
    }
    sdsfree(out);
}

static void addSelectedColumnNames() {
    CurrEresp->ncols  = NumSelectedCols;
    CurrEresp->cnames = SelectedCols;
}
void embeddedSaveSelectedColumnNames(int tmatch, int cmatchs[], int qcols) {
    if (!embeddedInited) return; // NOOP when not embedded
    //printf("embeddedSaveSelectedColumnNames\n");
    NumSelectedCols = qcols;
    SelectedCols    = malloc(sizeof(sds) * NumSelectedCols);
    for (int i = 0; i < NumSelectedCols; i++) {
        SelectedCols[i] = sdsdup(Tbl[tmatch].col[cmatchs[i]].name);
    }
    addSelectedColumnNames();
}
void embeddedSaveJoinedColumnNames(jb_t *jb) {
    if (!embeddedInited) return; // NOOP when not embedded
    //printf("embeddedSaveJoinedColumnNames: qcols: %d\n", jb->qcols);
    NumSelectedCols = jb->qcols;
    SelectedCols    = malloc(sizeof(sds) * NumSelectedCols);
    for (int i = 0; i < NumSelectedCols; i++) {
        SelectedCols[i] = sdscatprintf(sdsempty(), "%s.%s",
                          Tbl[jb->js[i].t].name,
                          Tbl[jb->js[i].t].col[jb->js[i].c].name);
    }
    addSelectedColumnNames();
}

static eresp_t *__e_alchemy(int argc, robj **rargv, select_callback *scb,
                            unsigned char freer) {
    initEmbeddedAlchemy();
    cli *c   = EmbeddedCli; c->scb   = scb;
    c->argc  = argc;        c->argv  = rargv;
    processCommand(c);
    redisReplyToEmbedResp(c, CurrEresp);

    if (freer) {
        for (int i = 0; i < argc; i++) decrRefCount(rargv[i]);
        zfree(rargv);
    }
    return CurrEresp;
}
eresp_t *e_alchemy(int argc, robj **rargv, select_callback *scb) {
    return __e_alchemy(argc, rargv, scb, 1);
}
eresp_t *e_alchemy_no_free(int argc, robj **rargv, select_callback *scb) {
    return __e_alchemy(argc, rargv, scb, 0);
}

eresp_t *e_alchemy_raw(char *sql, select_callback *scb) {
    initEmbeddedAlchemy();
    int    argc;
    sds   *argv  = sdssplitargs(sql, &argc);
    DXDB_cliSendCommand(&argc, argv);
    robj **rargv = zmalloc(sizeof(robj*) * argc);
    for (int j = 0; j < argc; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    zfree(argv);
    return e_alchemy(argc, rargv, scb);
}

#define CREATE_RESPONSE_ERROR                                   \
    CurrEresp->retcode = REDIS_ERR;                             \
    CurrEresp->nobj    = 1;                                     \
    CurrEresp->objs    = malloc(sizeof(aobj *));                \
    CurrEresp->objs[0] = createAobjFromString(err, sdslen(err), \
                                              COL_TYPE_STRING); \
    sdsfree(err);                                               \
    return CurrEresp;

eresp_t *e_alchemy_fast(ereq_t *ereq) {
    bool ret = 0;
    initEmbeddedAlchemy();
    sds  err = NULL;
    cli *c   = EmbeddedCli; c->argc = 0; c->argv = NULL;
    c->scb   = ereq->scb;
    if (ereq->op == INSERT) {
        sds      uset   = NULL; uint32 upd = 0; bool repl = 0;
        int      pcols  = 0; list *cmatchl = listCreate();
        int      tmatch = find_table(ereq->tablelist);
        if (tmatch == -1) { err = shared.nonexistenttable->ptr; goto efasterr; }
        r_tbl_t *rt     = &Tbl[tmatch];
        int      ncols  = rt->col_count;
        MATCH_INDICES(tmatch)
        ret = insertCommit(c, uset, ereq->insert_value_string, ncols, tmatch,
                           matches, inds, pcols, cmatchl, repl, upd,
                           NULL, 0, NULL);
        listRelease(cmatchl);
    } else if (ereq->op == SELECT) { //TODO SCAN
        ret = sqlSelectInnards(c, ereq->select_column_list, NULL,
                               ereq->tablelist, NULL, ereq->where_clause, 0,
                               ereq->save_queried_column_names);
    } else if (ereq->op == DELETE) {
        ret = deleteInnards(c, ereq->tablelist, ereq->where_clause);
    } else if (ereq->op == UPDATE) {
        int      tmatch = find_table(ereq->tablelist);
        if (tmatch == -1) { err = shared.nonexistenttable->ptr; goto efasterr; }
        ret = updateInnards(c, tmatch,
                            ereq->update_set_list, ereq->where_clause, 0, NULL);
    } else assert(!"e_alchemy_fast() ereq->op must be set");
    if (!ret) { assert(c->bufpos); //NOTE: all -ERRs < REDIS_REPLY_CHUNK_BYTES
        err = sdsnewlen(c->buf, c->bufpos); c->bufpos = 0; goto efasterr;
    }
    CurrEresp->retcode = REDIS_OK;
    return CurrEresp;

efasterr:
    CREATE_RESPONSE_ERROR
}

eresp_t *e_alchemy_thin_select(uchar qtype,  int tmatch, int cmatch, int imatch,
                               enum OP op,   int qcols, 
                               uint128 keyx, long keyl,  int keyi,
                               int *cmatchs, bool cstar, select_callback *scb,
                               bool save_cnames) {
    initEmbeddedAlchemy();
    sds  err = NULL;
    cli *c      = EmbeddedCli; c->argc = 0; c->argv = NULL;
    c->scb      = scb;
    cswc_t w; wob_t wb;
    init_check_sql_where_clause(&w, tmatch, NULL); init_wob(&wb);
    w.wtype     = qtype;
    w.wf.tmatch = tmatch; w.wf.cmatch = cmatch; w.wf.imatch = imatch;
    w.wf.op     = op;
    if      (keyi) initAobjInt (&w.wf.akey, keyi);
    else if (keyl) initAobjLong(&w.wf.akey, keyl);
    else if (keyx) initAobjU128(&w.wf.akey, keyx);
    else assert(!"e_alchemy_thin_select needs [keyi|keyl|keyx]");
    bool ret    = sqlSelectBinary(c, tmatch, cstar, cmatchs, qcols, &w, &wb,
                                  save_cnames);
    if (!cstar) resetIndexPosOn(qcols, cmatchs);
    destroy_wob(&wb); destroy_check_sql_where_clause(&w);
    if (!ret) { assert(c->bufpos); //NOTE: all -ERRs < REDIS_REPLY_CHUNK_BYTES
        err = sdsnewlen(c->buf, c->bufpos); c->bufpos = 0; goto ethinserr;
    }
    CurrEresp->retcode = REDIS_OK;
    return CurrEresp;

ethinserr:
    CREATE_RESPONSE_ERROR
}

#define CREATE_ARGV_SIMPLE_COMMAND                            \
    c->argc    = 2;                                           \
    c->argv    = &TwoRobj;                                    \
    c->argv[1] = createObject(REDIS_STRING, ereq->redis_key);
robj *TwoRobj  [2];
robj *ThreeRobj[3];

eresp_t *e_alchemy_redis(ereq_t *ereq) {
    int  cret = 0;
    initEmbeddedAlchemy();
    sds  err  = NULL;
    cli *c    = EmbeddedCli;
    c->scb    = ereq->scb;
    if        (ereq->rop == SET) {
        c->argc    = 3; c->argv = &ThreeRobj;
        c->argv[1] = createObject(REDIS_STRING, ereq->redis_key);
        c->argv[2] = createObject(REDIS_STRING, ereq->redis_value);
        setCommand(c);
        cret = strcmp(c->buf, shared.ok->ptr) ? REDIS_ERR : REDIS_OK;
    } else if (ereq->rop == GET) {
        CREATE_ARGV_SIMPLE_COMMAND
        cret = getGenericCommand(c);
    } else if (ereq->rop == DEL) {
        CREATE_ARGV_SIMPLE_COMMAND
        delCommand(c);
        cret = strcmp(c->buf, shared.cone->ptr) ? REDIS_ERR : REDIS_OK;
    }
    c->argc = 0; c->argv = NULL;
    CurrEresp->retcode = ret;
    return CurrEresp;
}

void embedded_exit() { //NOTE: good idea to use for valgrind debugging
    resetEmbeddedAlchemy();
    free(CurrEresp); freeClient(EmbeddedCli);
    lua_close(server.lua);
    DXDB_flushdbCommand();
    free(Index); free(Tbl);
}

void e_alc_got_obj(cli *c, robj *obj) {
    if (!c->scb) return; //TODO assert this
    erow_t er;
    er.ncols   = 1;
    er.cols    = malloc(sizeof(aobj *) * er.ncols);
    bool decme  = 0;
    if (obj->encoding != REDIS_ENCODING_RAW) {
        decme   = 1; obj = getDecodedObject(obj);
    }
    er.cols[0] = createAobjFromString(obj->ptr, sdslen(obj->ptr), COL_TYPE_STRING);
    (*c->scb)(&er);
    destroyAobj(er.cols[0]); free(er.cols);
    if (decme) decrRefCount(obj);
}

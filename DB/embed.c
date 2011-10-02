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
#include "query.h"
#include "aobj.h"
#include "common.h"
#include "embed.h"

#include "hiredis.h"
#include "redis.h"

extern r_tbl_t *Tbl;
extern uchar    OutputMode;

// PROTOTYPES
redisClient *createClient(int fd);              // from networking.c
int initEmbedded();                             // from redis.c (-DNO_MAIN)
redisContext *redisContextInit();               // from hiredis.c
void __redisCreateReplyReader(redisContext *c); // from hiredis.c

// GLOBALS
char               *ConfigFile      = NULL;
static redisClient *EmbeddedCli     = NULL;
eresp_t            *CurrEresp       = NULL;
static int          NumSelectedCols = 0;
static sds         *SelectedCols    = NULL;

void SetConfig(char *file) { ConfigFile = file; }

static inline void initEmbeddedClient() {
    if (!EmbeddedCli) { //printf("initEmbeddedClient\n");
        EmbeddedCli         = createClient(-1);  /* no fd -> embedded */
        EmbeddedCli->flags |= REDIS_LUA_CLIENT; /* do not write to socket */
    }
}
static inline void initEmbeddedResponse() {
    if (!CurrEresp) {
        eresp_t *ersp = malloc(sizeof(eresp_t));
        bzero(ersp, sizeof(eresp_t));
        CurrEresp     =  ersp;
    }
}

static bool embeddedInited = 0;
static void initEmbeddedAlchemy() {
    OutputMode = OUTPUT_EMBEDDED;
    if (!embeddedInited) {
        initEmbedded(); // defined in redis.c -DNO_MAIN
        initEmbeddedClient();
        initEmbeddedResponse();
        embeddedInited  = 1;
        NumSelectedCols = 0;
        SelectedCols    = NULL;
    } else {
        //TODO as much of "resetClient(EmbeddedCli)" as needed
        cli *c          = EmbeddedCli;
        listRelease(c->reply);
        c->reply        = listCreate();
        c->bufpos       = 0;
        for (int i = 0; i < NumSelectedCols; i++) sdsfree(SelectedCols[i]);
        free(SelectedCols);
        NumSelectedCols = 0;
        SelectedCols    = NULL;
    }
    CurrEresp->ncols  = 0;
    CurrEresp->cnames = NULL;
}

static uint32 numElementsReply(redisReply *r) {
    uint32 count = 0;
    switch (r->type) {
      case REDIS_REPLY_NIL:
        return 0;
      case REDIS_REPLY_ERROR:
      case REDIS_REPLY_STATUS:
      case REDIS_REPLY_STRING:
      case REDIS_REPLY_INTEGER:
        return 1;
      case REDIS_REPLY_ARRAY:
        for (size_t i = 0; i < r->elements; i++) {
            count += numElementsReply(r->element[i]);
        }
        return count;
      default:
        fprintf(stderr, "Unknown reply type: %d\n", r->type);
        exit(1);
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
        *cnt = *cnt + 1;
        break;
      case REDIS_REPLY_INTEGER:
        ersp->objs[*cnt] = createAobjFromLong(r->integer);
        *cnt = *cnt + 1;
        break;
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
            int cnt = 0;
            createEmbedRespFromReply(ersp, reply, &cnt);
        }
        sdsfree(out);
    }
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

eresp_t *e_alchemy(int argc, robj **rargv, select_callback *scb) {
    initEmbeddedAlchemy();
    cli *c  = EmbeddedCli;
    c->scb  = scb;
    c->argc = argc;
    c->argv = rargv;
    processCommand(c);
    redisReplyToEmbedResp(c, CurrEresp);

    for (int i = 0; i < argc; i++) decrRefCount(rargv[i]);
    zfree(rargv);
    return CurrEresp;
}

eresp_t *e_alchemy_raw(char *sql, select_callback *scb) {
    initEmbeddedAlchemy();
    int  argc;
    sds *argv = sdssplitargs(sql, &argc);
    DXDB_cliSendCommand(&argc, argv);

    robj **rargv = zmalloc(sizeof(robj*) * argc);
    for (int j = 0; j < argc; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    zfree(argv);

    return e_alchemy(argc, rargv, scb);
}


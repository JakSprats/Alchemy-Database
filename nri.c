/*
 * This file implements the NonReleationIndex logic in AlchemyDB
 *

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "redis.h"
#include "adlist.h"
#include "zmalloc.h"

#include "bt.h"
#include "row.h"
#include "rpipe.h"
#include "alsosql.h"
#include "common.h"
#include "nri.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer  server;
extern ulong               CurrCard;
extern redisClient        *CurrClient;

extern r_tbl_t Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t Index[MAX_NUM_DB][MAX_NUM_INDICES];

/* creates text for trigger's command */
static sds genNRL_Cmd(d_l_t  *nrlind,
                      robj   *pko,
                      char   *vals,
                      uint32  cofsts[],
                      bool    from_insert,
                      robj   *row,
                      int     tmatch) {
    sds       cmd     = sdsempty();
    list     *nrltoks = nrlind->l1;
    list     *nrlcols = nrlind->l2;
    listIter *li1     = listGetIterator(nrltoks, AL_START_HEAD);
    listIter *li2     = listGetIterator(nrlcols, AL_START_HEAD);
    listNode *ln1     = listNext(li1);
    listNode *ln2     = listNext(li2);
    while (ln1 || ln2) {
        if (ln1) {
            sds token = ln1->value;
            cmd       = sdscatlen(cmd, token, sdslen(token));
            ln1       = listNext(li1);
        }
        int cmatch = -1;
        if (ln2) {
            cmatch = ((int)(long)ln2->value) - 1; /* because (0 != NULL) */
            ln2    = listNext(li2);
        }
        if (cmatch != -1) {
            char *x;
            int   xlen;
            robj *col = NULL;
            if (from_insert) {
                if (!cmatch) { /* PK not in ROW */
                    x    = pko->ptr;
                    xlen = sdslen(x);
                } else {       /* get COL from cofsts */
                    x    = vals + cofsts[cmatch - 1];
                    xlen = cofsts[cmatch] - cofsts[cmatch - 1] - 1;
                }
            } else { /* not from INSERT -> fetch row */
                col  = createColObjFromRow(row, cmatch, pko, tmatch);
                x    = col->ptr;
                xlen = sdslen(col->ptr);
            }
            cmd = sdscatlen(cmd, x, xlen);
            if (col) decrRefCount(col);
        }
    }
    listReleaseIterator(li1);
    listReleaseIterator(li2);
    return cmd;
}

static bool emptyNonRelIndRespHandler(redisClient *c) {
    addReply(c, shared.nullbulk);
    CurrCard++;
    return 1;
}

uchar NriFlag = 0;
static bool nonRelIndRespHandler(redisClient *c,
                                 void        *x,
                                 robj        *key,
                                 long        *card,
                                 int          b,   /* variable ignored */
                                 int          n) { /* variable ignored */
    x = 0; b = 0; n = 0; /* compiler warnings */
    if (NriFlag == PIPE_ONE_LINER_FLAG) {
        char *s = key->ptr;
        robj *r = _createStringObject(s + 1); /* +1 skips '+','-',':' */
        decrRefCount(key);
        key    = r;
    }
    addReplyBulk(c, key);
    *card   = *card + 1;
    CurrCard++;
    return 1;
}

static void runCmdInFakeClient(sds s) {
    //RL4 "runCmdInFakeClient: %s", s);
    int           argc;
    sds          *argv = sdssplitlen(s, sdslen(s), " ", 1, &argc);
    if (!argv)    return;
    if (argc < 1) goto run_cmd_end;
    redisCommand *cmd  = lookupCommand(argv[0]);
    if (!cmd)     goto run_cmd_end;
    if ((cmd->arity > 0 && cmd->arity > argc) || (argc < -cmd->arity))
                  goto run_cmd_end;
    int    arity;
    robj **rargv;
    if (cmd->arity > 0 || cmd->proc == insertCommand    ||
                          cmd->proc == sqlSelectCommand ||
                          cmd->proc == tscanCommand)       {
        arity = abs(cmd->arity);
        rargv = zmalloc(sizeof(robj *) * arity);
        for (int j = 0; j < arity - 1; j++) {
            rargv[j] = createStringObject(argv[j], sdslen(argv[j]));
        }
        sds lastarg = sdsempty();
        for (int j = arity - 1; j < argc; j++) {
            if (j != (arity - 1)) lastarg = sdscatlen(lastarg, " ", 1);
            lastarg = sdscatlen(lastarg, argv[j], sdslen(argv[j]));
        }
        rargv[arity - 1] = createObject(REDIS_STRING, lastarg);
    } else {
        arity = argc;
        rargv = zmalloc(sizeof(robj *) * arity);
        for (int j = 0; j < arity; j++) {
            rargv[j] = createStringObject(argv[j], sdslen(argv[j]));
        }
    }
    redisClient *c  = CurrClient;
    redisClient *fc = rsql_createFakeClient();
    fc->argv        = rargv;
    fc->argc        = arity;
    fakeClientPipe(c, fc, NULL, 0, &NriFlag,
                   nonRelIndRespHandler, emptyNonRelIndRespHandler);
    rsql_freeFakeClient(fc);
    for (int j = 0; j < arity; j++) decrRefCount(rargv[j]);
    zfree(rargv);

run_cmd_end:
    if (argv) {
        for (int j = 0; j < argc; j++) sdsfree(argv[j]);
        zfree(argv);
    }
}

void nrlIndexAdd(robj *o, robj *pko, char *vals, uint32 cofsts[]) {
    sds cmd = genNRL_Cmd(o->ptr, pko, vals, cofsts, 1, NULL, -1);
    runCmdInFakeClient(cmd);
    sdsfree(cmd);
    return;
}

void runNrlIndexFromStream(uchar *stream, d_l_t *nrlind, int itbl) {
    robj  key, val;
    assignKeyRobj(stream,            &key);
    assignValRobj(stream, REDIS_ROW, &val, BTREE_TABLE);
    /* create command and run it */
    sds cmd = genNRL_Cmd(nrlind, &key, NULL, NULL, 0, &val, itbl);
    runCmdInFakeClient(cmd);
    sdsfree(cmd);
    destroyAssignKeyRobj(&key);
}

/* CREATE NON-RELATIONAL-INDEX */
/* Parses "xxx$col1 yyy$col2 zzz" -> "xxx[col1] yyy[col2] zzz" */
bool parseNRLcmd(char *o_s, list *nrltoks, list *nrlcols, int tmatch) {
    char *s   = strchr(o_s, '$');
    if (!s) {
       listAddNodeTail(nrltoks, sdsdup(o_s)); /* freed in freeNrlIndexObject */
    } else {
        while (1) {
            s++; /* advance past "$" */
            char *nxo  = s;
            while (isalnum(*nxo) || *nxo == '_') nxo++; /* col must be alpnum */
            int   cmatch = find_column_n(tmatch, s, nxo - s);
            if (cmatch == -1) return 0;
            listAddNodeTail(nrlcols, (void *)(long)(cmatch + 1)); /* 0!=NULL */
            listAddNodeTail(nrltoks, sdsnewlen(o_s, (s - 1) - o_s)); /*no "$"*/
            char *nexts  = strchr(s, '$');              /* var is '$' delimed */
            if (!nexts) { /* no more vars */
                if (*nxo) listAddNodeTail(nrltoks, sdsnewlen(nxo, strlen(nxo)));
                break;
            }
            o_s = nxo;
            s   = nexts;
        }
    }
    return 1;
}

/* for REWRITEAOF and DESC */
sds rebuildOrigNRLcmd(robj *o) {
    d_l_t    *nrlind  = o->ptr;
    int       tmatch  = Index[server.dbid][nrlind->num].table;
    list     *nrltoks = nrlind->l1;
    list     *nrlcols = nrlind->l2;
    listIter *li1     = listGetIterator(nrltoks, AL_START_HEAD);
    listNode *ln1     = listNext(li1);
    listIter *li2     = listGetIterator(nrlcols, AL_START_HEAD);
    listNode *ln2     = listNext(li2);
    sds       cmd     = sdsnewlen("\"", 1); /* has to be one arg */
    while (ln1 || ln2) {
        if (ln1) { 
            sds token  = ln1->value;
            cmd        = sdscatlen(cmd, token, sdslen(token));
            ln1 = listNext(li1);
        }
        if (ln2) {
            int cmatch = (int)(long)ln2->value;
            cmatch--; /* because (0 != NULL) */
            sds cname  = Tbl[server.dbid][tmatch].col_name[cmatch]->ptr;
            cmd        = sdscatlen(cmd, "$", 1); /* "$" variable delim */
            cmd        = sdscatlen(cmd, cname, sdslen(cname));
            ln2 = listNext(li2);
        }
    }
    listReleaseIterator(li1);
    listReleaseIterator(li2);
    cmd = sdscatlen(cmd, "\"", 1);          /* has to be one arg */
    return cmd;
}

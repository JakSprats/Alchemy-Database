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

#include "adlist.h"
#include "redis.h"

#include "bt.h"
#include "row.h"
#include "alsosql.h"
#include "common.h"
#include "nri.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer server;

extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// GLOBALS
r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

sds genNRL_Cmd(d_l_t  *nrlind,
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
            }
            int cmatch = -1;
            if (ln2) {
                cmatch = (int)(long)ln2->value;
                cmatch--; /* because (0 != NULL) */
            }
            if (cmatch != -1) {
                char *x;
                int   xlen;
                robj *col = NULL;
                if (from_insert) {
                    if (!cmatch) {
                        x    = pko->ptr;
                        xlen = sdslen(x);
                    } else {
                        x    = vals + cofsts[cmatch - 1];
                        xlen = cofsts[cmatch] - cofsts[cmatch - 1] - 1;
                    }
                } else {
                    col = createColObjFromRow(row, cmatch, pko, tmatch);
                    x    = col->ptr;
                    xlen = sdslen(col->ptr);
                }
                cmd = sdscatlen(cmd, x, xlen);
                if (col) decrRefCount(col);
            }
            ln1 = listNext(li1);
            ln2 = listNext(li2);
        }
    /*TODO destroy both listIter's */
    return cmd;
}

void runCmdInFakeClient(sds s) {
    //RL4 "runCmdInFakeClient: %s", s);
    char *end = strchr(s, ' ');
    if (!end) return;

    sds   *argv    = NULL; /* must come before first GOTO */
    int    a_arity = 0;
    sds cmd_name   = sdsnewlen(s, end - s);
    end++;
    struct redisCommand *cmd = lookupCommand(cmd_name);
    if (!cmd) goto run_cmd_err;
    int arity = abs(cmd->arity);

    char *args = NULL;
    if (arity > 2) {
        args = strchr(end, ' ');
        if (!args) goto run_cmd_err;
        args++;
    }

    argv                = malloc(sizeof(sds) * arity);
    argv[0]             = cmd_name;
    a_arity++;
    argv[1]             = args ? sdsnewlen(end, args - end - 1) :
                                 sdsnewlen(end, strlen(end)) ;
    a_arity++;
    if (arity == 3) {
        argv[2]         = sdsnewlen(args, strlen(args));
        a_arity++;
    } else if (arity > 3) {
        char *dlm       = strchr(args, ' ' );;
        if (!dlm) goto run_cmd_err;
        dlm++;
        argv[2]         = sdsnewlen(args, dlm - args - 1);
        a_arity++;
        if (arity == 4) {
            argv[3]     = sdsnewlen(dlm, strlen(dlm));
            a_arity++;
        } else { /* INSERT */
            char *vlist = strchr(dlm, ' ' );;
            if (!vlist) goto run_cmd_err;
            vlist++;
            argv[3]     = sdsnewlen(dlm, vlist - dlm - 1);
            a_arity++;
            argv[4]     = sdsnewlen(vlist, strlen(vlist));
            a_arity++;
        }
    }

    robj **rargv = malloc(sizeof(robj *) * arity);
    for (int j = 0; j < arity; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    redisClient *fc = rsql_createFakeClient();
    fc->argv        = rargv;
    fc->argc        = arity;
    rsql_resetFakeClient(fc);
    call(fc, cmd);
    rsql_freeFakeClient(fc);
    free(rargv);

run_cmd_err:
    if (!a_arity) sdsfree(cmd_name);
    if (argv)     free(argv);
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
    if (key.encoding == REDIS_ENCODING_RAW) {
        sdsfree(key.ptr); /* free from assignKeyRobj sflag[1,4] */
    }
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
            char *nxo = s;
            while (isalnum(*nxo) || *nxo == '_') nxo++; /* col must be alpnum */
            char *nexts = strchr(s, '$');               /* var is '$' delimed */

            int cmatch = -1;
            if (nxo) cmatch = find_column_n(tmatch, s, nxo - s);
            else     cmatch = find_column(tmatch, s);
            if (cmatch == -1) return 0;
            listAddNodeTail(nrlcols, (void *)(long)(cmatch + 1)); /* 0!=NULL */

            listAddNodeTail(nrltoks, sdsnewlen(o_s, (s - 1) - o_s)); /*no "$"*/
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

/* for REWRITEAOF */
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
    /*TODO destroy both listIter's */
    cmd = sdscatlen(cmd, "\"", 1); /* has to be one arg */
    return cmd;
}

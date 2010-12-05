/*
 *
 * This file implements Alchemy's DENORM command
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
#include "zmalloc.h"

#include "sql.h"
#include "store.h"
#include "join.h"
#include "bt_iterator.h"
#include "row.h"
#include "bt.h"
#include "parser.h"
#include "legacy.h"
#include "alsosql.h"

#include "denorm.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern int      Num_tbls     [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

bool emptyNoop(redisClient *c) {
    c = NULL; /* compiler warning */
    return 1;
}

/* NOTE: this function implements a fakeClient pipe */
long fakeClientPipe(redisClient *c,
                    redisClient *rfc,
                    void        *wfc, /* can be redisClient,list,LuaState */
                    int          is_ins,
                    flag        *flg,
                    bool (* adder)
                    (redisClient *c, void *x, robj *key, long *l, int b, int n),
                    bool (* emptyer) (redisClient *c)) {
    struct redisCommand *cmd = lookupCommand(rfc->argv[0]->ptr);
    cmd->proc(rfc);

    listNode *ln;
    *flg             = PIPE_NONE_FLAG;
    int       nlines = 0;
    long      card   = 1; /* ZER0 as pk can cause problems */
    bool      fline  = 1;
    listIter  *li = listGetIterator(rfc->reply, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        robj *o    = ln->value;
        sds   s    = o->ptr;
        bool  o_fl = fline;
        fline = 0;
        //RL4 "PIPE: %s", s);
        /* ignore protocol, we just want data */
        if (*s == '\r' && *(s + 1) == '\n') continue;
         /* TODO introduce more state -> data starting w/ '\r\n' ignored */
        if (o_fl) {
            if (*s == '-') {
                *flg = PIPE_ERR_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* error */
            }
            if (*s == '+') {
                *flg = PIPE_ONE_LINER_FLAG;
                if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
                break; /* OK */
            }
            if (*s == ':') {
                robj *r;
                char *x = s + 1;
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                if (!y) { /* colon is coming in next ln->value [incrCommand] */
                    if (!(ln = listNext(li))) return -1;
                    o = ln->value;
                    x = o->ptr;
                    y = strchr(x, '\r'); /* ignore the final \r\n */
                    if (!y) y = x + sdslen(x);
                }
                r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
                break; /* single integer reply */
            }
            if (*s == '*') {
                nlines = atoi(s+1); /* some pipes need to know num_lines */
                if (nlines == 0) {
                    *flg = PIPE_EMPTY_SET_FLAG;
                    break;
                }
                continue;
            }
        }
        if (*s == '$') { /* parse doubles which are w/in this list element */
            if (*(s + 1) == '-') continue; /* $-1 -> nil */
            char   *x    = strchr(s, '\r');
            if (!x) return -1;
            uint32  llen = x - s;
            if (llen + 2 < sdslen(s)) { /* got a double */
                x += 2; /* move past \r\n */
                char *y = strchr(x, '\r'); /* ignore the final \r\n */
                if (!y) return -1;
                robj *r = createStringObject(x, y - x);
                if (!(*adder)(c, wfc, r, &card, is_ins, nlines)) return -1;
            }
            continue;
        }
        /* all ranges are single */
        if (!(*adder)(c, wfc, o, &card, is_ins, nlines)) return -1;
    }
    listReleaseIterator(li);
    if (card == 1) { /* empty response from rfc */
        if (!(*emptyer)(c)) return -1;
    }
    return card - 1; /* started at 1 */
}

void denormCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    sds wildcard = c->argv[2]->ptr;
    if (!strchr(wildcard, '*')) {
        addReply(c, shared.denorm_wildcard_no_star);
        return;
    }

    uint32 wlen = sdslen(wildcard);
    uint32 spot = 0;
    for (uint32 i = 0; i < wlen; i++) {
        if (wildcard[i] == '*') {
            spot = i;
            break;
        }
    }
    uint32  restlen  = (spot < wlen - 2) ? wlen - spot - 1: 0;
    sds     s_wldcrd = sdsnewlen(wildcard, spot);
    s_wldcrd         = sdscatlen(s_wldcrd, "%s", 2);
    if (restlen) s_wldcrd = sdscatlen(s_wldcrd, &wildcard[spot + 1], restlen);
    sds     d_wldcrd = sdsdup(s_wldcrd);
    char   *fmt      = strstr(d_wldcrd, "%s"); /* written 2 lines up cant fail*/
    fmt++;
    *fmt             = 'd'; /* changes "%s" into "%d" - FIX: too complicated */

    robj               *argv[4];
    struct redisClient *fc = rsql_createFakeClient();
    fc->argv               = argv;
    fc->argc               = 4;

    btEntry          *be;
    robj             *o  = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    btSIter *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        robj *key = be->key;
        robj *row = be->val;

        sds hname = sdsempty();
        if (key->encoding == REDIS_ENCODING_RAW) {
            hname = sdscatprintf(hname, s_wldcrd, key->ptr);
        } else {
            hname = sdscatprintf(hname, d_wldcrd, key->ptr);
        }
        fc->argv[1] = createStringObject(hname, sdslen(hname));
        sdsfree(hname);

        /* PK is in name */
        for (int i = 1; i < Tbl[server.dbid][tmatch].col_count; i++) {
            robj *r     = createColObjFromRow(row, i, key, tmatch);
            sds tname   = Tbl[server.dbid][tmatch].col_name[i]->ptr;
            fc->argv[2] = createStringObject(tname, sdslen(tname));
            sds cname   = r->ptr;
            fc->argv[3] = createStringObject(cname, sdslen(cname));
            hsetCommand(fc);
        }
    }

    addReply(c, shared.ok);
    sdsfree(s_wldcrd);
    sdsfree(d_wldcrd);
    rsql_freeFakeClient(fc);
}

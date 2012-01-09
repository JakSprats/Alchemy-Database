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

#include <strings.h>

#include "redis.h"

#include "wc.h"
#include "find.h"
#include "colparse.h"
#include "alsosql.h"
#include "ctable.h"

extern r_tbl_t *Tbl;
extern cli     *CurrClient;

static bool Bdum;

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
            listNode *ln;
            list *tl   = listCreate(); //TODO combine: [tl & janl] //FREE 105
            list *janl = listCreate();                             //FREE 106
            if (!parseCommaSpaceList(c, argv[3]->ptr, 0, 1, 0, 0, 0, -1, NULL,
                                     NULL, tl, janl, NULL, NULL, &Bdum)) {
                goto ovrd_sel_end;
            }
            uint32    n_clstr = 0, n_shrd = 0;
            listIter *li      = listGetIterator(tl, AL_START_HEAD);
            while((ln = listNext(li))) {
                int tmatch = (int)(long)ln->value;
                r_tbl_t *rt = &Tbl[tmatch];
                if (rt->sk == -1) n_clstr++;
                else              n_shrd++;
            } listReleaseIterator(li);
            if (n_shrd > 1) { // check for FK relations
                //TODO parse WC, validate join-chain
                int fk_otmatch[MAX_JOIN_INDXS];
                int fk_ocmatch[MAX_JOIN_INDXS];
                int n_fk = 0;
                li       = listGetIterator(tl, AL_START_HEAD);
                while((ln = listNext(li))) {
                    int tmatch = (int)(long)ln->value;
                    r_tbl_t *rt = &Tbl[tmatch];
                    if (rt->sk != -1 && rt->fk_cmatch != -1) {
                        fk_otmatch[n_fk] = rt->fk_otmatch;
                        fk_ocmatch[n_fk] = rt->fk_ocmatch;
                        n_fk++;
                    }
                } listReleaseIterator(li);
                for (int i = 0; i < n_fk; i++) {
                    printf("%d: ot: %d oc: %d\n",
                           i, fk_otmatch[i], fk_ocmatch[i]);
                }
            }
            printf("n_clstr: %d n_shrd: %d\n", n_clstr, n_shrd);

ovrd_sel_end:
            listRelease(tl); listRelease(janl); // FREED 105 & 106
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
            parseWCplusQO(c, &w, &wb, SQL_SELECT);
            if (w.wtype == SQL_ERR_LKP)   return NULL;
            if (w.wtype == SQL_RANGE_LKP) return NULL;
            if (w.wtype == SQL_IN_LKP   ) return NULL; //TODO evaluate each key
            if (w.wf.cmatch != rt->sk) {
                *err = 1; addReply(c, shared.select_on_sk); return NULL;
            }
            aobj *afk = &w.wf.akey;
            sds   sk  = createSDSFromAobj(afk);
            sds   key  = sdscatprintf(sdsempty(), "%s=%s.%s",
                                 sk, tname, (char *)rt->col[rt->sk].name);
            //printf("sqlSelectCommand: key: %s\n", key);
            return key;
        }
    }
    return NULL;
}

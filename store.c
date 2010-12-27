/*
 * Implements istore
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

#include "zmalloc.h"
#include "redis.h"

#include "bt.h"
#include "bt_iterator.h"
#include "row.h"
#include "range.h"
#include "orderby.h"
#include "join.h"
#include "rpipe.h"
#include "index.h"
#include "legacy.h"
#include "colparse.h"
#include "parser.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "store.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern char *COLON;

extern r_tbl_t  Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];

extern char **Jrcols  [MAX_JOIN_INDXS * MAX_JOIN_COLS];
extern int    Jrc_lens[MAX_JOIN_INDXS * MAX_JOIN_COLS];

stor_cmd StorageCommands[NUM_STORAGE_TYPES];

/* PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE */
/* PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE PARSE_STORE */
static bool checkStoreTypeReply(redisClient *c, int *sto, char *stot) {
    *sto      = -1;
    char *x   = strchr(stot, ' ');
    int   len = x ? x - stot : (int)strlen(stot);
    for (int i = 0; i < NUM_STORAGE_TYPES; i++) {
        if (!strncasecmp(stot, StorageCommands[i].name, len)) {
            *sto = i;
            break;
        }
    }
    if (*sto == -1) {
        addReply(c, shared.storagetypeunkown);
        return 0;
    }
    return 1;
}

bool prepareToStoreReply(redisClient  *c,
                         cswc_t       *w,
                         char        **nname,
                         int          *nlen,
                         bool         *sub_pk,
                         int          *nargc,
                         char        **last,
                         int           qcols) {
    char *stot = next_token(w->stor);
    if (!stot) {
        addReply(c, shared.selectsyntax);
        return 0;
    }
    if (!checkStoreTypeReply(c, &w->sto, stot)) return 0;
    *nname = next_token(stot);
    if (!*nname) {
        addReply(c, shared.selectsyntax);
        return 0;
    }

    *nlen     = get_token_len(*nname);
    char *end = *nname + *nlen - 1;
    *sub_pk   = (StorageCommands[w->sto].argc < 0);
    *nargc    = abs(StorageCommands[w->sto].argc);
    if (*end == '$') { /* means final arg munging */
        *sub_pk = 1;
        *last   = end;
        *end    = '\0';   /* temporarily erase '$' - add in later for AOF */
        *nlen   = *nlen - 1;
        if ((*nargc + 1) != qcols) {
            addReply(c, shared.storagenumargsmismatch);
            return 0;
        }
    } else {
        if (*nargc != qcols) {
            addReply(c, shared.storagenumargsmismatch);
            return 0;
        }
    }
    return 1;
}

#define ISTORE_ERR_MSG \
  "-ERR IN(SELECT Range Query STORE) - inner command had error: "
#define JOIN_STORE_ERR_MSG \
  "-ERR IN(SELECT Join STORE) - inner command had error: "
bool performStoreCmdOrReply(redisClient *c,
                            redisClient *rfc,
                            int          sto,
                            bool         join) {
    /* TODO for aof-logging, "call(rfc, cmd);" would aof-write each op */
    rsql_resetFakeClient(rfc);
    (*StorageCommands[sto].func)(rfc);
    char *msg = join ? JOIN_STORE_ERR_MSG : ISTORE_ERR_MSG;
    if (!replyIfNestedErr(c, rfc, msg)) return 0;
    else                                return 1;
}

bool istoreAction(redisClient *c,
                  redisClient *fc,
                  int          tmatch,
                  int          cmatchs[],
                  int          qcols,
                  int          sto,
                  aobj        *apk,
                  void        *rrow,
                  char        *nname,
                  bool         sub_pk,
                  uint32       nargc) {
    aobj  cols[MAX_COLUMN_PER_TABLE];
    int   totlen = 0;
    for (int i = 0; i < qcols; i++) {
        cols[i]  = getRawCol(rrow, cmatchs[i], apk, tmatch, NULL, 1);
        totlen  += cols[i].len;
    }

    fc->argc     = qcols + 1;
    fc->argv[0]  = _createStringObject(StorageCommands[sto].name);
    fc->argv[1]  = _createStringObject(nname); /*NEW Objects NAME*/
    int    n     = 0;
    robj **argv  = fc->argv;
    if (sub_pk) { /* overwrite pk=nname:cols[0] */
        argv[1]->ptr = sdscatlen(argv[1]->ptr, COLON,   1);
        argv[1]->ptr = sdscatlen(argv[1]->ptr, cols[0].s, cols[0].len);
        n++;
    }
    argv[2] = createStringObject(cols[n].s, cols[n].len);
    if (nargc > 1) {
        n++;
        argv[3]  = createStringObject(cols[n].s, cols[n].len);
    }
    for (int i = 0; i < qcols; i++) {
        releaseAobj(&cols[i]);
    }

    rsql_resetFakeClient(fc);
    return performStoreCmdOrReply(c, fc, sto, 0);
}

bool istore_op(range_t *g, aobj *apk, void *rrow, bool q) {
    if (q) {
        addORowToRQList(g->co.ll, NULL, rrow, g->co.w->obc,
                        apk, g->co.w->tmatch, g->co.ctype);
    } else {
        if (!istoreAction(g->co.c, g->st.fc, g->co.w->tmatch, g->se.cmatchs,
                          g->se.qcols, g->co.w->sto, apk, rrow, g->st.nname,
                          g->st.sub_pk, g->st.nargc))
                              return 0;
    }
    return 1;
}

void istoreCommit(redisClient *c,
                  cswc_t      *w,
                  int          cmatchs[MAX_COLUMN_PER_TABLE],
                  int          qcols) {
    qr_t  q;
    setQueued(w, &q);
    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (q.qed) {
        ll      = listCreate();
        ctype   = Tbl[server.dbid][w->tmatch].col_type[w->obc];
    }

    long         card  = 0;    /* B4 GOTO */ 
    int          sent  = 0;    /* B4 GOTO */ 
    redisClient *fc    = NULL; /* B4 GOTO */
    bool         err   = 0;    /* B4 GOTO */

    int          nlen;
    char        *last  = NULL;
    range_t      g;
    init_range(&g, c, w, &q, ll, ctype);
    if (!prepareToStoreReply(c, w, &g.st.nname, &nlen, &g.st.sub_pk,
                             &g.st.nargc, &last, qcols)) {
        err = 1;
        goto istore_end;
    }

    robj *argv[STORAGE_MAX_ARGC + 1];
    fc           = rsql_createFakeClient();
    fc->argv     = argv;
    g.se.qcols   = qcols;
    g.se.cmatchs = cmatchs;
    g.st.fc      = fc;
    if (w->low) { /* RANGE QUERY */
        card = rangeOp(&g, istore_op);
    } else {    /* IN () QUERY */
        card = inOp(&g, istore_op);
    }
    if (card != -1) {
        if (q.qed) {
            obsl_t **v = sortOrderByToVector(ll, ctype, g.co.w->asc);
            sent       = sortedOrderByIstore(c, w, fc, g.se.cmatchs, qcols,
                                             g.st.nname, g.st.sub_pk,
                                             g.st.nargc, ctype, v,
                                             listLength(ll));
            sortedOrderByCleanup(v, listLength(ll), ctype, 0);
            free(v);
            if (sent == -1) err = 1;
        }
    } else {
        err = 1;
    }

istore_end:
    if (last) *last = '$';/* write back in "$" for AOF */

    if (ll) listRelease(ll);
    if (fc) rsql_freeFakeClient(fc);

    if (err) return;

    if (w->lim != -1 && sent < card) card = sent;
    addReplyLongLong(c, card);

    if (w->ovar) incrOffsetVar(c, w, card);
}

/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */
/* JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN */

//TODO refactor against istoreAction()
void prepare_jRowStore(jrow_reply_t *r) {
    robj **argv = r->fc->argv;
    argv[0]     = _createStringObject(StorageCommands[r->sto].name);
    argv[1]     = cloneRobj(r->nname);
    int    n    = 1;
    r->fc->argc = 3;
    if (r->sub_pk) { // pk =argv[1]:Rcols[0][0]
        argv[1]->ptr = sdscatlen(argv[1]->ptr, COLON,   1);
        argv[1]->ptr = sdscatlen(argv[1]->ptr, *Jrcols[0], Jrc_lens[0]);
        argv[2]      = createStringObject(*Jrcols[1], Jrc_lens[1]);
        n++;
    } else {
        argv[2]      = createStringObject(*Jrcols[0], Jrc_lens[0]);
    }
    if (r->nargc > 1) {
        argv[3]      = createStringObject(*Jrcols[n], Jrc_lens[n]);
        r->fc->argc  = 4;
    }
    //TODO check sixbit MEM LEAK here
}

bool jRowStore(jrow_reply_t *r) {
    prepare_jRowStore(r);
    rsql_resetFakeClient(r->fc);
    return performStoreCmdOrReply(r->c, r->fc, r->sto, 1);
}

/*
 * Implements istore and iselect
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

#include "alsosql.h"
#include "bt.h"
#include "bt_iterator.h"
#include "row.h"
#include "index.h"
#include "orderby.h"
#include "parser.h"
#include "common.h"
#include "store.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern int Num_tbls[MAX_NUM_DB];

extern char *EQUALS;
extern char *COLON;
extern char *PERIOD;

extern char *Col_type_defs[];

extern r_tbl_t  Tbl   [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index [MAX_NUM_DB][MAX_NUM_INDICES];

#define MAX_TBL_DEF_SIZE     1024

stor_cmd StorageCommands[NUM_STORAGE_TYPES];

// STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS
// STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS

/* LEGACY functions used on AOF readin */
void legacyInsertCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    int ncols = Tbl[server.dbid][tmatch].col_count;
    MATCH_INDICES(tmatch)

    char *vals   = c->argv[2]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices);
}

/* LEGACY functions used on AOF readin */
void legacyTableCommand(redisClient *c) {
    if (Num_tbls[server.dbid] >= MAX_NUM_TABLES) {
        addReply(c,shared.toomanytables);
        return;
    }

    sds tname = c->argv[1]->ptr;
    if (find_table(tname) != -1) {
        addReply(c,shared.nonuniquetablenames);
        return;
    }

    // parse column definitions
    char  col_names[MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    char *cname     = c->argv[2]->ptr;
    int   col_count = 0;
    while (1) {
        char *type  = strchr(cname, '=');
        char *nextc = strchr(cname, ',');
        if (!type) {
            addReply(c,shared.missingcolumntype);
            return;
        } else {
            *type = '\0';
            type++;
        }
        if (nextc) {
            *nextc = '\0';
            nextc++;
        }
        unsigned char miss = 1;
        for (unsigned char j = 0; j < NUM_COL_TYPES; j++) {
            if (!strcmp(type, Col_type_defs[j])) {
                Tbl[server.dbid][Num_tbls[server.dbid]].col_type[col_count] = j;
                miss = 0;
                break;
            }
        }
        if (miss) {
            addReply(c,shared.undefinedcolumntype);
            return;
        }
        if (strlen(cname) >= MAX_COLUMN_NAME_SIZE) {
            addReply(c,shared.columnnametoobig);
            return;
        }
        strcpy(col_names[col_count], cname);
        col_count++;
        if (!nextc) break;
        if (col_count == MAX_COLUMN_PER_TABLE) {
            addReply(c,shared.toomanycolumns);
            return;
        }
        cname = nextc;
    }
    createTableCommitReply(c, col_names, col_count, tname, sdslen(tname));
}

// TODO move to rpipe.c
unsigned char respOk(redisClient *c) {
    listNode *ln = listFirst(c->reply);
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    if (!strcmp(s, shared.ok->ptr)) return 1;
    else                            return 0;
}

// TODO move to rpipe.c
unsigned char respNotErr(redisClient *c) {
    listNode *ln = listFirst(c->reply);
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    if (!strncmp(s, "-ERR", 4)) return 0;
    else                        return 1;
}

static void cpyColDef(char *cdefs,
                      int  *slot,
                      int   tmatch,
                      int   cmatch,
                      int   qcols,
                      int   loop,
                      bool  has_conflicts,
                      bool  cname_cflix[]) {
    robj *col = Tbl[server.dbid][tmatch].col_name[cmatch];
    if (has_conflicts && cname_cflix[loop]) { // prepend tbl_name
        robj *tbl = Tbl[server.dbid][tmatch].name;
        memcpy(cdefs + *slot, tbl->ptr, sdslen(tbl->ptr));  
        *slot        += sdslen(tbl->ptr);        // tblname
        memcpy(cdefs + *slot, PERIOD, 1);
        *slot = *slot + 1;
    }
    memcpy(cdefs + *slot, col->ptr, sdslen(col->ptr));  
    *slot        += sdslen(col->ptr);            // colname
    memcpy(cdefs + *slot, EQUALS, 1);
    *slot = *slot + 1;
    char *ctype   = Col_type_defs[Tbl[server.dbid][tmatch].col_type[cmatch]];
    int   ctlen   = strlen(ctype);               // [INT,STRING]
    memcpy(cdefs + *slot, ctype, ctlen);
    *slot        += ctlen;
    if (loop != (qcols - 1)) {
        memcpy(cdefs + *slot, ",", 1);
        *slot = *slot + 1;                       // ,
    }
}

static bool _internalCreateTable(redisClient *c,
                                 redisClient *fc,
                                 int          qcols,
                                 int          cmatchs[],
                                 int          tmatch,
                                 int          j_tbls[],
                                 int          j_cols[],
                                 bool         cname_cflix[]) {
    if (find_table(c->argv[2]->ptr) > 0) return 1;

    char cdefs[MAX_TBL_DEF_SIZE];
    int  slot  = 0;
    for (int i = 0; i < qcols; i++) {
        if (tmatch != -1) {
            cpyColDef(cdefs, &slot, tmatch, cmatchs[i], qcols, i, 
                      0, cname_cflix);
        } else {
            cpyColDef(cdefs, &slot, j_tbls[i], j_cols[i], qcols, i,
                      1, cname_cflix);
        }
    }
    fc->argc    = 3;
    fc->argv[2] = createStringObject(cdefs, slot);
    legacyTableCommand(fc);
    if (!respOk(fc)) {
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    return 1;
}

bool internalCreateTable(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          cmatchs[],
                         int          tmatch) {
    int  idum[1];
    bool bdum[1];
    return _internalCreateTable(c, fc, qcols, cmatchs, tmatch,
                                idum, idum, bdum);
}

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          j_tbls [],
                         int          j_cols[]) {
    bool cname_cflix[MAX_JOIN_INDXS];
    for (int i = 0; i < qcols; i++) {
        for (int j = 0; j < qcols; j++) {
            if (i == j) continue;
            if (!strcmp(Tbl[server.dbid][j_tbls[i]].col_name[j_cols[i]]->ptr,
                        Tbl[server.dbid][j_tbls[j]].col_name[j_cols[j]]->ptr)) {
                cname_cflix[i] = 1;
                break;
            } else {
                cname_cflix[i] = 0;
            }
        }
    }

    int idum[1];
    return _internalCreateTable(c, fc, qcols, idum, -1,
                                j_tbls, j_cols, cname_cflix);
}

bool performStoreCmdOrReply(redisClient *c, redisClient *fc, int sto) {
    /* TODO in terms of aof-logging, smarter to do a "call(fc, cmd);"  */
    (*StorageCommands[sto].func)(fc);
    if (!respNotErr(fc)) {
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    return 1;
}

bool istoreAction(redisClient *c,
                  redisClient *fc,
                  int          tmatch,
                  int          cmatchs[],
                  int          qcols,
                  int          sto,
                  robj        *pko,
                  robj        *row,
                  char        *nname,
                  bool         sub_pk,
                  uint32       nargc) {
    aobj  cols[MAX_COLUMN_PER_TABLE];
    int   totlen = 0;
    for (int i = 0; i < qcols; i++) {
        cols[i]  = getColStr(row, cmatchs[i], pko, tmatch);
        totlen  += cols[i].len;
    }

    char *newrow = NULL;
    fc->argc     = qcols + 1;
    fc->argv[1]  = createStringObject(nname, strlen(nname));/*NEW Objects NAME*/
    //argv[0] NOT NEEDED
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
        if (cols[i].sixbit) free(cols[i].s);
    }

    if (newrow) zfree(newrow);
    return performStoreCmdOrReply(c, fc, sto);
}


#define ISTORE_OPERATION(Q)                                          \
    if (Q) {                                                         \
        addORowToRQList(ll, NULL, row, w->obc, key, tmatch, ctype);  \
    } else {                                                         \
        if (!istoreAction(c, fc, tmatch, cmatchs, qcols, w->sto,     \
                          key, row, nname, sub_pk, nargc)) {         \
            err = 1; /* TODO get err from fc */                      \
            goto istore_err;                                         \
        }                                                            \
    }

static bool checkStoreTypeReply(redisClient *c, int *sto, char *stot) {
    char *x   = strchr(stot, ' ');
    int   len = x ? x - stot : (int)strlen(stot);
    *sto      = -1;
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

//TODO should be in sql.c (used by istore and jstore)
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

    *nlen   = get_token_len(*nname);
    *sub_pk = (StorageCommands[w->sto].argc < 0);
    *nargc  = abs(StorageCommands[w->sto].argc);
    *last   = *nname + *nlen - 1;
    if (*(*last) == '$') { /* means final arg munging */
        *sub_pk  = 1;
        *nargc   = *nargc + 1;
        *(*last) = '\0';
        *nlen    = *nlen - 1;
    }
    if (*nargc != qcols) {
        addReply(c, shared.storagenumargsmismatch);
        return 0;
    }
    if (*sub_pk) *nargc = *nargc - 1;
    return 1;
}

void istoreCommit(redisClient *c,
                  cswc_t      *w,
                  int          tmatch,
                  int          cmatchs[MAX_COLUMN_PER_TABLE],
                  int          qcols) {
    char *nname;
    int   nlen;
    bool  sub_pk;
    int   nargc;
    char *last;
    if (!prepareToStoreReply(c, w, &nname, &nlen,
                             &sub_pk, &nargc, &last, qcols)) return;

    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (w->obc != -1) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][tmatch].col_type[w->obc];
    }

    robj        *argv[STORAGE_MAX_ARGC + 1];
    redisClient *fc = rsql_createFakeClient();
    fc->argv        = argv;

    int     sent  = 0; /* come before first goto */
    bool    cstar = 0;
    bool    err   = 0;
    bool    qed   = 0;
    ulong   card  = 0;
    btSIter *bi   = NULL;
    btSIter *nbi  = NULL;
    if (w->low) { /* RANGE QUERY */
        RANGE_QUERY_LOOKUP_START
            ISTORE_OPERATION(q_pk)
        RANGE_QUERY_LOOKUP_MIDDLE
                ISTORE_OPERATION(q_fk)
        RANGE_QUERY_LOOKUP_END
    } else {    /* IN () QUERY */
        IN_QUERY_LOOKUP_START
            ISTORE_OPERATION(q_pk)
        IN_QUERY_LOOKUP_MIDDLE
                ISTORE_OPERATION(q_fk)
        IN_QUERY_LOOKUP_END
    }

    if (qed) {
        obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
        sent            = sortedOrderByIstore(c, w, fc, tmatch, cmatchs, qcols,
                                              nname, sub_pk, nargc, ctype,
                                              vector, listLength(ll));
        if (sent == 0) err = 1;
        sortedOrderByCleanup(vector, listLength(ll), ctype, 0);
        free(vector);
    }

    if (sub_pk) *last = '$';/* write back in "$" for AOF and Slaves */

istore_err:
    if (nbi)  btReleaseRangeIterator(nbi);
    if (bi)   btReleaseRangeIterator(bi);
    if (ll)   listRelease(ll);
    rsql_freeFakeClient(fc);

    if (err) addReply(c, shared.istorecommit_err);
    else {
        if (w->lim != -1 && (uint32)sent < card) card = sent;
        addReplyLongLong(c, card);
    }
}

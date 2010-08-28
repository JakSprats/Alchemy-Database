/*
 * Implements jstore and join
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "alsosql.h"
#include "redis.h"
#include "bt.h"
#include "bt_iterator.h"
#include "row.h"
#include "index.h"
#include "store.h"
#include "common.h"
#include "join.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

// GLOBALS
extern int Num_tbls;

extern char  CCOMMA;
extern char  CEQUALS;
extern char  CMINUS;
extern char  CPERIOD;
extern char *EQUALS;
extern char *EMPTY_STRING;
extern char *OUTPUT_DELIM;
extern char *COLON;
extern char *COMMA;
extern char *PERIOD;

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern int            Tbl_col_count[MAX_NUM_TABLES];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];

extern robj          *Index_obj     [MAX_NUM_INDICES];
extern int            Index_on_table[MAX_NUM_INDICES];
extern bool           Index_virt    [MAX_NUM_INDICES];

extern stor_cmd StorageCommands[NUM_STORAGE_TYPES];

static unsigned int dictAppendHash(const void *key) {
    unsigned long long ll = (unsigned long long)key;
    return (unsigned int)(ll % UINT_MAX);
}

/* Appending hash, like a linked list but in a hash */
static dictType appendDictType = {
    dictAppendHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    NULL,                      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    dictRedisObjectDestructor  /* val destructor */
};

static robj *createAppendSetObject(void) {
    dict *d = dictCreate(&appendDictType, NULL);
    robj *r = createObject(REDIS_APPEND_SET, d);
    return r;
}

/* Stores per index join results */
static dictType valSetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    dictRedisObjectDestructor  /* val destructor */
};

static robj *createValSetObject(void) {
    dict *d = dictCreate(&valSetDictType, NULL);
    robj *r = createObject(REDIS_VAL_SET, d);
    return r;
}

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
int multiColCheckOrReply(redisClient *c,
                         char        *col_list,
                         int          j_tbls[],
                         int          j_cols[]) {
    int qcols = 0;
    while (1) {
        char *nextc = strchr(col_list, CCOMMA);
        char *nextp = strchr(col_list, CPERIOD);
        if (nextp) {
            *nextp = '\0';
            nextp++;
        } else {
            addReply(c,shared.indextargetinvalid);
            return 0;
        }
        if (nextc) {
            *nextc = '\0';
            nextc++;
        }
        int tmatch       = find_table(col_list);
        if (*nextp == '*') {
            for (int i = 0; i < Tbl_col_count[tmatch]; i++) {
                j_tbls[qcols]  = tmatch;
                j_cols[qcols] = i;
                qcols++;
            }
        } else {
            COLUMN_CHECK_OR_REPLY(nextp,0)
            j_tbls[qcols]  = tmatch;
            j_cols[qcols] = cmatch;
            qcols++;
        }
        if (!nextc) break;
        col_list = nextc;
    }
    return qcols;
}

int parseIndexedColumnListOrReply(redisClient *c, char *ilist, int j_indxs[]) {
    int   n_ind       = 0;
    char *curr_tname  = ilist;
    char *nextc       = ilist;
    while ((nextc = strchr(nextc, CCOMMA))) {
        if (n_ind == MAX_JOIN_INDXS) {
            addReply(c, shared.toomanyindicesinjoin);
            return 0;
        }
        *nextc = '\0';
        char *nextp = strchr(curr_tname, CPERIOD);
        if (!nextp) {
            addReply(c, shared.badindexedcolumnsyntax);
            return 0;
        }
        *nextp = '\0';
        TABLE_CHECK_OR_REPLY(curr_tname, 0)
        nextp++;
        COLUMN_CHECK_OR_REPLY(nextp, 0)
        int imatch = find_index(tmatch, cmatch);
        if (imatch == -1) {
            addReply(c, shared.nonexistentindex);
            return 0;
        }
        j_indxs[n_ind] = imatch;
        n_ind++;
        nextc++;
        curr_tname     = nextc;
    }
    {
        char *nextp = strchr(curr_tname, CPERIOD);
        if (!nextp) {
            addReply(c, shared.badindexedcolumnsyntax);
            return 0;
        }
        *nextp = '\0';
        TABLE_CHECK_OR_REPLY(curr_tname, 0)
        nextp++;
        COLUMN_CHECK_OR_REPLY(nextp, 0)
        int imatch = find_index(tmatch, cmatch);
        if (imatch == -1) {
            addReply(c, shared.nonexistentindex);
            return 0;
        }
        j_indxs[n_ind] = imatch;
        n_ind++;
    }

    if (n_ind < 2) {
        addReply(c, shared.toofewindicesinjoin);
        return 0;
    }
    return n_ind;
}

// JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN
// JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN JOIN
static bool jRowReply(redisClient  *c,
                      redisClient  *fc,
                      char         *reply,
                      int           lvl,
                      int           jind_ncols[],
                      char        **rcols[][MAX_JOIN_COLS],
                      int           rc_lens[][MAX_JOIN_COLS],
                      int           sto) {
    if (sto != -1 && StorageCommands[sto].argc) { // not INSERT
        robj **argv = fc->argv;
        if (StorageCommands[sto].argc < 0) { // pk =argv[1]:rcols[0][0]
            char *pre    = c->argv[2]->ptr;
            argv[1]      = createStringObject(pre, sdslen(pre));
            argv[1]->ptr = sdscatlen(argv[1]->ptr, COLON,   1);
            argv[1]->ptr = sdscatlen(argv[1]->ptr, *rcols[0][0], rc_lens[0][0]);
            if (!lvl) {
                argv[2]  = createStringObject(*rcols[0][1], rc_lens[0][1]);
            } else if (lvl == 1) {
                argv[2]  = createStringObject(*rcols[1][0], rc_lens[1][0]);
            }
            if (StorageCommands[sto].argc == -3) {
                int n    = jind_ncols[lvl] - 1;
                argv[3]  = createStringObject(*rcols[lvl][n], rc_lens[lvl][n]);
            }
        } else {
            argv[2] = createStringObject(*rcols[0][0], rc_lens[0][0]);
            if (StorageCommands[sto].argc == 2) {
                int n   = jind_ncols[lvl] - 1;
                argv[3] = createStringObject(*rcols[lvl][n], rc_lens[lvl][n]);
            }
        }
        if (!performStoreCmdOrReply(c, fc, sto)) return 0;
    } else {
        int slot = 0;
        for (int i = 0; i <= lvl; i++) {
            for (int j = 0; j < jind_ncols[i]; j++) {
                char *s   = *rcols[i][j];
                int rlen  = rc_lens[i][j];
                memcpy(reply + slot, s, rlen);
                slot     += rlen;
                if (sto != -1) { // insert
                    memcpy(reply + slot, COMMA, 1);
                } else {
                    memcpy(reply + slot, OUTPUT_DELIM, 1);
                }
                slot++;
            }
        }
        robj *resp = createStringObject(reply, slot -1);
        if (sto != -1) { // insert
            fc->argv[2] = resp;
            if (!performStoreCmdOrReply(c, fc, sto)) return 0;
        } else {
            addReplyBulk(c, resp);
            decrRefCount(resp);
        }
    }
    return 1;
}

//TODO only var:lvl changes here, the rest could be global vars
//     which avoids copying values on recursive function calls
static bool buildJRowReply(redisClient    *c,
                           redisClient    *fc,
                           char           *reply,
                           robj           *res_set[],
                           int             lvl,
                           int             qcols,
                           robj           *jk,
                           unsigned long  *card,
                           int             jind_ncols[],
                           char          **rcols[][MAX_JOIN_COLS],
                           int             rc_lens[][MAX_JOIN_COLS],
                           int             sto) {
    dictIterator *iter;
    dictEntry    *rde = dictFind(res_set[lvl]->ptr, jk);
    if (rde) {
        robj *setobj = dictGetEntryVal(rde);
        iter = dictGetIterator(setobj->ptr);
    } else { // this table does not have this fk
        for (int j = 0; j < jind_ncols[lvl]; j++) {
            rcols[lvl][j]   = &EMPTY_STRING;
            rc_lens[lvl][j] = 0;
        }
        if (lvl + 1 < qcols) {
            if(!buildJRowReply(c, fc, reply, res_set, lvl + 1, qcols, jk, card,
                               jind_ncols, rcols, rc_lens, sto)) return 0;
        } else {
            if (!jRowReply(c, fc, reply, lvl, jind_ncols, 
                           rcols, rc_lens, sto)) return 0;
            *card = *card + 1;
        }
        return 1;
    }

    dictEntry *sde;
    while ((sde = dictNext(iter)) != NULL) {
        robj *sel = sde->key;
        char *first_entry = (char *)(sel->ptr);
        for (int j = 0; j < jind_ncols[lvl]; j++) {
            rcols[lvl][j]  = (char **)first_entry;
            first_entry   += PTR_SIZE;
            memcpy(&rc_lens[lvl][j], first_entry, UINT_SIZE);
            first_entry   += UINT_SIZE;
        }
        if (lvl + 1 < qcols) {
            if (!buildJRowReply(c, fc, reply, res_set, lvl + 1, qcols, jk, card,
                                jind_ncols, rcols, rc_lens, sto)) return 0;
        } else {
            if (!jRowReply(c, fc, reply, lvl, jind_ncols,
                           rcols, rc_lens, sto)) return 0;;
            *card = *card + 1;
        }
    }
    dictReleaseIterator(iter);
    return 1;
}

static void m_strcpy_len(char *src, int len, char **dest) {
    char *m = malloc(len + 1);
    *dest   = m;
    memcpy(m, src, len);
    m[len]  = '\0';
}
static void joinAddColsFromInd(robj *o,
                               int   qcols,
                               int   j_tbls[],
                               int   itable,
                               int   j_cols[],
                               int   index,
                               int   jind_ncols[],
                               robj *jk,
                               robj *val,
                               int   j_ind_len[],
                               robj *res_set[],
                               bool  virt) {
    aobj  col_resp[MAX_JOIN_INDXS];
    int   row_len = 0;
    int   nresp   = 0;
    robj *row     = virt ? val : btFindVal(o, val, Tbl_col_type[itable][0]);
    robj *key     = virt ? jk  : val;

    // Alsosql understands INT encoding where redis doesnt(dict.c)
    jk = cloneRobj(jk); // copies BtRobj global in bt.c - NOT MEM_LEAK

    for (int j = 0; j < qcols; j++) {
        int tmatch  = j_tbls[j];
        if (tmatch == itable) {
            char *dest;
            col_resp[nresp]  = getColStr(row, j_cols[j], key, tmatch);
            // force_string(INT) comes from a buffer, must be copied here
            char *src         = col_resp[nresp].s;
            m_strcpy_len(src, col_resp[nresp].len, &dest); //freeD N joinGeneric
            col_resp[nresp].s = dest; 
            if (col_resp[nresp].sixbit) free(src);
            row_len         += col_resp[nresp].len + 1; // +1 for OUTPUT_DELIM
            nresp++;
        }
    }
    row_len--; // no DELIM on final col
    if (j_ind_len[index] < row_len) j_ind_len[index] = row_len;

    jind_ncols[index] = nresp;
    char *ind_row     = malloc(nresp * (PTR_SIZE + UINT_SIZE));
    char *o_ind_row   = ind_row;
    for (int j = 0; j < nresp; j++) {
        memcpy(ind_row, &col_resp[j].s, PTR_SIZE);
        ind_row  += PTR_SIZE;
        memcpy(ind_row, &col_resp[j].len, UINT_SIZE);
        ind_row  += UINT_SIZE;
    }
    robj *ind_row_obj = createObject(REDIS_JOINROW, o_ind_row);

    robj      *res_setobj;
    // find row per index key=jk
    dictEntry *rde = dictFind(res_set[index]->ptr, jk);
    if (!rde) {
        res_setobj = createAppendSetObject();
        dictAdd(res_set[index]->ptr, jk, res_setobj);
    } else {
        res_setobj = dictGetEntryVal(rde);
        decrRefCount(jk);
    }

    // store row per index key=jk
    dictAdd(res_setobj->ptr, ind_row_obj, NULL);
}


void joinGeneric(redisClient *c,
                 redisClient *fc,
                 int          j_indxs[],
                 int          j_tbls [],
                 int          j_cols[],
                 int          n_ind,
                 int          qcols,
                 robj        *low,
                 robj        *high,
                 int          sto) {
    // check for STRICT ordering of ilist and [table.column] list
    int col = 0;
    for (int i = 0; i < n_ind; i++) {
        while (j_tbls[col] == Index_on_table[j_indxs[i]]) {
            col++;
            if (col == qcols) break;
        }
    }
    if (col != qcols) {
        addReply(c, shared.indexordermismatchcolumndeclaration);
        return;
    }

    EMPTY_LEN_OBJ
    if (sto == -1) {
        INIT_LEN_OBJ
    }

    int   j_ind_len [MAX_JOIN_INDXS];
    int   jind_ncols[MAX_JOIN_INDXS];
    robj *res_set   [MAX_JOIN_INDXS];
    for (int i = 0; i < n_ind; i++) {
        res_set[i] = createValSetObject();
    }

    for (int i = 0; i < n_ind; i++) {                 // iterate indices
        btEntry    *be, *nbe;
        j_ind_len[i]       = 0;

        int         itable = Index_on_table[j_indxs[i]];
        robj       *o      = lookupKeyRead(c->db, Tbl_name[itable]);
        robj       *ind    = Index_obj [j_indxs[i]];
        bool        virt   = Index_virt[j_indxs[i]];
        robj       *bt     = virt ? o : lookupKey(c->db, ind);
        btIterator *bi     = btGetRangeIterator(bt, low, high, virt);
        while ((be = btRangeNext(bi, 1)) != NULL) {            // iterate btree
            if (virt) {
                robj *jk  = be->key;
                robj *val = be->val;
                joinAddColsFromInd(o, qcols, j_tbls, itable, j_cols, i,
                                   jind_ncols, jk, val, j_ind_len,
                                   res_set, virt);
            } else {
                robj       *jk  = be->key;
                robj       *val = be->val;
                btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
                while ((nbe = btRangeNext(nbi, 1)) != NULL) { // iterate NodeBT
                    robj *key = nbe->key;
                    joinAddColsFromInd(o, qcols, j_tbls, itable, j_cols, i,
                                       jind_ncols, jk, key, j_ind_len,
                                       res_set, virt);
                }
                btReleaseRangeIterator(nbi);
            }
        }
        btReleaseRangeIterator(bi);
    }
    decrRefCount(low);
    decrRefCount(high);

    /* cant join if one table had ZERO rows */
    bool one_empty = 0;
    for (int i = 0; i < n_ind; i++) {
        if (dictSize((dict *)res_set[i]->ptr) == 0) {
            one_empty = 1;
            break;
        }
    }

    if (!one_empty) {
        int   reply_size = 0;
        for (int i = 0; i < n_ind; i++) { // figger maxlen possible 4 joined row
            reply_size += j_ind_len[i] + 1;
        }
        char *reply      = malloc(reply_size); /* freed after while() loop */
    
        char         **rcols  [MAX_JOIN_INDXS][MAX_JOIN_COLS];
        int            rc_lens[MAX_JOIN_INDXS][MAX_JOIN_COLS];
        dictEntry     *pde;
        dictIterator  *pdi = dictGetIterator(res_set[0]->ptr);
        while ((pde = dictNext(pdi)) != NULL) {         // L-join on first index
            dictEntry    *sde;
            robj         *jk     = dictGetEntryKey(pde);
            robj         *setobj = dictGetEntryVal(pde);
            dictIterator *sdi    = dictGetIterator(setobj->ptr);
            while ((sde = dictNext(sdi)) != NULL) {
                robj *sel = sde->key;
                char *first_entry = (char *)(sel->ptr);
                for (int j = 0; j < jind_ncols[0]; j++) {
                    rcols[0][j]  = (char **)first_entry;
                    first_entry += PTR_SIZE;
                    memcpy(&rc_lens[0][j], first_entry, UINT_SIZE);
                    first_entry += UINT_SIZE;
                }
    
                if (!buildJRowReply(c, fc, reply, res_set, 1, n_ind, jk, &card,
                                    jind_ncols, rcols, rc_lens, sto))     break;
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(pdi);
        free(reply);
    }

    // free strdup() from joinAddColsFromInd()
    dictEntry *de, *ide;
    for (int i = 0; i < n_ind; i++) {
        dict         *set = res_set[i]->ptr;
        dictIterator *di  = dictGetIterator(set);
        while((de = dictNext(di)) != NULL) {
            robj         *val  = dictGetEntryVal(de);
            dict         *iset = val->ptr;
            dictIterator *idi  = dictGetIterator(iset);
            while((ide = dictNext(idi)) != NULL) {
                robj *ikey = dictGetEntryKey(ide);
                char *s    = (char *)(ikey->ptr);
                for (int j = 0; j < jind_ncols[i]; j++) {
                    char **x = (char **)s;
                    char **y = (char **)*x;
                    free(y);
                    s += PTR_SIZE + UINT_SIZE;
                }
            }
            dictReleaseIterator(idi);
        }
        dictReleaseIterator(di);
    }

    for (int i = 0; i < n_ind; i++) {
        decrRefCount(res_set[i]);
    }

    if (sto != -1) {
        addReplyLongLong(c, card);
    } else {
        lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
    }
}

void legacyJoinCommand(redisClient *c) {
    int j_indxs[MAX_JOIN_INDXS];
    int j_tbls [MAX_JOIN_INDXS];
    int j_cols [MAX_JOIN_INDXS];
    int n_ind = parseIndexedColumnListOrReply(c, c->argv[1]->ptr, j_indxs);
    if (!n_ind) {
        addReply(c, shared.joinindexedcolumnlisterror);
        return;
    }
    int qcols = multiColCheckOrReply(c, c->argv[2]->ptr, j_tbls, j_cols);
    if (!qcols) {
        addReply(c, shared.joincolumnlisterror);
        return;
    }
    RANGE_CHECK_OR_REPLY(c->argv[3]->ptr)

    joinGeneric(c, NULL, j_indxs, j_tbls, j_cols, n_ind, qcols, low, high, -1);
}

void jstoreCommit(redisClient *c,
                  int          sto,
                  robj        *range,
                  robj        *newname,
                  int          j_indxs[MAX_JOIN_INDXS],
                  int          j_tbls [MAX_JOIN_INDXS],
                  int          j_cols [MAX_JOIN_INDXS],
                  int          n_ind,
                  int          qcols) {
    robj               *argv[STORAGE_MAX_ARGC + 1];
    struct redisClient *fc = createFakeClient();
    fc->argv               = argv;
    fc->argv[1]            = newname;
    if (StorageCommands[sto].argc && abs(StorageCommands[sto].argc) != qcols) {
        addReply(c, shared.storagenumargsmismatch);
        return;
    }
    RANGE_CHECK_OR_REPLY(range->ptr)

    if (!StorageCommands[sto].argc) { // create table first if needed
        if (!createTableFromJoin(c, fc, qcols, j_tbls, j_cols)) {
            freeFakeClient(fc);
            return;
        }
    }

    joinGeneric(c, fc, j_indxs, j_tbls, j_cols, n_ind, qcols, low, high, sto);

    freeFakeClient(fc);
}

// CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP
// CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP

void freeJoinRowObject(robj *o) {
    //RL4 "freeJoinRowObject: %p", o->ptr);
    free(o->ptr);
}
void freeAppendSetObject(robj *o) {
    //RL4 "freeAppendSetObject: %p", o->ptr);
    dictRelease((dict*) o->ptr);
}
void freeValSetObject(robj *o) {
    //RL4 "freeValSetObject: %p", o->ptr);
    dictRelease((dict*) o->ptr);
}


// PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN
// PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN PARALLEL_JOIN
#if 0
static void joinGetColsFromRow(robj *row,
                               robj *key,
                               int   itable,
                               int   qcols, 
                               int   j_tbls[],
                               int   j_cols[]) {
    aobj  col_resp[MAX_JOIN_INDXS];
    int   nresp   = 0;

    for (int j = 0; j < qcols; j++) {
        int tmatch  = j_tbls[j];
        if (tmatch == itable) {
            // save pointer
            col_resp[nresp]  = getColStr(row, j_cols[j], key, tmatch);
            // force_string of INT comes from a buffer, gets overwritten
            char *x           = col_resp[nresp].s;
            col_resp[nresp].s = strdup(x); // freeD @end of joinGeneric
RL4 "%d: tbl: %d col: %d -> (%s)", nresp, itable, j_cols[j], col_resp[nresp].s);
            if (col_resp[nresp].sixbit) free(x);
            nresp++;
        }
    }
}

    btIterator *iter[MAX_JOIN_INDXS];
    for (int i = 0; i < n_ind; i++) {                 // iterate indices
        int         itable = Index_on_table[j_indxs[i]];
        robj       *o      = lookupKeyRead(c->db, Tbl_name[itable]);
        robj       *ind    = Index_obj [j_indxs[i]];
        bool        virt   = Index_virt[j_indxs[i]];
        robj       *bt     = virt ? o : lookupKey(c->db, ind);
        iter[i]            = btGetGenericRangeIterator(bt, low, high, virt, i);
RL4 "%d: iter: %p", i, iter[i]);
    }


    int curr_ind = 0;
    while(1) {
RL4 "curr_ind: %d", curr_ind);
        int      itable = Index_on_table[j_indxs[curr_ind]];
        bool     virt   = Index_virt[j_indxs[curr_ind]];
        btEntry *be     = btRangeNext(iter[curr_ind], 1);
        if (!be) { /* this index is done */
            curr_ind++;
            if (curr_ind == n_ind) break;
        }

        robj *row;
        if (virt) {
            robj *jk  = be->key;
            row       = be->val;
            joinGetColsFromRow(row, jk, itable, qcols, j_tbls, j_cols);
        } else {
            btEntry    *nbe;
            robj       *o   = lookupKeyRead(c->db, Tbl_name[itable]);
            robj       *jk  = be->key;
            robj       *val = be->val;
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) { // iterate NodeBT
                robj *key = nbe->key;
                row       = btFindVal(o, key, Tbl_col_type[itable][0]);
                joinGetColsFromRow(row, jk, itable, qcols, j_tbls, j_cols);
            }
        }
    }

    addReply(c, shared.ok);
    return;
#endif

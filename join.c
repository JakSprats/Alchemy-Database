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
extern struct redisServer server;

// GLOBALS
extern int Num_tbls[MAX_NUM_DB];

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

extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

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
            for (int i = 0; i < Tbl[server.dbid][tmatch].col_count; i++) {
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

/* NOTE these could be in jRowReply but then they get stack allocated often */
static char **Jrcols  [MAX_JOIN_INDXS * MAX_JOIN_COLS];
static int    Jrc_lens[MAX_JOIN_INDXS * MAX_JOIN_COLS];

/* NOTE these could be in joinGeneric but then they get stack allocated often */
static char **Rcols  [MAX_JOIN_INDXS][MAX_JOIN_COLS];
static int    Rc_lens[MAX_JOIN_INDXS][MAX_JOIN_COLS];

typedef struct jqo {
    int t;
    int i;
    int c;
    int n;
} jqo_t;

static int cmp_jqo(const void *a, const void *b) {
    jqo_t *ta = (jqo_t *)a;
    jqo_t *tb = (jqo_t *)b;
    return (ta->i == tb->i) ? 0 : (ta->i < tb->i) ? -1 : 1;
}

typedef struct jrow_reply {
    redisClient  *c;
    redisClient  *fc;
    int          *jind_ncols;
    int           qcols;
    int           sto;
    bool          sub_pk;
    int           nargc;
    robj         *newname;
    jqo_t        *col_sort_order;
    bool          reordered;
    char         *reply;
} jrow_reply_t;

static bool jRowStore(jrow_reply_t  *r) {
    robj **argv = r->fc->argv;
    argv[1]     = cloneRobj(r->newname);
    int n = 1;
    if (r->sub_pk) { // pk =argv[1]:Rcols[0][0]
        argv[1]->ptr = sdscatlen(argv[1]->ptr, COLON,   1);
        argv[1]->ptr = sdscatlen(argv[1]->ptr, *Jrcols[0], Jrc_lens[0]);
        argv[2]  = createStringObject(*Jrcols[1], Jrc_lens[1]);
        n++;
    } else {
        argv[2]  = createStringObject(*Jrcols[0], Jrc_lens[0]);
    }
    if (r->nargc > 1) {
        argv[3]  = createStringObject(*Jrcols[n], Jrc_lens[n]);
    }
    return performStoreCmdOrReply(r->c, r->fc, r->sto);
}

static bool jRowReply(jrow_reply_t  *r, int lvl) {
    int cnt = 0;
    for (int i = 0; i <= lvl; i++) { /* sort columns back to queried-order */
        for (int j = 0; j < r->jind_ncols[i]; j++) {
            int n       = r->reordered ? r->col_sort_order[cnt].n : cnt;
            Jrcols[n]   = Rcols[i][j];
            Jrc_lens[n] = Rc_lens[i][j];
            cnt++;
        }
    }

    if (r->sto != -1 && StorageCommands[r->sto].argc) { // not INSERT
        return jRowStore(r);
    } else {
        int slot = 0;
        for (int i = 0; i < cnt; i++) {
            char *s   = *Jrcols[i];
            int rlen  = Jrc_lens[i];
            memcpy(r->reply + slot, s, rlen);
            slot     += rlen;
            if (r->sto != -1) { // insert
                memcpy(r->reply + slot, COMMA, 1);
            } else {
                memcpy(r->reply + slot, OUTPUT_DELIM, 1);
            }
            slot++;
        }
        robj *resp = createStringObject(r->reply, slot -1);
        if (r->sto != -1) { // insert
            r->fc->argc    = 3;
            r->fc->argv[1] = cloneRobj(r->newname);
            r->fc->argv[2] = resp;
            if (!performStoreCmdOrReply(r->c, r->fc, r->sto)) return 0;
        } else {
            addReplyBulk(r->c, resp);
            decrRefCount(resp);
        }
        return 1;
    }
}

typedef struct build_jrow_reply {
    jrow_reply_t   j;
    int            n_ind;
    robj          *jk;
    unsigned long *card;
} build_jrow_reply_t;

static bool buildJRowReply(build_jrow_reply_t  *b,
                           int                  lvl,
                           robj                *rset[MAX_JOIN_INDXS]) {
    dictIterator *iter;
    dictEntry    *rde = dictFind(rset[lvl]->ptr, b->jk);
    if (rde) {
        robj *setobj = dictGetEntryVal(rde);
        iter         = dictGetIterator(setobj->ptr);
    } else { // this table does not have this column
        for (int j = 0; j < b->j.jind_ncols[lvl]; j++) {
            Rcols[lvl][j]   = &EMPTY_STRING;
            Rc_lens[lvl][j] = 0;
        }
        if (lvl + 1 < b->n_ind) {
            if(!buildJRowReply(b, lvl + 1, rset)) return 0;
        } else {
            if (!jRowReply(&(b->j), lvl)) return 0;
            *b->card = *b->card + 1;
        }
        return 1;
    }

    dictEntry *sde;
    while ((sde = dictNext(iter)) != NULL) {
        robj *sel         = sde->key;
        char *first_entry = (char *)(sel->ptr);
        for (int j = 0; j < b->j.jind_ncols[lvl]; j++) {
            Rcols[lvl][j]  = (char **)first_entry;
            first_entry        += PTR_SIZE;
            memcpy(&(Rc_lens[lvl][j]), first_entry, UINT_SIZE);
            first_entry        += UINT_SIZE;
        }
        if (lvl + 1 < b->n_ind) {
            if(!buildJRowReply(b, lvl + 1, rset)) return 0;
        } else {
            if (!jRowReply(&(b->j), lvl)) return 0;
            *b->card = *b->card + 1;
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

typedef struct join_add_cols {
    robj *o;
    int   qcols;
    int  *j_tbls;
    int   itable;
    int  *j_cols;
    int   index;
    int  *jind_ncols;
    robj *jk;
    robj *val;
    int  *j_ind_len;
    bool  virt;
    bt   *jbtr;
} join_add_cols_t;

static void joinAddColsFromInd(join_add_cols_t *a, robj *rset[]) {
    aobj  col_resp[MAX_JOIN_INDXS];
    int   row_len = 0;
    int   nresp   = 0;
    robj *row     = a->virt ? a->val :
                             btFindVal(a->o, a->val,
                                       Tbl[server.dbid][a->itable].col_type[0]);
    robj *key     = a->virt ? a->jk  : a->val;

    // Alsosql understands INT encoding where redis doesnt(dict.c)
    a->jk = cloneRobj(a->jk); // copies BtRobj global in bt.c - NOT MEM_LEAK

    for (int i = 0; i < a->qcols; i++) {
        int tmatch  = a->j_tbls[i];
        if (tmatch == a->itable) {
            char *dest;
            col_resp[nresp]    = getColStr(row, a->j_cols[i], key, tmatch);
            // force_string(INT) comes from a buffer, must be copied here
            char *src          = col_resp[nresp].s;
            m_strcpy_len(src, col_resp[nresp].len, &dest); //freeD N joinGeneric
            col_resp[nresp].s  = dest; 
            if (col_resp[nresp].sixbit) free(src);
            row_len           += col_resp[nresp].len + 1; // +1 for OUTPUT_DELIM
            nresp++;
        }
    }
    row_len--; // no DELIM on final col
    if (a->j_ind_len[a->index] < row_len) a->j_ind_len[a->index] = row_len;

    a->jind_ncols[a->index] = nresp;
    char *ind_row     = malloc(nresp * (PTR_SIZE + UINT_SIZE));
    char *o_ind_row   = ind_row;
    for (int i = 0; i < nresp; i++) {
        memcpy(ind_row, &col_resp[i].s, PTR_SIZE);
        ind_row  += PTR_SIZE;
        memcpy(ind_row, &col_resp[i].len, UINT_SIZE);
        ind_row  += UINT_SIZE;
    }

    // NOTE: for clarification -> TODO unify, refactor, etc..
    //       1st index: BT         of lists
    //       2nd index: ValSetDict of AppendSets
    if (a->index == 0) { // first joined index is BTREE to be sorted
        joinRowEntry  k;
        k.key           = a->jk;
        joinRowEntry *x = btJoinFindVal(a->jbtr, &k);
        if (x) {
            list *ll = (list *)x->val;
            listAddNodeTail(ll, o_ind_row);
        } else {
            joinRowEntry *jre = (joinRowEntry *)malloc(sizeof(joinRowEntry));
            list         *ll  = listCreate();
            jre->key          = a->jk;
            jre->val          = (void *)ll;
            listAddNodeHead(ll, o_ind_row);
            btJoinAddRow(a->jbtr, jre);
        }
    } else { // rest of the joined indices are redis DICTs for speed
        robj      *res_setobj;
        robj      *ind_row_obj = createObject(REDIS_JOINROW, o_ind_row);
        dictEntry *rde         = dictFind(rset[a->index]->ptr, a->jk);
        if (!rde) {
            // create "list"
            res_setobj = createAppendSetObject();
            dictAdd(rset[a->index]->ptr, a->jk, res_setobj);
        } else {
            res_setobj = dictGetEntryVal(rde);
            decrRefCount(a->jk);
        }
        dictAdd(res_setobj->ptr, ind_row_obj, NULL); // push to (list per jk)
    }
}

static void freeIndRow(char *s, int num_cols) {
    for (int j = 0; j < num_cols; j++) {
        char **x = (char **)s;
        char **y = (char **)*x;
        free(y);
        s += PTR_SIZE + UINT_SIZE;
    }
}

static void freeListOfIndRow(list *ll, int num_cols) {
    listIter *iter;
    listNode *node;
    iter = listGetIterator(ll, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        freeIndRow(node->value, num_cols);
        free(node->value);      /* free ind_row */
    }
    listReleaseIterator(iter);
    listRelease(ll);
}

void freeDictOfIndRow(dict *d, int num_cols) {
    dictEntry    *ide;
    dictIterator *idi  = dictGetIterator(d);
    while((ide = dictNext(idi)) != NULL) {
        robj *ikey = dictGetEntryKey(ide);
        char *s    = (char *)(ikey->ptr);
        freeIndRow(s, num_cols);
    }
    dictReleaseIterator(idi);
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
                 int          sto,
                 bool         sub_pk,
                 int          nargc,
                 robj        *newname) {

    /* sort queried-columns to queried-indices */
    jqo_t ocol_sort_order[MAX_COLUMN_PER_TABLE];
    jqo_t col_sort_order [MAX_COLUMN_PER_TABLE];
    for (int i = 0; i < qcols; i++) {
        for (int j = 0; j < n_ind; j++) {
            if (j_tbls[i] == Index_on_table[j_indxs[j]]) {
                col_sort_order[i].t = j_tbls[i];
                col_sort_order[i].i = j;
                col_sort_order[i].c = j_cols[i];
                col_sort_order[i].n = i;
            }
        }
    }
    memcpy(&ocol_sort_order, &col_sort_order, sizeof(jqo_t) * qcols);
    qsort(&col_sort_order, qcols, sizeof(jqo_t), cmp_jqo);

    /* reorder queried-columns to queried-indices, will sort @ output time */
    bool reordered = 0;
    for (int i = 0; i < qcols; i++) {
        if (j_tbls[i] != col_sort_order[i].t ||
            j_cols[i] != col_sort_order[i].c) {
                reordered = 1;
                j_tbls[i] = col_sort_order[i].t;
                j_cols[i] = col_sort_order[i].c;
        }
    }


    EMPTY_LEN_OBJ
    if (sto == -1) {
        INIT_LEN_OBJ
    }

    int    j_ind_len [MAX_JOIN_INDXS];
    int    jind_ncols[MAX_JOIN_INDXS];

    uchar  pktype = Tbl[server.dbid][Index_on_table[j_indxs[0]]].col_type[0];
    bt    *jbtr   = createJoinResultSet(pktype);

    robj  *rset[MAX_JOIN_INDXS];
    for (int i = 1; i < n_ind; i++) {
        rset[i] = createValSetObject();
    }

    join_add_cols_t jac; /* these dont change in the loop below */
    jac.qcols      = qcols;
    jac.j_tbls     = j_tbls;
    jac.j_cols     = j_cols;
    jac.jind_ncols = jind_ncols;
    jac.j_ind_len  = j_ind_len;
    jac.jbtr       = jbtr;
    
    for (int i = 0; i < n_ind; i++) {                 // iterate indices
        btEntry    *be, *nbe;
        j_ind_len[i] = 0;
        jac.index    = i;

        jac.itable    = Index_on_table[j_indxs[i]];
        jac.o         = lookupKeyRead(c->db, Tbl[server.dbid][jac.itable].name);
        robj *ind     = Index_obj [j_indxs[i]];
        jac.virt      = Index_virt[j_indxs[i]];
        robj *bt      = jac.virt ? jac.o : lookupKey(c->db, ind);
        btStreamIterator *bi = btGetRangeIterator(bt, low, high, jac.virt);
        while ((be = btRangeNext(bi, 1)) != NULL) {            // iterate btree
            if (jac.virt) {
                jac.jk  = be->key;
                jac.val = be->val;
                joinAddColsFromInd(&jac, rset);
            } else {
                jac.jk  = be->key;
                robj             *val = be->val;
                btStreamIterator *nbi = btGetFullRangeIterator(val, 0, 0);
                while ((nbe = btRangeNext(nbi, 1)) != NULL) { // iterate NodeBT
                    jac.val = nbe->key;
                    joinAddColsFromInd(&jac, rset);
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
    if (jbtr->numkeys == 0) one_empty = 1;
    else {
        for (int i = 1; i < n_ind; i++) {
            if (dictSize((dict *)rset[i]->ptr) == 0) {
                one_empty = 1;
                break;
            }
        }
    }

    if (!one_empty) {
        int   reply_size = 0;
        for (int i = 0; i < n_ind; i++) { // figger maxlen possible 4 joined row
            reply_size += j_ind_len[i] + 1;
        }
        char *reply = malloc(reply_size); /* freed after while() loop */
    
        build_jrow_reply_t bjr; /* none of these change during a join */
        bjr.j.c              = c;
        bjr.j.fc             = fc;
        bjr.j.jind_ncols     = jind_ncols;
        bjr.j.reply          = reply;
        bjr.j.sto            = sto;
        bjr.j.sub_pk         = sub_pk;
        bjr.j.nargc          = nargc; 
        bjr.j.newname        = newname;
        bjr.j.col_sort_order = col_sort_order;
        bjr.j.reordered      = reordered;
        bjr.j.qcols          = qcols;
        bjr.n_ind            = n_ind;
        bjr.card             = &card;

        joinRowEntry *be;
        btIterator   *bi = btGetJoinFullRangeIterator(jbtr, pktype);
        while ((be = btJoinRangeNext(bi, pktype)) != NULL) { /* iter BT */
            listIter *iter;
            listNode *node;
            bjr.jk       = be->key;
            list     *ll = (list *)be->val;
            iter         = listGetIterator(ll, AL_START_HEAD);
            while((node = listNext(iter)) != NULL) {        /* iter LIST */
                robj *sel         = node->value;
                char *first_entry = (char *)(sel);
                for (int j = 0; j < jind_ncols[0]; j++) {
                    Rcols[0][j]  = (char **)first_entry;
                    first_entry += PTR_SIZE;
                    memcpy(&Rc_lens[0][j], first_entry, UINT_SIZE);
                    first_entry += UINT_SIZE;
                }
    
                if (!buildJRowReply(&bjr, 1, rset)) break;
            }
            listReleaseIterator(iter);
        }
        btReleaseJoinRangeIterator(bi);

        free(reply);
    }

    // free strdup()s from joinAddColsFromInd()
    btJoinRelease(jbtr, jind_ncols[0], freeListOfIndRow);

    // free strdup()s from joinAddColsFromInd()
    dictEntry *de;
    for (int i = 1; i < n_ind; i++) {
        dict         *set = rset[i]->ptr;
        dictIterator *di  = dictGetIterator(set);
        while((de = dictNext(di)) != NULL) {
            robj *val  = dictGetEntryVal(de);
            dict *iset = val->ptr;
            freeDictOfIndRow(iset, jind_ncols[i]);
        }
        dictReleaseIterator(di);
    }

    for (int i = 1; i < n_ind; i++) {
        decrRefCount(rset[i]);
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

    joinGeneric(c, NULL, j_indxs, j_tbls, j_cols, n_ind, qcols, low, high,
                -1, 0, 0, NULL);
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
    struct redisClient *fc = rsql_createFakeClient();
    fc->argv               = argv;

    bool sub_pk    = (StorageCommands[sto].argc < 0);
    int  nargc     = abs(StorageCommands[sto].argc);
    sds  last_argv = c->argv[c->argc - 1]->ptr;
    if (nargc) { /* if NOT INSERT check nargc */
        if (last_argv[sdslen(last_argv) -1] == '$') {
            sub_pk = 1; 
            nargc++;
            last_argv[sdslen(last_argv) -1] = '\0';
            sdsupdatelen(last_argv);
        }
        if (nargc != qcols) {
            addReply(c, shared.storagenumargsmismatch);
            return;
        }
        if (sub_pk) nargc--;
    }
    RANGE_CHECK_OR_REPLY(range->ptr)

    if (!StorageCommands[sto].argc) { // create table first if needed
        fc->argv[1] = cloneRobj(newname);
        if (!createTableFromJoin(c, fc, qcols, j_tbls, j_cols)) {
            freeFakeClient(fc);
            return;
        }
    }

    joinGeneric(c, fc, j_indxs, j_tbls, j_cols, n_ind, qcols, low, high, sto,
                sub_pk, nargc, newname);

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

/*
 * Implements join-store and join
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

#include "bt.h"
#include "bt_iterator.h"
#include "row.h"
#include "index.h"
#include "store.h"
#include "orderby.h"
#include "parser.h"
#include "alsosql.h"
#include "common.h"
#include "join.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern char *EMPTY_STRING;
extern char *OUTPUT_DELIM;
extern char *COLON;

extern r_tbl_t  Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_DB][MAX_NUM_INDICES];

/* PROTOTYPES */
static void freeListOfIndRow(list *ll, int num_cols, bool is_ob);
static void freeDictOfIndRow(dict *d, int num_cols, bool is_ob);


/* appends, not really a hashing function ... always evals to new key */
static unsigned int dictAppendHash(const void *key) {
    ull ll = (ull)key;
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

/* NOTE these could be in jRowReply but then they get stack allocated often */
char **Jrcols  [MAX_JOIN_INDXS * MAX_JOIN_COLS];
int    Jrc_lens[MAX_JOIN_INDXS * MAX_JOIN_COLS];

/* NOTE these could be in joinGeneric but then they get stack allocated often */
static char **Rcols  [MAX_JOIN_INDXS][MAX_JOIN_COLS];
static int    Rc_lens[MAX_JOIN_INDXS][MAX_JOIN_COLS];

static bool   Order_by         = 0;
char         *Order_by_col_val = NULL;

static int cmp_jqo(const void *a, const void *b) {
    jqo_t *ta = (jqo_t *)a;
    jqo_t *tb = (jqo_t *)b;
    return (ta->i == tb->i) ? 0 : (ta->i < tb->i) ? -1 : 1;
} /* TODO do "ta->i - tb->a" */


static bool jRowReply(jrow_reply_t *r, int lvl) {
    int cnt = 0;
    for (int i = 0; i <= lvl; i++) { /* sort columns back to queried-order */
        for (int j = 0; j < r->jind_ncols[i]; j++) {
            int n       = r->reordered ? r->csort_order[cnt].n : cnt;
            Jrcols[n]   = Rcols[i][j];
            Jrc_lens[n] = Rc_lens[i][j];
            cnt++;
        }
    }

    if (r->sto != -1) { /* JOIN_STORE */
        if (Order_by) {
            prepare_jRowStore(r);
            robj **argv = copyArgv(r->fc->argv, r->fc->argc);
            addJoinOutputRowToList(r, argv);
            return 1;
        } else {
            return jRowStore(r);
        }
    } else {
        if (!Order_by && r->cstar) return 1; /* NOOP just count */
        int slot = 0;
        for (int i = 0; i < cnt; i++) {
            char *s   = *Jrcols[i];
            int rlen  = Jrc_lens[i];
            memcpy(r->reply + slot, s, rlen);
            slot     += rlen;
            memcpy(r->reply + slot, OUTPUT_DELIM, 1);
            slot++;
        }
        robj *resp = createStringObject(r->reply, slot -1);

        if (Order_by) {
            addJoinOutputRowToList(r, resp);
        } else {
            addReplyBulk(r->c, resp);
            decrRefCount(resp);
        }
        return 1;
    }
}

static bool buildJRowReply(build_jrow_reply_t  *b,
                           int                  lvl,
                           robj                *rset[MAX_JOIN_INDXS]) {
    dictIterator *iter;
    dictEntry    *rde = dictFind(rset[lvl]->ptr, b->jk);
    if (rde) {
        robj *setobj = dictGetEntryVal(rde);
        iter         = dictGetIterator(setobj->ptr);
    } else { // this table does not have this column
        if (b->j.obt == Index[server.dbid][b->j_indxs[lvl]].table) {
            Order_by_col_val = NULL;
        }
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
        char *first_entry;
        robj *item = sde->key;
        if (b->j.obt == Index[server.dbid][b->j_indxs[lvl]].table) {
            obsl_t *ob       = (obsl_t *)item->ptr;
            Order_by_col_val = (char *)ob->val;
            first_entry      = (char *)ob->row;
        } else {
            first_entry      = (char *)item->ptr;
        }
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

static void m_strcpy_len(char *src, int len, char **dest) {
    char *m = malloc(len + 1);
    *dest   = m;
    memcpy(m, src, len);
    m[len]  = '\0';
}

/* makes Copy - must be freeD */
char *getCopyColStr(robj   *row,
                    int     cmatch,
                    robj   *okey,
                    int     tmatch,
                    uint32 *len) {
    char   *dest;
    aobj    ao  = getColStr(row, cmatch, okey, tmatch);
    // force_string(INT) comes from a buffer, must be copied here
    char   *src = ao.s;
    m_strcpy_len(src, ao.len, &dest); /* freeD in freeIndRowEntries */
    if (ao.sixbit) free(src);
    *len        = ao.len;
    return dest;
}

typedef struct char_uint32 {
    char *s;
    uint32 len;
} cu32_t;

/* EXPLANATION: The join algorithm
    0.) for each table in join
    1.) pointers to ALL candidate columns & lengths are saved to a tuplet
    2.) this tuplet is then stored in a (Btree or HashTable) for L8R joining
  NOTE: (CON) This is only efficient for small table joins [if at all :)]
*/
static void joinAddColsFromInd(join_add_cols_t *a,
                               robj            *rset[],
                               int              obt,
                               int              obc) {
    cu32_t  cresp[MAX_JOIN_INDXS];
    int     row_len = 0;
    int     nresp   = 0;
    uchar   pktype  = Tbl[server.dbid][a->itable].col_type[0];
    robj   *row     = a->virt ? a->val : btFindVal(a->o, a->val, pktype);
    robj   *key     = a->virt ? a->jk  : a->val;

    // Redisql understands INT encoding where redis doesnt(dict.c)
    a->jk = cloneRobj(a->jk); // copies BtRobj global in bt.c - NOT MEM_LEAK

    for (int i = 0; i < a->qcols; i++) {
        int tmatch = a->j_tbls[i];
        if (tmatch == a->itable) {
            cresp[nresp].s  = getCopyColStr(row, a->j_cols[i], key, tmatch,
                                            &cresp[nresp].len);
            row_len        += cresp[nresp].len + 1; // +1 for OUTPUT_DELIM
            nresp++;
        }
    }
    row_len--; // no DELIM on final col
    if (a->j_ind_len[a->index] < row_len) a->j_ind_len[a->index] = row_len;

    a->jind_ncols[a->index] = nresp;
    char *ind_row           = malloc(nresp * (PTR_SIZE + UINT_SIZE));
    char *list_val          = ind_row;
    for (int i = 0; i < nresp; i++) { /* fill in ind_row */
        memcpy(ind_row, &cresp[i].s, PTR_SIZE);
        ind_row += PTR_SIZE;
        memcpy(ind_row, &cresp[i].len, UINT_SIZE);
        ind_row += UINT_SIZE;
    }

    /* only index matching "obt" needs to have ORDER_BY column */
    /* TODO does this for every column for this index - only 1st is needed */
    if (obt == a->itable) {
        uint32  vlen;
        obsl_t *ob = (obsl_t *)malloc(sizeof(obsl_t)); /*freed N freeIndRow */
        ob->val    = getCopyColStr(row, obc, key, obt, &vlen);
        ob->row    = list_val;
        list_val   = (char *)ob; /* in ORDER BY list_val -> obsl_t */
    }

    // TODO unify, refactor, etc..
    //
    // NOTE: (clarification of clusterfuct implementation)
    //       1st index: BT         of Lists
    //       2nd index: ValSetDict of AppendSets (should be List)
    if (a->index == 0) { // first joined index needs BTREE (i.e. sorted)
        joinRowEntry  k;
        k.key           = a->jk;
        joinRowEntry *x = btJoinFindVal(a->jbtr, &k);

        if (x) {
            list *ll = (list *)x->val;
            listAddNodeTail(ll, list_val);
        } else {
            joinRowEntry *jre = (joinRowEntry *)malloc(sizeof(joinRowEntry));
            list         *ll  = listCreate(); /* listRelease freeListOfIndRow*/
            jre->key          = a->jk;
            jre->val          = (void *)ll;
            listAddNodeHead(ll, list_val);
            btJoinAddRow(a->jbtr, jre);
        }
    } else { // rest of the joined indices are redis DICTs for speed
        robj      *res_setobj;
        robj      *ind_row_obj = createObject(REDIS_JOINROW, list_val);
        dictEntry *rde         = dictFind(rset[a->index]->ptr, a->jk);
        if (!rde) {
            // create AppendSet "list"
            res_setobj = createAppendSetObject();
            dictAdd(rset[a->index]->ptr, a->jk, res_setobj);
        } else {
            res_setobj = dictGetEntryVal(rde);
            decrRefCount(a->jk);
        }
        dictAdd(res_setobj->ptr, ind_row_obj, NULL); // push to (list per jk)
    }
}

void joinGeneric(redisClient *c,
                 redisClient *fc,
                 jb_t        *jb,
                 bool         sub_pk,
                 int          nargc) {
    Order_by         = (jb->w.obt != -1);
    Order_by_col_val = NULL;

    /* sort queried-columns to queried-indices */
    jqo_t o_csort_order[MAX_COLUMN_PER_TABLE];
    jqo_t csort_order  [MAX_COLUMN_PER_TABLE];
    for (int i = 0; i < jb->qcols; i++) {
        for (int j = 0; j < jb->n_ind; j++) {
            if (jb->j_tbls[i] == Index[server.dbid][jb->j_indxs[j]].table) {
                csort_order[i].t = jb->j_tbls[i];
                csort_order[i].i = j;
                csort_order[i].c = jb->j_cols[i];
                csort_order[i].n = i;
            }
        }
    }
    memcpy(&o_csort_order, &csort_order, sizeof(jqo_t) * jb->qcols);
    qsort(&csort_order, jb->qcols, sizeof(jqo_t), cmp_jqo);

    /* reorder queried-columns to queried-indices, will sort @ output time */
    bool reordered = 0;
    for (int i = 0; i < jb->qcols; i++) {
        if (jb->j_tbls[i] != csort_order[i].t ||
            jb->j_cols[i] != csort_order[i].c) {
                reordered = 1;
                jb->j_tbls[i] = csort_order[i].t;
                jb->j_cols[i] = csort_order[i].c;
        }
    }

    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (Order_by) { /* ORDER BY logic */
        ll    = listCreate();
        ctype = Tbl[server.dbid][jb->w.obt].col_type[jb->w.obc];
    }

    EMPTY_LEN_OBJ
    if (jb->w.sto == -1) { INIT_LEN_OBJ } /* JoinStore can throw nested errs */

    int    j_ind_len [MAX_JOIN_INDXS];
    int    jind_ncols[MAX_JOIN_INDXS];

    uchar  pk1type = jb->w.inl? COL_TYPE_STRING :
                       Tbl[server.dbid]
                         [Index[server.dbid][jb->j_indxs[0]].table].col_type[0];
    bt    *jbtr    = createJoinResultSet(pk1type);

    robj  *rset[MAX_JOIN_INDXS];
    for (int i = 1; i < jb->n_ind; i++) {
        rset[i] = createValSetObject();
    }

    join_add_cols_t jc; /* these dont change in the loop below */
    jc.qcols      = jb->qcols;
    jc.j_tbls     = jb->j_tbls;
    jc.j_cols     = jb->j_cols;
    jc.jind_ncols = jind_ncols;
    jc.j_ind_len  = j_ind_len;
    jc.jbtr       = jbtr;
    
    for (int i = 0; i < jb->n_ind; i++) { /* iterate join indices */
        btEntry    *be, *nbe;
        j_ind_len[i]  = 0;
        jc.index      = i;
        jc.itable     = Index[server.dbid][jb->j_indxs[i]].table;
        jc.o          = lookupKeyRead(c->db, Tbl[server.dbid][jc.itable].name);
        jc.virt       = Index[server.dbid][jb->j_indxs[i]].virt;
        robj *ind     = Index[server.dbid][jb->j_indxs[i]].obj;
     
        if (jb->w.low) {     /* RANGE QUERY */
            robj    *bt = jc.virt ? jc.o : lookupKey(c->db, ind);
            btSIter *bi = btGetRangeIterator(bt, jb->w.low, jb->w.high,
                                             jc.virt);
            while ((be = btRangeNext(bi)) != NULL) {
                if (jc.virt) {
                    jc.jk  = be->key;
                    jc.val = be->val;
                    joinAddColsFromInd(&jc, rset, jb->w.obt, jb->w.obc);
                } else {
                    jc.jk                 = be->key;
                    robj             *val = be->val;
                    btSIter *nbi = btGetFullRangeIterator(val, 0);
                    while ((nbe = btRangeNext(nbi)) != NULL) {
                        jc.val = nbe->key;
                        joinAddColsFromInd(&jc, rset, jb->w.obt, jb->w.obc);
                    }
                    btReleaseRangeIterator(nbi);
                }
            }
            btReleaseRangeIterator(bi);
        } else {             /* IN() QUERY */
            listNode *ln;
            listIter *li  = listGetIterator(jb->w.inl, AL_START_HEAD);
            if (jc.virt) {
                uchar pktype = Tbl[server.dbid][jc.itable].col_type[0];
                while((ln = listNext(li)) != NULL) {
                    jc.jk  = ln->value;
                    jc.val = btFindVal(jc.o, jc.jk, pktype);
                    if (jc.val) joinAddColsFromInd(&jc, rset,
                                                  jb->w.obt, jb->w.obc);
                }
            } else {
                btSIter *nbi;
                int   ind_col = (int)Index[server.dbid][jb->j_indxs[i]].column;
                bool  fktype  = Tbl[server.dbid][jc.itable].col_type[ind_col];
                robj *ibt     = lookupKey(c->db, ind);   
                while((ln = listNext(li)) != NULL) {
                    jc.jk     = ln->value;
                    robj *val = btIndFindVal(ibt->ptr, jc.jk, fktype);
                    if (val) {
                        nbi = btGetFullRangeIterator(val, 0);
                        while ((nbe = btRangeNext(nbi)) != NULL) {
                            jc.val = nbe->key;
                            joinAddColsFromInd(&jc, rset, jb->w.obt, jb->w.obc);
                        }
                        btReleaseRangeIterator(nbi);
                    }
                }
            }
            listReleaseIterator(li);
        }
    }

    /* cant join if one table had ZERO rows */
    bool one_empty = 0;
    if (jbtr->numkeys == 0) one_empty = 1;
    else {
        for (int i = 1; i < jb->n_ind; i++) {
            if (dictSize((dict *)rset[i]->ptr) == 0) {
                one_empty = 1;
                break;
            }
        }
    }

    bool        err   = 0;
    int         sent  = 0;
    btIterator *bi    = NULL; /* B4 GOTO */
    char       *reply = NULL; /* B4 GOTO */
    if (!one_empty) {
        int reply_size = 0;
        for (int i = 0; i < jb->n_ind; i++) { // get maxlen possbl 4 joined row
            reply_size += j_ind_len[i] + 1;
        }
        reply = malloc(reply_size); /* freed after while() loop */
    
        build_jrow_reply_t bjr; /* none of these change during a join */
        bzero(&bjr, sizeof(build_jrow_reply_t));
        bjr.j.c           = c;
        bjr.j.fc          = fc;
        bjr.j.jind_ncols  = jind_ncols;
        bjr.j.reply       = reply;
        bjr.j.sto         = jb->w.sto;
        bjr.j.sub_pk      = sub_pk;
        bjr.j.nargc       = nargc; 
        bjr.j.nname       = jb->nname;
        bjr.j.csort_order = csort_order;
        bjr.j.reordered   = reordered;
        bjr.j.qcols       = jb->qcols;
        bjr.n_ind         = jb->n_ind;
        bjr.card          = &card;
        bjr.j.obt         = jb->w.obt;
        bjr.j.obc         = jb->w.obc;
        bjr.j_indxs       = jb->j_indxs;
        bjr.j.ll          = ll;
        bjr.j.ctype       = ctype;
        bjr.j.cstar       = jb->cstar;

        joinRowEntry *be;
        bi = btGetJoinFullRangeIterator(jbtr, pk1type);
        while ((be = btJoinRangeNext(bi, pk1type)) != NULL) { /* iter BT */
            listNode *ln;
            bjr.jk        = be->key;
            list     *jll = (list *)be->val;
            listIter *li  = listGetIterator(jll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {        /* iter LIST */
                char *first_entry;
                char *item = ln->value;
                if (bjr.j.obt == Index[server.dbid][bjr.j_indxs[0]].table) {
                    obsl_t *ob       = (obsl_t *)item;
                    Order_by_col_val = (char *)ob->val;
                    first_entry      = (char *)ob->row;
                } else {
                    first_entry      = item;
                }
                for (int j = 0; j < jind_ncols[0]; j++) {
                    Rcols[0][j]  = (char **)first_entry;
                    first_entry += PTR_SIZE;
                    memcpy(&Rc_lens[0][j], first_entry, UINT_SIZE);
                    first_entry += UINT_SIZE;
                }
    
                if (!buildJRowReply(&bjr, 1, rset)) {
                    err = 1;
                    goto join_end;
                }
            }
            listReleaseIterator(li);
        }

        if (Order_by) {
            sent = sortJoinOrderByAndReply(c, &bjr, &jb->w);
            if (sent == -1) err = 1;
            listRelease(ll);
        }
    }

join_end:
    if (bi)    btReleaseJoinRangeIterator(bi);
    if (reply) free(reply);

    /* free joinRowEntry malloc from joinAddColsFromInd() */
    bool  is_ob = (jb->w.obt == Index[server.dbid][jb->j_indxs[0]].table);
    btJoinRelease(jbtr, jind_ncols[0], is_ob, freeListOfIndRow);

    /* free joinRowEntry malloc from joinAddColsFromInd() */
    dictEntry *de;
    for (int i = 1; i < jb->n_ind; i++) {
        dict         *set   = rset[i]->ptr;
        bool          is_ob = (jb->w.obt ==
                               Index[server.dbid][jb->j_indxs[i]].table);
        dictIterator *di    = dictGetIterator(set);
        while((de = dictNext(di)) != NULL) {
            robj *val  = dictGetEntryVal(de);
            dict *iset = val->ptr;
            freeDictOfIndRow(iset, jind_ncols[i], is_ob);
        }
        dictReleaseIterator(di);
    }

    for (int i = 1; i < jb->n_ind; i++) {
        decrRefCount(rset[i]);
    }

    if (err) return;
    if (jb->w.lim != -1 && (uint32)sent < card) card = sent;
    if (jb->w.sto != -1) {
        addReplyLongLong(c, card);
        if (jb->w.ovar) incrOffsetVar(c, &jb->w, card);
    } else if (jb->cstar) {
        lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    } else {
        lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
        if (jb->w.ovar) incrOffsetVar(c, &jb->w, card);
    }
}

void joinStoreCommit(redisClient *c, jb_t *jb) {
    char *nname;
    int   nlen;  
    bool  sub_pk;
    int   nargc; 
    char *last = NULL;
    if (!prepareToStoreReply(c, &jb->w, &nname, &nlen, 
                             &sub_pk, &nargc, &last, jb->qcols)) return;
    jb->nname = createStringObject(nname, nlen);

    robj        *argv[STORAGE_MAX_ARGC + 1];
    redisClient *fc = rsql_createFakeClient();
    fc->argv        = argv;

    joinGeneric(c, fc, jb, sub_pk, nargc);

    if (last) *last = '$';/* write back in "$" for AOF and Slaves */

    rsql_freeFakeClient(fc);
}

/* CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP */
/* CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP */
static void freeIndRowEntries(char *s, int num_cols) {
    for (int j = 0; j < num_cols; j++) {
        char **x = (char **)s;
        char **y = (char **)*x;
        free(y);
        s += PTR_SIZE + UINT_SIZE;
    }
}

static void freeIndRow(void *s, int num_cols, bool is_ob, bool free_ptr) {
    if (is_ob) {
        obsl_t *ob = (obsl_t *)s;
        freeIndRowEntries(ob->row, num_cols);
        free(ob->row);          /* free ind_row */
        free(ob->val);          /* free getCopyColStr */
        if (free_ptr) free(ob); /* free malloc */
    } else {
        freeIndRowEntries(s, num_cols);
        if (free_ptr) free(s);  /* free ind_row */
    }
}

static void freeListOfIndRow(list *ll, int num_cols, bool is_ob) {
    listNode *ln;
    listIter *li = listGetIterator(ll, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        freeIndRow(ln->value, num_cols, is_ob, 1);
    }
    listReleaseIterator(li);
    listRelease(ll);
}

static void freeDictOfIndRow(dict *d, int num_cols, bool is_ob) {
    dictEntry    *ide;
    dictIterator *idi  = dictGetIterator(d);
    while((ide = dictNext(idi)) != NULL) {
        robj *ikey = dictGetEntryKey(ide);
        freeIndRow(ikey->ptr, num_cols, is_ob, 0);
    }
    dictReleaseIterator(idi);
}

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

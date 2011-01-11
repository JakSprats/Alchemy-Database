/*
 * Implements AlchemyDB Denormalised table joins
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
#include "orderby.h"
#include "rpipe.h"
#include "colparse.h"
#include "parser.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "join.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS
extern char *EMPTY_STRING;
extern char *OUTPUT_DELIM;

extern r_tbl_t  Tbl  [MAX_NUM_DB][MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_DB][MAX_NUM_INDICES];

extern bool  OB_asc  [MAX_ORDER_BY_COLS];
extern uchar OB_ctype[MAX_ORDER_BY_COLS];

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
void         *Order_by_col_val = NULL;

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
            Order_by_col_val = ob->keys[0];
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
    bt   *btr;
    int   qcols;
    int  *j_tbls;
    int   itable;
    int  *j_cols;
    int   index;
    int  *jind_ncols;
    aobj *ajk;
    void *rrow;
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
static char *getCopyColStr(void   *rrow,
                           int     cmatch,
                           aobj   *aopk,
                           int     tmatch,
                           uint32 *len) {
    char   *dest;
    aobj    ao  = getRawCol(rrow, cmatch, aopk, tmatch, NULL, 1);
    // force_string(INT) comes from a buffer, must be copied here
    char   *src = ao.s;
    m_strcpy_len(src, ao.len, &dest); /* freeD in freeIndRowEntries */
    *len        = ao.len;
    releaseAobj(&ao);
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
static void joinAddColsFromInd(join_add_cols_t *a, robj *rset[], cswc_t *w) {
    cu32_t  cresp[MAX_JOIN_INDXS];
    int     row_len = 0;
    int     nresp   = 0;
    aobj   *akey    = a->ajk;

    for (int i = 0; i < a->qcols; i++) {
        int tmatch = a->j_tbls[i];
        if (tmatch == a->itable) {
            cresp[nresp].s  = getCopyColStr(a->rrow, a->j_cols[i], akey, tmatch,
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
    if (w->obt[0] == a->itable) {
        obsl_t *ob  = create_obsl(list_val, 1); /* DESTROY ME 011 */
        list_val    = (char *)ob; /* in ORDER BY list_val -> obsl_t */
        OB_asc[0]   = w->asc[0];
        OB_ctype[0] = Tbl[server.dbid][w->tmatch].col_type[w->obc[0]];
        assignObKey(w, a->rrow, akey, 0, ob);
    }

    // TODO unify, refactor, etc..
    //
    // NOTE: (clarification of clusterFUcT implementation)
    //       1st index: BT         of Lists
    //       2nd index: ValSetDict of AppendSets (should be List)
    if (a->index == 0) { // first joined index needs BTREE (i.e. sorted)
        joinRowEntry  k;
        k.key           = createStringRobjFromAobj(a->ajk);
        joinRowEntry *x = btJoinFindVal(a->jbtr, &k);

        if (x) {
            list *ll = (list *)x->val;
            listAddNodeTail(ll, list_val);
        } else {
            joinRowEntry *jre = (joinRowEntry *)malloc(sizeof(joinRowEntry));
            list         *ll  = listCreate(); /* listRelease freeListOfIndRow*/
            jre->key          = k.key;
            jre->val          = (void *)ll;
            listAddNodeHead(ll, list_val);
            btJoinAddRow(a->jbtr, jre);
        }
    } else { // rest of the joined indices are redis DICTs for speed
        robj      *res_setobj;
        robj      *ind_row_obj = createObject(REDIS_JOINROW, list_val);
        robj      *key         = createStringRobjFromAobj(a->ajk);
        dictEntry *rde         = dictFind(rset[a->index]->ptr, key);
        if (!rde) {
            // create AppendSet "list"
            res_setobj = createAppendSetObject();
            dictAdd(rset[a->index]->ptr, key, res_setobj);
        } else {
            res_setobj = dictGetEntryVal(rde);
            decrRefCount(key);
        }
        dictAdd(res_setobj->ptr, ind_row_obj, NULL); // push to (list per jk)
    }
}

void joinGeneric(redisClient *c,
                 jb_t        *jb) {
    if (jb->w.nob > 1) {
        addReply(c, shared.join_m_obc);
        return;
    }
    Order_by         = jb->w.nob;
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

    cswc_t *w      = &jb->w; /* makes coding more compact */
    w->tmatch      = w->obt[0]; /* HACK: initOBsort needs w->tmatch */
    list   *ll     = initOBsort(Order_by, w);
    uchar  pk1type = Tbl[server.dbid]
                         [Index[server.dbid][jb->j_indxs[0]].table].col_type[0];
    bt    *jbtr    = createJoinResultSet(pk1type);

    robj  *rset[MAX_JOIN_INDXS];
    for (int i = 1; i < jb->n_ind; i++) {
        rset[i] = createValSetObject();
    }

    int j_ind_len [MAX_JOIN_INDXS];
    int jind_ncols[MAX_JOIN_INDXS];
    join_add_cols_t jc; /* these dont change in the loop below */
    jc.qcols      = jb->qcols;
    jc.j_tbls     = jb->j_tbls;
    jc.j_cols     = jb->j_cols;
    jc.jind_ncols = jind_ncols;
    jc.j_ind_len  = j_ind_len;
    jc.jbtr       = jbtr;
    
    for (int i = 0; i < jb->n_ind; i++) { /* iterate join indices */
        btEntry    *be, *nbe;
        j_ind_len[i] = 0;
        jc.index     = i;
        jc.itable    = Index[server.dbid][jb->j_indxs[i]].table;
        robj *btt    = lookupKeyRead(c->db, Tbl[server.dbid][jc.itable].name);
        jc.btr       = (bt *)btt->ptr;
        jc.virt      = Index[server.dbid][jb->j_indxs[i]].virt;
        uchar pktype = jc.btr->ktype;
     
        if (w->low) { /* RANGE QUERY */
            if (jc.virt) { /* PK */
                btSIter *bi = btGetRangeIterator(jc.btr, w->low, w->high);
                while ((be = btRangeNext(bi)) != NULL) {
                    jc.ajk    = be->key;
                    jc.rrow   = be->val;
                    joinAddColsFromInd(&jc, rset, w);
                }
                btReleaseRangeIterator(bi);
            } else {       /* FK */
                robj    *ind  = Index[server.dbid][jb->j_indxs[i]].obj;
                robj    *ibtt = lookupKey(c->db, ind);
                bt      *ibtr = (bt *)ibtt->ptr;
                btSIter *bi   = btGetRangeIterator(ibtr, w->low, w->high);
                while ((be = btRangeNext(bi)) != NULL) {
                    jc.ajk        = be->key;
                    bt      *nbtr = be->val;
                    btSIter *nbi  = btGetFullRangeIterator(nbtr);
                    while ((nbe = btRangeNext(nbi)) != NULL) {
                        jc.rrow = btFindVal(jc.btr, nbe->key, pktype);
                        joinAddColsFromInd(&jc, rset, w);
                    }
                    btReleaseRangeIterator(nbi);
                }
                btReleaseRangeIterator(bi);
            }
        } else {      /* IN() QUERY */
            listNode *ln;
            listIter *li  = listGetIterator(w->inl, AL_START_HEAD);
            if (jc.virt) {
                while((ln = listNext(li)) != NULL) {
                    jc.ajk  = ln->value;
                    jc.rrow = btFindVal(jc.btr, jc.ajk, pktype);
                    if (jc.rrow) joinAddColsFromInd(&jc, rset, w);
                }
            } else {
                btSIter *nbi;
                robj *ind    = Index[server.dbid][jb->j_indxs[i]].obj;
                int   indcol = (int)Index[server.dbid][jb->j_indxs[i]].column;
                bool  fktype = Tbl[server.dbid][jc.itable].col_type[indcol];
                robj *ibtt   = lookupKey(c->db, ind);   
                bt   *ibtr   = (bt *)ibtt->ptr;
                while((ln = listNext(li)) != NULL) {
                    jc.ajk   = ln->value;
                    bt *nbtr = btIndFindVal(ibtr, jc.ajk, fktype);
                    if (nbtr) {
                        nbi  = btGetFullRangeIterator(nbtr);
                        while ((nbe = btRangeNext(nbi)) != NULL) {
                            jc.rrow = btFindVal(jc.btr, nbe->key, pktype);
                            joinAddColsFromInd(&jc, rset, w);
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

    LEN_OBJ

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
        bjr.j.jind_ncols  = jind_ncols;
        bjr.j.reply       = reply;
        bjr.j.csort_order = csort_order;
        bjr.j.reordered   = reordered;
        bjr.j.qcols       = jb->qcols;
        bjr.n_ind         = jb->n_ind;
        bjr.card          = &card;
        bjr.j.obt         = w->obt[0];
        bjr.j.obc         = w->obc[0];
        bjr.j_indxs       = jb->j_indxs;
        bjr.j.ll          = ll;
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
                    Order_by_col_val = ob->keys[0];
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
            sent = sortJoinOrderByAndReply(c, &bjr, w);
            if (sent == -1) err = 1;
            releaseOBsort(ll);
        }
    }

join_end:
    if (bi)    btReleaseJoinRangeIterator(bi);
    if (reply) free(reply);

    /* free joinRowEntry malloc from joinAddColsFromInd() */
    bool  is_ob = (w->obt[0] == Index[server.dbid][jb->j_indxs[0]].table);
    btJoinRelease(jbtr, jind_ncols[0], is_ob, freeListOfIndRow);

    /* free joinRowEntry malloc from joinAddColsFromInd() */
    dictEntry *de;
    for (int i = 1; i < jb->n_ind; i++) {
        dict *set   = rset[i]->ptr;
        bool  is_ob = (w->obt[0] == Index[server.dbid][jb->j_indxs[i]].table);
        dictIterator *di = dictGetIterator(set);
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
    if (w->lim != -1 && (uint32)sent < card) card = sent;
    if (jb->cstar) {
        lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    } else {
        lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
        if (w->ovar) incrOffsetVar(c, w, card);
    }
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
        obsl_t *ob = (obsl_t *)s;                            /* DESTROYED 011 */
        freeIndRowEntries(ob->row, num_cols);
        if (OB_ctype[0] == COL_TYPE_STRING) free(ob->keys[0]); /* FREED 003 */
        free(ob->keys);                                        /* FREED 006 */
        free(ob->row);          /* free ind_row */
        if (free_ptr) free(ob);                                /* FREED 011 */
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

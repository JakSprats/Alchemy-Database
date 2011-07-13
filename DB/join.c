/*
 * Implements ALCHEMY_DATABASE StarSchema table joins
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "redis.h"

#include "qo.h"
#include "row.h"
#include "index.h"
#include "lru.h"
#include "orderby.h"
#include "filter.h"
#include "range.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "join.h"

// FROM redis.c
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

// GLOBALS

extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_INDICES];
extern uchar    OutputMode;
extern bool     Explain;
extern bool     LruColInSelect;

extern bool  OB_asc  [MAX_ORDER_BY_COLS];
extern uchar OB_ctype[MAX_ORDER_BY_COLS];

extern char *EMPTY_STRING;
extern char  OUTPUT_DELIM;
extern aobj *JoinKey;

bool JoinErr   =  0;
long JoinCard  =  0;
long JoinLoops =  0;
long JoinLim   = -1;
long JoinOfst  = -1;
bool JoinQed   =  0;

static aobj Resp;
static sl_t Jcols[MAX_JOIN_COLS];

static char *memclone(char *s, int len) {
    char *x = malloc(len);
    memcpy(x, s, len);
    return x;
}

void init_ijp(ijp_t *ij) {
    bzero(ij, sizeof(ijp_t));
    initFilter(&ij->lhs); initFilter(&ij->rhs);
    ij->nrows   = UINT_MAX;
    ij->kimatch = -1;
}
void switchIJ(ijp_t *ij) {
    f_t temp;
    memcpy(&temp,    &ij->rhs, sizeof(f_t));
    memcpy(&ij->rhs, &ij->lhs, sizeof(f_t));
    memcpy(&ij->lhs, &temp,    sizeof(f_t));
}

bool validateJoinOrderBy(cli *c, jb_t *jb) {
    if (jb->wb.nob) { /* ORDER BY -> Table must be in join */
        for (uint32 j = 0; j < jb->wb.nob; j++) {
            bool hit = 0;
            for (int i = 0; i < jb->qcols; i++) {
                if (jb->js[i].t == jb->wb.obt[j]) { hit = 1; break; }
            }
            if (!hit) { addReply(c, shared.join_table_not_in_query); return 0; }
        }
    }
    return 1;
}

#define JOP_DEBUG_0                                          \
   printf("  top join_op: lvl: %d\n", g->co.lvl);            \
   printf("  join_op W:\n"); dumpW(printf, w);               \
   dumpFL(printf, "\t", "JOIN FILTER", flist);               \
   if (rrow) {                                               \
       printf("  join_op ROW:\n");                           \
       dumpRow(printf, g->co.btr, rrow, akey, w->wf.tmatch); \
   }
#define JOP_DEBUG_1 \
    printf("  filters passed: qcols: %d tmatch: %d jan: %d\n", \
             g->se.qcols, w->wf.tmatch, w->wf.jan);
#define JOP_DEBUG_2                                                            \
    printf("    lvl: %d t: %d c: %d len: %d Jcols[%d](%p): ",                  \
            g->co.lvl, jb->js[i].t, jb->js[i].c, Jcols[i].len, i, Jcols[i].s); \
    dumpAobj(printf, &Resp);
#define JOP_DEBUG_3                                                       \
    printf("lvl: %d rrow: %p lhs.T: %d lhs.C: %d lhs.I: %d\n",            \
        g->co.lvl, rrow, ij->lhs.tmatch, ij->lhs.cmatch, ij->lhs.imatch); \
    if (nkl) dumpFL(printf, "\t", "KLIST", nkl);                          \
    else     printf("nkey: "); dumpAobj(printf, &nk);
#define JOP_DEBUG_4                                                       \
    printf("nkey: "); dumpAobj(printf, &nk);                              \
    printf("imatch: %d tmatch: %d\n", nimatch, ri->table);
#define JOP_DEBUG_5                                                       \
    {   printf("rrow: %p\n", rrow);                                       \
        uchar ctype = Tbl[w->obt[i]].col_type[w->obc[i]];           \
        printf("ASSIGN: "); dumpObKey(printf, i, jb->ob->keys[i], ctype); }
#define JOP_DEBUG_6                                    \
    printf("    join_op ROW:\n");                      \
    dumpRow(printf, g->co.btr, rrow, akey, w->wf.tmatch); \
    dumpFL(printf, "\t", "JOIN-FFLIST", jb->fflist);
#define DEBUG_PRE_RECURSE \
  printf("PRE RECURSE: join_op\n"); dumpW(printf, g2.co.w);
#define DEBUG_JOIN_GEN \
    printf("joinGeneric W\n"); dumpW(printf, w); dumpWB(printf, &jb->wb); \
    printf("joinGeneric JB\n"); dumpJB(printf, jb);
#define DUMP_JOIN_QED \
  (*prn)("\t\tJoinQed: %d JoinLim: %ld JoinOfst: %ld\n", \
         JoinQed, JoinLim, JoinOfst);
#define DEBUG_JOIN_QED \
  printer *prn = printf; \
  DUMP_JOIN_QED

static bool checkOfst() {
    if (JoinQed) return 1;
    INCR(JoinLoops);
    return (JoinLoops >= JoinOfst);
}
static bool checkLimit() {
    if (JoinQed)       return 1;
    if (JoinLim == -1) return 1;
    if (JoinLim ==  0) return 0;
    JoinLim--;
    return 1;
}

static robj *join_reply_redis(range_t *g) {
    char  pbuf[128];
    sl_t  outs[g->se.qcols];
    uint32 prelen = output_start(pbuf, 128, g->se.qcols);
    uint32 totlen = prelen;
    for (int i = 0; i < g->se.qcols; i++) {
        outs[i]  = outputSL(Jcols[i].type, Jcols[i]);
        totlen  += outs[i].len;
    }
    return write_output_row(g->se.qcols, prelen, pbuf, totlen, outs);
}
static robj *join_reply_norm(range_t *g) {
    uint32  totlen = 0;
    for (int i = 0; i < g->se.qcols; i++) {
        totlen += (Jcols[i].len + 1);
        if (C_IS_S(Jcols[i].type)) totlen += 2; /* 2 quotes per col */
    }
    totlen--; /* -1 no final comma */
    return write_output_row(g->se.qcols, 0, NULL, totlen, Jcols);
}
static void join_reply(range_t *g, long *card) {
    jb_t *jb    = g->jb;   /* code compaction */
    robj *r = (OREDIS) ? join_reply_redis(g) : join_reply_norm(g);
    if (JoinQed) {
        jb->ob->row = cloneRobj(r);
        listAddNodeTail(g->co.ll, jb->ob);
        jb->ob      = cloneOb(jb->ob, jb->wb.nob);       /* DESTROY ME 057 */
    } else {
        if (OREDIS) addReply(    g->co.c, r);
        else        addReplyBulk(g->co.c, r);
    }
    decrRefCount(r);
    INCR(JoinCard); INCR(*card);
}

static bool popKlist(range_t *g,   int   imatch, list **klist,
                     aobj    *apk, void *rrow,   int    jcmatch) {
    r_ind_t *ri   = &Index[imatch];
    list    *o_kl = *klist;
    o_kl->dup     = vcloneFilter;
    *klist        = listDup(o_kl);
    for (int i = 0; i < ri->nclist; i++) {
        listNode *ln = listIndex(*klist, i);
        if (!ln) continue;
        f_t  *flt     = ln->value;
        if (flt->op == NONE) {
            flt->akey = getCol(g->co.btr, rrow, ri->bclist[i], apk, ri->table);
            flt->op   = EQ;
        } else if (jcmatch == flt->cmatch) {
            aobj akey = getCol(g->co.btr, rrow, ri->bclist[i], apk, ri->table);
            bool ok = aobjEQ(&flt->akey, &akey);
            releaseAobj(&akey);
            if (!ok) return 0;
        }
    } //dumpFL(printf, "\t", "KLIST", *klist);
    return 1;
}
static bool join_op(range_t *g, aobj *apk, void *rrow, bool q, long *card) {
    q = 0; /* compiler warning */
    if (JoinErr)  return 0;
    if (!JoinLim) return 1; /* this means LIMIT OFFSET has been fulfilled */
    jb_t   *jb     = g->jb;   /* code compaction */
    cswc_t *w      = g->co.w; /* code compaction */
    int     tmatch = w->wf.tmatch; /* code compaction */
    list   *flist  = jb->ij[g->co.lvl].flist;                      //JOP_DEBUG_0
    if (!passFilters(g->co.btr, apk, rrow, flist, tmatch)) return 1;
    /* NOTE: update LRU for ALL rows in JOINs*/
    GET_LRUC updateLru(g->co.c, tmatch, apk, lruc); /* NOTE: updateLRU (JOIN) */
    char *freeme[g->se.qcols];                                     //JOP_DEBUG_1
    int   nfree = 0;
    for (int i = 0; i < g->se.qcols; i++) {
        if (jb->js[i].jan == w->wf.jan) {
            Resp = getSCol(g->co.btr, rrow, jb->js[i].c, /* FREE ME 037 */
                             rrow ? apk : JoinKey, jb->js[i].t);
            Jcols[i].len    = Resp.len;
            Jcols[i].freeme = 0; /* freed via freeme[] */
            Jcols[i].type   = Tbl[jb->js[i].t].col_type[jb->js[i].c];
            if (Resp.freeme) Jcols[i].s = Resp.s;
            else             Jcols[i].s = memclone(Resp.s, Resp.len);
            freeme[nfree++] = Jcols[i].s;                          //JOP_DEBUG_2
        }
    }
    int obfreei[jb->wb.nob];
    int obnfree = 0;
    if (jb->wb.nob) {
        if (!jb->ob) jb->ob = create_obsl(NULL, jb->wb.nob); /*DESTROY ME 057*/
        for (uint32 i = 0; i < jb->wb.nob; i++) {
            if (jb->wb.obt[i] == tmatch) {
                uchar ctype = Tbl[jb->wb.obt[i]].col_type[jb->wb.obc[i]];
                assignObKey(&jb->wb, g->co.btr, rrow, apk, i, jb->ob);
                if (C_IS_S(ctype)) obfreei[obnfree++] = i;
            }
        }
    }
    if (g->co.lvl == (uint32)jb->hw) {                             //JOP_DEBUG_6
        if (passFilters(g->co.btr, apk, rrow, jb->fflist, tmatch)) {
            if (jb->cstar) { INCR(JoinCard); INCR(*card); }
            else if (checkOfst()) {
                if (checkLimit()) join_reply(g, card);
            }
        }
        for (int i = 0; i < obnfree; i++) {
            free(jb->ob->keys[obfreei[i]]); jb->ob->keys[obfreei[i]] = NULL;
        }
        for (int i = 0; i < nfree; i++) free(freeme[i]); /* FREE 037 */
        return 1;
    }
    { /* NOTE: this is the recursion step */
        aobj nk; initAobj(&nk); list *nkl; int nimatch; int jcmatch;
        ijp_t *ij   = &jb->ij[g->co.lvl];
        if (g->co.lvl == ((uint32)jb->hw - 1)) {
            nkl     = jb->fklist;
            nimatch = nkl ? jb->fkimatch : ij->rhs.imatch;
            jcmatch = jb->ij[g->co.lvl].rhs.cmatch; /* RIGHT-HAND-SIDE */
        } else { /* next ij determines Join Index */
            nkl     = jb->ij[g->co.lvl + 1].lhs.klist;
            nimatch = nkl ? jb->ij[g->co.lvl + 1].lhs.imatch : ij->rhs.imatch;
            jcmatch = jb->ij[g->co.lvl].lhs.cmatch; /* LEFT-HAND-SIDE */
        }
        bool ok = 1;
        if (nkl) ok = popKlist(g, nimatch, &nkl, apk, rrow, jcmatch);
        else     nk = getCol(g->co.btr, rrow, ij->lhs.cmatch,
                             apk, ij->lhs.tmatch);
        cswc_t w2; range_t g2; qr_t q2;                            //JOP_DEBUG_3
        r_ind_t *ri = &Index[nimatch];                       //JOP_DEBUG_4
        init_check_sql_where_clause(&w2, ri->table, NULL);
        if (ok) {
            init_range(&g2, g->co.c, &w2, &jb->wb, &q2, g->co.ll,
                        g->co.ofree, jb);
            bzero(&q2, sizeof(qr_t)); //TODO only zeroed once, GLOBAL
            g2.se.qcols  = g->se.qcols;
            g2.co.lvl    = g->co.lvl + 1; /* INCR RECURSION LEVEL */
            w2.flist     = w->flist;
            if (nkl) promoteKLorFLtoW(&w2, &nkl, &w2.flist, 1);
            else {
                w2.wf.jan    = ij->rhs.jan;
                w2.wf.akey   = nk;
                w2.wf.imatch = nimatch;
                w2.wf.tmatch = ri->table;
                w2.wf.cmatch = ri->column;
                w2.wf.op     = EQ;
                w2.wtype     = (ri->virt) ? SQL_SINGLE_LKP : SQL_SINGLE_FK_LKP;
            }                                                //DEBUG_PRE_RECURSE
            if (w2.wf.imatch == -1) { JoinErr = 1; }
            else {
                long rcard = keyOp(&g2, join_op); /* RECURSE */
                INCRBY(*card, rcard);
            }
        }
        if (nkl) {
            nkl->free = destroyFilter;
            listRelease(nkl); /* w2.wf.klist NULLed to not double-free */
            w2.wf.klist = NULL; releaseFilterR_KL(&w2.wf);
        }
        releaseAobj(&nk);
    }
    for (int i = 0; i < nfree; i++) { free(freeme[i]); /* FREE 037 */ }
    for (int i = 0; i < obnfree; i++) { /* just free this level's ob->leys[] */
        free(jb->ob->keys[obfreei[i]]); jb->ob->keys[obfreei[i]] = NULL;
    }
    return 1;
}
static void setupFirstJoinStep(jb_t *jb, cswc_t *w) {
    init_check_sql_where_clause(w, -1, NULL);
    ijp_t  *ij  = &jb->ij[0];
    promoteKLorFLtoW(w, &ij->lhs.klist, &ij->flist, 0);         //DEBUG_JOIN_GEN
    r_ind_t *ri = &Index[w->wf.imatch];
    JoinQed     = (jb->wb.nob && 
                   (ri->table != jb->wb.obt[0] || ri->column != jb->wb.obc[0]));
    JoinLim     = jb->wb.lim;
    JoinOfst    = jb->wb.ofst;                                  //DEBUG_JOIN_QED
}
void joinGeneric(redisClient *c, jb_t *jb) {
    qr_t q; bzero(&q, sizeof(qr_t)); //TODO make GLOBAL
    cswc_t w; setupFirstJoinStep(jb, &w);
    if (w.wf.imatch == -1) { addReply(c, shared.join_qo_err); return; }
    LruColInSelect = initLRUCS_J(jb);
    list *ll       = initOBsort(JoinQed, &jb->wb);
    range_t g;
    init_range(&g, c, &w, &jb->wb, &q, ll, OBY_FREE_ROBJ, jb);
    g.se.qcols     = jb->qcols;
    JoinLoops      = -1; JoinCard = 0; JoinErr = 0;
    void *rlen     = addDeferredMultiBulkLength(c);
    long  card     = 0;
    Op(&g, join_op); /* NOTE Op() retval ignored as SELECTs can NOT FAIL */
    if (JoinErr) { addReply(c, shared.join_qo_err); return; }
    card           = JoinCard;
    long sent      =  0;
    if (card) {
        if (JoinQed) opSelectOnSort(c, ll, &jb->wb, g.co.ofree, &sent, -1);
        else         sent = card;
    }
    if (JoinLim != -1 && sent < card) card = sent;
    if (jb->cstar) setDeferredMultiBulkLong(c, rlen, card);
    else           setDeferredMultiBulkLength(c, rlen, card);
    if (jb->wb.ovar) { incrOffsetVar(c, &jb->wb, card); } //TODO done use w

    releaseOBsort(ll);
    releaseAobj(&w.wf.akey); releaseAobj(&w.wf.alow); releaseAobj(&w.wf.ahigh);
    return;
}

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
//TODO move to explain.c
void dumpIJ(printer *prn, int i, ijp_t *ij, ijp_t *nij) {
    int lt = ij->lhs.tmatch; int lc = ij->lhs.cmatch;
    int lj = ij->lhs.jan;    int li = ij->lhs.imatch;
    if (ij->rhs.tmatch != -1) {
       int ki = ij->kimatch;
       int rt = ij->rhs.tmatch; int rc = ij->rhs.cmatch;
       int rj = ij->rhs.jan;    int ri = ij->rhs.imatch;
       (*prn)("\t\t\t\t%d: ki: %d LHS[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s)" \
              " RHS[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s)] nrows: %u\n", i,
          ki, lt, (lt == -1) ? "" : (char *)Tbl[lt].name->ptr,
              lc, (lc == -1) ? "" : (char *)Tbl[lt].col_name[lc]->ptr,
              li, (li == -1) ? "" : (char *)Index[li].obj->ptr,
              lj, (lj == -1) ? "" : getJoinAlias(lj),
                  rt, (rt == -1) ? "" : (char *)Tbl[rt].name->ptr,
                  rc, (rc == -1) ? "" : (char *)Tbl[rt].col_name[rc]->ptr,
                  ri, (ri == -1) ? "" : (char *)Index[ri].obj->ptr,
                  rj, (rj == -1) ? "" : getJoinAlias(rj),
                  ij->nrows);
        if (Explain) {
            if (nij) {
                dumpFL(prn, "\t\t\t\t", "KLIST", nij->lhs.klist);
                dumpFL(prn, "\t\t\t\t", "FLIST", nij->flist);
            }
        }
        else {
            dumpFL(prn, "\t\t\t\t", "FLIST", ij->flist);
            dumpFL(prn, "\t\t\t\t", "KLIST", ij->lhs.klist);
        }
    } else {
        (*prn)("\t\t\t\t%d: FLT[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s) op: %d",
               i, lt, (lt == -1) ? "" : (char *)Tbl[lt].name->ptr,
                  lc, (lc == -1) ? "" : (char *)Tbl[lt].col_name[lc]->ptr,
                  li, (li == -1) ? "" : (char *)Index[li].obj->ptr,
                  lj, (lj == -1) ? "" : getJoinAlias(lj), ij->op);
        if (ij->lhs.key) {
            dumpSds(prn, ij->lhs.key,  " key: %s]\n");
        } else if (ij->lhs.low) {
            dumpSds(prn, ij->lhs.low,  " low: %s");
            dumpSds(prn, ij->lhs.high, " high: %s]\n");
        } else if (ij->lhs.inl) {
            (*prn)(" INL: len: %d:\n", listLength(ij->lhs.inl));
            listNode *ln;
            listIter *li = listGetIterator(ij->lhs.inl, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                aobj *apk = ln->value;
                (*prn)("\t\t       \t\t"); dumpAobj(prn, apk);
            }
            listReleaseIterator(li);
            (*prn)("\t\t       \t]\n");
        }
    }
}
void explainJoin(cli *c, jb_t *jb) {
    initQueueOutput();
    (*queueOutput)("QUERY: ");
    for (int i = 0; i < c->argc; i++) {
        (*queueOutput)("%s ", (char *)c->argv[i]->ptr);
    } (*queueOutput)("\n");
    dumpJB(queueOutput, jb);
    dumpQueueOutput(c);
}
void dumpJB(printer *prn, jb_t *jb) {
    (*prn)("\tSTART dumpJB\n");
    (*prn)("\t\tqcols:         %d\n", jb->qcols);
    for (int i = 0; i < jb->qcols; i++) {
        int lt = jb->js[i].t; int lc = jb->js[i].c; int jan = jb->js[i].jan;
        (*prn)("\t\t\t%d: jan: %d t: %d c: %d (%s: %s.%s)\n",
                 i, jan, lt, lc,
                 getJoinAlias(jan),
                 (lt == -1) ? "" : (char *)Tbl[lt].name->ptr,
                 (lc == -1) ? "" : (char *)Tbl[lt].col_name[lc]->ptr);
    }
    if (Explain) {
        cswc_t  w; setupFirstJoinStep(jb, &w);
        dumpFilter(prn, &w.wf, "\t");
        dumpFL(prn, "\t\t\t\t", "FLIST", jb->ij[0].flist);
    }
    (*prn)("\t\tn_jind:        %d\n", jb->n_jind);
    if (jb->hw != -1) {
        (*prn)("\t\thw:     %d\n", jb->hw);
        for (int k = 0; k < jb->hw; k++) {
            ijp_t *nij = (k == jb->hw -1) ? NULL : &jb->ij[k + 1];
            dumpIJ(prn, k, &jb->ij[k], nij);
        }
    } else {
        for (uint32 i = 0; i < jb->n_jind; i++) {
            dumpIJ(prn, i, &jb->ij[i], NULL);
        }
    }
    if (jb->fklist) {
        (*prn)("\t\tfklist: fki: %d fnrows: %u\n", jb->fkimatch, jb->fnrows);
        dumpFL(prn, "\t\t\t", "JB-FKLIST", jb->fklist);
    }
    if (jb->fflist) {
        (*prn)("\t\tfflist: fnrows: %u\n", jb->fnrows);
        dumpFL(prn, "\t\t\t", "JB-FFLIST", jb->fflist);
    }
    if (!Explain) {
        if (jb->hw != -1) {
            (*prn)("\t\tn_filters:     %d\n", (jb->n_jind - jb->hw));
            for (int j = jb->hw; j < (int)jb->n_jind; j++) {
                dumpIJ(prn, j, &jb->ij[j], NULL);
            }
        }
    }
    dumpWB(prn, &jb->wb);
    DUMP_JOIN_QED
    (*prn)("\tEND dumpJB\n");
}

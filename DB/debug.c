/*
  *
  * This file implements the DDL SQL commands of AlchemyDatabase
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
#include <ctype.h>
#include <assert.h>

#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "redis.h"

#include "qo.h"
#include "join.h"
#include "filter.h"
#include "index.h"
#include "parser.h"
#include "colparse.h"
#include "find.h"
#include "query.h"
#include "alsosql.h"
#include "common.h"
#include "debug.h"

extern char    *RangeType[5];
extern r_tbl_t *Tbl;
extern r_ind_t *Index;
extern dict    *StmtD;

extern long JoinLim; extern long JoinOfst; extern bool JoinQed;

#define DUMP_JOIN_QED \
  (*prn)("\t\tJoinQed: %d JoinLim: %ld JoinOfst: %ld\n", \
         JoinQed, JoinLim, JoinOfst);

void setDeferredMultiBulkError(redisClient *c, void *node, sds error) {
    if (!node) return; /* Abort when addDeferredMultiBulkLength not called. */
    listNode *ln  = (listNode*)node;
    robj     *len = listNodeValue(ln);
    len->ptr      = error;
    if (ln->next) {
        robj *next = listNodeValue(ln->next);
        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr) {
            len->ptr = sdscatlen(len->ptr, next->ptr, sdslen(next->ptr));
            listDelNode(c->reply,ln->next);
        }
    }
}
void setDeferredMultiBulkLong(redisClient *c, void *node, long card) {
    sds rep_int = sdscatprintf(sdsempty(), ":%ld\r\n", card);
    setDeferredMultiBulkError(c, node, rep_int);
}

// QUEUE_OUTPUT QUEUE_OUTPUT QUEUE_OUTPUT QUEUE_OUTPUT QUEUE_OUTPUT
sds DumpOutput;
void initQueueOutput() {
    DumpOutput = sdsempty();                             /* DESTROY ME 060 */
}
int queueOutput(const char *fmt, ...) { // copy of sdscatprintf()
    va_list ap; char *buf; size_t buflen = 16;
    while(1) {
        buf             = malloc(buflen);
        buf[buflen - 2] = '\0';
        va_start(ap, fmt); vsnprintf(buf, buflen, fmt, ap); va_end(ap);
        if (buf[buflen-2] != '\0') { free(buf); buflen *= 2; continue; }
        break;
    }
    DumpOutput = sdscat(DumpOutput, buf); free(buf); return sdslen(DumpOutput);
}
void dumpQueueOutput(cli *c) {
    robj *r  = createObject(REDIS_STRING, DumpOutput);   /* DESTROY ME 059 */
    addReplyBulk(c, r);
    decrRefCount(r);                                     /* DESTROYED 059,060 */
}

// EXPLAIN EXPLAIN EXPLAIN EXPLAIN EXPLAIN EXPLAIN EXPLAIN EXPLAIN EXPLAIN
void explainRQ(cli *c,     cswc_t *w, wob_t *wb, bool cstar,
               int  qcols, int    *cmatchs) {
    initQueueOutput();
    (*queueOutput)("QUERY: ");
    for (int i = 0; i < c->argc; i++) {
        (*queueOutput)("%s ", (char *)c->argv[i]->ptr);
    } (*queueOutput)("\n");
    dumpQcols(queueOutput, w->wf.tmatch, cstar, qcols, cmatchs);
    dumpW(queueOutput, w); dumpWB(queueOutput, wb);
    qr_t    q;
    setQueued(w, wb, &q);
    dumpQueued(queueOutput, w, wb, &q, 0);
    dumpQueueOutput(c);
}
void explainJoin(cli *c, jb_t *jb) {
    initQueueOutput();
    (*queueOutput)("QUERY: ");
    for (int i = 0; i < c->argc; i++) {
        (*queueOutput)("%s ", (char *)c->argv[i]->ptr);
    } (*queueOutput)("\n");
    dumpJB(c, queueOutput, jb);
    dumpQueueOutput(c);
}
void explainCommand(redisClient *c) {
    c->Explain  = 1;
    int   oargc = c->argc;
    void *argv0 = c->argv[0];
    c->argc--;
    for (int i = 0; i < c->argc; i++) { /* shift argv[]s down once */
        c->argv[i] = c->argv[i + 1];
    }
    c->argv[oargc - 1] = argv0;         /* push first argv onto end */
    if      (!strcasecmp(c->argv[0]->ptr, "SCAN"))   tscanCommand(c);
    else if (!strcasecmp(c->argv[0]->ptr, "SELECT")) sqlSelectCommand(c);
    c->argc    = oargc;                 /* so all argv[] get freed */
    c->Explain = 0;
}

//TODO move to prep_stmt.c
// PREPARED_STATEMENTS PREPARED_STATEMENTS PREPARED_STATEMENTS
//TODO "SHOW  STATEMENTS"
//TODO "PRINT STATEMENT stmt_name"
//TODO "DROP  STATEMENT stmt_name"

#define COMP_FUNC_FMT "__PLAN_%s"

static bool has_prepare_arg(sds arg) {
    if (*arg == '$') {
        char *s = arg + 1; char *endptr = NULL;
        if (!strtoul(s, &endptr, 10)) return 1; // OK:DELIM:[\0]
        if (endptr && !*endptr)       return 1;
    }
    return 0;
}
static void storePreparedStatement(cli *c, uchar *blob, int size) {
    sds   pname = sdscatprintf(sdsempty(), COMP_FUNC_FMT, c->Prepare);
    sds   s     = sdsnewlen(blob, size); free(blob);     // FREED 111
    robj *val   = createObject(REDIS_STRING, s);
    robj  *o    = dictFetchValue(StmtD, pname);
    if (o) ASSERT_OK(dictReplace(StmtD, pname, val));
    else   ASSERT_OK(dictAdd    (StmtD, pname, val));
    addReply(c, shared.ok);
}
bool prepareJoin(cli *c, jb_t *jb) {
    //COMPUTE size -> [cargs[], JTA, cstar, qols, js[], n_jind, ij[], wb]
    list *cargl = listCreate();
    for (uint32 i = 0; i < jb->n_jind; i++) {
        ijp_t *ij = &jb->ij[i];
        if (ij->lhs.key) {
            if (has_prepare_arg(ij->lhs.key)) listAddNodeTail(cargl, VOIDINT i);
        }
    }
    int jtsize = getJTASize();
    int size   = 1 + sizeof(int) + (sizeof(int) * cargl->len) + // isj, cargs[]
                 jtsize + sizeof(bool)                        + // JTA, cstar
                 sizeof(int) + (jb->qcols  * sizeof(int) * 3) + // qcols [t,c,j]
                 sizeof(int);                                   // n_jind
    for (uint32 i = 0; i < jb->n_jind; i++) {
        int lhsize = getSizeFLT(&jb->ij[i].lhs);
        int rhsize = getSizeFLT(&jb->ij[i].rhs);
        if (lhsize == -1 || rhsize == -1) {
            addReply(c, shared.supported_prepare);             return 0;
        }
        size += lhsize + rhsize + 1; // +1 for has_rhs
    }
    int wbsize = getSizeWB(&jb->wb);
    if (wbsize == -1) { addReply(c, shared.supported_prepare); return 0; }
    size += wbsize;

    //SERIALISE -> ORDER [cargs[], JTA, cstar, qols, js[], n_jind, ij[], wb]
    uchar *blob = malloc(size);                          // FREE ME 111
    uchar *x    = blob;
    *x          = 1;                            x++; // FLAG: JOIN
    memcpy(x, &cargl->len,  sizeof(int));       x += sizeof(int); 
    listNode *ln;
    listIter *li = listGetIterator(cargl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        int carg = (int)(long)ln->value;
        memcpy(x, &carg, sizeof(int));          x += sizeof(int); 
    } listReleaseIterator(li);

    uchar *jtblob  = serialiseJTA(jtsize);
    memcpy(x, jtblob, jtsize);                  x += jtsize;
    free(jtblob);                                        // FREED 112
    
    memcpy(x, &jb->cstar,  sizeof(bool));       x += sizeof(bool);
    memcpy(x, &jb->qcols,  sizeof(int));        x += sizeof(int); 
    for (int i = 0; i < jb->qcols; i++) {
        memcpy(x, &jb->js[i].t,   sizeof(int)); x += sizeof(int); 
        memcpy(x, &jb->js[i].c,   sizeof(int)); x += sizeof(int); 
        memcpy(x, &jb->js[i].jan, sizeof(int)); x += sizeof(int); 
    }
    memcpy(x, &jb->n_jind, sizeof(int));        x += sizeof(int); 
    for (uint32 i = 0; i < jb->n_jind; i++) {
        int lhsize = getSizeFLT(&jb->ij[i].lhs);
        int rhsize = getSizeFLT(&jb->ij[i].rhs);
        *x         = rhsize ? 1 : 0;            x++; // FLAG: has_rhs
        uchar *lblob  = serialiseFLT(&jb->ij[i].lhs);
        memcpy(x, lblob, lhsize);               x += lhsize;
        if (rhsize) {
            uchar *rblob = serialiseFLT(&jb->ij[i].rhs);
            memcpy(x, rblob, rhsize);           x += rhsize;
        }
    }

    uchar *wbblob  = serialiseWB(&jb->wb);
    memcpy(x, wbblob, wbsize);                  x += wbsize;

    storePreparedStatement(c, blob, size);                     return 1;
}
static void executeCommand_Join(cli *c, uchar *x) {
    //DESERIALISE -> ORDER [cargs[], JTA, cstar, qols, js[], n_jind, ij[], wb]
    int    nargs; memcpy(&nargs, x, sizeof(int));         x += sizeof(int);
    if (nargs != (c->argc - 2)) {
        addReply(c, shared.execute_argc); return;
    }
    int cargs[nargs];
    for (int i = 0; i < nargs; i++) {
        memcpy(&cargs[i], x, sizeof(int));                x += sizeof(int);
    }
    int jtsize = deserialiseJTA(x);                       x += jtsize;

    jb_t jb; init_join_block(&jb);
    memcpy(&jb.cstar, x, sizeof(bool));                   x += sizeof(bool);
    memcpy(&jb.qcols, x, sizeof(int));                    x += sizeof(int); 
    jb.js = malloc(sizeof(jc_t) * jb.qcols);
    for (int i = 0; i < jb.qcols; i++) {
        memcpy(&jb.js[i].t,   x, sizeof(int));            x += sizeof(int); 
        memcpy(&jb.js[i].c,   x, sizeof(int));            x += sizeof(int); 
        memcpy(&jb.js[i].jan, x, sizeof(int));            x += sizeof(int); 
    }
    memcpy(&jb.n_jind, x, sizeof(int));                   x += sizeof(int); 
    jb.ij = malloc(sizeof(ijp_t) * jb.n_jind);
    for (uint32 i = 0; i < jb.n_jind; i++) {
        init_ijp(&jb.ij[i]);
        bool has_rhs = *x;                                x++;
        int lsize = deserialiseFLT(x, &jb.ij[i].lhs);     x += lsize;
        if (has_rhs) {
            int rsize = deserialiseFLT(x, &jb.ij[i].rhs); x += rsize;
        }
    }
    int wbsize = deserialiseWB(x, &jb.wb);                x += wbsize;

    for (int i = 2; i < c->argc; i++) { // put c->argc[] into join-plan
        jb.ij[cargs[i - 2]].lhs.key = sdsdup(c->argv[i]->ptr);
    }

    bool ok = optimiseJoinPlan(c, &jb) && validateChain(c, &jb);
    if (ok)   executeJoin     (c, &jb);

    destroy_join_block(c, &jb);
}
void prepareRQ(cli *c,     cswc_t *w,      wob_t *wb, bool cstar,
               int  qcols, int    *cmatchs) {
    //COMPUTE size -> ORDER [cstar, qcols, cmatchs, wtype, wf]
    int size   = 1 + sizeof(bool)      + sizeof(int)  + // isj, cstar, qcols
                 (sizeof(int) * qcols) + sizeof(bool) + // cmatchs, wtype
                 getSizeFLT(&w->wf);                    // wf
    int wbsize = getSizeWB(wb);
    if (wbsize == -1) { addReply(c, shared.supported_prepare); return; }
    size += wbsize;

    //SERIALISE    -> ORDER [cstar, qcols, cmatchs, wtype, wf]
    uchar *blob = malloc(size);                          // FREE ME 111
    uchar *x    = blob;
    *x          = 0;                          x++; // FLAG: NOT-JOIN -> RQ
    memcpy(x, &cstar,          sizeof(bool)); x += sizeof(bool);
    memcpy(x, &qcols,          sizeof(int));  x += sizeof(int); 
    for (int i = 0; i < qcols; i++) {
        memcpy(x, &cmatchs[i], sizeof(int));  x += sizeof(int); 
    }
    memcpy(x, &w->wtype, sizeof(bool));       x += sizeof(bool);
    int    wfsize = getSizeFLT  (&w->wf);
    uchar *wfblob = serialiseFLT(&w->wf);
    memcpy(x, wfblob, wfsize);                x += wfsize;
    
    uchar *wbblob  = serialiseWB(wb);
    memcpy(x, wbblob, wbsize);                x += wbsize;

    storePreparedStatement(c, blob, size);
}
static void executeCommand_RQ(cli *c, uchar *x) {
    if (c->argc > 3) { addReply(c, shared.execute_argc); return; }

    //DESERIALISE  -> ORDER [cstar, qcols, cmatchs, wtype, wf]
    bool cstar; memcpy(&cstar, x, sizeof(bool));    x += sizeof(bool);
    int  qcols; memcpy(&qcols, x, sizeof(int));     x += sizeof(int);
    int  cmatchs[qcols];
    for (int i = 0; i < qcols; i++) {
                memcpy(&cmatchs[i], x,sizeof(int)); x += sizeof(int); 
    }
    cswc_t w; wob_t wb;
    init_check_sql_where_clause(&w, -1, NULL); init_wob(&wb);
    memcpy(&w.wtype, x, sizeof(bool));              x += sizeof(bool);

    f_t *flt   = newEmptyFilter();
    int wfsize = deserialiseFLT(x, flt);            x += wfsize;
    flt->key   = sdsdup(c->argv[2]->ptr); // put c->argc[] into query-plan
    w.flist    = listCreate();
    listAddNodeTail(w.flist, flt);

    int wbsize = deserialiseWB(x, &wb);             x += wbsize;

    if (!optimiseRangeQueryPlan(c, &w, &wb)) return;
    bool need_cn = 0; //TODO incorporate need_cn into API
    sqlSelectBinary(c, w.wf.tmatch, cstar, cmatchs, qcols, &w, &wb, need_cn);

    if (!cstar) resetIndexPosOn(qcols, cmatchs);
    destroy_wob(&wb); destroy_check_sql_where_clause(&w);
}
void executeCommand(cli *c) {
    sds    pname = sdscatprintf(sdsempty(), COMP_FUNC_FMT, 
                                            (char *)c->argv[1]->ptr);
    robj  *o     = dictFetchValue(StmtD, pname);
    sdsfree(pname);
    if (!o) { addReply(c, shared.execute_miss); return; }
    uchar *x     = o->ptr;
    bool   isj   = *x;                              x++;
    if (isj) executeCommand_Join(c, x);
    else     executeCommand_RQ  (c, x);
}
void prepareCommand(redisClient *c) {
    if (strcasecmp(c->argv[2]->ptr, "AS")) {
        addReply(c, shared.prepare_syntax); return;
    }
    int   oargc = c->argc;
    void *a0    = c->argv[0]; void *a1 = c->argv[1]; void *a2 = c->argv[2];
    c->Prepare  = c->argv[1]->ptr;
    c->argc -= 3;
    for (int i = 0; i < c->argc; i++) { /* shift argv[]s down 2 */
        c->argv[i] = c->argv[i + 3];
    }
    if      (!strcasecmp(c->argv[0]->ptr, "SCAN"))   tscanCommand(c);
    else if (!strcasecmp(c->argv[0]->ptr, "SELECT")) sqlSelectCommand(c);
    // free argv[] "PREPARE planname AS"
    c->argv[oargc - 3] = a0; c->argv[oargc - 2] = a1; c->argv[oargc - 1] = a2;
    c->argc     = oargc;                 /* so all argv[] get freed */
    c->Prepare  = NULL;
}

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpRobj(printer *prn, robj *r, char *smsg, char *dmsg) {
    if (!r) return;
    if (r->encoding == REDIS_ENCODING_RAW) (*prn)(smsg, r->ptr);
    else                                   (*prn)(dmsg, r->ptr);
}
void dumpSds(printer *prn, sds s, char *smsg) {
    if (!s) return; (*prn)(smsg, s);
}
void dumpFL(printer *prn, char *prfx, char *title, list *flist) {
    if (flist) {
        (*prn)("%s%s: len: %d (%p)\n",
                prfx, title, listLength(flist), (void *)flist);
        listNode *ln;
        listIter *li = listGetIterator(flist, AL_START_HEAD);
        while((ln = listNext(li)) != NULL) {
            f_t *flt = ln->value;
            dumpFilter(prn, flt, prfx);
        } listReleaseIterator(li);
    }
}
void dumpQcols(printer *prn, int tmatch, bool cstar, int qcols, int *cmatchs) {
    (*prn)("\ttmatch: %d (%s)\n", tmatch, Tbl[tmatch].name);
    if (cstar) (*prn)("\t\tCOUNT(*)\n");
    else {
        for (int i = 0; i < qcols; i++) {
            int lc = cmatchs[i];
            (*prn)("\t\t%d: c: %d (%s)\n", i, lc,
                   (lc == -1) ? "" : Tbl[tmatch].col[lc].name);//TODO indx.pos()
        }
    }
}
void dumpWB(printer *prn, wob_t *wb) {
    if (wb->nob) {
        (*prn)("\t\tnob:    %d\n", wb->nob);
        for (uint32 i = 0; i < wb->nob; i++) {
            (*prn)("\t\t\tobt[%d](%s): %d\n", i, 
              (wb->obt[i] == -1) ? "" : Tbl[wb->obt[i]].name, wb->obt[i]);
            (*prn)("\t\t\tobc[%d](%s): %d\n", i,
                        (wb->obc[i] == -1) ? "" :
                        (char *)Tbl[wb->obt[i]].col[wb->obc[i]].name,
                         wb->obc[i]);
            (*prn)("\t\t\tasc[%d]: %d\n", i, wb->asc[i]);
        }
        (*prn)("\t\tlim:    %ld\n", wb->lim);
        (*prn)("\t\tofst:   %ld\n", wb->ofst);
    }
    dumpSds(prn, wb->ovar,  "\t\tovar:    %s\n");
}
void dumpW(printer *prn, cswc_t *w) {
    (*prn)("\tSTART dumpW: type: %d (%s)\n", w->wtype, RangeType[w->wtype]);
    dumpFilter(prn, &w->wf, "\t");
    dumpFL(prn, "\t\t", "FLIST", w->flist);
    (*prn)("\tEND dumpW\n");
}
void dumpIJ(cli *c, printer *prn, int i, ijp_t *ij, ijp_t *nij) {
    int lt = ij->lhs.tmatch; int lc = ij->lhs.cmatch;
    int lj = ij->lhs.jan;    int li = ij->lhs.imatch;
    if (ij->rhs.tmatch != -1) {
       int ki = ij->kimatch;
       int rt = ij->rhs.tmatch; int rc = ij->rhs.cmatch;
       int rj = ij->rhs.jan;    int ri = ij->rhs.imatch;
       (*prn)("\t\t\t\t%d: ki: %d LHS[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s)" \
              " RHS[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s)] nrows: %u\n", i,
          ki, lt, (lt == -1) ? "" : Tbl[lt].name,
              lc, (lc == -1) ? "" : Tbl[lt].col[lc].name,
              li, (li == -1) ? "" : Index[li].name,
              lj, (lj == -1) ? "" : getJoinAlias(lj),
                  rt, (rt == -1) ? "" : Tbl[rt].name,
                  rc, (rc == -1) ? "" : Tbl[rt].col[rc].name,
                  ri, (ri == -1) ? "" : Index[ri].name,
                  rj, (rj == -1) ? "" : getJoinAlias(rj),
                  ij->nrows);
        if (c->Explain) {
            if (nij) {
                dumpFL(prn, "\t\t\t\t", "KLIST", nij->lhs.klist);
                dumpFL(prn, "\t\t\t\t", "FLIST", nij->flist);
            }
        } else {
            dumpFL(prn, "\t\t\t\t", "FLIST", ij->flist);
            dumpFL(prn, "\t\t\t\t", "KLIST", ij->lhs.klist);
        }
    } else {
        (*prn)("\t\t\t\t%d: FLT[t: %d(%s) c: %d(%s) i: %d(%s) j: %d(%s) op: %d",
               i, lt, (lt == -1) ? "" : Tbl[lt].name,
                  lc, (lc == -1) ? "" : Tbl[lt].col[lc].name,
                  li, (li == -1) ? "" : Index[li].name,
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
            } listReleaseIterator(li);
            (*prn)("\t\t       \t]\n");
        }
    }
}
void dumpJB(cli *c, printer *prn, jb_t *jb) {
    (*prn)("\tSTART dumpJB\n");
    (*prn)("\t\tqcols:         %d\n", jb->qcols);
    for (int i = 0; i < jb->qcols; i++) {
        int lt = jb->js[i].t; int lc = jb->js[i].c; int jan = jb->js[i].jan;
        (*prn)("\t\t\t%d: jan: %d t: %d c: %d (%s: %s.%s)\n",
                 i, jan, lt, lc,
                 getJoinAlias(jan),
                 (lt == -1) ? "" : Tbl[lt].name, //TODO index.pos()
                 (lc == -1) ? "" : Tbl[lt].col[lc].name); //TODO index.pos()
    }
    if (c->Explain) {
        qr_t q; bzero(&q, sizeof(qr_t));
        cswc_t w; setupFirstJoinStep(&w, jb, &q);
        dumpFilter(prn, &w.wf, "\t");
        dumpFL(prn, "\t\t\t\t", "FLIST", jb->ij[0].flist);
    }
    (*prn)("\t\tn_jind:        %d\n", jb->n_jind);
    if (jb->hw != -1) {
        (*prn)("\t\thw:     %d\n", jb->hw);
        for (int k = 0; k < jb->hw; k++) {
            ijp_t *nij = (k == jb->hw -1) ? NULL : &jb->ij[k + 1];
            dumpIJ(c, prn, k, &jb->ij[k], nij);
        }
    } else {
        for (uint32 i = 0; i < jb->n_jind; i++) {
            dumpIJ(c, prn, i, &jb->ij[i], NULL);
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
    if (!c->Explain) {
        if (jb->hw != -1) {
            (*prn)("\t\tn_filters:     %d\n", (jb->n_jind - jb->hw));
            for (int j = jb->hw; j < (int)jb->n_jind; j++) {
                dumpIJ(c, prn, j, &jb->ij[j], NULL);
            }
        }
    }
    dumpWB(prn, &jb->wb);
    DUMP_JOIN_QED
    (*prn)("\tEND dumpJB\n");
}

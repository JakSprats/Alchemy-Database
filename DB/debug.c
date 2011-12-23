/*
  *
  * Implements ALCHEMY_DATABASE EXPLAIN & debug object dump functions
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

extern long JoinLim; extern long JoinOfst; extern bool JoinQed;

#define DUMP_JOIN_QED \
  (*prn)("\t\tJoinQed: %d JoinLim: %ld JoinOfst: %ld\n", \
         JoinQed, JoinLim, JoinOfst);

//TODO move to output.c
// DEFERRED_ADD_REPLY_BULK DEFERRED_ADD_REPLY_BULK DEFERRED_ADD_REPLY_BULK
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
void resetDeferredMultiBulk_ToError(redisClient *c, void *node, sds error) {
    if (!node) return; /* Abort when addDeferredMultiBulkLength not called. */
    listRelease(c->reply);
    c->reply = listCreate();
    listSetFreeMethod(c->reply, decrRefCount);
    listSetDupMethod (c->reply, dupClientReplyValue);
    robj *r  = createStringObject(error, sdslen(error));
    listAddNodeTail(c->reply, r);
}
void prependDeferredMultiBulkError(redisClient *c, void *node, sds error) {
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
    if (!node) return; /* Abort when addDeferredMultiBulkLength not called. */
    sds rep_int = sdscatprintf(sdsempty(), ":%ld\r\n", card);
    prependDeferredMultiBulkError(c, node, rep_int);
}
void replaceDMB_WithDirtyMissErr(cli *c, void *node) {
    if (!node) addReply(c, shared.dirty_miss); // SELECT "COUNT(*)"
    else {
        sds err = sdsdup(shared.dirty_miss->ptr); // FREE ME 111
        resetDeferredMultiBulk_ToError(c, node, err);
        sdsfree(err);                             // FREED 111
    }
}
void replaceDMB_With_QO_Err(cli *c, void *node) {
    if (!node) addReply(c, shared.join_qo_err); // SELECT "COUNT(*)"
    else {
        sds err = sdsdup(shared.join_qo_err->ptr); // FREE ME 111
        resetDeferredMultiBulk_ToError(c, node, err);
        sdsfree(err);                             // FREED 111
    }
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

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
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

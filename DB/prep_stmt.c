/*
  *
  * Implements ALCHEMY_DATABASE PREPARE & EXECUTE commands
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
#include "prep_stmt.h"

extern dict *StmtD;
extern bool  GlobalNeedCn;
extern long  JoinLim; extern long JoinOfst; extern bool JoinQed;

// PREPARED_STATEMENTS PREPARED_STATEMENTS PREPARED_STATEMENTS
//TODO "SHOW  STATEMENTS"
//TODO "PRINT STATEMENT stmt_name"
//TODO "DROP  STATEMENT stmt_name"

static bool has_prepare_arg(sds arg) {
    if (*arg == '$') {
        char *s = arg + 1; char *endptr = NULL;
        if (!strtoul(s, &endptr, 10)) return 1; // OK:DELIM:[\0]
        if (endptr && !*endptr)       return 1;
    }
    return 0;
}
static void storePreparedStatement(cli *c, uchar *blob, int size) {
    sds   pname = sdsdup(c->Prepare);
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

#ifdef EXECUTE_DOES_QUERY_OPTIMISATION
    f_t *flt   = newEmptyFilter();
    int wfsize = deserialiseFLT(x, flt);            x += wfsize;
    flt->key   = sdsdup(c->argv[2]->ptr); // put c->argc[] into query-plan
    w.flist    = listCreate();
    listAddNodeTail(w.flist, flt);
#else
    int wfsize = deserialiseFLT(x, &w.wf);          x += wfsize;
    w.wf.key   = sdsdup(c->argv[2]->ptr); // put c->argc[] into query-plan
    releaseAobj(&w.wf.akey);              // take out the "$1"
    convertFilterSDStoAobj(&w.wf);
#endif

    int wbsize = deserialiseWB(x, &wb);             x += wbsize;

#ifdef EXECUTE_DOES_QUERY_OPTIMISATION
    if (!optimiseRangeQueryPlan(c, &w, &wb)) return;
#endif

    sqlSelectBinary(c, w.wf.tmatch, cstar, cmatchs, qcols, &w, &wb, 
                    GlobalNeedCn);

    if (!cstar) resetIndexPosOn(qcols, cmatchs);
    destroy_wob(&wb); destroy_check_sql_where_clause(&w);
}
bool executeCommandBinary(cli *c, uchar *x) {
    bool   isj = *x;                                x++;
    if (isj) executeCommand_Join(c, x);
    else     executeCommand_RQ  (c, x);
    return 1;
}
bool executeCommandInnards(cli *c) {
    robj  *o   = dictFetchValue(StmtD, (sds)c->argv[1]->ptr);
    if (!o) { addReply(c, shared.execute_miss); return 0; }
    uchar *x   = o->ptr;
    return executeCommandBinary(c, x);
}
void executeCommand(cli *c) {
    executeCommandInnards(c);
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

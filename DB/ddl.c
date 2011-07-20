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

#include "redis.h"
#include "dict.h"

#include "bt.h"
#include "luatrigger.h"
#include "filter.h"
#include "query.h"
#include "index.h"
#include "lru.h"
#include "cr8tblas.h"
#include "parser.h"
#include "colparse.h"
#include "alsosql.h"
#include "common.h"
#include "ddl.h"

extern int     Num_tbls;
extern r_tbl_t Tbl[MAX_NUM_TABLES];
extern int     Num_indx;
extern r_ind_t Index[MAX_NUM_INDICES];

extern char *RangeType[5];

// GLOBALS
dict *Constraints = NULL;

// CONSTANT GLOBALS
char *Col_type_defs[] = {"INT", "LONG", "TEXT", "FLOAT", "NONE"};

static bool checkRepeatCnames(cli *c, int tmatch, sds cname) {
    if (!strcasecmp(cname, "LRU")) { addReply(c, shared.col_lru); return 0; }
    r_tbl_t *rt = &Tbl[tmatch];
    for (int i = 0; i < rt->col_count; i++) {
        if (!strcasecmp(cname, rt->col_name[i]->ptr)) {
            addReply(c, shared.nonuniquecolumns); return 0;
    }}
    return 1;
}

/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
static void createTableCommitReply(redisClient *c,
                                   char         cnames[][MAX_COLUMN_NAME_SIZE],
                                   int          ccount,
                                   char        *tname,
                                   int          tlen) {
    if (Num_tbls == MAX_NUM_TABLES) {
        addReply(c, shared.toomanytables); return;
    }
    if (ccount < 2) { addReply(c, shared.toofewcolumns); return; }
    for (int i = 0; i < ccount; i++) { /* check for repeat column names */
        if (!strcasecmp(cnames[i], "LRU")) {
            addReply(c, shared.col_lru); return;
        }
        for (int j = 0; j < ccount; j++) {
            if (i == j) continue;
            if (!strcasecmp(cnames[i], cnames[j])) {
                addReply(c, shared.nonuniquecolumns); return;
            }
        }
    }

    /* BTREE implies an index on "tbl_pk_index" -> autogenerate */
    int  vimatch  = Num_indx;
    sds  iname    = P_SDS_EMT "%s_%s_%s", tname, cnames[0], INDEX_DELIM); //D073
    bool ok       = newIndex(c, iname, Num_tbls, 0, NULL, 0, 1, 0, NULL);
    sdsfree(iname);                                      /* DESTROYED 073 */
    if (!ok) return;
    addReply(c, shared.ok); /* commited */
    int      tmatch = Num_tbls;
    robj    *tbl    = createStringObject(tname, tlen);
    r_tbl_t *rt     = &Tbl[tmatch];

    //bzero(rt); NOTE: not possible col_types assigned in parseCreateTable
    // TODO FIX & make function initTable(), user in rdbLoadBT() & emptyTable()
    bzero(rt->col_indxd, MAX_COLUMN_PER_TABLE);
    rt->nmci        = rt->lrud       = rt->ainc       = rt->n_intr    = \
                      rt->lastts     = rt->nextts     =  0;
    rt->lruc        = rt->lrui       = rt->sk         = rt->fk_cmatch = \
                      rt->fk_otmatch = rt->fk_ocmatch = -1;
    rt->rn          = NULL;
    rt->name        = tbl;
    rt->vimatch     = vimatch;
    rt->col_count   = ccount;
    for (int i = 0; i < rt->col_count; i++) {
        rt->col_name[i] = _createStringObject(cnames[i]);
    }
    rt->btr         = createDBT(rt->col_type[0], tmatch);
    Num_tbls++;
}
static void createTable(redisClient *c) { //printf("createTable\n");
    char *tname = c->argv[2]->ptr;
    int   tlen  = sdslen(c->argv[2]->ptr);
    tname       = rem_backticks(tname, &tlen); /* Mysql compliant */
    if (find_table_n(tname, tlen) != -1) {
        addReply(c, shared.nonuniquetablenames); return;
    }
    if (!strncasecmp(c->argv[3]->ptr, "SELECT ", 7) ||
        !strncasecmp(c->argv[3]->ptr, "SCAN ",   5)) {
        createTableSelect(c); return;
    }
    char cnames[MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    int  ccount = 0;
    if (parseCreateTable(c, cnames, &ccount, c->argv[3]->ptr)) {
        createTableCommitReply(c, cnames, ccount, tname, tlen);
    }
}

static cst_t *init_constraint(sds rname,  int tmatch, int cmatch,
                              int imatch, bool asc) {
    cst_t *rn  = malloc(sizeof(cst_t)); // FREE ME 080
    rn->name   = sdsnew(rname);         // DEST ME 081
    rn->tmatch = tmatch;
    rn->cmatch = cmatch;
    rn->imatch = imatch;
    rn->asc    = asc;
    return rn;
}
static void destroy_constraint(void *v) {
    if (!v) return;
    cst_t *rn = (cst_t *)v;
    sdsfree(rn->name); // DESTROYED 081
    free(rn);          // FREED     080
}

//TODO this will be replace by "ALTER INDEX iname ORDER BY cname ASC"
// SYNTAX: CREATE CONSTRAINT c_foo ON foo (ts) RESPECTS INDEX (i_foo) [ASC|DESC]
static void createConstraint(cli *c) { //printf("createConstraint\n");
    if (c->argc < 9                             ||
        strcasecmp(c->argv[3]->ptr, "ON")       ||
        strcasecmp(c->argv[6]->ptr, "RESPECTS") ||
        strcasecmp(c->argv[7]->ptr, "INDEX")) {
         addReply(c, shared.constraint_wrong_nargs); return;
    }
    char    *rname  = c->argv[2]->ptr;
    TABLE_CHECK_OR_REPLY(c->argv[4]->ptr,)
    r_tbl_t *rt     = &Tbl[tmatch];
    char    *token  = c->argv[5]->ptr;
    char    *end    = strchr(token, ')');
    if (!end || (*token != '(')) {
        addReply(c, shared.constraint_wrong_nargs); return;
    }
    STACK_STRDUP(cname, (token + 1), (end - token - 1))
    int      cmatch = find_column(tmatch, cname);
    if (cmatch == -1) { addReply(c, shared.constraint_wrong_nargs); return; }
    if (find_index(tmatch, cmatch) != -1) {
         addReply(c, shared.constraint_col_indexed); return;
    }
    token           = c->argv[8]->ptr;
    end             = strchr(token, ')');
    if (!end || (*token != '(')) {
        addReply(c, shared.constraint_wrong_nargs); return;
    }
    STACK_STRDUP(iname, (token + 1), (end - token - 1))
    int      imatch = match_index_name(iname);
    if (imatch == -1) { addReply(c, shared.constraint_wrong_nargs); return; }
    r_ind_t *ri     = &Index[imatch];
    if (ri->table != tmatch) {
        addReply(c, shared.constraint_table_mismatch); return;
    }
    uchar    ctype  = rt->col_type[cmatch];
    uchar    itype  = Tbl[ri->table].col_type[ri->column];
    if (!C_IS_NUM(ctype) || !C_IS_NUM(itype)) {
         addReply(c, shared.constraint_not_num); return;
    }

    //TODO no UNIQUE indexes - makes no sense
    //TODO support MCIs - need logic
    bool     asc    = 1;
    if (c->argc == 10 && !strcasecmp(c->argv[9]->ptr, "DESC")) asc = 0;
    cst_t   *rn     = init_constraint(rname, tmatch, cmatch, imatch, asc);//D082
    int      retval = dictAdd(Constraints, sdsnew(rname), rn);
    if (retval != DICT_OK) {
         addReply(c, shared.constraint_nonuniq); free(rn); return; // FREED 080
    }
    rt->rn          = rn;

printf("createConstraint: rname: %s tmatch: %d cname: %s cmatch: %d iname: %s imatch: %d asc: %d\n", rname, tmatch, cname, cmatch, iname, imatch, asc);

    addReply(c, shared.ok);
}
//TODO dropConstraint(cli *c) { }

void createCommand(redisClient *c) { //printf("createCommand\n");
    bool  tbl  = 0; bool ind = 0; bool lru = 0; bool luat = 0; bool cnstr = 0;
    uchar slot = 2;
    if      (!strcasecmp(c->argv[1]->ptr, "TABLE"))      { tbl   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "INDEX"))      { ind   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "CONSTRAINT")) { cnstr = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "UNIQUE"))     { ind   = 1; slot = 3;}
    else if (!strcasecmp(c->argv[1]->ptr, "LRUINDEX"))   { lru   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "LUATRIGGER")) { luat  = 1; }
    else                           { addReply(c, shared.createsyntax); return; }
    robj *o = lookupKeyRead(c->db, c->argv[slot]);
    if (o) { addReply(c, shared.nonuniquekeyname); return; }
    if      (tbl)     createTable     (c);
    else if (ind)     createIndex     (c);
    else if (lru)     createLruIndex  (c);
    else if (luat)    createLuaTrigger(c);
    else  /* cnstr */ createConstraint(c);
    server.dirty++; /* for appendonlyfile */
}

/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
unsigned long emptyTable(int tmatch) {
    r_tbl_t *rt      = &Tbl[tmatch];
    if (!rt->name) return 0;                 /* already deleted */
    decrRefCount(rt->name);
    rt->name         = NULL;
    MATCH_INDICES(tmatch)
    ulong    deleted = 0;
    if (matches) {                          /* delete indices first */
        for (int i = 0; i < matches; i++) { /* build list of robj's to delete */
            emptyIndex(inds[i]); deleted++;
    }} //TODO shuffle indices to make space for deleted indices
    for (int j = 0; j < Tbl[tmatch].col_count; j++) {
        decrRefCount(rt->col_name[j]); rt->col_name[j] = NULL;
        rt->col_type[j] = -1;
    }
    bt_destroy(rt->btr);
    //TODO remove rt->rn from dict: Constraints
    destroy_constraint(rt->rn);                             // DESTROYED 082
    bzero(rt, sizeof(r_tbl_t));
    rt->vimatch = rt->lruc = rt->lrui    = -1; //TODO use initTable()
    deleted++; //TODO shuffle tables to make space for deleted indices
    return deleted;
}
static void dropTable(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)
    unsigned long deleted = emptyTable(tmatch);
    addReplyLongLong(c, deleted);
    server.dirty++;
} 
void dropCommand(redisClient *c) {
    bool tbl = 0; bool ind = 0; bool luat = 0;
    if      (!strcasecmp(c->argv[1]->ptr, "TABLE"))      { tbl  = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "INDEX"))      { ind  = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "LUATRIGGER")) { luat = 1; }
    /* NOTE: LRUINDEX can not be dropped */
    else                             { addReply(c, shared.dropsyntax); return; }
    if      (tbl)    dropTable(c);
    else if (ind)    dropIndex(c);
    else  /* luat */ dropLuaTrigger(c);
    server.dirty++; /* for appendonlyfile */
}

void addColumn(int tmatch, char *cname, int ctype) {
    r_tbl_t *rt = &Tbl[tmatch];
    rt->col_name [rt->col_count] = _createStringObject(cname);
    rt->col_type [rt->col_count] = ctype;
    rt->col_indxd[rt->col_count] = 0;
    rt->col_count++;
}
void alterCommand(cli *c) {
    bool altc = 0, altsk = 0, altfk = 0;
    if (strcasecmp(c->argv[1]->ptr, "TABLE") ||
        strcasecmp(c->argv[3]->ptr, "ADD")) {
        addReply(c, shared.altersyntax);                              return;
    }
    if      (!strcasecmp(c->argv[4]->ptr, "COLUMN"))   altc  = 1;
    else if (!strcasecmp(c->argv[4]->ptr, "SHARDKEY")) altsk = 1;
    else if (!strcasecmp(c->argv[4]->ptr, "FOREIGN") &&
             !strcasecmp(c->argv[5]->ptr, "KEY"))      altfk = 1;
    else { addReply(c, shared.altersyntax);                           return; }
    uchar  ctype;
    int    len   = sdslen(c->argv[2]->ptr);
    char  *tname = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    if (OTHER_BT(getBtr(tmatch))) { addReply(c, shared.alter_other);  return; }
    if         (altc) {
        if (c->argc < 7) { addReply(c, shared.altersyntax);           return; }
        if (!checkRepeatCnames(c, tmatch, c->argv[5]->ptr))           return;
        if (!parseColType     (c, c->argv[6]->ptr, &ctype))           return;
        addColumn(tmatch, c->argv[5]->ptr, ctype);
    } else if  (altsk) {
        if (c->argc < 6) { addReply(c, shared.altersyntax);           return; }
        sds cname  = c->argv[5]->ptr;
        if (Tbl[tmatch].sk != -1) { addReply(c, shared.alter_sk_rpt); return; }
        int cmatch = find_column_n(tmatch, cname, sdslen(cname));
        if (cmatch == -1)      { addReply(c, shared.altersyntax);     return; }
        int imatch = find_index(tmatch, cmatch);
        if (imatch == -1)      { addReply(c, shared.alter_sk_no_i);   return; }
        if (Index[imatch].lru) { addReply(c, shared.alter_sk_no_lru); return; }
        Tbl[tmatch].sk = cmatch;
    } else { /* altfk */
        if (c->argc < 10) { addReply(c, shared.altersyntax);           return; }
        if (strcasecmp(c->argv[7]->ptr, "REFERENCES")) {
            addReply(c, shared.altersyntax);                           return;
        }
        sds   fkname  = c->argv[6]->ptr;
        int   fklen   = sdslen(fkname);
        char *fkend   = fkname + fklen - 1;
        if (*fkname != '(' || *fkend != ')') {
            addReply(c, shared.altersyntax);                           return;
        }
        sds   o_tname = c->argv[8]->ptr;
        sds   o_cname = c->argv[9]->ptr;
        int   oclen   = sdslen(o_cname);
        char *o_cend  = o_cname + oclen - 1;
        if (*o_cname != '(' || *o_cend != ')') {
            addReply(c, shared.altersyntax);                           return;
        }
        int cmatch = find_column_n(tmatch, fkname + 1, fklen - 2);
        if (cmatch == -1) {
            addReply(c, shared.altersyntax);                           return;
        }
        int otmatch = find_table(o_tname);
        if (otmatch == -1) {
            addReply(c, shared.altersyntax);                           return;
        }
        int ocmatch = find_column_n(otmatch, o_cname + 1, oclen - 2);
        if (ocmatch == -1) {
            addReply(c, shared.altersyntax);                           return;
        }
        r_tbl_t *rt  = &Tbl[tmatch];
        r_tbl_t *ort = &Tbl[otmatch];
        if (rt->sk != cmatch || ort->sk != ocmatch) {
            addReply(c, shared.alter_fk_not_sk);                       return;
        }
        //NOTE: BOTH are indexed because shardkey's must be
        if (rt->fk_cmatch != -1) {
            addReply(c, shared.alter_fk_repeat);                       return;
        }
        rt->fk_cmatch  = cmatch;
        rt->fk_otmatch = otmatch;
        rt->fk_ocmatch = ocmatch;
    }
    addReply(c, shared.ok);
}
/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
sds DumpOutput;
void initQueueOutput() {
    DumpOutput = sdsempty();                             /* DESTROY ME 060 */
}
int queueOutput(const char *fmt, ...) { /* straight copy of sdscatprintf() */
    va_list ap; char *buf; size_t buflen = 16;
    while(1) {
        buf             = malloc(buflen);
        buf[buflen - 2] = '\0';
        va_start(ap, fmt);
        vsnprintf(buf, buflen, fmt, ap);
        va_end(ap);
        if (buf[buflen-2] != '\0') { free(buf); buflen *= 2; continue; }
        break;
    }
    DumpOutput = sdscat(DumpOutput, buf);
    free(buf);
    return sdslen(DumpOutput);
}
void dumpQueueOutput(cli *c) {
    robj *r  = createObject(REDIS_STRING, DumpOutput);   /* DESTROY ME 059 */
    addReplyBulk(c, r);
    decrRefCount(r);                                     /* DESTROYED 059,060 */
}
void explainRQ(cli *c, cswc_t *w, wob_t *wb) {
    initQueueOutput();
    (*queueOutput)("QUERY: ");
    for (int i = 0; i < c->argc; i++) {
        (*queueOutput)("%s ", (char *)c->argv[i]->ptr);
    } (*queueOutput)("\n");
    dumpW(queueOutput, w);
    dumpWB(queueOutput, wb);
    qr_t    q;
    setQueued(w, wb, &q);
    dumpQueued(queueOutput, w, wb, &q, 0);
    dumpQueueOutput(c);

}
void dumpRobj(printer *prn, robj *r, char *smsg, char *dmsg) {
    if (!r) return;
    if (r->encoding == REDIS_ENCODING_RAW) {
        (*prn)(smsg, r->ptr);
    } else {
        (*prn)(dmsg, r->ptr);
    }
}
void dumpSds(printer *prn, sds s, char *smsg) {
    if (!s) return;
    (*prn)(smsg, s);
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
        }
        listReleaseIterator(li);
    }
}
void dumpWB(printer *prn, wob_t *wb) {
    if (wb->nob) {
        (*prn)("\t\tnob:    %d\n", wb->nob);
        for (uint32 i = 0; i < wb->nob; i++) {
            (*prn)("\t\t\tobt[%d](%s): %d\n", i, 
              (wb->obt[i] == -1) ? "" : (char *)Tbl[wb->obt[i]].name->ptr,
                wb->obt[i]);
            (*prn)("\t\t\tobc[%d](%s): %d\n", i,
                        (wb->obc[i] == -1) ? "" :
                        (char *)Tbl[wb->obt[i]].col_name[wb->obc[i]]->ptr,
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

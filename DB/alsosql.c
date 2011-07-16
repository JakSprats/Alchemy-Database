/*
  *
  * This file implements the basic SQL commands of Alsosql (single row ops)
  *  and calls the range-query and join ops
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

#include "redis.h"
#include "zmalloc.h"

#include "bt.h"
#include "filter.h"
#include "query.h"
#include "index.h"
#include "lru.h"
#include "range.h"
#include "rpipe.h"
#include "desc.h"
#include "cr8tblas.h"
#include "lua_integration.h"
#include "wc.h"
#include "parser.h"
#include "colparse.h"
#include "aobj.h"
#include "common.h"
#include "alsosql.h"

extern int     Num_tbls;
extern r_tbl_t Tbl[MAX_NUM_TABLES];
extern int     Num_indx;
extern r_ind_t Index[MAX_NUM_INDICES];

// GLOBALS
uchar  OutputMode = OUTPUT_NORMAL;

// CONSTANT GLOBALS
char *EMPTY_STRING = "";
char  OUTPUT_DELIM = ',';

char *Col_type_defs[] = {"INT", "LONG", "TEXT", "FLOAT", "NONE"};

/* enum OP              {NONE,   EQ,     NE,     GT,     GE,     LT,    LE}; */
char *OP_Desc   [NOP] = {"",    "=",    "!=",   ">",    ">=",   "<",   "<=", 
                            "RangeQuery", "IN"};
uchar OP_len    [NOP] = {0,      1,      2,      1,      2,      1,     2,
                            -1,           -1};
aobj_cmp *OP_CMP[NOP] = {NULL, aobjEQ, aobjNE, aobjLT, aobjLE, aobjGT, aobjGE,
                            NULL,         NULL};
/* NOTE ranges (<,<=,>,>=) comparison functions are opposite of intuition */

char *RangeType[5] = {"ERROR", "SINGLE_PK", "RANGE", "IN", "SINGLE_FK"};

/* PROTOTYPES */
static int updateAction(cli *c, char *u_vallist, aobj *u_apk, int u_tmatch);

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
    //bzero(rt); NOTE: not possible col_types assigned in parseCreateTable - FIX
    bzero(rt->col_indxd, MAX_COLUMN_PER_TABLE);
    rt->nmci        = rt->ainc = rt->lrud = rt->nltrgr    = rt->n_intr     =
                                            rt->lastts    = rt->nextts     =  0;
    rt->lruc        = rt->lrui = rt->sk   = rt->fk_cmatch = rt->fk_otmatch = 
                                                            rt->fk_ocmatch = -1;
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
void createCommand(redisClient *c) { //printf("createCommand\n");
    bool  tbl  = 0; bool ind = 0; bool lru = 0; bool luat = 0;
    uchar slot = 2;
    if      (!strcasecmp(c->argv[1]->ptr, "TABLE"))      { tbl  = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "INDEX"))      { ind  = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "UNIQUE"))     { ind  = 1; slot = 3;}
    else if (!strcasecmp(c->argv[1]->ptr, "LRUINDEX"))   { lru  = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "LUATRIGGER")) { luat = 1; }
    else                           { addReply(c, shared.createsyntax); return; }
    robj *o = lookupKeyRead(c->db, c->argv[slot]);
    if (o) { addReply(c, shared.nonuniquekeyname); return; }
    if      (tbl)    createTable     (c);
    else if (ind)    createIndex     (c);
    else if (lru)    createLruIndex  (c);
    else /*  luat */ createLuaTrigger(c);
    server.dirty++; /* for appendonlyfile */
}

/* INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT */
static void addRowSizeReply(cli *c, int tmatch, bt *btr, int len) {
    char buf[128];
    ull  index_size = get_sum_all_index_size_for_table(tmatch);
    snprintf(buf, 127,
          "INFO: BYTES: [ROW: %d BT-TOTAL: %ld [BT-DATA: %ld] INDEX: %lld]",
               len, btr->msize, btr->dsize, index_size);
    buf[127] = '\0';
    robj *r  = _createStringObject(buf); addReplyBulk(c, r); decrRefCount(r);
}
#define DEBUG_INSERT_DEBUG \
  printf("DO update: %d (%s) tmatch: %d apk: ", \
          update, c->argv[update]->ptr, tmatch); dumpAobj(printf, &apk);

#define DEBUG_REPLY_LIST \
  { listNode  *ln; listIter  *li = listGetIterator(c->reply, AL_START_HEAD); \
   while((ln = listNext(li)) != NULL) { robj *r = ln->value; printf("REPLY: %s\n", r->ptr); } }

#define INS_ERR 0
#define INS_INS 1
#define INS_UP  2
static uchar insertCommit(cli  *c,      robj **argv,   sds     vals,
                          int   ncols,  int    tmatch, int     matches,
                          int   inds[], int    pcols,  int     cmatchs[],
                          bool  repl,   bool   upd,    uint32 *tsize,
                          bool  parse,  sds   *key) {
    twoint cofsts[ncols];
    if (pcols) for (int i = 0; i < ncols; i++) cofsts[i].i = cofsts[i].j = -1;
    aobj     apk; initAobj(&apk);
    bool     ret    = INS_ERR;    /* presume failure */
    void    *nrow   = NULL;       /* B4 GOTO */
    sds      pk     = NULL;
    int      pklen  = 0;                              // NEEDED? use sdslen(pk)
    r_tbl_t *rt     = &Tbl[tmatch];
    int      lncols = rt->lrud ? ncols - 1 : ncols; /* w/o LRU */
    char    *mvals  = parseRowVals(vals, &pk, &pklen, lncols, cofsts, tmatch,
                                   pcols, cmatchs);  /* ERR now GOTO */
    if (!mvals) { addReply(c, shared.insertcolumn);            goto insc_end; }
    if (parse) { // used in cluster-mode to get sk's value
        int skl = cofsts[rt->sk].j - cofsts[rt->sk].i;
        sds sk  = rt->sk ? sdsnewlen(mvals + cofsts[rt->sk].i, skl) : pk;
        *key    = sdscatprintf(sdsempty(), "%s=%s.%s", sk,
                      (char *)rt->name->ptr, (char *)rt->col_name[rt->sk]->ptr);
        return ret;
    }
    int      pktyp  = rt->col_type[0];
    apk.type        = apk.enc = pktyp;
    if        (C_IS_I(pktyp)) {
        long l      = atol(pk);                              /* OK: DELIM: \0 */
        if (l >= TWO_POW_32) { addReply(c, shared.uint_pkbig); goto insc_end; }
        apk.i       = (int)l;
    } else if (C_IS_L(pktyp)) apk.l = strtoul(pk, NULL, 10); /* OK: DELIM: \0 */
      else if (C_IS_F(pktyp)) apk.f = atof(pk);              /* OK: DELIM: \0 */
      else { /* COL_TYPE_STRING */
        apk.s       = pk; apk.len = pklen; apk.freeme = 0; /* "pk freed below */
    }
    bt   *btr  = getBtr(tmatch);
    void *rrow = btFind(btr, &apk);
    int   len  = 0;
    if (rrow && !upd && !repl) {
         addReply(c, shared.insert_ovrwrt);                    goto insc_end;
    } else if (rrow && upd) {                              //DEBUG_INSERT_DEBUG
        len = updateAction(c, argv[upd]->ptr, &apk, tmatch);
        if (len == -1)                                         goto insc_end;
        ret = INS_UP;             /* negate presumed failure */
    } else {
        nrow = createRow(c, btr, tmatch, lncols, mvals, cofsts);
        if (!nrow) /* e.g. (UINT_COL > 4GB) error */           goto insc_end;
        if (matches) { /* Add to Indexes */
            for (int i = 0; i < matches; i++) { /* REQ: addIndex B4 delIndex */
                if (!addToIndex(c, btr, &apk, nrow, inds[i]))  goto insc_end;
            }
            if (repl && rrow) { /* Delete repld row's Indexes - same PK */
                for (int i = 0; i < matches; i++) {
                    delFromIndex(btr, &apk, rrow, inds[i]);
            }}
        }
        len = (repl && rrow) ? btReplace(btr, &apk, nrow) :
                               btAdd    (btr, &apk, nrow);
        UPDATE_AUTO_INC(pktyp, apk)
        ret = INS_INS;            /* negate presumed failure */
    }
    if (tsize) *tsize = *tsize + len;
    server.dirty++;

insc_end:
    if (nrow && NORM_BT(btr)) free(nrow);                /* FREED 023 */
    if (pk)                   free(pk);                  /* FREED 021 */
    releaseAobj(&apk);
    return ret;
}
#define DEBUG_INSERT_ACTION_1 \
  for (int i = 0; i < c->argc; i++) \
    printf("INSERT: cargv[%d]: %s\n", i, c->argv[i]->ptr);

#define AEQ(a,b) !strcasecmp(c->argv[a]->ptr, b)


void insertParse(cli *c, robj **argv, bool repl, int tmatch,
                 bool parse, sds *key) {
    MATCH_INDICES(tmatch)
    r_tbl_t *rt    = &Tbl[tmatch];
    int      ncols = rt->col_count; /* NOTE: need space for LRU */
    int      cmatchs[MAX_COLUMN_PER_TABLE]; /* for partial inserts */
    int      pcols = 0;
    int      valc  = 3;
    if (strcasecmp(argv[valc]->ptr, "VALUES")) {//TODO break block into func
        bool ok   = 0;
        sds  cols = argv[valc]->ptr;
        if (cols[0] == '(' && cols[sdslen(cols) - 1] == ')' ) { /* COL DECL */
            STACK_STRDUP(clist, (cols + 1), (sdslen(cols) - 2));
            parseCommaSpaceList(c, clist, 1, 0, 0, tmatch, cmatchs,
                                0, NULL, NULL, NULL, &pcols, NULL);
            if (pcols) {
                if (initLRUCS(tmatch, cmatchs, pcols)) { /* LRU in ColDecl */
                    addReply(c, shared.insert_lru); return;
                }
                if (OTHER_BT(getBtr(tmatch)) && pcols != 2 && !cmatchs[0]) {
                    addReply(c, shared.part_insert_other); return;
                }
                valc++; if (!strcasecmp(argv[valc]->ptr, "VALUES")) ok = 1;
            }
        }
        if (!ok) { addReply(c, shared.insertsyntax_no_values); return; }
    }
    bool print = 0; uint32 upd = 0; int largc = c->argc;
    if (largc > 5) {
        if (AEQ((largc - 1), "RETURN SIZE")) { print = 1; largc--; }
        if (largc > 6) {
            if (AEQ((largc - 2), "ON DUPLICATE KEY UPDATE")) {
                upd = (uint32)largc - 1; largc -= 2;
        }}
    }
    if (upd && repl) { addReply(c, shared.insert_replace_update); return; }
    uchar ret = INS_ERR; uint32 tsize = 0;
    for (int i = valc + 1; i < largc; i++) {
        ret = insertCommit(c, argv, argv[i]->ptr, ncols, tmatch, matches, inds,
                           pcols, cmatchs, repl, upd, print ? &tsize : NULL,
                           parse, key);
        if (ret == INS_ERR) return;
    }
    if (print) addRowSizeReply(c, tmatch, getBtr(tmatch), tsize);
    else       addReply(c, shared.ok);
}
static void insertAction(cli *c, bool repl) {           //DEBUG_INSERT_ACTION_1
   if (strcasecmp(c->argv[1]->ptr, "INTO")) {
        addReply(c, shared.insertsyntax_no_into); return;
    }
    int      len   = sdslen(c->argv[2]->ptr);
    char    *tname = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    insertParse(c, c->argv, repl, tmatch, 0, NULL);
}
/* NOTE: INSERT HAS 4 SYNTAXES
     1: INSERT INTO tbl VALUES "(,,,,)"
     2: INSERT INTO tbl VALUES "(,,,,)" "(,,,,)" "(,,,,)"
     3: INSERT INTO tbl VALUES "(,,,,)" "ON DUPLICATE KEY UPDATE" update_stmt
     4: INSERT INTO tbl VALUES "(,,,,)" "RETURN SIZE" */
void insertCommand (cli *c) { insertAction(c, 0); }
void replaceCommand(cli *c) { insertAction(c, 1); }

//TODO move to wc.c
void init_wob(wob_t *wb) {
    bzero(wb, sizeof(wob_t));
    wb->nob    =  0;
    wb->lim    = -1;
    wb->ofst   = -1;
    wb->ovar   = NULL;
}
void destroy_wob(wob_t *wb) {
    if (wb->ovar) sdsfree(wb->ovar);
}
void init_check_sql_where_clause(cswc_t *w, int tmatch, sds token) {
    bzero(w, sizeof(cswc_t));
    w->wtype     = SQL_ERR_LKP;
    initFilter(&w->wf);                                  /* DESTROY ME 065 */
    w->wf.tmatch = tmatch; //TODO tmatch not needed here, cuz promoteKLorFLtoW()
    w->token     = token;
}
void destroyINLlist(list **inl) {
    if (*inl)   {
        (*inl)->free = destroyAobj;
        listRelease(*inl);
        *inl         = NULL;
    }
}
void releaseFlist(list **flist) {
    if (*flist) {
        (*flist)->free = NULL;
        listRelease(*flist);
        *flist         = NULL;
    }
}
void destroyFlist(list **flist) {
    if (*flist) {
        (*flist)->free = destroyFilter;
        listRelease(*flist);
        *flist         = NULL;
    }
}
void destroy_check_sql_where_clause(cswc_t *w) {
    releaseFilterD_KL(&w->wf);                           /* DESTROYED 065 */
    destroyFlist( &w->flist);
    if (w->lvr)   sdsfree(w->lvr);
}

bool leftoverParsingReply(redisClient *c, char *x) {
    if (!x) return 1;
    while (ISBLANK(*x)) x++;
    if (*x) {
        addReplySds(c, P_SDS_EMT "-ERR could not parse '%s'\r\n", x)); return 0;
    }
    return 1;
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
    c->argc    = oargc;                    /* so all argv[] get freed */
    c->Explain = 0;
}
void addReplyRow(cli *c, robj *r, int tmatch, aobj *apk, uchar *lruc) {
    updateLru(c, tmatch, apk, lruc); /* NOTE: updateLRU (SELECT) */
    if (OREDIS) addReply(c,     r);
    else        addReplyBulk(c, r);
}

/* LruColInSelect LruColInSelect LruColInSelect LruColInSelect */
inline bool initLRUCS(int tmatch, int cmatchs[], int qcols) {
    r_tbl_t *rt    = &Tbl[tmatch];
    if (rt->lrud) {
        for (int i = 0; i < qcols; i++) {
            if (cmatchs[i] == rt->lruc) return 1;
    }}
    return 0;
}
inline bool initLRUCS_J(jb_t *jb) {
    for (int i = 0; i < jb->qcols; i++) {
        if (Tbl[jb->js[i].t].lruc == jb->js[i].c) return 1;
    }
    return 0;
}

void sqlSelectCommand(redisClient *c) {
    if (c->argc == 2) { /* this is a REDIS "select DB" command" */
        selectCommand(c); return;
    }
    if (c->argc != 6) { addReply(c, shared.selectsyntax); return; }
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  =  0;
    int  qcols  =  0;
    int  tmatch = -1;
    bool join   =  0;
    sds  tlist  = c->argv[3]->ptr;
    if (!parseSelectReply(c, 0, NULL, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          tlist, c->argv[4]->ptr)) return;
    if (join) { joinReply(c);                      return; }

    c->LruColInSelect = initLRUCS(tmatch, cmatchs, qcols);
    cswc_t w; wob_t wb;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr);
    init_wob(&wb);
    parseWCReply(c, &w, &wb, SQL_SELECT);
    if (w.wtype == SQL_ERR_LKP)                             goto sel_cmd_end;
    if (!leftoverParsingReply(c, w.lvr))                    goto sel_cmd_end;
    if (cstar && wb.nob) { /* SELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);                  goto sel_cmd_end;
    }
    if (c->Explain) { explainRQ(c, &w, &wb);                goto sel_cmd_end; }
    //dumpW(printf, &w); dumpWB(printf, &wb);
    if (w.wtype != SQL_SINGLE_LKP) { /* FK, RQ, IN */
        if (w.wf.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found); goto sel_cmd_end;
        }
        if (w.wf.imatch == Tbl[tmatch].lrui) c->LruColInSelect = 1;
        iselectAction(c, &w, &wb, cmatchs, qcols, cstar);
    } else {                                /* SQL_SINGLE_LKP */
        bt    *btr   = getBtr(w.wf.tmatch);
        aobj  *apk   = &w.wf.akey;
        void *rrow = btFind(btr, apk);
        if (!rrow) { addReply(c, shared.nullbulk);          goto sel_cmd_end; }
        if (cstar) { addReply(c, shared.cone);              goto sel_cmd_end; }
        robj *r = outputRow(btr, rrow, qcols, cmatchs, apk, tmatch);
        addReply(c, shared.singlerow);
        GET_LRUC addReplyRow(c, r, tmatch, apk, lruc);
        decrRefCount(r);
        if (wb.ovar) incrOffsetVar(c, &wb, 1);
    }

sel_cmd_end:
    destroy_wob(&wb);
    destroy_check_sql_where_clause(&w);
}

/* DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE */
void deleteCommand(redisClient *c) {
    if (strcasecmp(c->argv[1]->ptr, "FROM")) {
        addReply(c, shared.deletesyntax); return;
    }
    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)
    if (strcasecmp(c->argv[3]->ptr, "WHERE")) {
        addReply(c, shared.deletesyntax_nowhere); return;
    }
    cswc_t w; wob_t wb;
    init_check_sql_where_clause(&w, tmatch, c->argv[4]->ptr);
    init_wob(&wb);
    parseWCReply(c, &w, &wb, SQL_DELETE);
    if (w.wtype == SQL_ERR_LKP)                             goto delete_cmd_end;
    if (!leftoverParsingReply(c, w.lvr))                    goto delete_cmd_end;
    //dumpW(printf, &w); dumpWB(printf, &wb);
    if (w.wtype != SQL_SINGLE_LKP) { /* FK, RQ, IN */
        if (w.wf.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found); goto delete_cmd_end;
        }
        ideleteAction(c, &w, &wb);
    } else {                         /* SQL_SINGLE_DELETE */
        MATCH_INDICES(w.wf.tmatch)
        aobj  *apk   = &w.wf.akey;
        bool   del   = deleteRow(w.wf.tmatch, apk, matches, inds);
        addReply(c, del ? shared.cone :shared.czero);
        if (wb.ovar) incrOffsetVar(c, &wb, 1);
    }

delete_cmd_end:
    destroy_wob(&wb);
    destroy_check_sql_where_clause(&w);
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
static bool ovwrPKUp(cli    *c,        int    pkupc, char *mvals[],
                     uint32  mvlens[], uchar  pktyp, bt   *btr) {
    aobj *ax   = createAobjFromString(mvals[pkupc], mvlens[pkupc], pktyp);
    void *xrow = btFind(btr, ax);
    destroyAobj(ax);
    if (xrow) { addReply(c, shared.update_pk_overwrite); return 1; }
    return 0;
}
static bool assignMisses(cli   *c,      int    tmatch,    int   ncols,
                         int   qcols,   int    cmatchs[], uchar cmiss[],
                         char *vals[],  uint32 vlens[],   ue_t  ue[],
                         char *mvals[], uint32 mvlens[]) {
    r_tbl_t *rt = &Tbl[tmatch];
    for (int i = 0; i < ncols; i++) {
        unsigned char miss = 1;
        ue[i].yes = 0;
        for (int j = 0; j < qcols; j++) {
            if (i == cmatchs[j]) {
                miss   = 0; vals[i] = mvals[j]; vlens[i] = mvlens[j];
                char e = isExpression(vals[i], vlens[i]);
                if (e) {
                    if (!parseExpr(c, e, tmatch, cmatchs[j], rt->col_type[i],
                                   vals[i], vlens[i], &ue[i])) return 0;
                    ue[i].yes = 1;
                }
                break;
            }
        }
        cmiss[i] = miss;
    }
    return 1;
}
static int getPkUpdateCol(int qcols, int cmatchs[]) {
    int pkupc = -1; /* PK UPDATEs that OVERWRITE rows disallowed */
    for (int i = 0; i < qcols; i++) {
        if (!cmatchs[i]) { pkupc = i; break; }
    }
    return pkupc;
}
static int updateAction(cli *c, char *u_vallist, aobj *u_apk, int u_tmatch) {
    if (!u_vallist) {
        TABLE_CHECK_OR_REPLY(c->argv[1]->ptr, -1)
        if (strcasecmp(c->argv[2]->ptr, "SET")) {
            addReply(c, shared.updatesyntax);         return -1;
        }
        if (strcasecmp(c->argv[4]->ptr, "WHERE")) {
            addReply(c, shared.updatesyntax_nowhere); return -1;
        }
        u_tmatch = tmatch;
    }
    int     tmatch = u_tmatch;
    int     cmatchs[MAX_COLUMN_PER_TABLE];
    char   *mvals  [MAX_COLUMN_PER_TABLE];
    uint32  mvlens [MAX_COLUMN_PER_TABLE];
    char   *vallist = u_vallist ? u_vallist : c->argv[3]->ptr;
    int     qcols   = parseUpdateColListReply(c, tmatch, vallist, cmatchs,
                                               mvals, mvlens);
    if (!qcols)                                       return -1;
    if (initLRUCS(tmatch, cmatchs, qcols)) {
        addReply(c, shared.update_lru);               return -1;
    }
    int pkupc = getPkUpdateCol(qcols, cmatchs);
    MATCH_INDICES(tmatch)

    /* Figure out which columns get updated(HIT) and which dont(MISS) */
    r_tbl_t *rt    = &Tbl[tmatch];
    int      ncols = rt->col_count;
    uchar    cmiss[ncols]; ue_t    ue   [ncols];
    char    *vals [ncols]; uint32  vlens[ncols];
    if (!assignMisses(c, tmatch, ncols, qcols, cmatchs, cmiss, vals, vlens, ue,
                      mvals, mvlens))                 return -1;
    int nsize = -1; /* B4 GOTO */
    cswc_t w; wob_t wb; init_wob(&wb);
    if (u_vallist) { /* comes from "INSERT ON DUPLICATE KEY UPDATE" */
        init_check_sql_where_clause(&w, tmatch, NULL);           /* ERR->GOTO */
        w.wtype     = SQL_SINGLE_LKP; /* JerryRig WhereClause to "pk = X" */
        w.wf.imatch = rt->vimatch;    /* pk index */
        w.wf.tmatch = u_tmatch;       /* table from INSERT UPDATE */
        w.wf.akey   = *u_apk;         /* PK from INSERT UPDATE */
    } else {        /* normal UPDATE -> parse WhereClause */
        init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr);/* ERR->GOTO */
        parseWCReply(c, &w, &wb, SQL_UPDATE);
        if (w.wtype == SQL_ERR_LKP)                            goto upc_end;
        if (!leftoverParsingReply(c, w.lvr))                   goto upc_end;
    } //dumpW(printf, &w); dumpWB(printf, &wb);

    if (w.wtype != SQL_SINGLE_LKP) { /* FK, RQ, IN -> RANGE UPDATE */
        if (pkupc != -1) {
            addReply(c, shared.update_pk_range_query);         goto upc_end;
        }
        if (w.wf.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);    goto upc_end;
        }
        iupdateAction(c, &w, &wb, ncols, matches, inds, vals, vlens, cmiss, ue);
    } else {                         /* SQL_SINGLE_UPDATE */
        uchar  pktyp = rt->col_type[0];
        bt    *btr   = getBtr(w.wf.tmatch);
        if (pkupc != -1) { /* disallow pk updts that overwrite other rows */
            if (ovwrPKUp(c, pkupc, mvals, mvlens, pktyp, btr)) goto upc_end;
        }
        aobj  *apk   = &w.wf.akey;
        void  *row   = btFind(btr, apk);
        if (!row) { addReply(c, shared.czero);                 goto upc_end; }
        nsize        = updateRow(c, btr, apk, row, w.wf.tmatch, ncols, matches,
                                 inds, vals, vlens, cmiss, ue);
        if (nsize == -1)                                       goto upc_end;
        if (!u_vallist) addReply(c, shared.cone);
        if (wb.ovar)    incrOffsetVar(c, &wb, 1);
    }

upc_end:
    destroy_wob(&wb);
    destroy_check_sql_where_clause(&w);
    return nsize;
}
void updateCommand(redisClient *c) {
    updateAction(c, NULL, NULL, -1);
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
    bzero(rt, sizeof(r_tbl_t));
    rt->vimatch = rt->lruc = rt->lrui    = -1;
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

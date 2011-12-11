/*
  *
  * This file implements basic SQL commands of AlchemyDatabase (single row ops)
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

#include "debug.h"
#include "embed.h"
#include "lru.h"
#include "lfu.h"
#include "rpipe.h"
#include "desc.h"
#include "bt.h"
#include "filter.h"
#include "index.h"
#include "range.h"
#include "cr8tblas.h"
#include "wc.h"
#include "parser.h"
#include "colparse.h"
#include "find.h"
#include "query.h"
#include "aobj.h"
#include "common.h"
#include "alsosql.h"

extern int      Num_tbls;
extern r_tbl_t *Tbl;
extern int      Num_indx;
extern r_ind_t *Index;

// GLOBALS
uchar  OutputMode = OUTPUT_NORMAL;

// CONSTANT GLOBALS
char *EMPTY_STRING = "";
char  OUTPUT_DELIM = ',';

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
   while((ln = listNext(li))) { robj *r = ln->value; printf("REPLY: %s\n", r->ptr); } listReleaseIterator(li); }

#define INS_ERR 0
#define INS_INS 1
#define INS_UP  2
static uchar insertCommit(cli  *c,      robj **argv,   sds     vals,
                          int   ncols,  int    tmatch, int     matches,
                          int   inds[], int    pcols,  list   *cmatchl,
                          bool  repl,   bool   upd,    uint32 *tsize,
                          bool  parse,  sds   *key) {
    CMATCHS_FROM_CMATCHL
    twoint cofsts[ncols];
    for (int i = 0; i < ncols; i++) cofsts[i].i = cofsts[i].j = -1;
    aobj     apk; initAobj(&apk);
    bool     ret    = INS_ERR;    /* presume failure */
    void    *nrow   = NULL;       /* B4 GOTO */
    sds      pk     = NULL;
    int      pklen  = 0;                              // NEEDED? use sdslen(pk)
    r_tbl_t *rt     = &Tbl[tmatch];
    int      lncols = ncols;
    if (rt->lrud) lncols--; // INSERT can NOT have LRU
    if (rt->lfu)  lncols--; // INSERT can NOT have LFU
    char    *mvals  = parseRowVals(vals, &pk, &pklen, ncols, cofsts, tmatch,
                                   pcols, cmatchs, lncols);  // ERR now GOTO
    if (!mvals) { addReply(c, shared.insertcolumn);            goto insc_end; }
    if (parse) { // used in cluster-mode to get sk's value
        int skl = cofsts[rt->sk].j - cofsts[rt->sk].i;
        sds sk  = rt->sk ? sdsnewlen(mvals + cofsts[rt->sk].i, skl) : pk;
        *key    = sdscatprintf(sdsempty(), "%s=%s.%s", 
                               sk, rt->name, rt->col[rt->sk].name);
        return ret;
    }
    int      pktyp  = rt->col[0].type;
    apk.type        = apk.enc = pktyp;
    if        (C_IS_I(pktyp)) {
        long l      = atol(pk);                              /* OK: DELIM: \0 */
        if (l >= TWO_POW_32) { addReply(c, shared.uint_pkbig); goto insc_end; }
        apk.i       = (int)l;
    } else if (C_IS_L(pktyp)) apk.l = strtoul(pk, NULL, 10); /* OK: DELIM: \0 */
      else if (C_IS_X(pktyp)) {
          bool r = parseU128(pk, &apk.x); 
          if (!r) { addReply(c, shared.u128_parse); goto insc_end; }
    } else if (C_IS_F(pktyp)) apk.f = atof(pk);              /* OK: DELIM: \0 */
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
        nrow = createRow(c, btr, tmatch, ncols, mvals, cofsts);
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

static bool checkRepeatHashCnames(cli *c, int tmatch) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->tcols < 2) return 1;
    for (uint32 i = 0; i < rt->tcols; i++) {
        for (uint32 j = 0; j < rt->tcols; j++) { if (i == j) continue;
            if (!strcmp(rt->tcnames[i], rt->tcnames[j])) {
                addReply(c, shared.repeat_hash_cnames); return 0;
            }}}
    return 1;
}
static bool checkRepeatInsertCnames(cli *c, int *cmatchs, int matches) {
    if (matches < 2) return 1;
    for (int i = 0; i < matches; i++) {
        for (int j = 0; j < matches; j++) { if (i == j) continue;
            if (cmatchs[i] > 0 && cmatchs[i] == cmatchs[j]) {
                addReply(c, shared.nonuniquecolumns); return 0;
            }}}
    return 1;

}
static void resetTCNames(int tmatch) {
    r_tbl_t *rt  = &Tbl[tmatch]; if (!rt->tcols) return;
    for (uint32 i = 0; i < rt->tcols; i++) sdsfree(rt->tcnames[i]); //FREED 107
    free(rt->tcnames);                                              //FREED 106
    rt->tcnames = NULL; rt->tcols = 0; rt->ctcol = 0;
}
void insertParse(cli *c, robj **argv, bool repl, int tmatch,
                 bool parse, sds *key) {
    resetTCNames(tmatch); MATCH_INDICES(tmatch)
    r_tbl_t *rt      = &Tbl[tmatch];
    int      ncols   = rt->col_count; /* NOTE: need space for LRU */
    list    *cmatchl = listCreate();
    int      pcols   = 0;
    int      valc    = 3;
    if (strcasecmp(argv[valc]->ptr, "VALUES")) {//TODO break block into func
        bool ok   = 0;
        sds  cols = argv[valc]->ptr;
        if (cols[0] == '(' && cols[sdslen(cols) - 1] == ')' ) { /* COL DECL */
            STACK_STRDUP(clist, (cols + 1), (sdslen(cols) - 2));
            if (!parseCommaSpaceList(c, clist, 1, 0, 0, 0, 1, tmatch, cmatchl,
                                     NULL, NULL, NULL, &pcols, NULL)) return;
            if (rt->tcols && !checkRepeatHashCnames(c, tmatch)) goto insprserr;
            CMATCHS_FROM_CMATCHL //TODO unneeded work, need: initLRUCS(cmatchl)
            if (!checkRepeatInsertCnames(c, cmatchs, matches))  goto insprserr;
            if (pcols) {
                if (initLRUCS(tmatch, cmatchs, pcols)) { /* LRU in ColDecl */
                    addReply(c, shared.insert_lru);             goto insprserr;
                }
                if (initLFUCS(tmatch, cmatchs, pcols)) { /* LFU in ColDecl */
                    addReply(c, shared.insert_lfu);             goto insprserr;
                }
                if (OTHER_BT(getBtr(tmatch)) && pcols != 2 && !cmatchs[0]) {
                    addReply(c, shared.part_insert_other);      goto insprserr;
                }
                valc++; if (!strcasecmp(argv[valc]->ptr, "VALUES")) ok = 1;
            }
        }
        if (!ok) { addReply(c, shared.insertsyntax_no_values);  goto insprserr;}

    }
    bool print = 0; uint32 upd = 0; int largc = c->argc;
    if (largc > 5) {
        if (AEQ((largc - 1), "RETURN SIZE")) {
            largc--; print = 1; // DO NOT REPLICATE "RETURN SIZE"
            sdsfree(c->argv[largc]->ptr); c->argv[largc]->ptr = sdsempty();
        }
        if (largc > 6) {
            if (AEQ((largc - 2), "ON DUPLICATE KEY UPDATE")) {
                upd = (uint32)largc - 1; largc -= 2;
        }}
    }
    if (upd && repl) {
        addReply(c, shared.insert_replace_update);             goto insprserr;
    }
    uchar ret  = INS_ERR; uint32 tsize = 0;
    ncols     += rt->tcols; // ADD in HASHABILITY columns
    for (int i = valc + 1; i < largc; i++) {
        ret = insertCommit(c, argv, argv[i]->ptr, ncols, tmatch, matches, inds,
                           pcols, cmatchl, repl, upd, print ? &tsize : NULL,
                           parse, key);
        if (ret == INS_ERR)                                    goto insprserr;
    }
    if (print) addRowSizeReply(c, tmatch, getBtr(tmatch), tsize);
    else       addReply(c, shared.ok);
    return;

insprserr:
    listRelease(cmatchl);
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
    wb->lim = wb->ofst = -1;
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
    if (*inl) { (*inl)->free = destroyAobj; listRelease(*inl); *inl = NULL; }
}
void releaseFlist(list **flist) {
    if (*flist) { (*flist)->free = NULL; listRelease(*flist); *flist = NULL; }
}
void destroyFlist(list **flist) {
    if (*flist) {
        (*flist)->free = destroyFilter; listRelease(*flist); *flist = NULL;
    }
}
void destroy_check_sql_where_clause(cswc_t *w) {
    releaseFilterD_KL(&w->wf);                           /* DESTROYED 065 */
    destroyFlist( &w->flist);
    if (w->lvr) sdsfree(w->lvr);
}

bool leftoverParsingReply(redisClient *c, char *x) {
    if (!x) return 1;
    while (ISBLANK(*x)) x++;
    if (*x) {
        addReplySds(c, P_SDS_EMT "-ERR could not parse '%s'\r\n", x)); return 0;
    }
    return 1;
}

// SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
void sqlSelectCommand(redisClient *c) {
    if (c->argc == 2) { /* this is a REDIS "select DB" command" */
        selectCommand(c); return;
    }
    if (c->argc != 6) { addReply(c, shared.selectsyntax); return; }
    list *cmatchl = listCreate();
    bool  cstar   =  0;
    int   qcols   =  0;
    int   tmatch  = -1;
    bool  join    =  0;
    sds   tlist   = c->argv[3]->ptr;
    if (!parseSelect(c, 0, NULL, &tmatch, cmatchl, &qcols, &join,
                     &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                     tlist, c->argv[4]->ptr)) { listRelease(cmatchl); return; }
    if (join) { joinReply(c);                   listRelease(cmatchl); return; }
    CMATCHS_FROM_CMATCHL listRelease(cmatchl); 

    c->LruColInSelect = initLRUCS(tmatch, cmatchs, qcols);
    c->LfuColInSelect = initLFUCS(tmatch, cmatchs, qcols);
    cswc_t w; wob_t wb;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr);
    init_wob(&wb);
    parseWCReply(c, &w, &wb, SQL_SELECT);
    if (w.wtype == SQL_ERR_LKP)                                     goto sel_e;
    if (!leftoverParsingReply(c, w.lvr))                            goto sel_e;
    if (cstar && wb.nob) { /* SELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);                          goto sel_e;
    }
    if (c->Explain) { explainRQ(c, &w, &wb);                        goto sel_e;}
    //dumpW(printf, &w); dumpWB(printf, &wb);

    if (EREDIS) embeddedSaveSelectedColumnNames(tmatch, cmatchs, qcols);
    if (w.wtype != SQL_SINGLE_LKP) { /* FK, RQ, IN */
        if (w.wf.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);         goto sel_e;
        }
        if (w.wf.imatch == Tbl[tmatch].lrui) c->LruColInSelect = 1;
        if (w.wf.imatch == Tbl[tmatch].lfui) c->LfuColInSelect = 1;
        iselectAction(c, &w, &wb, cmatchs, qcols, cstar);
    } else {                         /* SQL_SINGLE_LKP */
        bt    *btr   = getBtr(w.wf.tmatch);
        aobj  *apk   = &w.wf.akey;
        void *rrow = btFind(btr, apk);
        if (!rrow) { addReply(c, shared.nullbulk);                  goto sel_e;}
        if (cstar) { addReply(c, shared.cone);                      goto sel_e;}
        robj *r = outputRow(btr, rrow, qcols, cmatchs, apk, tmatch);
        addReply(c, shared.singlerow);
        GET_LRUC GET_LFUC
        if (!addReplyRow(c, r, tmatch, apk, lruc, lrud, lfuc, lfu)) goto sel_e;
        decrRefCount(r);
        if (wb.ovar) incrOffsetVar(c, &wb, 1);
    }

sel_e:
    if (!cstar) resetIndexPosOn(qcols, cmatchs);
    destroy_wob(&wb); destroy_check_sql_where_clause(&w);
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
                         char *mvals[], uint32 mvlens[],  lue_t le[]) {
    for (int i = 0; i < ncols; i++) {
        uchar miss  = 1;
        uchar ctype = Tbl[tmatch].col[i].type;
        ue[i].yes   = 0; le[i].yes = 0;
        for (int j = 0; j < qcols; j++) {
            int cmatch = cmatchs[j];
            if (i == cmatch) {
                bool simp = 0;
                miss      = 0; vals[i] = mvals[j]; vlens[i] = mvlens[j];
                if        (C_IS_I(ctype) || C_IS_L(ctype)) {
                    if (getExprType(vals[i], vlens[i]) == UETYPE_INT)  simp = 1;
                } else if C_IS_X(ctype) {
                    if (getExprType(vals[i], vlens[i]) == UETYPE_U128) simp = 1;
                } else if C_IS_F(ctype) {
                    if (getExprType(vals[i], vlens[i]) == UETYPE_FLT)  simp = 1;
                } else {// S_IS_S() 
                    if (is_text(vals[i], vlens[i]))                    simp = 1;
                }
                if (simp) break;
                if C_IS_X(ctype) { // update expr's (lua 2) not allowed on U128
                    addReply(c, shared.update_u128_complex); return 0;
                }
                int k = parseExpr(c, tmatch, cmatch, vals[i], vlens[i], &ue[i]);
                if (k == -1) return 0;
                if (k) { ue[i].yes = 1; break; }
                if (!parseLuaExpr(tmatch, cmatch, vals[i], vlens[i], &le[i])) {
                    addReply(c, shared.updatesyntax); return 0;
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
    int     tmatch  = u_tmatch;
    list   *cmatchl = listCreate();
    list   *mvalsl  = listCreate();
    list   *mvlensl = listCreate();
    char   *vallist = u_vallist ? u_vallist : c->argv[3]->ptr;
    int     qcols   = parseUpdateColListReply(c,      tmatch, vallist, cmatchl,
                                              mvalsl, mvlensl);
    UPDATES_FROM_UPDATEL
    if (!qcols)                                       return -1;
    if (initLRUCS(tmatch, cmatchs, qcols)) {
        addReply(c, shared.update_lru);               return -1;
    }
    int pkupc = getPkUpdateCol(qcols, cmatchs);
    MATCH_INDICES(tmatch)

    /* Figure out which columns get updated(HIT) and which dont(MISS) */
    r_tbl_t *rt    = &Tbl[tmatch];
    int      ncols = rt->col_count;
    uchar    cmiss[ncols]; ue_t    ue   [ncols]; lue_t le[ncols];
    char    *vals [ncols]; uint32  vlens[ncols];
    if (!assignMisses(c, tmatch, ncols, qcols, cmatchs, cmiss, vals, vlens, ue,
                      mvals, mvlens, le))             return -1;
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
        iupdateAction(c, &w, &wb, ncols, matches, inds, vals, vlens, cmiss,
                     ue, le);
    } else {                         /* SQL_SINGLE_UPDATE */
        uchar  pktyp = rt->col[0].type;
        bt    *btr   = getBtr(w.wf.tmatch);
        if (pkupc != -1) { /* disallow pk updts that overwrite other rows */
            if (ovwrPKUp(c, pkupc, mvals, mvlens, pktyp, btr)) goto upc_end;
        }
        aobj  *apk   = &w.wf.akey;
        void  *rrow  = btFind(btr, apk);
        if (!rrow) { addReply(c, shared.czero);                goto upc_end; }
        uc_t uc;
        init_uc(&uc, btr, w.wf.tmatch, ncols, matches, inds, vals, vlens,
                cmiss, ue, le);
        nsize        = updateRow(c, &uc, apk, rrow); release_uc(&uc);
        //NOTE: rrow is no longer valid, updateRow() can change it
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

/*
  *
  * This file implements the basic SQL commands of Alsosql (single row ops)
  *  and calls the range-query and join ops
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
#include <ctype.h>
#include <assert.h>

#include "redis.h"
#include "zmalloc.h"

#include "bt.h"
#include "row.h"
#include "index.h"
#include "range.h"
#include "desc.h"
#include "join.h"
#include "store.h"
#include "cr8tblas.h"
#include "wc.h"
#include "parser.h"
#include "colparse.h"
#include "aobj.h"
#include "common.h"
#include "alsosql.h"


// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;
extern ulong              CurrCard;

// GLOBALS
extern int     Num_tbls[MAX_NUM_DB];
extern r_tbl_t Tbl     [MAX_NUM_DB][MAX_NUM_TABLES];

char *EMPTY_STRING = "";
char *OUTPUT_DELIM = ",";
char *COLON        = ":";

char *Col_type_defs[] = {"TEXT", "INT", "FLOAT" };

extern r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
bool cCpyOrReply(redisClient *c, char *src, char *dest, uint32 len) {
    if (len >= MAX_COLUMN_NAME_SIZE) {
        addReply(c, shared.columnnametoobig);
        return 0;
    }
    memcpy(dest, src, len);
    dest[len] = '\0';
    return 1;
}

void createTableCommitReply(redisClient *c,
                            char         cnames[][MAX_COLUMN_NAME_SIZE],
                            int          ccount,
                            char        *tname,
                            int          tlen) {
    if (Num_tbls[server.dbid] == MAX_NUM_TABLES) {
        addReply(c, shared.toomanytables);
        return;
    }
    if (ccount < 2) {
        addReply(c, shared.toofewcolumns);
        return;
    }

    // check for repeat column names
    for (int i = 0; i < ccount; i++) {
        for (int j = 0; j < ccount; j++) {
            if (i == j) continue;
            if (!strcmp(cnames[i], cnames[j])) {
                addReply(c, shared.nonuniquecolumns);
                return;
            }
        }
    }

    /* BTREE implies an index on "tbl:pk:index" -> autogenerate */
    sds  iname = sdscatprintf(sdsempty(), "%s:%s:%s",
                                          tname, cnames[0], INDEX_DELIM);
    if (!newIndexReply(c, iname, Num_tbls[server.dbid], 0, 1, NULL)) {
        sdsfree(iname);
        return;
    }
    sdsfree(iname);

    int ntbls = Num_tbls[server.dbid];

    addReply(c, shared.ok);
    // commit table definition
    for (int i = 0; i < ccount; i++) {
        Tbl[server.dbid][ntbls].col_name[i] = _createStringObject(cnames[i]);
    }
    int   pktype = Tbl[server.dbid][ntbls].col_type[0];
    robj *bt     = createBtreeObject(pktype, ntbls, BTREE_TABLE);
    robj *tbl    = createStringObject(tname, tlen);
    Tbl[server.dbid][ntbls].name      = tbl;
    Tbl[server.dbid][ntbls].col_count = ccount;
    dictAdd(c->db->dict, tbl, bt);

    Num_tbls[server.dbid]++;
}

static void createTable(redisClient *c) {
    int   tlen  = sdslen(c->argv[2]->ptr);
    char *tname = c->argv[2]->ptr;
    /* Mysql denotes strings w/ backticks */
    tname       = rem_backticks(tname, &tlen);
    if (find_table_n(tname, tlen) != -1) {
        addReply(c, shared.nonuniquetablenames);
        return;
    }

    if (!strncasecmp(c->argv[3]->ptr, "AS ", 3)) {
        createTableAsObject(c);
        return;
    }

    char cnames[MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    int  ccount = 0;
    if (parseCreateTable(c, cnames, &ccount, c->argv[3]->ptr))
        createTableCommitReply(c, cnames, ccount, tname, tlen);
}

void createCommand(redisClient *c) {
    bool create_table = 0;
    bool create_index = 0;
    if (!strcasecmp(c->argv[1]->ptr, "TABLE")) {
        create_table = 1;
    }
    if (!strcasecmp(c->argv[1]->ptr, "INDEX")) {
        create_index = 1;
    }

    if (!create_table && !create_index) {
        addReply(c, shared.createsyntax);
        return;
    }

    if (create_table) createTable(c);
    if (create_index) createIndex(c);
    server.dirty++; /* for appendonlyfile */
}

/* INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT */
/* INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT */
static void addSizeToInsertResponse(redisClient *c,
                                    int          tmatch,
                                    bt          *btr,
                                    int          len) {
    char buf[128];
    ull  index_size = get_sum_all_index_size_for_table(c, tmatch);
    snprintf(buf, 127,
          "INFO: BYTES: [ROW: %d BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
               len, btr->data_size, btr->malloc_size, index_size);
    buf[127] = '\0';
    robj *r  = _createStringObject(buf);
    addReplyBulk(c, r);
    decrRefCount(r);
}

void insertCommitReply(redisClient *c,
                       char        *vals,
                       int          ncols,
                       int          tmatch,
                       int          matches,
                       int          indices[],
                       bool         ret_size) {
    uint32  cofsts[MAX_COLUMN_PER_TABLE];
    aobj  apk;
    initAobj(&apk);
    void   *nrow   = NULL; /* B4 GOTO */
    int     pklen  = 0;
    sds     pk     = NULL;
    vals           = parseRowVals(vals, &pk, &pklen, ncols, cofsts);
    if (!vals) {
        addReply(c, shared.insertcolumnmismatch);
        goto insert_commit_end;
    }

    int pktype = Tbl[server.dbid][tmatch].col_type[0];
    apk.type   = pktype;
    apk.enc    = pktype;
    if (       apk.type == COL_TYPE_INT) {
        long l = atol(pk);
        if (!checkUIntReply(c, l, 1)) goto insert_commit_end;
        apk.i = (int)l;
    } else if (apk.type == COL_TYPE_FLOAT) {
        float f = atof(pk);
        apk.f = f;
    } else {            /* COL_TYPE_STRING */
        apk.s        = pk;
        apk.freeme   = 0; /* "pk will free() itself */
        apk.len      = pklen;
    }

    robj *btt = lookupKeyWrite(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr = (bt *)btt->ptr;
    void *row = btFindVal(btr, &apk, pktype);
    if (row) {
        addReply(c, shared.insertcannotoverwrite);
        goto insert_commit_end;
    }

    nrow = createRow(c, tmatch, ncols, vals, cofsts);
    if (!nrow) goto insert_commit_end; /* value did not match cdef */

    EMPTY_LEN_OBJ
    bool nrl = 0;
    if (matches) { /* Add to Indices */
        for (int i = 0; i < matches; i++) {
            if (Index[server.dbid][indices[i]].nrl) {
                if (!nrl) { INIT_LEN_OBJ }
                nrl = 1;
            }
            addToIndex(c->db, &apk, vals, cofsts, indices[i]);
        }
    }
    int len = btAdd(btr, &apk, nrow, pktype);
    if (ret_size) { /* print SIZE stats */
        addSizeToInsertResponse(c, tmatch, btr, len);
    } else {
        if (nrl) { /* responses come from NonRelationalIndex Cmd responses */
            card        = CurrCard;
            lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
        } else {
            addReply(c, shared.ok);
        }
    }

insert_commit_end:
    if (nrow) free(nrow);
    if (pk)   free(pk);
    releaseAobj(&apk);
}

/* SYNTAX: INSERT INTO tbl VALUES (aaa,bbb,ccc) [RETURN SIZE] */
void insertCommand(redisClient *c) {
   if (strcasecmp(c->argv[1]->ptr, "INTO")) {
        addReply(c, shared.insertsyntax_no_into);
        return;
    }

    int   len   = sdslen(c->argv[2]->ptr);
    char *tname = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliance */
    TABLE_CHECK_OR_REPLY(tname,)
    int   ncols = Tbl[server.dbid][tmatch].col_count;
    MATCH_INDICES(tmatch)

    if (strcasecmp(c->argv[3]->ptr, "VALUES")) {
        addReply(c, shared.insertsyntax_no_values);
        return;
    }

    bool ret_size = 0;
    if (c->argc == 6) {
        leftoverParsingReply(c, c->argv[5]->ptr);
        return;
    } else if (c->argc > 6) {
        if (!strcasecmp(c->argv[5]->ptr, "RETURN") &&
            !strcasecmp(c->argv[6]->ptr, "SIZE")) {
           ret_size = 1;
        } else {
            sds s = sdsnewlen(c->argv[5]->ptr, sdslen(c->argv[5]->ptr));
            s = sdscatprintf(s, " %s", (char *)c->argv[6]->ptr);
            leftoverParsingReply(c, s);
            sdsfree(s);
            return;
        }
    }
    
    sds vals = c->argv[4]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices, ret_size);
}

/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
/* SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT */
static void selectOnePKReply(redisClient  *c,
                             bt           *btr,
                             aobj         *apk,
                             int           tmatch,
                             int           cmatchs[],
                             int           qcols,
                             bool          cstar,
                             uchar         pktype) {
    if (cstar) {
        addReply(c, shared.cone);
        return;
    }
    void *rrow = btFindVal(btr, apk, pktype);
    if (!rrow) {
        addReply(c, shared.nullbulk);
        return;
    }

    addReply(c, shared.singlerow);
    robj *r = outputRow(rrow, qcols, cmatchs, apk, tmatch, 0);
    addReplyBulk(c, r);
    decrRefCount(r);
}

void init_check_sql_where_clause(cswc_t *w, int tmatch, sds token) {
    w->key    = NULL; //TODO -> aobj
    w->low    = NULL; //TODO -> aobj
    w->high   = NULL; //TODO -> aobj
    w->inl    = NULL;
    w->stor   = NULL;
    w->lvr    = NULL;
    w->ovar   = NULL;
    w->imatch = -1;
    w->tmatch = tmatch;
    w->cmatch = -1;
    w->obc    = -1;
    w->obt    = -1;
    w->lim    = -1;
    w->ofst   = -1;
    w->sto    = -1;
    w->asc    = 1;
    w->token  = token;
}

void destroy_check_sql_where_clause(cswc_t *w) {
    if (w->key)  decrRefCount(w->key);
    if (w->low)  decrRefCount(w->low);
    if (w->high) decrRefCount(w->high);
    if (w->inl)  listRelease(w->inl);
    if (w->ovar) sdsfree(w->ovar);
}

/* TODO need a single FK iterator ... built into RANGE_QUERY_LOOKUP_START */
void singleFKHack(cswc_t *w, uchar *wtype) {
    *wtype  = SQL_RANGE_QUERY;
    w->low  = cloneRobj(w->key);
    w->high = cloneRobj(w->key);
}

bool leftoverParsingReply(redisClient *c, char *x) {
    if (x) {
        while (isblank(*x)) x++;
        if (*x) {
            addReplySds(c, sdscatprintf(sdsempty(),
                        "-ERR could not parse '%s'\r\n", x));
            return 0;
        }
    }
    return 1;
}

/* SELECT cols,,,,, FROM tbls,,,, WHERE clause - (6 args) */
void sqlSelectCommand(redisClient *c) {
    if (c->argc == 2) { /* this is a REDIS "select DB" command" */
        selectCommand(c);
        return;
    }
    if (c->argc != 6) {
        addReply(c, shared.selectsyntax);
        return;
    }

    int  cmatchs[MAX_COLUMN_PER_TABLE];
    bool cstar  =  0;
    int  qcols  =  0;
    int  tmatch = -1;
    bool join   =  0;
    sds  tlist  = c->argv[3]->ptr;
    if (!parseSelectReply(c, 0, NULL, &tmatch, cmatchs, &qcols, &join,
                          &cstar, c->argv[1]->ptr, c->argv[2]->ptr,
                          tlist, c->argv[4]->ptr)) return;
    if (join) {
        joinReply(c);
        return;
    }

    cswc_t w;
    uchar  sop   = SQL_SELECT;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr);
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto select_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto select_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (cstar && w.obc != -1) { /* SELECT COUNT(*) ORDER BY -> stupid */
        addReply(c, shared.orderby_count);
        goto select_cmd_end;
    }
    if (w.stor) { /* DENORM e.g.: STORE LPUSH list */
        if (!w.low && !w.inl) {
            addReply(c, shared.selectsyntax_store_norange);
            goto select_cmd_end;
        } else if (cstar) {
            addReply(c, shared.select_store_count);
            goto select_cmd_end;
        }
        if (server.maxmemory && zmalloc_used_memory() > server.maxmemory) {
            addReplySds(c, sdsnew(
                "-ERR command not allowed when used memory > 'maxmemory'\r\n"));
            goto select_cmd_end;
        }
        istoreCommit(c, &w, cmatchs, qcols);
    } else if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) { /* RQ */
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto select_cmd_end;
        }

        iselectAction(c, &w, cmatchs, qcols, cstar);
    } else {
        uchar pktype = Tbl[server.dbid][tmatch].col_type[0];
        robj *btt    = lookupKeyRead(c->db, Tbl[server.dbid][w.tmatch].name);
        bt   *btr    = (bt *)btt->ptr;
        aobj *apk    = copyRobjToAobj(w.key, pktype); //TODO LAME
        selectOnePKReply(c, btr, apk, w.tmatch, cmatchs, qcols, cstar, pktype);
        destroyAobj(apk);                             //TODO LAME
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

select_cmd_end:
    destroy_check_sql_where_clause(&w);
}

/* DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE */
/* DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE */
void deleteCommand(redisClient *c) {
    if (strcasecmp(c->argv[1]->ptr, "FROM")) {
        addReply(c, shared.deletesyntax);
        return;
    }

    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)

    if (strcasecmp(c->argv[3]->ptr, "WHERE")) {
        addReply(c, shared.deletesyntax_nowhere);
        return;
    }

    cswc_t w;
    uchar  sop   = SQL_DELETE;
    init_check_sql_where_clause(&w, tmatch, c->argv[4]->ptr);
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto delete_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto delete_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto delete_cmd_end;
        }
        ideleteAction(c, &w);
    } else {
        MATCH_INDICES(w.tmatch)
        uchar  pktype = Tbl[server.dbid][tmatch].col_type[0];
        aobj  *apk    = copyRobjToAobj(w.key, pktype); //TODO LAME
        bool   del    = deleteRow(c, w.tmatch, apk, matches, indices);
        destroyAobj(apk);                              //TODO LAME
        addReply(c, del ? shared.cone :shared.czero);
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

delete_cmd_end:
    destroy_check_sql_where_clause(&w);
}

/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
/* UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE */
static bool checkOverWritePKUpdate(redisClient *c,
                                   int          pkupc,
                                   char        *mvals[],
                                   uint32       mvlens[],
                                   uchar        pktype,
                                   bt          *btr) {
    aobj *ax   = createAobjFromString(mvals[pkupc], mvlens[pkupc], pktype);
    void *xrow = btFindVal(btr, ax, pktype);
    destroyAobj(ax);
    if (xrow) {
        addReply(c, shared.update_pk_overwrite);
        return 1;
    }
    return 0;
}
static bool assignMisses(redisClient *c,
                         int          tmatch,
                         int          ncols,
                         int          qcols,
                         int          cmatchs[],
                         uchar        cmiss[],
                         char        *vals[],
                         uint32       vlens[],
                         ue_t         ue[],
                         char        *mvals[],
                         uint32       mvlens[]) {
    for (int i = 0; i < ncols; i++) {
        unsigned char miss = 1;
        ue[i].yes = 0;
        for (int j = 0; j < qcols; j++) {
            if (i == cmatchs[j]) {
                miss     = 0;
                vals[i]  = mvals[j];
                vlens[i] = mvlens[j];
                char e   = isExpression(vals[i], vlens[i]);
                if (e) {
                    uchar ctype = Tbl[server.dbid][tmatch].col_type[i];
                    if (!parseExprReply(c, e, tmatch, cmatchs[j], ctype,
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
        if (!cmatchs[i]) {
            pkupc = i;
            break;
        }
    }
    return pkupc;
}

void updateCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    if (strcasecmp(c->argv[2]->ptr, "SET")) {
        addReply(c, shared.updatesyntax);
        return;
    }
    if (strcasecmp(c->argv[4]->ptr, "WHERE")) {
        addReply(c, shared.updatesyntax_nowhere);
        return;
    }

    int     cmatchs[MAX_COLUMN_PER_TABLE];
    char   *mvals  [MAX_COLUMN_PER_TABLE];
    uint32  mvlens [MAX_COLUMN_PER_TABLE];
    char   *vallist = c->argv[3]->ptr;
    int     qcols   = parseUpdateColListReply(c, tmatch, vallist, cmatchs,
                                               mvals, mvlens);
    if (!qcols) return;

    MATCH_INDICES(tmatch)
    int pkupc = getPkUpdateCol(qcols, cmatchs);

    /* Figure out which columns get updated(HIT) and which dont(MISS) */
    uchar   cmiss[MAX_COLUMN_PER_TABLE];
    char   *vals [MAX_COLUMN_PER_TABLE];
    uint32  vlens[MAX_COLUMN_PER_TABLE];
    ue_t    ue   [MAX_COLUMN_PER_TABLE];
    int     ncols = Tbl[server.dbid][tmatch].col_count;
    if (!assignMisses(c, tmatch, ncols, qcols, cmatchs, cmiss, vals, vlens, ue,
                      mvals, mvlens)) return;

    cswc_t w;
    aobj  *akey  = NULL; /* B4 GOTO */
    uchar  sop   = SQL_UPDATE;
    init_check_sql_where_clause(&w, tmatch, c->argv[5]->ptr); /* ERR now GOTO */
    uchar  wtype = checkSQLWhereClauseReply(c, &w, sop, 0);
    if (wtype == SQL_ERR_LOOKUP)         goto update_cmd_end;
    if (!leftoverParsingReply(c, w.lvr)) goto update_cmd_end;

    if (wtype == SQL_SINGLE_FK_LOOKUP) singleFKHack(&w, &wtype);

    if (wtype == SQL_RANGE_QUERY || wtype == SQL_IN_LOOKUP) {
        if (pkupc != -1) {
            addReply(c, shared.update_pk_range_query);
            goto update_cmd_end;
        }
        if (w.imatch == -1) {
            addReply(c, shared.rangequery_index_not_found);
            goto update_cmd_end;
        }
        iupdateAction(c, &w, ncols, matches, indices,
                      vals, vlens, cmiss, ue);
    } else {
        uchar  pktype = Tbl[server.dbid][w.tmatch].col_type[0];
        robj  *btt    = lookupKeyRead(c->db, Tbl[server.dbid][w.tmatch].name);
        bt    *btr    = (bt *)btt->ptr;
        if (pkupc != -1) { /* disallow pk updts that overwrite other rows */
            if (checkOverWritePKUpdate(c, pkupc, mvals, mvlens, pktype, btr))
                goto update_cmd_end;
        }
        akey      = copyRobjToAobj(w.key, pktype); //TODO LAME
        void *row = btFindVal(btr, akey, pktype);
        if (!row) { /* no row to update */
            addReply(c, shared.czero);
            goto update_cmd_end;
        }
        if (!updateRow(c, btr, akey, row, w.tmatch, ncols, matches, indices, 
                       vals, vlens, cmiss, ue)) goto update_cmd_end;
        addReply(c, shared.cone);
        if (w.ovar) incrOffsetVar(c, &w, 1);
    }

update_cmd_end:
    if (akey) destroyAobj(akey);                   //TODO LAME
    destroy_check_sql_where_clause(&w);
}

/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
unsigned long emptyTable(redisDb *db, int tmatch) {
    if (!Tbl[server.dbid][tmatch].name) return 0; /* already deleted */

    MATCH_INDICES(tmatch)
    unsigned long deleted = 0;
    if (matches) { // delete indices first
        for (int i = 0; i < matches; i++) { // build list of robj's to delete
            emptyIndex(db, indices[i]);
            deleted++;
        }
        //TODO shuffle indices to make space for deleted indices
    }

    //TODO there is a bug in redis' dictDelete (need to update redis code)
    //int retval = 
    dictDelete(db->dict,Tbl[server.dbid][tmatch].name);
    //assert(retval == DICT_OK);
    Tbl[server.dbid][tmatch].name      = NULL;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        decrRefCount(Tbl[server.dbid][tmatch].col_name[j]);
        Tbl[server.dbid][tmatch].col_name[j] = NULL;
        Tbl[server.dbid][tmatch].col_type[j] = -1;
    }
    Tbl[server.dbid][tmatch].col_count = 0;
    Tbl[server.dbid][tmatch].virt_indx = -1;

    deleted++;
    //TODO shuffle tables to make space for deleted indices

    return deleted;
}

static void dropTable(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[2]->ptr,)
    unsigned long deleted = emptyTable(c->db, tmatch);
    addReplyLongLong(c, deleted);
    server.dirty++;
}

void dropCommand(redisClient *c) {
    bool drop_table = 0;
    bool drop_index = 0;
    if (!strcasecmp(c->argv[1]->ptr, "table")) {
        drop_table = 1;
    }
    if (!strcasecmp(c->argv[1]->ptr, "index")) {
        drop_index = 1;
    }

    if (!drop_table && !drop_index) {
        addReply(c, shared.dropsyntax);
        return;
    }

    if (drop_table) dropTable(c);
    if (drop_index) dropIndex(c);
}

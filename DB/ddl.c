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

#include "dict.h"
#include "redis.h"

#include "bt.h"
#include "luatrigger.h"
#include "filter.h"
#include "query.h"
#include "index.h"
#include "lru.h"
#include "lfu.h"
#include "cr8tblas.h"
#include "parser.h"
#include "colparse.h"
#include "find.h"
#include "alsosql.h"
#include "common.h"
#include "ddl.h"

extern uint32   Tbl_HW; extern dict *TblD; extern list *DropT;
extern r_ind_t *Index;

extern char *RangeType[5];

// GLOBALS
int      Num_tbls;
r_tbl_t *Tbl = NULL; /* ALCHEMY_DATABASE table info stored here */

// CONSTANT GLOBALS
char *Col_type_defs[]       =
  {"NONE", "INT UNSIGNED", "BIGINT UNSIGNED", "TEXT", "FLOAT", "U128"};

// PROTOTYPES
// from redis.c
unsigned int dictSdsCaseHash(const void *key);
int          dictSdsKeyCaseCompare(void *privdata, const void *key1, 
                                   const void *key2);
void         dictSdsDestructor(void *privdata, void *val);

dictType sdsDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE */
void initTable(r_tbl_t *rt) { // NOTE: also used in rdbLoadBT
    bzero(rt, sizeof(r_tbl_t));
    rt->vimatch = rt->lruc      = rt->lrui       = rt->sk         = \
                  rt->fk_cmatch = rt->fk_otmatch = rt->fk_ocmatch = \
                  rt->lfuc      = rt->lfui       = -1;
}
static void addTable() { //printf("addTable: Tbl_HW: %d\n", Tbl_HW);
    Tbl_HW++;
    r_tbl_t *tbls = malloc(sizeof(r_tbl_t) * Tbl_HW);
    bzero(tbls, sizeof(r_tbl_t) * Tbl_HW);
    for (int i = 0; i < Num_tbls; i++) {
        memcpy(&tbls[i], &Tbl[i], sizeof(r_tbl_t)); // copy table metadata
    }
    free(Tbl); Tbl = tbls;
}

static bool validateCreateTableCnames(cli *c, list *cnames) {
    listNode *ln, *lnj;
    int       i  = 0;
    listIter *li = listGetIterator(cnames, AL_START_HEAD);
    while((ln = listNext(li))) {
        sds cnamei = ln->value;
        if (!strcasecmp(cnamei, "LRU")) {
            addReply(c, shared.col_lru);              return 0;
        }
        int       j   = 0;
        listIter *lij = listGetIterator(cnames, AL_START_HEAD);
        while((lnj = listNext(lij))) {
            sds cnamej = lnj->value;
            if (i == j) continue;
            if (!strcasecmp(cnamei, cnamej)) {
                addReply(c, shared.nonuniquecolumns); return 0;
            } j++;
        } listReleaseIterator(lij); i++; 
    } listReleaseIterator(li);
    return 1;
}
static void newTable(cli *c, list *ctypes, list *cnames, int ccount, sds tname){
    if (ccount < 2) { addReply(c, shared.toofewcolumns); return; }
    if (!validateCreateTableCnames(c, cnames))           return;

    if (!DropT && Num_tbls >= (int)Tbl_HW) addTable();
    addReply(c, shared.ok); /* commited */

    int      tmatch;
    if (DropT) {
        listNode *ln = (DropT)->head;
        tmatch       = (int)(long)ln->value;
        DEL_NODE_ON_EMPTY_RELEASE_LIST(DropT, ln);
    } else {
        tmatch       = Num_tbls; Num_tbls++;
    } //printf("newTable: tmatch: %d\n", tmatch);
    r_tbl_t *rt      = &Tbl[tmatch]; initTable(rt);
    rt->name         = sdsdup(tname);
    rt->col_count    = ccount;
    rt->col          = malloc(sizeof(r_col_t) * rt->col_count); // FREE 081
    bzero(rt->col, sizeof(r_col_t) * rt->col_count);
    rt->cdict        = dictCreate(&sdsDictType, NULL);          // FREE 090
    for (int i = 0; i < rt->col_count; i++) {
        listNode *lnn     = listIndex(cnames, i);
        sds       cname   = (sds)lnn->value;
        rt->col[i].name   = sdsdup(cname);                  // DEST 082
        ASSERT_OK(dictAdd(rt->cdict, sdsdup(cname), VOIDINT (i + 1)));
        listNode *lnt     = listIndex(ctypes, i);
        rt->col[i].type   = (uchar)(long)lnt->value;
        rt->col[i].imatch = -1;
    }
    rt->btr          = createDBT(rt->col[0].type, tmatch);
    ASSERT_OK(dictAdd(TblD, sdsdup(rt->name), VOIDINT(tmatch + 1)));
    /* BTREE implies an index on "tbl_pk_index" -> autogenerate */
    sds  pkname  = rt->col[0].name;
    sds  iname   = P_SDS_EMT "%s_%s_%s", rt->name, pkname, INDEX_DELIM); //D073
    newIndex(c, iname, tmatch, 0, NULL, 0, 1, 0, NULL, -1, 0, 0);// Can not fail
    sdsfree(iname);                                      /* DESTROYED 073 */
}

static inline void v_sdsfree(void *v) { sdsfree((sds)v); }

static void createTable(redisClient *c) { //printf("createTable\n");
    char *tn    = c->argv[2]->ptr;
    int   tlen  = sdslen(c->argv[2]->ptr);
    tn          = rem_backticks(tn, &tlen); /* Mysql compliant */
    sds   tname = sdsnewlen(tn, tlen);                                //DEST 089
    if (find_table(tname) != -1) { sdsfree(tname);                    //DESTD089
        addReply(c, shared.nonuniquetablenames); return;
    }
    if (!strncasecmp(c->argv[3]->ptr, "SELECT ", 7) ||
        !strncasecmp(c->argv[3]->ptr, "SCAN ",   5)) { sdsfree(tname);//DESTD089
        createTableSelect(c); return;
    }
    list *cnames = listCreate(); cnames->free = v_sdsfree;
    list *ctypes = listCreate();
    int  ccount = 0;
    if (parseCreateTable(c, ctypes, cnames, &ccount, c->argv[3]->ptr)) {
        newTable(c, ctypes, cnames, ccount, tname);
    }
    sdsfree(tname); listRelease(cnames); listRelease(ctypes);
}

void createCommand(redisClient *c) { //printf("createCommand\n");
    bool  tbl  = 0, ind = 0, lru = 0, lfu = 0, luat = 0;
    uchar slot = 2;
    if      (!strcasecmp(c->argv[1]->ptr, "TABLE"))      { tbl   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "INDEX"))      { ind   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "UNIQUE"))     { ind   = 1; slot = 3;}
    else if (!strcasecmp(c->argv[1]->ptr, "LRUINDEX"))   { lru   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "LFUINDEX"))   { lfu   = 1; }
    else if (!strcasecmp(c->argv[1]->ptr, "LUATRIGGER")) { luat  = 1; }
    else                           { addReply(c, shared.createsyntax); return; }
    robj *o = lookupKeyRead(c->db, c->argv[slot]);
    if (o) { addReply(c, shared.nonuniquekeyname); return; }
    if      (tbl)     createTable     (c);
    else if (ind)     createIndex     (c);
    else if (lru)     createLruIndex  (c);
    else if (lfu)     createLfuIndex  (c);
    else /* (luat) */ createLuaTrigger(c);
    server.dirty++; /* for appendonlyfile */
}

/* DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP DROP */
unsigned long emptyTable(int tmatch) { //printf("emptyTable: %d\n", tmatch);
    r_tbl_t *rt      = &Tbl[tmatch];
    if (!rt->name) return 0;                 /* already deleted */
    dictDelete(TblD, rt->name); sdsfree(rt->name);
    MATCH_INDICES(tmatch)
    ulong    deleted = 0;
    if (matches) {                          /* delete indices first */
        for (int i = 0; i < matches; i++) { emptyIndex(inds[i]); deleted++;
    }}
    for (int j = 0; j < rt->col_count; j++) sdsfree(rt->col[j].name);//DESTD 082
    free(rt->col);                                                   //FREED 081
    bt_destroy(rt->btr);
    listRelease(rt->ilist);                                          //DESTD 088
    dictRelease(rt->cdict);                                          //DESTD 090
    initTable(rt);
    if (tmatch == (Num_tbls - 1)) Num_tbls--; // if last -> reuse
    else {                                    // else put on DropT for reuse
        if (!DropT) DropT = listCreate();
        listAddNodeTail(DropT, VOIDINT tmatch);
    }
    return deleted++;
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

// ALTER ALTER ALTER ALTER ALTER ALTER ALTER ALTER ALTER ALTER ALTER ALTER
static bool checkRepeatCnames(cli *c, int tmatch, sds cname) {
    if (!strcasecmp(cname, "LRU")) { addReply(c, shared.col_lru); return 0; }
    if (!strcasecmp(cname, "LFU")) { addReply(c, shared.col_lfu); return 0; }
    if (find_column(tmatch, cname) == -1)        return 1;
    else { addReply(c, shared.nonuniquecolumns); return 0; }
}
// addColumn(): Used by ALTER TABLE & LRU & HASHABILITY
void addColumn(int tmatch, char *cname, int ctype) {
    r_tbl_t *rt        = &Tbl[tmatch];
    int      col_count = rt->col_count;
    rt->col_count++;
    r_col_t *tcol      = malloc(sizeof(r_col_t) * rt->col_count); // FREE 081
    bzero(tcol, sizeof(r_col_t) * rt->col_count);
    for (int i = 0; i < col_count; i++) {
        memcpy(&tcol[i], &rt->col[i], sizeof(r_col_t)); // copy column metadata
    }
    free(rt->col);                                                // FREED 081
    rt->col                   = tcol;
    rt->col[col_count].name   = sdsnew(cname);
    rt->col[col_count].type   = ctype;
    rt->col[col_count].imatch = -1;
    ASSERT_OK(dictAdd(rt->cdict, sdsnew(cname), VOIDINT (col_count + 1)));
}

//TODO ALTER TABLE DROP [COLUMN cname] [HASHABILITY]
void alterCommand(cli *c) {
    bool altc = 0, altsk = 0, altfk = 0, althsh = 0;
    if (strcasecmp(c->argv[1]->ptr, "TABLE") ||
        strcasecmp(c->argv[3]->ptr, "ADD")) {
        addReply(c, shared.altersyntax);                                return;
    }
    if      (!strcasecmp(c->argv[4]->ptr, "COLUMN"))      altc  = 1;
    else if (!strcasecmp(c->argv[4]->ptr, "SHARDKEY"))    altsk = 1;
    else if (!strcasecmp(c->argv[4]->ptr, "HASHABILITY")) althsh = 1;
    else if (!strcasecmp(c->argv[4]->ptr, "FOREIGN") &&
             !strcasecmp(c->argv[5]->ptr, "KEY"))         altfk = 1;
    else { addReply(c, shared.altersyntax);                             return;}
    uchar  ctype;
    int    len   = sdslen(c->argv[2]->ptr);
    char  *tname = rem_backticks(c->argv[2]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    if (OTHER_BT(getBtr(tmatch))) { addReply(c, shared.alter_other);    return;}
    if         (altc) {
        if (c->argc < 7) { addReply(c, shared.altersyntax);             return;}
        if (!checkRepeatCnames(c, tmatch, c->argv[5]->ptr))             return;
        if (!parseColType     (c, c->argv[6]->ptr, &ctype))             return;
        addColumn(tmatch, c->argv[5]->ptr, ctype);
    } else if  (althsh) {
        if (c->argc > 5) { addReply(c, shared.altersyntax);             return;}
        Tbl[tmatch].hashy = 1; addReply(c, shared.ok);                  return;
    } else if  (altsk) {
        if (c->argc < 6) { addReply(c, shared.altersyntax);             return;}
        sds cname  = c->argv[5]->ptr;
        if (Tbl[tmatch].sk != -1) { addReply(c, shared.alter_sk_rpt);   return;}
        int cmatch = find_column_n(tmatch, cname, sdslen(cname));
        if (cmatch == -1)      { addReply(c, shared.altersyntax);       return;}
        int imatch = find_index(tmatch, cmatch);
        if (imatch == -1)      { addReply(c, shared.alter_sk_no_i);     return;}
        if (Index[imatch].lru) { addReply(c, shared.alter_sk_no_lru);   return;}
        if (Index[imatch].lfu) { addReply(c, shared.alter_sk_no_lfu);   return;}
        Tbl[tmatch].sk = cmatch;
    } else { /* altfk */
        if (c->argc < 10) { addReply(c, shared.altersyntax);            return;}
        if (strcasecmp(c->argv[7]->ptr, "REFERENCES")) {
            addReply(c, shared.altersyntax);                            return;
        }
        sds   fkname  = c->argv[6]->ptr;
        int   fklen   = sdslen(fkname);
        char *fkend   = fkname + fklen - 1;
        if (*fkname != '(' || *fkend != ')') {
            addReply(c, shared.altersyntax);                            return;
        }
        sds   o_tname = c->argv[8]->ptr;
        sds   o_cname = c->argv[9]->ptr;
        int   oclen   = sdslen(o_cname);
        char *o_cend  = o_cname + oclen - 1;
        if (*o_cname != '(' || *o_cend != ')') {
            addReply(c, shared.altersyntax);                            return;
        }
        int cmatch = find_column_n(tmatch, fkname + 1, fklen - 2);
        if (cmatch == -1) { addReply(c, shared.altersyntax);            return;}
        int otmatch = find_table(o_tname);
        if (otmatch == -1) { addReply(c, shared.altersyntax);           return;}
        int ocmatch = find_column_n(otmatch, o_cname + 1, oclen - 2);
        if (ocmatch == -1) { addReply(c, shared.altersyntax);           return;}
        r_tbl_t *rt  = &Tbl[tmatch];
        r_tbl_t *ort = &Tbl[otmatch];
        if (rt->sk != cmatch || ort->sk != ocmatch) {
            addReply(c, shared.alter_fk_not_sk);                        return;
        }
        //NOTE: BOTH are indexed because shardkey's must be
        if (rt->fk_cmatch != -1) { addReply(c, shared.alter_fk_repeat); return;}
        rt->fk_cmatch  = cmatch;
        rt->fk_otmatch = otmatch;
        rt->fk_ocmatch = ocmatch;
    }
    addReply(c, shared.ok);
}

/*
 * This file implements saving alsosql datastructures to rdb files
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

#include "redis.h"
#include "dict.h"

#include "bt.h"
#include "index.h"
#include "nri.h"
#include "stream.h"
#include "alsosql.h"
#include "common.h"
#include "rdb_alsosql.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct redisServer server;

extern int      Num_tbls     [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

extern int      Num_indx[MAX_NUM_DB];
extern r_ind_t  Index   [MAX_NUM_DB][MAX_NUM_INDICES];

extern char    *COLON;

unsigned char VIRTUAL_INDEX_TYPE = 255;

int rdbSaveNRL(FILE *fp, robj *o) {
    listNode *ln;
    d_l_t    *nrlind  = o->ptr;
    list     *nrltoks = nrlind->l1;

    int imatch = nrlind->num;
    if (rdbSaveLen(fp, imatch) == -1) return -1;
    robj *iname  = Index[server.dbid][imatch].obj;
    if (rdbSaveStringObject(fp, iname) == -1) return -1;
    int   tmatch = Index[server.dbid][imatch].table;
    if (rdbSaveLen(fp, tmatch) == -1) return -1;

    if (rdbSaveLen(fp, listLength(nrltoks)) == -1) return -1;
    listIter    *li = listGetIterator(nrltoks, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sds   s = ln->value;
        robj *r = createStringObject(s, sdslen(s));
        if (rdbSaveStringObject(fp, r) == -1) return -1;
        decrRefCount(r);
    }
    listReleaseIterator(li);

    list  *nrlcols = nrlind->l2;
    if (rdbSaveLen(fp, listLength(nrlcols)) == -1) return -1;
    li = listGetIterator(nrlcols, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        uint32 i = (uint32)(long)ln->value;
        if (rdbSaveLen(fp, i) == -1) return -1;
    }
    listReleaseIterator(li);

    return 0;
}

robj *rdbLoadNRL(FILE *fp) {
    robj         *iname;
    unsigned int  u;
    d_l_t *nrlind  = malloc(sizeof(d_l_t));
    nrlind->l1     = listCreate();
    list  *nrltoks = nrlind->l1;
    nrlind->l2     = listCreate();
    list  *nrlcols = nrlind->l2;

    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    nrlind->num = (int)u;
    int imatch  = nrlind->num;
    if (!(iname = rdbLoadStringObject(fp))) return NULL;
    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    int tmatch  = (int)u;

    unsigned int ssize;
    if ((ssize = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    for (uint32 i = 0; i < ssize; i++) {
        robj *r;
        if (!(r = rdbLoadStringObject(fp))) return NULL;
        listAddNodeTail(nrltoks, sdsdup(r->ptr));
        decrRefCount(r);
    }

    if ((ssize = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    for (uint32 i = 0; i < ssize; i++) {
        uint32 col;
        if ((col = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        listAddNodeTail(nrlcols, (void *)(long)col);
    }
    Index[server.dbid][imatch].obj     = iname;
    Index[server.dbid][imatch].table   = tmatch;
    Index[server.dbid][imatch].column  = -1;
    Index[server.dbid][imatch].type    = COL_TYPE_NONE;
    Index[server.dbid][imatch].virt    = 0;
    Index[server.dbid][imatch].nrl     = 1;
    int dbid = server.dbid;
    if (Num_indx[dbid] < (imatch + 1)) Num_indx[dbid] = imatch + 1;

    return createObject(REDIS_NRL_INDEX, nrlind);
}

static int rdbSaveAllRows(FILE *fp, bt *btr, bt_n *x) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream = KEYS(btr, x)[i];
        int    ssize  = getStreamMallocSize(stream, btr->btype);
        if (rdbSaveLen(fp, ssize)        == -1) return -1;
        if (fwrite(stream, ssize, 1, fp) == 0) return -1;
    }

    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            if (rdbSaveAllRows(fp, btr, NODES(btr, x)[i]) == -1) return -1;
        }
    }
    return 0;
}

int rdbSaveBT(FILE *fp, robj *o) {
    struct btree *btr  = (struct btree *)(o->ptr);
    if (!btr) {
        if (fwrite(&VIRTUAL_INDEX_TYPE, 1, 1, fp) == 0) return -1;
        return 0;
    }

    if (fwrite(&(btr->btype), 1, 1, fp) == 0) return -1;
    int tmatch = btr->num;
    if (rdbSaveLen(fp, tmatch) == -1) return -1;

    int dbid   = server.dbid;
    if (btr->btype == BTREE_TABLE) { /* BTree w/ DATA */
        //RL4 "%d: saving table: %s virt_index: %d",
            //tmatch, Tbl[dbid][tmatch].name->ptr, Tbl[dbid][tmatch].virt_indx);
        if (rdbSaveLen(fp, Tbl[dbid][tmatch].virt_indx) == -1) return -1;
        if (rdbSaveStringObject(fp, Tbl[dbid][tmatch].name) == -1) return -1;
        if (rdbSaveLen(fp, Tbl[dbid][tmatch].col_count) == -1) return -1;
        for (int i = 0; i < Tbl[dbid][tmatch].col_count; i++) {
            if (rdbSaveStringObject(fp, Tbl[dbid][tmatch].col_name[i]) == -1)
                return -1;
            if (rdbSaveLen(fp, (int)Tbl[dbid][tmatch].col_type[i]) == -1)
                return -1;
        }
        if (fwrite(&(btr->ktype),    1, 1, fp) == 0) return -1;
        if (rdbSaveLen(fp, btr->numkeys)       == -1) return -1;
        if (btr->root && btr->numkeys > 0) {
            if (rdbSaveAllRows(fp, btr, btr->root) == -1) return -1;
        }
    } else {                           /* INDEX */
        int imatch = tmatch;
        //RL4 "%d: save index: %s tbl: %d col: %d type: %d",
            //imatch, Index[dbid][imatch].obj->ptr, Index[dbid][imatch].table,
            //Index[dbid][imatch].column, Index[dbid][imatch].type);
        if (rdbSaveStringObject(fp, Index[dbid][imatch].obj) == -1) return -1;
        if (rdbSaveLen(fp, Index[dbid][imatch].table) == -1) return -1;
        if (rdbSaveLen(fp, Index[dbid][imatch].column) == -1) return -1;
        if (rdbSaveLen(fp, (int)Index[dbid][imatch].type) == -1) return -1;
        if (fwrite(&(btr->ktype),    1, 1, fp) == 0) return -1;
    }
    return 0;
}

static int rdbLoadRow(FILE *fp, bt *btr) {
    unsigned int ssize;
    if ((ssize = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return -1;
    char *bt_val = bt_malloc(ssize, btr); // mem bookkeeping done in BT
    if (fread(bt_val, ssize, 1, fp) == 0) return -1;
    if (btr->numkeys == TRANSITION_ONE_MAX) {
        btr = abt_resize(btr, TRANSITION_TWO_BTREE_BYTES);
    }
    bt_insert(btr, bt_val);
    return 0;
}

//TODO minimize malloc defragmentation HERE as we know the size of each row
robj *rdbLoadBT(FILE *fp, redisDb *db) {
    unsigned int   u;
    unsigned char  btype;
    robj          *o = NULL;
    if (fread(&btype, 1, 1, fp) == 0) return NULL;
    if (btype == VIRTUAL_INDEX_TYPE) {
        return createEmptyBtreeObject();
    }

    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    int tmatch = (int)u;
    int dbid = server.dbid;

    if (btype == BTREE_TABLE) { /* BTree w/ DATA */
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        int inum                        = u;
        Tbl[dbid][tmatch].virt_indx     = inum;
        Index[server.dbid][inum].virt   = 1;
        Index[server.dbid][inum].nrl    = 0;
        Index[server.dbid][inum].table  = tmatch;
        Index[server.dbid][inum].column = 0;
        if (!(Tbl[dbid][tmatch].name = rdbLoadStringObject(fp))) return NULL;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        Tbl[dbid][tmatch].col_count = u;
        for (int i = 0; i < Tbl[dbid][tmatch].col_count; i++) {
            if (!(Tbl[dbid][tmatch].col_name[i] = rdbLoadStringObject(fp)))
                return NULL;
            if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
            Tbl[dbid][tmatch].col_type[i] = (unsigned char)u;
        }

        /* BTREE implies an index on "tbl:pk:index" -> autogenerate */
        Index[server.dbid][inum].type = Tbl[dbid][tmatch].col_type[0];

        sds s = sdscatprintf(sdsempty(), "%s:%s:%s",
                              (char *)Tbl[dbid][tmatch].name->ptr, 
                              (char *)Tbl[dbid][tmatch].col_name[0]->ptr,
                              INDEX_DELIM);
        Index[server.dbid][inum].obj = createStringObject(s, sdslen(s));

        dictAdd(db->dict, Index[server.dbid][inum].obj, NULL);
        if (Num_indx[dbid] < (inum + 1)) {
            Num_indx[dbid] = inum + 1;
        }

        unsigned char ktype;
        if (fread(&ktype,    1, 1, fp) == 0) return NULL;

        o = createBtreeObject(ktype, tmatch, btype);
        struct btree *btr  = (struct btree *)(o->ptr);

        unsigned int bt_nkeys; 
        if ((bt_nkeys = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;

        for (int unsigned i = 0; i < bt_nkeys; i++) {
            if (rdbLoadRow(fp, btr) == -1) return NULL;
        }
        //RL4 "load tmatch: %d name: %s inum: %d imatch: %s", tmatch,
        //Tbl[dbid][tmatch].name->ptr, inum, Index[server.dbid][inum].obj->ptr);
        if (Num_tbls[dbid] < (tmatch + 1)) Num_tbls[dbid] = tmatch + 1;
    } else {                        /* INDEX */
        int imatch = tmatch;
        Index[server.dbid][imatch].nrl = 0;
        Index[server.dbid][imatch].obj = rdbLoadStringObject(fp);
        if (!(Index[server.dbid][imatch].obj)) return NULL;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][imatch].table = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][imatch].column = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][imatch].type = (unsigned char)u;
        unsigned char ktype;
        if (fread(&ktype,    1, 1, fp) == 0) return NULL;
        o = createBtreeObject(ktype, imatch, btype);
        Index[server.dbid][imatch].virt = 0;
        if (Num_indx[dbid] < (imatch + 1)) Num_indx[dbid] = imatch + 1;
    }
    return o;
}


void rdbLoadFinished(redisDb *db) {
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (Index[server.dbid][i].virt) continue;
        if (Index[server.dbid][i].nrl)  continue; /* on rebild nrlind is NOOP */
        robj *ind  = Index[server.dbid][i].obj;
        if (!ind) continue;
        robj *ibt  = lookupKey(db, ind);
        bt   *ibtr = (struct btree *)(ibt->ptr);
        int   itbl = Index[server.dbid][i].table;
        int   icol = Index[server.dbid][i].column;
        robj *o    = lookupKey(db, Tbl[server.dbid][itbl].name);
        bt   *btr  = (struct btree *)(o->ptr);
        buildIndex(btr, btr->root, ibtr, icol, itbl, 0);
#if 0
        struct btree *ibtr  = (struct btree *)(ibt->ptr);
        RL4 "INDEX: %d", ibtr->num);
        bt_dumptree(ibtr, 0, 0);
#endif
    }
}

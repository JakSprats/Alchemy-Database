/*
 * This file implements saving alsosql datastructures to rdb files
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "redis.h"
#include "dict.h"
#include "index.h"
#include "bt.h"
#include "common.h"
#include "alsosql.h"
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

static int rdbSaveRow(FILE *fp, bt *btr, bt_n *x) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream = KEYS(btr, x)[i];
        int    ssize  = getStreamMallocSize(stream, REDIS_ROW, btr->is_index);
        if (rdbSaveLen(fp, ssize)        == -1) return -1;
        if (fwrite(stream, ssize, 1, fp) == 0) return -1;
    }

    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            if (rdbSaveRow(fp, btr, NODES(btr, x)[i]) == -1) return -1;
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

    if (fwrite(&(btr->is_index), 1, 1, fp) == 0) return -1;
    if (rdbSaveLen(fp, btr->num) == -1) return -1;

    int tmatch = btr->num;
    int dbid   = server.dbid;
    if (btr->is_index == BTREE_TABLE) {
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
            if (rdbSaveRow(fp, btr, btr->root) == -1) return -1;
        }
    } else { //index
        //RL4 "%d: save index: %s tbl: %d col: %d type: %d",
            //tmatch, Index[dbid][tmatch].obj->ptr, Index[dbid][tmatch].table,
            //Index[dbid][tmatch].column, Index[dbid][tmatch].type);
        if (rdbSaveStringObject(fp, Index[dbid][tmatch].obj) == -1) return -1;
        if (rdbSaveLen(fp, Index[dbid][tmatch].table) == -1) return -1;
        if (rdbSaveLen(fp, Index[dbid][tmatch].column) == -1) return -1;
        if (rdbSaveLen(fp, (int)Index[dbid][tmatch].type) == -1) return -1;
        if (fwrite(&(btr->ktype),    1, 1, fp) == 0) return -1;
    }
    return 0;
}

static int rdbLoadRow(FILE *fp, bt *btr) {
    unsigned int ssize;
    if ((ssize = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return -1;
    char *bt_val = bt_malloc(ssize, btr); // mem bookkeeping done in BT
    if (fread(bt_val, ssize, 1, fp) == 0) return -1;
    bt_insert(btr, bt_val);
    return 0;
}

//TODO minimize malloc defragmentation HERE as we know the size of each row
robj *rdbLoadBT(FILE *fp, redisDb *db) {
    unsigned int   u;
    unsigned char  is_index;
    robj          *o = NULL;
    if (fread(&is_index, 1, 1, fp) == 0) return NULL;
    if (is_index == VIRTUAL_INDEX_TYPE) {
        return createEmptyBtreeObject();
    }

    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
    int tmatch = (int)u;
    int dbid = server.dbid;

    if (is_index == BTREE_TABLE) {
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        int inum = u;
        Tbl[dbid][tmatch].virt_indx  = inum;
        Index[server.dbid][inum].virt   = 1;
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

        Index[server.dbid][inum].type = Tbl[dbid][tmatch].col_type[0];
        Index[server.dbid][inum].obj =
          createStringObject(Tbl[dbid][tmatch].name->ptr,
                             sdslen(Tbl[dbid][tmatch].name->ptr));
        Index[server.dbid][inum].obj->ptr =
           sdscatprintf(Index[server.dbid][inum].obj->ptr, "%s%s%s%s", COLON, 
                         (char *)Tbl[dbid][tmatch].col_name[0]->ptr,
                         COLON, INDEX_DELIM);
        dictAdd(db->dict, Index[server.dbid][inum].obj, NULL);
        if (Num_indx[dbid] < (inum + 1)) {
            Num_indx[dbid] = inum + 1;
        }

        unsigned char ktype;
        if (fread(&ktype,    1, 1, fp) == 0) return NULL;

        o = createBtreeObject(ktype, tmatch, is_index);
        struct btree *btr  = (struct btree *)(o->ptr);

        unsigned int bt_num; 
        if ((bt_num = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;

        for (int unsigned i = 0; i < bt_num; i++) {
            if (rdbLoadRow(fp, btr) == -1) return NULL;
        }

        if (Num_tbls[dbid] < (tmatch + 1)) {
             Num_tbls[dbid] = tmatch + 1;
        }
    } else { /* BTREE_INDEX */
        Index[server.dbid][tmatch].obj = rdbLoadStringObject(fp);
        if (!(Index[server.dbid][tmatch].obj)) return NULL;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][tmatch].table = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][tmatch].column = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index[server.dbid][tmatch].type = (unsigned char)u;
        unsigned char ktype;
        if (fread(&ktype,    1, 1, fp) == 0) return NULL;
        o = createBtreeObject(ktype, tmatch, is_index);
        Index[server.dbid][tmatch].virt = 0;
        if (Num_indx[dbid] < (tmatch + 1)) {
            Num_indx[dbid] = tmatch + 1;
        }
    }
    return o;
}

static void makeIndexFromStream(uchar *stream, bt *ibtr, int icol, int itbl) {
    robj  key, val;
    assignKeyRobj(stream,            &key);
    assignValRobj(stream, REDIS_ROW, &val, ibtr->is_index);
    //get the pk and the fk and then call iAdd()
    robj *fk = createColObjFromRow(&val, icol, &key, itbl); // freeME
    iAdd(ibtr, fk, &key, Tbl[server.dbid][itbl].col_type[0]);
    decrRefCount(fk);
    if (key.encoding == REDIS_ENCODING_RAW) {
        sdsfree(key.ptr); /* free from assignKeyRobj sflag[1,4] */
    }
}

int buildIndex(bt *btr, bt_n *x, bt *ibtr, int icol, int itbl) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream = KEYS(btr, x)[i];
        makeIndexFromStream(stream, ibtr, icol, itbl);
    }

    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            buildIndex(btr, NODES(btr, x)[i], ibtr, icol, itbl);
        }
    }
    return 0;
}

void rdbLoadFinished(redisDb *db) {
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (Index[server.dbid][i].virt) continue;
        robj *ind  = Index[server.dbid][i].obj;
        if (!ind) continue;
        robj *ibt  = lookupKey(db, ind);
        bt   *ibtr = (struct btree *)(ibt->ptr);
        int   itbl = Index[server.dbid][i].table;
        int   icol = Index[server.dbid][i].column;
        robj *o    = lookupKey(db, Tbl[server.dbid][itbl].name);
        bt   *btr  = (struct btree *)(o->ptr);
        buildIndex(btr, btr->root, ibtr, icol, itbl);
#if 0
        struct btree *ibtr  = (struct btree *)(ibt->ptr);
        RL4 "INDEX: %d", ibtr->num);
        bt_dumptree(ibtr, 0, 0);
#endif
    }
}


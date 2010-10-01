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

extern int            Num_tbls     [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_TABLES];

extern int            Num_indx      [MAX_NUM_INDICES];
extern robj          *Index_obj     [MAX_NUM_INDICES];
extern int            Index_on_table[MAX_NUM_INDICES];
extern int            Indexed_column[MAX_NUM_INDICES];
extern unsigned char  Index_type    [MAX_NUM_INDICES];
extern bool           Index_virt    [MAX_NUM_INDICES];

extern char *COLON;

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
    if (btr->is_index == BTREE_TABLE) {
        //RL4 "%d: saving table: %s virt_index: %d",
             //tmatch, Tbl[tmatch].name->ptr, Tbl[tmatch].virt_indx);
        if (rdbSaveLen(fp, Tbl[tmatch].virt_indx) == -1) return -1;
        if (rdbSaveStringObject(fp, Tbl[tmatch].name) == -1) return -1;
        if (rdbSaveLen(fp, Tbl[tmatch].col_count) == -1) return -1;
        for (int i = 0; i < Tbl[tmatch].col_count; i++) {
            if (rdbSaveStringObject(fp, Tbl[tmatch].col_name[i]) == -1)
                return -1;
            if (rdbSaveLen(fp, (int)Tbl[tmatch].col_type[i]) == -1)
                return -1;
        }
        if (fwrite(&(btr->ktype),    1, 1, fp) == 0) return -1;
        if (rdbSaveLen(fp, btr->numkeys)       == -1) return -1;
        if (btr->root && btr->numkeys > 0) {
            if (rdbSaveRow(fp, btr, btr->root) == -1) return -1;
        }
    } else { //index
        //RL4 "%d: save index: %s tbl: %d col: %d type: %d",
             //tmatch, Index_obj[tmatch]->ptr, Index_on_table[tmatch],
             //Indexed_column[tmatch], Index_type[tmatch]);
        if (rdbSaveStringObject(fp, Index_obj[tmatch]) == -1) return -1;
        if (rdbSaveLen(fp, Index_on_table[tmatch]) == -1) return -1;
        if (rdbSaveLen(fp, Indexed_column[tmatch]) == -1) return -1;
        if (rdbSaveLen(fp, (int)Index_type[tmatch]) == -1) return -1;
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

    if (is_index == BTREE_TABLE) {
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        int inum = u;
        Tbl[tmatch].virt_indx  = inum;
        Index_virt    [inum]   = 1;
        Index_on_table[inum]   = tmatch;
        Indexed_column[inum]   = 0;
        if (!(Tbl[tmatch].name = rdbLoadStringObject(fp))) return NULL;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
        Tbl[tmatch].col_count = u;
        for (int i = 0; i < Tbl[tmatch].col_count; i++) {
            if (!(Tbl[tmatch].col_name[i] = rdbLoadStringObject(fp)))
                return NULL;
            if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return NULL;
            Tbl[tmatch].col_type[i] = (unsigned char)u;
        }

        Index_type[inum]     = Tbl[tmatch].col_type[0];
        Index_obj [inum]     = createStringObject(Tbl[tmatch].name->ptr,
                                                 sdslen(Tbl[tmatch].name->ptr));
        Index_obj[inum]->ptr = sdscatprintf(Index_obj[inum]->ptr,
                                           "%s%s%s%s", COLON, 
                                           (char *)Tbl[tmatch].col_name[0]->ptr,
                                           COLON, INDEX_DELIM);
        dictAdd(db->dict, Index_obj[inum], NULL);
        if (Num_indx[server.curr_db_id] < (inum + 1)) {
            Num_indx[server.curr_db_id] = inum + 1;
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

        if (Num_tbls[server.curr_db_id] < (tmatch + 1)) {
             Num_tbls[server.curr_db_id] = tmatch + 1;
        }
    } else { /* BTREE_INDEX */
        if (!(Index_obj[tmatch] = rdbLoadStringObject(fp))) return NULL;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index_on_table[tmatch] = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Indexed_column[tmatch] = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)   return NULL;
        Index_type[tmatch] = (unsigned char)u;
        unsigned char ktype;
        if (fread(&ktype,    1, 1, fp) == 0) return NULL;
        o = createBtreeObject(ktype, tmatch, is_index);
        Index_virt[tmatch] = 0;
        if (Num_indx[server.curr_db_id] < (tmatch + 1)) {
            Num_indx[server.curr_db_id] = tmatch + 1;
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
    iAdd(ibtr, fk, &key, Tbl[itbl].col_type[0]);
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
    for (int i = 0; i < Num_indx[server.curr_db_id]; i++) {
        if (Index_virt[i]) continue;
        robj *ind  = Index_obj[i];
        if (!ind) continue;
        robj *ibt  = lookupKey(db, ind);
        bt   *ibtr = (struct btree *)(ibt->ptr);
        int   itbl = Index_on_table[i];
        int   icol = Indexed_column[i];
        robj *o    = lookupKey(db, Tbl[itbl].name);
        bt   *btr  = (struct btree *)(o->ptr);
        buildIndex(btr, btr->root, ibtr, icol, itbl);
#if 0
        struct btree *ibtr  = (struct btree *)(ibt->ptr);
        RL4 "INDEX: %d", ibtr->num);
        bt_dumptree(ibtr, 0, 0);
#endif
    }
}


/*
 * This file implements saving alsosql datastructures to rdb files
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
#include <assert.h>

#include "redis.h"
#include "rdb.h"

#include "luatrigger.h"
#include "bt.h"
#include "index.h"
#include "lru.h"
#include "stream.h"
#include "alsosql.h"
#include "common.h"
#include "rdb_alsosql.h"

extern int      Num_tbls;
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern int      Num_indx;
extern r_ind_t  Index[MAX_NUM_INDICES];

// CONSTANT GLOBALS
static uchar VIRTUAL_INDEX_TYPE = 255;
static uchar LUAT_JUST_ADD      = 0;
static uchar LUAT_WITH_DEL      = 1;

/* PROTOTYPES */
int rdbSaveRawString(FILE *fp, sds s, size_t len);

static int saveLtc(FILE *fp, ltc_t *ltc) {
    if (rdbSaveRawString(fp, ltc->fname, sdslen(ltc->fname)) == -1) return -1;
    if (rdbSaveLen(fp, ltc->ncols)          == -1)                  return -1;
    for (int j = 0; j < ltc->ncols; j++) {
        if (rdbSaveLen(fp, ltc->cmatchs[j]) == -1)                  return -1;
    }
    return 0;
}
int rdbSaveLuaTrigger(FILE *fp, r_ind_t *ri) {
    int     tmatch = ri->table;
    luat_t *luat   = (luat_t *)ri->btr;
    if (rdbSaveStringObject(fp, ri->obj) == -1)                     return -1;
    if (rdbSaveLen(fp, tmatch)    == -1)                            return -1;
    if (luat->del.ncols) {
        if (fwrite(&LUAT_WITH_DEL, 1, 1, fp) == 0)                  return -1;
    } else {
        if (fwrite(&LUAT_JUST_ADD, 1, 1, fp) == 0)                  return -1;
    }
    if                    (saveLtc(fp, &luat->add)         == -1)   return -1;
    if (luat->del.ncols && saveLtc(fp, &luat->del)         == -1)   return -1;
    return 0;
}
static bool loadLtc(FILE *fp, ltc_t *ltc) {
    robj *o = rdbLoadStringObject(fp);
    ltc->fname = sdsdup(o->ptr);
    decrRefCount(o);
    uint32 u;
    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)             return 0;
    ltc->ncols = u;
    for (int j = 0; j < ltc->ncols; j++) {
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0; 
        ltc->cmatchs[j] = (int)u;
    }
    return 1;
}
bool rdbLoadLuaTrigger(FILE *fp) {
    uint32 u;
    luat_t *luat   = init_lua_trigger();
    robj   *trname = rdbLoadStringObject(fp);
    if ((u     = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
    int     tmatch = (int)u;
    uchar which;
    if (fread(&which, 1, 1, fp) == 0)                               return 0;
    if (loadLtc(fp, &luat->add)              == 0)                  return 0;
    if (which == LUAT_WITH_DEL && loadLtc(fp, &luat->del) == 0)     return 0;

    if (!newIndex(NULL, trname->ptr, tmatch, -1, NULL, 0, 0, 0, luat, -1)) {
                                                                    return 0;
    }
    decrRefCount(trname);
    return 1;
}

static int rdbSaveAllRows(FILE *fp, bt *btr, bt_n *x) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream  = (uchar *)KEYS(btr, x, i);
        int    ssize   = getStreamRowSize(btr, stream);
        uchar *wstream = UU(btr) ? (uchar *)&stream : stream;
        if (rdbSaveLen(fp, ssize)        == -1)                     return -1;
        if (fwrite(wstream, ssize, 1, fp) == 0)                     return -1;
    }
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            if (rdbSaveAllRows(fp, btr, NODES(btr, x)[i]) == -1)    return -1;
        }
    }
    return 0;
}
int rdbSaveBT(FILE *fp, bt *btr) {
    if (!btr) {
        if (fwrite(&VIRTUAL_INDEX_TYPE, 1, 1, fp) == 0)         return -1;
        return 0;
    }
    if (fwrite(&(btr->s.btype), 1, 1, fp) == 0)                 return -1;
    int tmatch = btr->s.num;
    if (rdbSaveLen(fp, tmatch) == -1)                           return -1;
    if (btr->s.btype == BTREE_TABLE) { /* DATA */
        r_tbl_t *rt = &Tbl[tmatch];
        if (rdbSaveLen(fp, rt->vimatch) == -1)                  return -1;
        if (rdbSaveStringObject(fp, rt->name) == -1)            return -1;
        if (rdbSaveLen(fp, rt->col_count) == -1)                return -1;
        for (int i = 0; i < rt->col_count; i++) {
            if (rdbSaveStringObject(fp, rt->col_name[i]) == -1) return -1;
            if (rdbSaveLen(fp, (int)rt->col_type[i]) == -1)     return -1;
        }
        if (fwrite(&(btr->s.ktype),    1, 1, fp) == 0)          return -1;
        if (rdbSaveLen(fp, btr->numkeys)       == -1)           return -1;
        if (btr->root && btr->numkeys > 0) {
            if (rdbSaveAllRows(fp, btr, btr->root) == -1)       return -1;
        }
    } else {                           /* INDEX */
        int imatch = tmatch;
        r_ind_t *ri = &Index[imatch];
        if (rdbSaveStringObject(fp, ri->obj) == -1)             return -1;
        if (rdbSaveLen(fp, ri->table) == -1)                    return -1;
        if (ri->clist) {
            if (rdbSaveLen(fp, ri->nclist) == -1)               return -1;
            for (int i = 0; i < ri->nclist; i++) {
                if (rdbSaveLen(fp, ri->bclist[i]) == -1)        return -1;
            }
        } else {
            if (rdbSaveLen(fp, 1) == -1)                        return -1;
            if (rdbSaveLen(fp, ri->column) == -1)               return -1;
        }
        if (rdbSaveLen(fp, ri->cnstr) == -1)                    return -1;
        if (rdbSaveLen(fp, ri->lru)   == -1)                    return -1;
        // NOTE: obc: -1 not handled well, so incr on SAVE, decr on LOAD
        if (rdbSaveLen(fp, (ri->obc + 1)) == -1)                return -1;//INCR
        if (fwrite(&(btr->s.ktype),    1, 1, fp) == 0)          return -1;
    }
    return 0;
}

ulk UL_RDBPointer; luk LU_RDBPointer; llk LL_RDBPointer;
static int rdbLoadRow(FILE *fp, bt *btr, int tmatch) {
    void   *UUbuf;
    uint32  ssize;
    if ((ssize = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)     return -1;
    void *stream = UU(btr) ? &UUbuf : row_malloc(btr, ssize);
    if (fread(stream, ssize, 1, fp) == 0)                       return -1;
    if (btr->numkeys == TRANS_ONE_MAX) btr = abt_resize(btr, TRANS_TWO);
    if      UU(btr) {
        bt_insert(btr, UUbuf);
    } else if LU(btr) {
        luk *lu           = (luk *)stream;
        LU_RDBPointer.key = lu->key; LU_RDBPointer.val = lu->val;
        bt_insert(btr, &LU_RDBPointer);
    } else if UL(btr) {
        ulk *ul           = (ulk *)stream;
        UL_RDBPointer.key = ul->key; UL_RDBPointer.val = ul->val;
        bt_insert(btr, &UL_RDBPointer);
    } else if LL(btr) {
        llk *ll           = (llk *)stream;
        LL_RDBPointer.key = ll->key; LL_RDBPointer.val = ll->val;
        bt_insert(btr, &LL_RDBPointer);
    } else {
        bt_insert(btr, stream);
    }
    aobj apk;
    convertStream2Key(stream, &apk, btr);
    r_tbl_t *rt = &Tbl[tmatch];
    UPDATE_AUTO_INC(rt->col_type[0], apk);
    releaseAobj(&apk);
    return 0;
}

bool rdbLoadBT(FILE *fp) {
    uint32  u; uchar   btype;
    if (fread(&btype, 1, 1, fp) == 0)                               return 0;
    if (btype == VIRTUAL_INDEX_TYPE)                                return 1;
    if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)             return 0;
    int tmatch = (int)u;
    if (btype == BTREE_TABLE) { /* BTree w/ DATA */
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        int imatch    = u;
        r_tbl_t *rt   = &Tbl[tmatch]; bzero(rt, sizeof(r_tbl_t));
        bzero(rt, sizeof(r_tbl_t)); //TODO use initTable();
        rt->lruc      = rt->lrui       = rt->sk         = rt->fk_cmatch = \
                        rt->fk_otmatch = rt->fk_ocmatch = -1;
        //TODO [lruc, lrui, sk, fk_cmatch, fk_otmatch, fk_ocmatch] -> PERSISTENT
        rt->vimatch   = imatch;
        r_ind_t *ri   = &Index[imatch]; bzero(ri, sizeof(r_ind_t));
        ri->virt      =  1;
        ri->table     =  tmatch;
        ri->column    =  0;
        ri->obc       = -1;
        if (!(rt->name = rdbLoadStringObject(fp)))                  return 0;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        rt->col_count = u;
        for (int i = 0; i < Tbl[tmatch].col_count; i++) {
            if (!(rt->col_name[i] = rdbLoadStringObject(fp)))       return 0;
            if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)     return 0;
            rt->col_type[i] = (uchar)u;
        }

        /* BTREE implies an index on "tbl_pk_index" -> autogenerate */
        sds s = P_SDS_EMT "%s_%s_%s", (char *)rt->name->ptr,
                                      (char *)rt->col_name[0]->ptr,
                                      INDEX_DELIM);
        ri->obj = createStringObject(s, sdslen(s));
        ri->btr = NULL;
        if (Num_indx < (imatch + 1)) Num_indx = imatch + 1;
        uchar ktype;
        if (fread(&ktype,    1, 1, fp) == 0)                        return 0;
        rt->btr = createDBT(ktype, tmatch);
        uint32 bt_nkeys; 
        if ((bt_nkeys = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)  return 0;
        for (uint32 i = 0; i < bt_nkeys; i++) {
            if (rdbLoadRow(fp, rt->btr, tmatch) == -1)              return 0;
        }
        if (Num_tbls < (tmatch + 1)) Num_tbls = tmatch + 1;
    } else {                        /* INDEX */
        int imatch  = tmatch;
        r_ind_t *ri = &Index[imatch]; bzero(ri, sizeof(r_ind_t));
        ri->obj     = rdbLoadStringObject(fp);
        if (!(ri->obj))                                             return 0;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        ri->table   = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        if (u == 1) { /* Single Column */
            if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)     return 0;
            ri->column = (int)u;
            ri->nclist = 0; ri->bclist = NULL; ri->clist  = NULL;
        } else { /* MultipleColumnIndexes */
            Tbl[ri->table].nmci++;
            ri->nclist  = u;
            ri->bclist  = malloc(ri->nclist * sizeof(int)); /* FREE ME 053 */
            ri->clist   = listCreate();                  /* DESTROY ME 054 */
            for (int i = 0; i < ri->nclist; i++) {
                if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR) return 0;
                if (!addC2MCI(NULL, u, ri->clist))                  return 0;
                if (!i) ri->column = u;
                ri->bclist[i] = u;
            }
        }
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        ri->cnstr   = (int)u;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        ri->lru     = (int)u;
        if (ri->lru) {
            r_tbl_t *rt  = &Tbl[ri->table];
            rt->lrui     = imatch;
            rt->lruc     = ri->column;
            rt->lrud     = (uint32)getLru(ri->table);
        }
        ri->luat    = 0;
        if ((u = rdbLoadLen(fp, NULL)) == REDIS_RDB_LENERR)         return 0;
        // NOTE: obc: -1 not handled well, so incr on SAVE, decr on LOAD
        ri->obc     = ((int)u) - 1; //DECR
        uchar ktype;
        if (fread(&ktype,    1, 1, fp) == 0)                        return 0;
        ri->btr = (ri->clist) ?  createMCIndexBT(ri->clist, imatch) :
                                 createIndexBT  (ktype,     imatch);
        ri->virt    = 0;
        if (Num_indx < (imatch + 1)) Num_indx = imatch + 1;
    }
    return 1;
}

void rdbLoadFinished() { // Indexes are built AFTER data is loaded
    for (int imatch = 0; imatch < Num_indx; imatch++) {
        r_ind_t *ri = &Index[imatch];
        if (ri->virt) continue;
        bt   *ibtr = getIBtr(imatch);
        if (!ibtr)    continue; /* if deleted */
        bt   *btr  = getBtr(ri->table);
        buildIndex(NULL, btr, btr->root, ibtr, imatch, 0);
    }
}

/*
 * This file implements the indexing logic of Alsosql
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
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>

#include "redis.h"
#include "bt.h"
#include "btreepriv.h"
#include "bt_iterator.h"
#include "row.h"
#include "common.h"
#include "alsosql.h"
#include "rdb_alsosql.h"
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern char     CMINUS;
extern char     CPERIOD;
extern char    *Col_type_defs[];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// GLOBALS
int     Num_indx[MAX_NUM_DB];
r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
int find_index(int tmatch, int cmatch) {
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (Index[server.dbid][i].obj) {
            if (Index[server.dbid][i].table  == tmatch &&
                Index[server.dbid][i].column == cmatch) {
                return i;
            }
        }
    }
    return -1;
}

int match_index(int tmatch, int indices[]) {
    int matches = 0;
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (Index[server.dbid][i].obj) {
            if (Index[server.dbid][i].table == tmatch) {
                indices[matches] = i;
                matches++;
            }
        }
    }
    return matches;
}

int match_index_name(char *iname) {
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (Index[server.dbid][i].obj) {
            if (!strcmp(iname, (char *)Index[server.dbid][i].obj->ptr)) {
                return i;
            }
        }
    }
    return -1;
}

int checkIndexedColumnOrReply(redisClient *c, char *curr_tname) {
    char *nextp = strchr(curr_tname, CPERIOD);
    if (!nextp) {
        addReply(c, shared.badindexedcolumnsyntax);
        return -1;
    }
    *nextp = '\0';
    TABLE_CHECK_OR_REPLY(curr_tname, -1)
    nextp++;
    COLUMN_CHECK_OR_REPLY(nextp, -1)
    int imatch = find_index(tmatch, cmatch);
    if (imatch == -1) {
        addReply(c, shared.nonexistentindex);
        return -1;
    }
    return imatch;
}

// INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE
// INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE
void iAdd(bt *ibtr, robj *i_key, robj *i_val, uchar pktype) {
    bt   *nbtr;
    robj *nbt = btIndFindVal(ibtr, i_key, ibtr->ktype);
    if (!nbt) {
        nbt  = createIndexNode(pktype);
        btIndAdd(ibtr, i_key, nbt, ibtr->ktype);
        nbtr = (bt *)(nbt->ptr);
        ibtr->malloc_size += nbtr->malloc_size; /* ibtr inherits nbtr */
    } else {
        nbtr = (bt *)(nbt->ptr);
    }
    ull pre_size  = nbtr->malloc_size;
    btIndNodeAdd(nbtr, i_val, pktype);
    ull post_size = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* ibtr inherits nbtr */
}

static void iRem(bt *ibtr, robj *i_key, robj *i_val, int pktype) {
    robj *nbt          = btIndFindVal(ibtr, i_key, ibtr->ktype);
    bt   *nbtr         = (bt *)(nbt->ptr);
    ull   pre_size     = nbtr->malloc_size;
    int   n_size       = btIndNodeDelete(nbtr, i_val, pktype);
    ull   post_size    = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* inherits nbtr */
    if (!n_size) {
        btIndDelete(ibtr, i_key, ibtr->ktype);
        btRelease(nbtr, NULL);
    }
}

void addToIndex(redisDb *db, robj *pko, char *vals, uint cofsts[], int inum) {
    if (Index[server.dbid][inum].virt) return;
    robj *ind        = Index[server.dbid][inum].obj;
    robj *ibt        = lookupKey(db, ind);
    bt   *ibtr       = (bt *)(ibt->ptr);
    int   i          = Index[server.dbid][inum].column;
    int   j          = i - 1;
    int   end        = cofsts[j];
    int   len        = cofsts[i] - end - 1;
    robj *col_key    = createStringObject(vals + end, len); /* freeME */
    int   itm        = Index[server.dbid][inum].table;
    int   pktype     = Tbl[server.dbid][itm].col_type[0];

    iAdd(ibtr, col_key, pko, pktype);
    decrRefCount(col_key);
}

void delFromIndex(redisDb *db, robj *old_pk, robj *row, int inum, int tmatch) {
    if (Index[server.dbid][inum].virt) return;
    robj *ind     = Index[server.dbid][inum].obj;
    int   cmatch  = Index[server.dbid][inum].column;
    robj *ibt     = lookupKey(db, ind);
    bt   *ibtr    = (bt *)(ibt->ptr);
    robj *old_val = createColObjFromRow(row, cmatch, old_pk, tmatch); //freeME
    int   itm     = Index[server.dbid][inum].table;
    int   pktype  = Tbl[server.dbid][itm].col_type[0];

    iRem(ibtr, old_val, old_pk, pktype);
    decrRefCount(old_val);
}

void updateIndex(redisDb *db,
                 robj    *old_pk,
                 robj    *new_pk,
                 robj    *new_val,
                 robj    *row,
                 int       inum,
                 uchar     pk_update,
                 int       tmatch) {
    if (Index[server.dbid][inum].virt) return;
    int   cmatch  = Index[server.dbid][inum].column;
    robj *ind     = Index[server.dbid][inum].obj;
    robj *ibt     = lookupKey(db, ind);
    bt   *ibtr    = (bt *)(ibt->ptr);
    robj *old_val = createColObjFromRow(row, cmatch, old_pk, tmatch); //freeME
    int   itm     = Index[server.dbid][inum].table;
    int   pktype  = Tbl[server.dbid][itm].col_type[0];

    iRem(ibtr, old_val, old_pk, pktype);
    if (pk_update) iAdd(ibtr, old_val, new_pk, pktype);
    else           iAdd(ibtr, new_val, new_pk, pktype);
    decrRefCount(old_val);
}

// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
void newIndex(redisClient *c, char *iname, int tmatch, int cmatch, bool virt) {
    // commit index definition
    robj *ind              = createStringObject(iname, strlen(iname));
    int   imatch           = Num_indx[server.dbid];
    Index[server.dbid][imatch].obj      = ind;
    Index[server.dbid][imatch].table = tmatch;
    Index[server.dbid][imatch].column   = cmatch;
    Index[server.dbid][imatch].type     = Tbl[server.dbid][tmatch].col_type[cmatch];
    Index[server.dbid][imatch].virt     = virt;

    robj *ibt;
    if (virt) {
        ibt                   = createEmptyBtreeObject();
        Tbl[server.dbid][tmatch].virt_indx = imatch;
    } else {
        int ctype = Tbl[server.dbid][tmatch].col_type[cmatch];
        ibt       = createBtreeObject(ctype, imatch, BTREE_INDEX);
    }
    //store BtreeObject in HashTable key: indexname
    dictAdd(c->db->dict, ind, ibt);
    Num_indx[server.dbid]++;
}

static void indexCommit(redisClient *c, char *iname, char *trgt) {
    if (Num_indx[server.dbid] >= MAX_NUM_INDICES) {
        addReply(c, shared.toomanyindices);
        return;
    }

    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames); 
        return;
    }

    // parse tablename.columnname
    sds   target   = sdsdup(trgt);
    char *o_target = target;
    if (target[sdslen(target) - 1] == ')') target[sdslen(target) - 1] = '\0';
    if (*target                    == '(') target++;
    char *column = strchr(target, CPERIOD);
    if (!column) {
        addReply(c, shared.indextargetinvalid);
        goto ind_commit_err;
    }
    *column = '\0';
    column++;
    TABLE_CHECK_OR_REPLY(target,)

    int cmatch = find_column(tmatch, column);
    if (cmatch == -1) {
        addReply(c, shared.indextargetinvalid);
        goto ind_commit_err;
    }

    for (int i = 0; i < Num_indx[server.dbid]; i++) { /* already indxd? */
        if (Index[server.dbid][i].table == tmatch &&
            Index[server.dbid][i].column == cmatch) {
            addReply(c, shared.indexedalready);
            goto ind_commit_err;
        }
    }

    newIndex(c, iname, tmatch, cmatch, 0);
    addReply(c, shared.ok);

    robj *o   = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr = (bt *)o->ptr;
    if (btr->numkeys > 0) { /* table has rows - loop thru and populate index */
        robj *ind  = Index[server.dbid][Num_indx[server.dbid] - 1].obj;
        robj *ibt  = lookupKey(c->db, ind);
        buildIndex(btr, btr->root, ibt->ptr, cmatch, tmatch);
    }
ind_commit_err:
    sdsfree(o_target);
}

void createIndex(redisClient *c) {
    if (c->argc < 6) {
        addReply(c, shared.index_wrong_num_args);
        return;
    }

    /* TODO lazy programming, change legacyIndex syntax */
    sds legacy_column       = sdstrim(c->argv[5]->ptr, "()");
    sds legacy_index_syntax = sdscatprintf(sdsempty(), "%s.%s",
                                           (char *)c->argv[4]->ptr,
                                           (char *)legacy_column);
    indexCommit(c, c->argv[2]->ptr, legacy_index_syntax);
    sdsfree(legacy_index_syntax);
}

void legacyIndexCommand(redisClient *c) {
    indexCommit(c, c->argv[1]->ptr, c->argv[2]->ptr);
}


void indexEmpty(redisDb *db, int inum) {
    robj *ind                       = Index[server.dbid][inum].obj;
    deleteKey(db, ind);
    Index[server.dbid][inum].table  = -1;
    Index[server.dbid][inum].column = -1;
    Index[server.dbid][inum].type   = 0;
    Index[server.dbid][inum].virt   = 0;
    Index[server.dbid][inum].obj    = NULL;
    server.dirty++;
    //TODO shuffle indices to make space for deleted indices
}

void dropIndex(redisClient *c) {
    char *iname  = c->argv[2]->ptr;
    int   inum   = match_index_name(iname);

    if (Index[server.dbid][inum].virt) {
        addReply(c, shared.drop_virtual_index);
        return;
    }

    indexEmpty(c->db, inum);
    addReply(c, shared.cone);
}

// INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS
// INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS INDEX_COMMANDS
void iselectAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          imatch,
                   char        *col_list) {
    int cmatchs[MAX_COLUMN_PER_TABLE];
    int qcols = parseColListOrReply(c, tmatch, col_list, cmatchs);
    if (!qcols) {
        addReply(c, shared.nullbulk);
        return;
    }
    RANGE_CHECK_OR_REPLY(range)
    robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    LEN_OBJ

    btEntry    *be, *nbe;
    robj       *ind  = Index[server.dbid][imatch].obj;
    bool        virt = Index[server.dbid][imatch].virt;
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    btStreamIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *pko = be->key;
            robj *row = be->val;
            robj *r   = outputRow(row, qcols, cmatchs, pko, tmatch, 0);
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
        } else {
            robj       *val = be->val;
            btStreamIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {     // iterate NodeBT
                robj *nkey = nbe->key;
                selectReply(c, o, nkey, tmatch, cmatchs, qcols);
                card++;
            }
            btReleaseRangeIterator(nbi);
        }
    }
    btReleaseRangeIterator(bi);

    decrRefCount(low);
    decrRefCount(high);
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}

#if 0
void iselectCommand(redisClient *c) {
    int imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    int tmatch = Index[server.dbid][imatch].table;

    iselectAction(c, c->argv[2]->ptr, tmatch, imatch, c->argv[3]->ptr);
}
#endif

void iupdateAction(redisClient   *c,
                   char          *range,
                   int            tmatch,
                   int            imatch,
                   int            ncols,
                   int            matches,
                   int            indices[],
                   char          *vals[],
                   uint   vlens[],
                   uchar  cmiss[]) {
    RANGE_CHECK_OR_REPLY(range)
    btEntry    *be, *nbe;
    robj       *o    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    robj       *ind  = Index[server.dbid][imatch].obj;
    bool        virt = Index[server.dbid][imatch].virt;
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    robj       *uset = createSetObject();
    btStreamIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *nkey = be->key;
            dictAdd(uset->ptr, nkey, NULL);
        } else {
            robj       *val = be->val;
            btStreamIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {     // iterate NodeBT
                robj *nkey = nbe->key;
                robj *cln  = cloneRobj(nkey);
                dictAdd(uset->ptr, cln, NULL);
            }
            btReleaseRangeIterator(nbi);
        }
    }
    btReleaseRangeIterator(bi);
    decrRefCount(low);
    decrRefCount(high);

    dictEntry     *ude;
    unsigned long  updated = 0;
    dictIterator  *udi     = dictGetIterator(uset->ptr);
    while ((ude = dictNext(udi)) != NULL) {                    // iterate uset
        robj *nkey = ude->key;
        robj *row  = btFindVal(o, nkey, Tbl[server.dbid][tmatch].col_type[0]);
        updateRow(c, o, nkey, row, tmatch, ncols, matches, indices,
                  vals, vlens, cmiss);
        updated++;
    }
    dictReleaseIterator(udi);
    decrRefCount(uset);

    addReplyLongLong(c, updated);
}

#if 0
void iupdateCommand(redisClient *c) {
    int   imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    int   tmatch = Index[server.dbid][imatch].table;
    int   ncols   = Tbl[server.dbid][tmatch]._col_count;

    int   cmatchs  [MAX_COLUMN_PER_TABLE];
    char *mvals    [MAX_COLUMN_PER_TABLE];
    int   mvlens   [MAX_COLUMN_PER_TABLE];
    int   qcols = parseUpdateOrReply(c, tmatch, c->argv[3]->ptr, cmatchs,
                                     mvals, mvlens);
    if (!qcols) return;

    MATCH_INDICES(tmatch)
    ASSIGN_UPDATE_HITS_AND_MISSES

    iupdateAction(c, c->argv[2]->ptr, tmatch, imatch, ncols, matches, indices,
                  vals, vlens, cmiss);
}
#endif

void ideleteAction(redisClient *c, char *range, int tmatch, int imatch) {
    RANGE_CHECK_OR_REPLY(range)
    MATCH_INDICES(tmatch)

    btEntry    *be,  *nbe;
    robj       *o    = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    robj       *ind  = Index[server.dbid][imatch].obj;
    bool        virt = Index[server.dbid][imatch].virt;
    robj       *dset = createSetObject();
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    btStreamIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *nkey = be->key;
            robj *cln  = cloneRobj(nkey);
            dictAdd(dset->ptr, cln, NULL);
        } else {
            robj       *val = be->val;
            btStreamIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {     // iterate NodeBT
                robj *nkey = nbe->key;
                robj *cln  = cloneRobj(nkey);
                dictAdd(dset->ptr, cln, NULL);
            }
            btReleaseRangeIterator(nbi);
        }
    }
    btReleaseRangeIterator(bi);
    decrRefCount(low);
    decrRefCount(high);

    dictEntry     *dde;
    unsigned long  deleted = 0;
    dictIterator  *ddi     = dictGetIterator(dset->ptr);
    while ((dde = dictNext(ddi)) != NULL) {                    // iterate dset
        robj *key = dde->key;
        deleteRow(c, tmatch, key, matches, indices);
        deleted++;
    }
    dictReleaseIterator(ddi);
    decrRefCount(dset);

    addReplyLongLong(c, deleted);
}

#if 0
void ideleteCommand(redisClient *c) {
    int   imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    int   tmatch = Index[server.dbid][imatch].table;
    ideleteAction(c, c->argv[2]->ptr, tmatch, imatch);
}
#endif

void ikeysCommand(redisClient *c) {
    int   imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    RANGE_CHECK_OR_REPLY(c->argv[2]->ptr)
    LEN_OBJ

    btEntry    *be, *nbe;
    robj       *ind  = Index[server.dbid][imatch].obj;
    bool        virt = Index[server.dbid][imatch].virt;
    robj       *bt   = lookupKey(c->db, ind);
    btStreamIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *pko = be->key;
            addReplyBulk(c, pko);
            card++;
        } else {
            robj       *val = be->val;
            btStreamIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {     // iterate NodeBT
                robj *nkey = nbe->key;
                addReplyBulk(c, nkey);
                card++;
            }
            btReleaseRangeIterator(nbi);
        }
    }
    btReleaseRangeIterator(bi);
    decrRefCount(low);
    decrRefCount(high);

    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}

#define ADD_REPLY_BULK(r, buf)                \
    r = createStringObject(buf, strlen(buf)); \
    addReplyBulk(c, r);                       \
    decrRefCount(r);                          \
    card++;

void dumpCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);

    bt *btr = (bt *)o->ptr;
    int   cmatchs[MAX_COLUMN_PER_TABLE];
    int   qcols = parseColListOrReply(c, tmatch, "*", cmatchs);
    char *tname = Tbl[server.dbid][tmatch].name->ptr;

    LEN_OBJ

    bool  to_mysql = 0;
    bool  ret_size = 0;
    char *m_tname  = tname;
    if (c->argc > 3) {
        if (!strcasecmp(c->argv[2]->ptr, "TO") &&
            !strcasecmp(c->argv[3]->ptr, "MYSQL")      ) {
            to_mysql = 1;
            if (c->argc > 4) m_tname = c->argv[4]->ptr;
            robj *r;
            sprintf(buf, "DROP TABLE IF EXISTS `%s`;", m_tname);
            ADD_REPLY_BULK(r, buf)
            sprintf(buf, "CREATE TABLE `%s` ( ", m_tname);
            r = createStringObject(buf, strlen(buf));
            for (int i = 0; i < Tbl[server.dbid][tmatch].col_count; i++) {
                bool is_int =
                         (Tbl[server.dbid][tmatch].col_type[i] == COL_TYPE_INT);
                r->ptr = sdscatprintf(r->ptr, "%s %s %s%s",
                          (i == 0) ? ""        : ",",
                          (char *)Tbl[server.dbid][tmatch].col_name[i]->ptr,
                          is_int ? "INT" : (i == 0) ? "VARCHAR(512)" : "TEXT",
                          (i == 0) ? " PRIMARY KEY" : "");
            }
            r->ptr = sdscat(r->ptr, ");");
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
            sprintf(buf, "LOCK TABLES `%s` WRITE;", m_tname);
            ADD_REPLY_BULK(r, buf)
        } else if (!strcasecmp(c->argv[2]->ptr, "RETURN") &&
                   !strcasecmp(c->argv[3]->ptr, "SIZE")      ) {
            ret_size = 1;
            sprintf(buf, "KEYS: %d BT-DATA: %lld BT-MALLOC: %lld",
                          btr->numkeys, btr->data_size, btr->malloc_size);
            robj *r = createStringObject(buf, strlen(buf));
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
        }
    }

    if (btr->numkeys) {
        btEntry    *be;
        btStreamIterator *bi = btGetFullRangeIterator(o, 0, 1);
        while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
            robj *pko = be->key;
            robj *row = be->val;
            robj *r   = outputRow(row, qcols, cmatchs, pko, tmatch, to_mysql);
            if (!to_mysql) {
                addReplyBulk(c, r);
                decrRefCount(r);
            } else {
                sprintf(buf, "INSERT INTO `%s` VALUES (", m_tname);
                robj *ins = createStringObject(buf, strlen(buf));
                ins->ptr  = sdscatlen(ins->ptr, r->ptr, sdslen(r->ptr));
                ins->ptr  = sdscatlen(ins->ptr, ");", 2);
                addReplyBulk(c, ins);
                decrRefCount(ins);
            }
            card++;
        }
        btReleaseRangeIterator(bi);
    }

    if (to_mysql) {
        robj *r;
        sprintf(buf, "UNLOCK TABLES;");
        ADD_REPLY_BULK(r, buf)
    }
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}
 
ull get_sum_all_index_size_for_table(redisClient *c, int tmatch) {
    ull isize = 0;
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (!Index[server.dbid][i].virt && Index[server.dbid][i].table == tmatch) {
            robj *ind   = Index[server.dbid][i].obj;
            robj *ibt   = lookupKey(c->db, ind);
            bt   *ibtr  = (bt *)(ibt->ptr);
            isize      += ibtr->malloc_size;
        }
    }
    return isize;
}

static void zero(robj *r) {
    r->encoding = REDIS_ENCODING_RAW;
    r->ptr      = 0;
}

void descCommand(redisClient *c) {
    char buf[256];
    TABLE_CHECK_OR_REPLY( c->argv[1]->ptr,)
    robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);

    LEN_OBJ;
    for (int j = 0; j < Tbl[server.dbid][tmatch].col_count; j++) {
        robj *r      = createObject(REDIS_STRING, NULL);
        int   imatch = find_index(tmatch, j);
        if (imatch == -1) {
            r->ptr  = sdscatprintf(sdsempty(), "%s | %s ",
                           (char *)Tbl[server.dbid][tmatch].col_name[j]->ptr,
                           Col_type_defs[Tbl[server.dbid][tmatch].col_type[j]]);
        } else {
            robj *ind    = Index[server.dbid][imatch].obj;
            ull   isize  = 0;
            if (!Index[server.dbid][imatch].virt) {
                robj *ibt  = lookupKey(c->db, ind);
                bt   *ibtr = (bt *)(ibt->ptr);
                isize      = ibtr ? ibtr->malloc_size : 0;
            }
            r->ptr = sdscatprintf(sdsempty(),
                            "%s | %s | INDEX: %s [BYTES: %lld]",
                            (char *)Tbl[server.dbid][tmatch].col_name[j]->ptr,
                            Col_type_defs[Tbl[server.dbid][tmatch].col_type[j]],
                            (char *)ind->ptr, isize);
        }
        addReplyBulk(c, r);
        decrRefCount(r);
	card++;
    }
    ull  index_size = get_sum_all_index_size_for_table(c, tmatch);
    bt  *btr        = (bt *)o->ptr;
    robj minkey, maxkey;
    if (!btr->numkeys || !assignMinKey(btr, &minkey)) zero(&minkey);
    if (!btr->numkeys || !assignMaxKey(btr, &maxkey)) zero(&maxkey);

    if (minkey.encoding == REDIS_ENCODING_RAW) {
        if (minkey.ptr && sdslen(minkey.ptr) > 64) {
            char *x = (char *)(minkey.ptr);
            x[64] ='\0';
        }
        if (maxkey.ptr && sdslen(maxkey.ptr) > 64) {
            char *x = (char *)(maxkey.ptr);
            x[64] ='\0';
        }
        sprintf(buf, "INFO: KEYS: [NUM: %d MIN: %s MAX: %s]"\
                          " BYTES: [BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
                btr->numkeys, (char *)minkey.ptr, (char *)maxkey.ptr,
                btr->data_size, btr->malloc_size, index_size);
    } else {
        sprintf(buf, "INFO: KEYS: [NUM: %d MIN: %u MAX: %u]"\
                          " BYTES: [BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
            btr->numkeys, (uint)(long)minkey.ptr, (uint)(long)maxkey.ptr,
            btr->data_size, btr->malloc_size, index_size);
    }
    robj *r = createStringObject(buf, strlen(buf));
    addReplyBulk(c, r);
    decrRefCount(r);
    card++;
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}

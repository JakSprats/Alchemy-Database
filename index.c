/*
 * This file implements the indexing logic of Alsosql
 *

MIT License

Copyright (c) 2010 Russell Sullivan

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
#include "rdb_alsosql.h"
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern char  CMINUS;
extern char  CPERIOD;

extern char *Col_type_defs[];

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern int            Tbl_col_count[MAX_NUM_TABLES];
extern robj          *Tbl_col_name [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern int            Tbl_virt_indx[MAX_NUM_TABLES];

// GLOBALS
int Num_indx;
//TODO make these 4 a struct
robj          *Index_obj     [MAX_NUM_INDICES];
int            Index_on_table[MAX_NUM_INDICES];
int            Indexed_column[MAX_NUM_INDICES];
unsigned char  Index_type    [MAX_NUM_INDICES];
bool           Index_virt    [MAX_NUM_INDICES];

// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
// HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS
int find_index(int tmatch, int cmatch) {
    for (int i = 0; i < Num_indx; i++) {
        if (Index_obj[i]) {
            if (Index_on_table[i] == tmatch &&
                Indexed_column[i] == cmatch) {
                return i;
            }
        }
    }
    return -1;
}

int match_index(int tmatch, int indices[]) {
    int matches = 0;
    for (int i = 0; i < Num_indx; i++) {
        if (Index_obj[i]) {
            if (Index_on_table[i] == tmatch) {
                indices[matches] = i;
                matches++;
            }
        }
    }
    return matches;
}

int match_index_name(char *iname) {
    for (int i = 0; i < Num_indx; i++) {
        if (Index_obj[i]) {
            if (!strcmp(iname, (char *)Index_obj[i]->ptr)) {
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
void iAdd(bt *ibtr, robj *i_key, robj *i_val, unsigned char pktype) {
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
    int pre_size       = nbtr->malloc_size;
    btIndNodeAdd(nbtr, i_val, pktype);
    int post_size      = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* ibtr inherits nbtr */
}

static void iRem(bt *ibtr, robj *i_key, robj *i_val, int pktype) {
    robj *nbt          = btIndFindVal(ibtr, i_key, ibtr->ktype);
    bt   *nbtr         = (bt *)(nbt->ptr);
    int   pre_size     = nbtr->malloc_size;
    int   n_size       = btIndNodeDelete(nbtr, i_val, pktype);
    int   post_size    = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* ibtr inherits nbtr */
    if (!n_size) {
        btIndDelete(ibtr, i_key, ibtr->ktype);
        btRelease(nbtr, NULL);
    }
}

void addToIndex(redisDb      *db,
                robj         *pko,
                char         *vals,
                unsigned int  col_ofsts[],
                int           inum) {
    if (Index_virt[inum]) return;
    robj *ind        = Index_obj[inum];
    robj *ibt        = lookupKey(db, ind);
    bt   *ibtr       = (bt *)(ibt->ptr);
    int   i          = Indexed_column[inum];
    int   j          = i - 1;
    int   end        = col_ofsts[j];
    int   len        = col_ofsts[i] - end - 1;
    robj *col_key    = createStringObject(vals + end, len); /* freeME */
    int   pktype     = Tbl_col_type[Index_on_table[inum]][0];

    iAdd(ibtr, col_key, pko, pktype);
    decrRefCount(col_key);
}

void delFromIndex(redisClient *c,
                  robj        *old_pk,
                  robj        *row,
                  int          inum,
                  int          tmatch) {
    if (Index_virt[inum]) return;
    robj *ind     = Index_obj     [inum];
    int   cmatch  = Indexed_column[inum];
    robj *ibt     = lookupKey(c->db, ind);
    bt   *ibtr    = (bt *)(ibt->ptr);
    robj *old_val = createColObjFromRow(row, cmatch, old_pk, tmatch); //freeME
    int   pktype  = Tbl_col_type[Index_on_table[inum]][0];

    iRem(ibtr, old_val, old_pk, pktype);
    decrRefCount(old_val);
}

void updateIndex(redisClient  *c,
                 robj          *old_pk,
                 robj          *new_pk,
                 robj          *new_val,
                 robj          *row,
                 int            inum,
                 unsigned char  pk_update,
                 int            tmatch) {
    if (Index_virt[inum]) return;
    int   cmatch  = Indexed_column[inum];
    robj *ind     = Index_obj     [inum];
    robj *ibt     = lookupKey(c->db, ind);
    bt   *ibtr    = (bt *)(ibt->ptr);
    robj *old_val = createColObjFromRow(row, cmatch, old_pk, tmatch); //freeME
    int   pktype  = Tbl_col_type[Index_on_table[inum]][0];

    iRem(ibtr, old_val, old_pk, pktype);
    if (pk_update) iAdd(ibtr, old_val, new_pk, pktype);
    else           iAdd(ibtr, new_val, new_pk, pktype);
    decrRefCount(old_val);
}

// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
// SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS SIMPLE_COMMANDS
void newIndex(redisClient *c, char *iname, int tmatch, int cmatch, bool virt) {
    // commit index definition
    robj *ind                = createStringObject(iname, strlen(iname));
    Index_obj     [Num_indx] = ind;
    Index_on_table[Num_indx] = tmatch;
    Indexed_column[Num_indx] = cmatch;
    Index_type    [Num_indx] = Tbl_col_type[tmatch][cmatch];
    Index_virt    [Num_indx] = virt;

    robj *ibt;
    if (virt) {
        ibt                   = createEmptyBtreeObject();
        Tbl_virt_indx[tmatch] = Num_indx;
    } else {
        int ctype = Tbl_col_type[tmatch][cmatch];
        ibt       = createBtreeObject(ctype, Num_indx, BTREE_INDEX);
    }
    //store BtreeObject in HashTable key: indexname
    dictAdd(c->db->dict, ind, ibt);
    Num_indx++;
}

void indexCommit(redisClient *c, char *iname, char *trgt) {
    if (Num_indx >= MAX_NUM_INDICES) {
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

    for (int i = 0; i < Num_indx; i++) { /* check if already indexed */
        if (Index_on_table[i] == tmatch &&
            Indexed_column[i] == cmatch) {
            addReply(c, shared.indexedalready);
            goto ind_commit_err;
        }
    }

    newIndex(c, iname, tmatch, cmatch, 0);
    addReply(c, shared.ok);

    robj *o   = lookupKeyRead(c->db, Tbl_name[tmatch]);
    bt   *btr = (bt *)o->ptr;
    if (btr->numkeys > 0) { /* table has rows - loop thru and populate index */
        robj *ind  = Index_obj[Num_indx - 1];
        robj *ibt  = lookupKey(c->db, ind);
        buildIndex(btr, btr->root, ibt->ptr, cmatch, tmatch);
    }
ind_commit_err:
    sdsfree(o_target);
}
void createIndex(redisClient *c) {
    if (c->argc < 5) {
        addReplySds(c, sdscatprintf(sdsempty(),
              "-ERR wrong number of arguments for 'CREATE INDEX' command\r\n"));
        return;
    }
    indexCommit(c, c->argv[2]->ptr, c->argv[4]->ptr);
}
#if 0
void legacyIndexCommand(redisClient *c) {
    indexCommit(c, c->argv[1]->ptr, c->argv[2]->ptr);
}
#endif

void dropIndex(redisClient *c) {
    char *iname          = c->argv[2]->ptr;
    int   inum           = match_index_name(iname);
    robj *ind            = Index_obj[inum];
    if (Index_virt[inum]) {
        addReply(c, shared.drop_virtual_index);
        return;
    }

    Index_on_table[inum] = -1;
    Indexed_column[inum] = -1;
    Index_obj     [inum] = NULL;
    deleteKey(c->db, ind);
    server.dirty++;
    //TODO shuffle indices to make space for deleted indices
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
    robj *o = lookupKeyRead(c->db, Tbl_name[tmatch]);
    LEN_OBJ

    btEntry    *be, *nbe;
    robj       *ind  = Index_obj [imatch];
    bool        virt = Index_virt[imatch];
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    btIterator *bi   = btGetRangeIterator(bt, low, high, virt);
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
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
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
    int tmatch = Index_on_table[imatch];

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
                   unsigned int   vlens[],
                   unsigned char  cmiss[]) {
    RANGE_CHECK_OR_REPLY(range)
    btEntry    *be, *nbe;
    robj       *o    = lookupKeyRead(c->db, Tbl_name[tmatch]);
    robj       *ind  = Index_obj [imatch];
    bool        virt = Index_virt[imatch];
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    robj       *uset = createSetObject();
    btIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *nkey = be->key;
            dictAdd(uset->ptr, nkey, NULL);
        } else {
            robj       *val = be->val;
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
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
        robj *row  = btFindVal(o, nkey, Tbl_col_type[tmatch][0]);
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
    int   tmatch = Index_on_table[imatch];
    int   ncols   = Tbl_col_count [tmatch];

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
    robj       *o    = lookupKeyRead(c->db, Tbl_name[tmatch]);
    robj       *ind  = Index_obj [imatch];
    bool        virt = Index_virt[imatch];
    robj       *dset = createSetObject();
    robj       *bt   = virt ? o : lookupKey(c->db, ind);
    btIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *nkey = be->key;
            robj *cln  = cloneRobj(nkey);
            dictAdd(dset->ptr, cln, NULL);
        } else {
            robj       *val = be->val;
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
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
    int   tmatch = Index_on_table[imatch];
    ideleteAction(c, c->argv[2]->ptr, tmatch, imatch);
}
#endif

void ikeysCommand(redisClient *c) {
    int   imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    RANGE_CHECK_OR_REPLY(c->argv[2]->ptr)
    LEN_OBJ

    btEntry    *be, *nbe;
    robj       *ind  = Index_obj [imatch];
    bool        virt = Index_virt[imatch];
    robj       *bt   = lookupKey(c->db, ind);
    btIterator *bi   = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *pko = be->key;
            addReplyBulk(c, pko);
            card++;
        } else {
            robj       *val = be->val;
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
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
    robj *o = lookupKeyReadOrReply(c, Tbl_name[tmatch], shared.nullbulk);
    if (!o) return;

    bt *btr = (bt *)o->ptr;
    if (!btr->numkeys) {
        addReply(c, shared.czero);
        return;
    }
    int   cmatchs[MAX_COLUMN_PER_TABLE];
    int   qcols = parseColListOrReply(c, tmatch, "*", cmatchs);
    char *tname = Tbl_name[tmatch]->ptr;

    LEN_OBJ

    bool to_mysql = 0;
    if (c->argc > 2) {
        if (!strcasecmp(c->argv[2]->ptr, "TO") &&
            !strcasecmp(c->argv[3]->ptr, "MYSQL")      ) {
            to_mysql = 1;
            robj *r;
            sprintf(buf, "DROP TABLE IF EXISTS `%s`;", tname);
            ADD_REPLY_BULK(r, buf)
            sprintf(buf, "CREATE TABLE `%s` ( ", tname);
            r = createStringObject(buf, strlen(buf));
            for (int i = 0; i < Tbl_col_count[tmatch]; i++) {
                bool is_int = (Tbl_col_type[tmatch][i] == COL_TYPE_INT);
                r->ptr = sdscatprintf(r->ptr, "%s %s %s%s",
                          (i == 0) ? ""        : ",",
                          (char *)Tbl_col_name[tmatch][i]->ptr,
                          is_int ? "INT" : (i == 0) ? "VARCHAR(512)" : "TEXT",
                          (i == 0) ? " PRIMARY KEY" : "");
            }
            r->ptr = sdscat(r->ptr, ");");
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
            sprintf(buf, "LOCK TABLES `%s` WRITE;", tname);
            ADD_REPLY_BULK(r, buf)
        } else if (!strcasecmp(c->argv[2]->ptr, "RETURN") &&
                   !strcasecmp(c->argv[3]->ptr, "SIZE")      ) {
            sprintf(buf, "KEYS: %d BT-DATA: %d BT-MALLOC: %d",
                          btr->numkeys, btr->data_size, btr->malloc_size);
            robj *r = createStringObject(buf, strlen(buf));
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
        }
    }

    btEntry    *be;
    btIterator *bi = btGetFullRangeIterator(o, 0, 1);
    while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
        robj *pko = be->key;
        robj *row = be->val;
        robj *r   = outputRow(row, qcols, cmatchs, pko, tmatch, to_mysql);
        if (!to_mysql) {
            addReplyBulk(c, r);
            decrRefCount(r);
        } else {
            sprintf(buf, "INSERT INTO `%s` VALUES (", tname);
            robj *ins = createStringObject(buf, strlen(buf));
            ins->ptr  = sdscatlen(ins->ptr, r->ptr, sdslen(r->ptr));
            ins->ptr  = sdscatlen(ins->ptr, ");", 2);
            addReplyBulk(c, ins);
            decrRefCount(ins);
        }
        card++;
    }
    btReleaseRangeIterator(bi);

    if (to_mysql) {
        robj *r;
        sprintf(buf, "UNLOCK TABLES;");
        ADD_REPLY_BULK(r, buf)
    }
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}
 
int get_sum_all_index_size_for_table(redisClient *c, int tmatch) {
    int isize = 0;
    for (int i = 0; i < Num_indx; i++) {
        if (!Index_virt[i] && Index_on_table[i] == tmatch) {
            robj *ind   = Index_obj[i];
            robj *ibt   = lookupKey(c->db, ind);
            bt   *ibtr  = (bt *)(ibt->ptr);
            isize      += ibtr->malloc_size;
        }
    }
    return isize;
}

void descCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY( c->argv[1]->ptr,)
    robj *o = lookupKeyReadOrReply(c, Tbl_name[tmatch], shared.nullbulk);
    if (!o) return;

    LEN_OBJ;
    for (int j = 0; j < Tbl_col_count[tmatch]; j++) {
        robj *r      = createObject(REDIS_STRING, NULL);
        int   imatch = find_index(tmatch, j);
        if (imatch == -1) {
            r->ptr  = sdscatprintf(sdsempty(), "%s | %s ",
                                        (char *)Tbl_col_name[tmatch][j]->ptr,
                                        Col_type_defs[Tbl_col_type[tmatch][j]]);
        } else {
            robj *ind    = Index_obj[imatch];
            int   isize  = 0;
            if (!Index_virt[imatch]) {
                robj *ibt  = lookupKey(c->db, ind);
                bt   *ibtr = (bt *)(ibt->ptr);
                isize      = ibtr ? ibtr->malloc_size : 0;
            }
            r->ptr = sdscatprintf(sdsempty(), "%s | %s | INDEX: %s [BYTES: %d]",
                                        (char *)Tbl_col_name[tmatch][j]->ptr,
                                        Col_type_defs[Tbl_col_type[tmatch][j]],
                                        (char *)ind->ptr, 
                                        isize);
        }
        addReplyBulk(c, r);
        decrRefCount(r);
	card++;
    }
    int  index_size = get_sum_all_index_size_for_table(c, tmatch);
    bt  *btr        = (bt *)o->ptr;
    sprintf(buf, "INFO: KEYS: %d BYTES: [BT-DATA: %d BT-TOTAL: %d INDEX: %d]",
             btr->numkeys, btr->data_size, btr->malloc_size, index_size);
    robj *r = createStringObject(buf, strlen(buf));
    addReplyBulk(c, r);
    decrRefCount(r);
    card++;
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}

/*
 * This file implements the indexing logic of Alsosql
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
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <limits.h>

#include "adlist.h"
#include "redis.h"

#include "bt.h"
#include "btreepriv.h"
#include "bt_iterator.h"
#include "row.h"
#include "orderby.h"
#include "nri.h"
#include "colparse.h"
#include "stream.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "index.h"

// FROM redis.c
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

extern char    *Col_type_defs[];
extern r_tbl_t  Tbl[MAX_NUM_DB][MAX_NUM_TABLES];

// GLOBALS
int     Num_indx[MAX_NUM_DB];
r_ind_t Index   [MAX_NUM_DB][MAX_NUM_INDICES];

// PROTOTYPES
static d_l_t *init_d_l_t();
static void destroy_d_l_t(d_l_t *nrlind);


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

/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
static void iAdd(bt *ibtr, aobj *acol, aobj *apk, uchar pktype) {
    bt *nbtr = btIndFindVal(ibtr, acol);
    if (!nbtr) {
        nbtr               = createIndexNode(pktype);
        btIndAdd(ibtr, acol, nbtr);
        ibtr->malloc_size += nbtr->malloc_size; /* ibtr inherits nbtr */
    }
    ull pre_size  = nbtr->malloc_size;
    btIndNodeAdd(nbtr, apk);
    ull post_size = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* ibtr inherits nbtr */
}

static void iRem(bt *ibtr, aobj *acol, aobj *apk) {
    bt   *nbtr         = btIndFindVal(ibtr, acol);
    ull   pre_size     = nbtr->malloc_size;
    int   n_size       = btIndNodeDelete(nbtr, apk);
    ull   post_size    = nbtr->malloc_size;
    ibtr->malloc_size += (post_size - pre_size); /* inherits nbtr */
    if (!n_size) {
        btIndDelete(ibtr, acol); /* remove Ind ref to NodeBT */
        btDestroy(nbtr, ibtr);   /* must come after btIndDelete()*/
    }
}

void addToIndex(redisDb *db, aobj *apk, char *vals, uint32 cofsts[], int inum) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    robj *ind     = Index[server.dbid][inum].obj;
    robj *ibtt    = lookupKey(db, ind);
    if (nrl) {
        nrlIndexAdd(ibtt, apk, vals, cofsts);
        return;
    }
    bt   *ibtr    = (bt *)(ibtt->ptr);
    int   itm     = Index[server.dbid][inum].table;
    int   pktype  = Tbl[server.dbid][itm].col_type[0];
    int   icol    = Index[server.dbid][inum].column;
    int   jcol    = icol - 1;
    int   end     = cofsts[jcol];
    int   len     = cofsts[icol] - end - 1;
    uchar itype   = Tbl[server.dbid][itm].col_type[icol];
    aobj  acol;
    initAobjFromString(&acol, vals + end, len, itype);
    iAdd(ibtr, &acol, apk, pktype);
}

void delFromIndex(redisDb *db, aobj *apk, void *rrow, int inum, int tmatch) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    if (nrl) { /* TODO add in nrldel */ return; }
    int   cmatch  = Index[server.dbid][inum].column;
    robj *ind     = Index[server.dbid][inum].obj;
    robj *ibtt    = lookupKey(db, ind);
    bt   *ibtr    = (bt *)(ibtt->ptr);
    aobj  acol    = getRawCol(rrow, cmatch, apk, tmatch, NULL, 0);
    iRem(ibtr, &acol, apk);
    releaseAobj(&acol);
}

static void _updateIndex(redisDb *db,
                         aobj    *aopk,
                         aobj    *anpk,
                         aobj    *ncol,
                         void    *rrow,
                         int      inum,
                         uchar    pkup,
                         int      tmatch) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    if (nrl) { /* TODO add nrldel/add */ return; }
    int   cmatch  = Index[server.dbid][inum].column;
    robj *ind     = Index[server.dbid][inum].obj;
    int   itm     = Index[server.dbid][inum].table;
    int   pktype  = Tbl[server.dbid][itm].col_type[0];
    robj *ibtt    = lookupKey(db, ind);
    bt   *ibtr    = (bt *)(ibtt->ptr);
    aobj  acol    = getRawCol(rrow, cmatch, aopk, tmatch, NULL, 0);
    iRem(ibtr, &acol, aopk);
    if (pkup) iAdd(ibtr, &acol, anpk, pktype);
    else      iAdd(ibtr, ncol,  anpk, pktype);
    releaseAobj(&acol);
}
void updateIndex(redisDb *db,
                 aobj    *aopk,
                 aobj    *anpk,
                 aobj    *newval,
                 void    *rrow,
                 int      inum,
                 int      tmatch) {
    _updateIndex(db, aopk, anpk, newval, rrow, inum, 0, tmatch);
}
void updatePKIndex(redisDb *db,
                   aobj    *aopk,
                   aobj    *anpk,
                   void    *rrow,
                   int      inum,
                   int      tmatch) {
    _updateIndex(db, aopk, anpk, NULL, rrow, inum, 1, tmatch);
}

/* CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX */
/* CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX */
bool newIndexReply(redisClient *c,
                   sds          iname,
                   int          tmatch,
                   int          cmatch,
                   bool         virt,
                   d_l_t       *nrlind) {
    if (Num_indx[server.dbid] == MAX_NUM_INDICES) {
        addReply(c, shared.toomanyindices);
        return 0;
    }

    // commit index definition
    robj *ind    = createStringObject(iname, sdslen(iname));
    int   imatch = Num_indx[server.dbid];
    Index[server.dbid][imatch].obj     = ind;
    Index[server.dbid][imatch].table   = tmatch;
    Index[server.dbid][imatch].column  = cmatch;
    Index[server.dbid][imatch].type    = cmatch ?
        Tbl[server.dbid][tmatch].col_type[cmatch] : COL_TYPE_NONE;
    Index[server.dbid][imatch].virt    = virt;
    Index[server.dbid][imatch].nrl     = nrlind ? 1 : 0;

    robj *ibt;
    if (virt) {
        ibt         = createEmptyBtreeObject();
        Tbl[server.dbid][tmatch].virt_indx = imatch;
    } else if (Index[server.dbid][imatch].nrl) {
        nrlind->num = imatch;
        ibt         = createObject(REDIS_NRL_INDEX, nrlind);
    } else {
        int ctype   = Tbl[server.dbid][tmatch].col_type[cmatch];
        ibt         = createBtreeObject(ctype, imatch, BTREE_INDEX);
    }
    //store BtreeObject in HashTable key: indexname
    dictAdd(c->db->dict, ind, ibt);
    Num_indx[server.dbid]++;
    return 1;
}

static void makeIndexFromStream(bt    *btr,
                                uchar *stream,
                                bt    *ibtr,
                                int    icol,
                                int    itbl) {
    /* get the pk and the fk and then call iAdd() */
    aobj akey;
    convertStream2Key(stream, &akey, btr);
    void *rrow   = parseStream(stream, btr);
    aobj  acol   = getRawCol(rrow, icol, &akey, itbl, NULL, 0);
    uchar pktype = Tbl[server.dbid][itbl].col_type[0];
    iAdd(ibtr, &acol, &akey, pktype);
    releaseAobj(&acol);
}

/* CREATE INDEX on Table w/ Columns */
int buildIndex(bt *btr, bt_n *x, bt *ibtr, int icol, int itbl, bool nrl) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream = (uchar *)KEYS(btr, x, i);
        if (nrl) runNrlIndexFromStream(btr, stream, (d_l_t *)ibtr, itbl);
        else     makeIndexFromStream(  btr, stream, ibtr, icol,    itbl);
    }

    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            buildIndex(btr, NODES(btr, x)[i], ibtr, icol, itbl, nrl);
        }
    }
    return 0;
}

//TODO break out NRI_IndexCommit()
static void indexCommit(redisClient *c,
                        sds          iname,
                        char        *tname,
                        char        *cname,
                        bool         nrl,
                        char        *nrltbl,
                        char        *nrladd,
                        char        *nrldel) {
    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames); 
        return;
    }

    d_l_t *nrlind = NULL;
    int    cmatch = -1;
    int    tmatch = -1;
    if (!nrl) {
        tmatch  = find_table(tname);
        if (tmatch == -1) {
            addReply(c, shared.nonexistenttable);
            return;
        }
        cmatch = find_column(tmatch, cname);
        if (cmatch == -1) {
            addReply(c, shared.indextargetinvalid);
            return;
        }
        for (int i = 0; i < Num_indx[server.dbid]; i++) { /* already indxd? */
            if (Index[server.dbid][i].table == tmatch &&
                Index[server.dbid][i].column == cmatch) {
                addReply(c, shared.indexedalready);
                return;
            }
        }
    } else {
        tmatch = find_table(nrltbl);
        if (tmatch == -1) {
            addReply(c, shared.nonexistenttable);
            return;
        }

        nrlind = init_d_l_t(); /* freed in freeNrlIndexObject */
        if (!parseNRLcmd(nrladd, nrlind->l1, nrlind->l2, tmatch)) {
            addReply(c, shared.index_nonrel_decl_fmt);
            destroy_d_l_t(nrlind);
            return;
        }
    }

    robj *btt = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
    bt   *btr = (bt *)btt->ptr;
    if (nrlind && btr->numkeys > 0) {
        listNode *ln  = nrlind->l1->head;
        sds       cmd = ln->value;
        if (!strncasecmp(cmd, "DELETE", 6)) {
            addReply(c, shared.nrl_suicide);
            return;
        }
    }

    if (!newIndexReply(c, iname, tmatch, cmatch, 0, nrlind)) {
        if (nrlind) destroy_d_l_t(nrlind);
        return;
    }
    addReply(c, shared.ok);

    /* IF table has rows - loop thru and populate index */
    if (btr->numkeys > 0) {
        robj *ind  = Index[server.dbid][Num_indx[server.dbid] - 1].obj;
        robj *ibt  = lookupKey(c->db, ind);
        buildIndex(btr, btr->root, ibt->ptr, cmatch, tmatch, nrl);
    }
}

void createIndex(redisClient *c) {
    if (c->argc < 6) {
        addReply(c, shared.index_wrong_num_args);
        return;
    }

    char *token = c->argv[5]->ptr;
    char *end   = strchr(token, ')');
    if (!end || (*token != '(')) { /* NonRelationalIndex */
        char *nrldel  = (c->argc > 6) ? c->argv[6]->ptr : NULL;
        indexCommit(c, c->argv[2]->ptr, NULL, NULL,
                    1, c->argv[4]->ptr, c->argv[5]->ptr, nrldel);
    } else {
        sds cname = sdsnewlen(token + 1, end - token - 1);
        indexCommit(c, c->argv[2]->ptr, c->argv[4]->ptr, cname,
                    0, NULL, NULL, NULL);
        sdsfree(cname);
    }
}

void emptyIndex(redisDb *db, int inum) {
    robj *ind                       = Index[server.dbid][inum].obj;
    if (!ind) return;
    //TODO there is a bug in redis' dictDelete (need to update redis code)
    //int retval = 
    dictDelete(db->dict, ind);
    //assert(retval == DICT_OK);
    Index[server.dbid][inum].obj    = NULL;
    Index[server.dbid][inum].table  = -1;
    Index[server.dbid][inum].column = -1;
    Index[server.dbid][inum].type   =  0;
    Index[server.dbid][inum].virt   =  0;
    Index[server.dbid][inum].nrl    =  0;
    server.dirty++;
    //TODO shuffle indices to make space for deleted indices
}

void dropIndex(redisClient *c) {
    char *iname = c->argv[2]->ptr;
    int   inum  = match_index_name(iname);

    if (inum == -1) {
        addReply(c, shared.nullbulk);
        return;
    }
    if (Index[server.dbid][inum].virt) {
        addReply(c, shared.drop_virtual_index);
        return;
    }

    emptyIndex(c->db, inum);
    addReply(c, shared.cone);
}

/* CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP */
/* CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP CLEANUP */
static d_l_t *init_d_l_t() {
    d_l_t *nrlind = malloc(sizeof(d_l_t)); /* freed in freeNrlIndexObject */
    nrlind->l1    = listCreate();
    nrlind->l2    = listCreate();
    return nrlind;
}

static void destroy_d_l_t(d_l_t *nrlind) {
    listNode *ln;
    listIter *li = listGetIterator(nrlind->l1, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sdsfree(ln->value); /* free sds* from parseNRLcmd() */
    }
    listReleaseIterator(li);
    listRelease(nrlind->l1);
    listRelease(nrlind->l2);
    free(nrlind);
}

void freeNrlIndexObject(robj *o) {
    d_l_t    *nrlind = (d_l_t *)o->ptr;
    destroy_d_l_t(nrlind);
}

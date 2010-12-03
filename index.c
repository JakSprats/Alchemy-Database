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
#include "common.h"
#include "alsosql.h"
#include "rdb_alsosql.h"
#include "orderby.h"
#include "index.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;
extern struct redisServer server;

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
    char *nextp = strchr(curr_tname, '.');
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

/* NON_RELATIONAL_INDEX NON_RELATIONAL_INDEX NON_RELATIONAL_INDEX */
/* NON_RELATIONAL_INDEX NON_RELATIONAL_INDEX NON_RELATIONAL_INDEX */
sds genNRL_Cmd(d_l_t  *nrlind,
               robj   *pko,
               char   *vals,
               uint32  cofsts[],
               bool    from_insert,
               robj   *row,
               int     tmatch) {
        sds       cmd     = sdsempty();
        list     *nrltoks = nrlind->l1;
        list     *nrlcols = nrlind->l2;
        listIter *li1     = listGetIterator(nrltoks, AL_START_HEAD);
        listIter *li2     = listGetIterator(nrlcols, AL_START_HEAD);
        listNode *ln1     = listNext(li1);
        listNode *ln2     = listNext(li2);
        while (ln1 || ln2) {
            if (ln1) {
                sds token = ln1->value;
                cmd       = sdscatlen(cmd, token, sdslen(token));
            }
            int cmatch = -1;
            if (ln2) {
                cmatch = (int)(long)ln2->value;
                cmatch--; /* because (0 != NULL) */
            }
            if (cmatch != -1) {
                char *x;
                int   xlen;
                robj *col = NULL;
                if (from_insert) {
                    if (!cmatch) {
                        x    = pko->ptr;
                        xlen = sdslen(x);
                    } else {
                        x    = vals + cofsts[cmatch - 1];
                        xlen = cofsts[cmatch] - cofsts[cmatch - 1] - 1;
                    }
                } else {
                    col = createColObjFromRow(row, cmatch, pko, tmatch);
                    x    = col->ptr;
                    xlen = sdslen(col->ptr);
                }
                cmd = sdscatlen(cmd, x, xlen);
                if (col) decrRefCount(col);
            }
            ln1 = listNext(li1);
            ln2 = listNext(li2);
        }
    /*TODO destroy both listIter's */
    return cmd;
}

void runCmdInFakeClient(sds s) {
    //RL4 "runCmdInFakeClient: %s", s);
    char *end = strchr(s, ' ');
    if (!end) return;

    sds   *argv    = NULL; /* must come before first GOTO */
    int    a_arity = 0;
    sds cmd_name   = sdsnewlen(s, end - s);
    end++;
    struct redisCommand *cmd = lookupCommand(cmd_name);
    if (!cmd) goto run_cmd_err;
    int arity = abs(cmd->arity);

    char *args = NULL;
    if (arity > 2) {
        args = strchr(end, ' ');
        if (!args) goto run_cmd_err;
        else      args++;
    }

    argv                = malloc(sizeof(sds) * arity);
    argv[0]             = cmd_name;
    a_arity++;
    argv[1]             = args ? sdsnewlen(end, args - end - 1) :
                                 sdsnewlen(end, strlen(end)) ;
    a_arity++;
    if (arity == 3) {
        argv[2]         = sdsnewlen(args, strlen(args));
        a_arity++;
    } else if (arity > 3) {
        char *dlm       = strchr(args, ' ' );;
        if (!dlm) goto run_cmd_err;
        dlm++;
        argv[2]         = sdsnewlen(args, dlm - args - 1);
        a_arity++;
        if (arity == 4) {
            argv[3]     = sdsnewlen(dlm, strlen(dlm));
            a_arity++;
        } else { /* INSERT */
            char *vlist = strchr(dlm, ' ' );;
            if (!vlist) goto run_cmd_err;
            vlist++;
            argv[3]     = sdsnewlen(dlm, vlist - dlm - 1);
            a_arity++;
            argv[4]     = sdsnewlen(vlist, strlen(vlist));
            a_arity++;
        }
    }

    robj **rargv = malloc(sizeof(robj *) * arity);
    for (int j = 0; j < arity; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    redisClient *fc = rsql_createFakeClient();
    fc->argv        = rargv;
    fc->argc        = arity;
    call(fc, cmd);
    rsql_freeFakeClient(fc);
    free(rargv);

run_cmd_err:
    if (!a_arity) sdsfree(cmd_name);
    if (argv)     free(argv);
}

static void nrlIndexAdd(robj *o, robj *pko, char *vals, uint32 cofsts[]) {
    sds cmd = genNRL_Cmd(o->ptr, pko, vals, cofsts, 1, NULL, -1);
    runCmdInFakeClient(cmd);
    sdsfree(cmd);
    return;
}
/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
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

void addToIndex(redisDb *db, robj *pko, char *vals, uint32 cofsts[], int inum) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    robj *ind     = Index[server.dbid][inum].obj;
    robj *ibt     = lookupKey(db, ind);
    if (nrl) {
        nrlIndexAdd(ibt, pko, vals, cofsts);
        return;
    }
    bt   *ibtr    = (bt *)(ibt->ptr);
    int   i       = Index[server.dbid][inum].column;
    int   j       = i - 1;
    int   end     = cofsts[j];
    int   len     = cofsts[i] - end - 1;
    robj *col_key = createStringObject(vals + end, len); /* freeME */
    int   itm     = Index[server.dbid][inum].table;
    int   pktype  = Tbl[server.dbid][itm].col_type[0];

    iAdd(ibtr, col_key, pko, pktype);
    decrRefCount(col_key);
}

void delFromIndex(redisDb *db, robj *old_pk, robj *row, int inum, int tmatch) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    if (nrl) {
        RL4 "NRL delFromIndex");
        /* TODO add in nrldel */
        return;
    }
    robj *ind     = Index[server.dbid][inum].obj;
    int   cmatch  = Index[server.dbid][inum].column;
    robj *ibt     = lookupKey(db, ind);
    bt   *ibtr    = (bt *)(ibt->ptr);
    robj *old_val = createColObjFromRow(row, cmatch, old_pk, tmatch); /*freeME*/
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
                 int      inum,
                 uchar    pk_update,
                 int      tmatch) {
    if (Index[server.dbid][inum].virt) return;
    bool  nrl     = Index[server.dbid][inum].nrl;
    if (nrl) {
        RL4 "NRL updateIndex");
        /* TODO add in nrldel */
        return;
    }
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
void newIndex(redisClient *c,
              char        *iname,
              int          tmatch,
              int          cmatch,
              bool         virt,
              d_l_t       *nrlind) {
    // commit index definition
    robj *ind    = createStringObject(iname, strlen(iname));
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
        ibt                   = createEmptyBtreeObject();
        Tbl[server.dbid][tmatch].virt_indx = imatch;
    } else if (Index[server.dbid][imatch].nrl) {
        nrlind->num = imatch;
        ibt = createObject(REDIS_NRL_INDEX, nrlind);
    } else {
        int ctype = Tbl[server.dbid][tmatch].col_type[cmatch];
        ibt       = createBtreeObject(ctype, imatch, BTREE_INDEX);
    }
    //store BtreeObject in HashTable key: indexname
    dictAdd(c->db->dict, ind, ibt);
    Num_indx[server.dbid]++;
}

static bool parseNRLcmd(char *o_s,
                        list *nrltoks,
                        list *nrlcols,
                        int   tmatch) {
    char *s   = strchr(o_s, '$');
    if (!s) {
       listAddNodeTail(nrltoks, sdsdup(o_s)); /* freed in freeNrlIndexObject */
    } else {
        while (1) {
            s++; /* advance past "$" */
            char *nxo = s;
            while (isalnum(*nxo) || *nxo == '_') nxo++; /* col must be alpnum */
            char *nexts = strchr(s, '$');               /* var is '$' delimed */

            int cmatch = -1;
            if (nxo) cmatch = find_column_n(tmatch, s, nxo - s);
            else     cmatch = find_column(tmatch, s);
            if (cmatch == -1) return 0;
            listAddNodeTail(nrlcols, (void *)(long)(cmatch + 1)); /* 0!=NULL */

            listAddNodeTail(nrltoks, sdsnewlen(o_s, (s - 1) - o_s)); /*no "$"*/
            if (!nexts) { /* no more vars */
                if (*nxo) listAddNodeTail(nrltoks, sdsnewlen(nxo, strlen(nxo)));
                break;
            }
            o_s = nxo;
            s   = nexts;
        }
    }
    return 1;
}

sds rebuildOrigNRLcmd(robj *o) {
    d_l_t    *nrlind  = o->ptr;
    int       tmatch  = Index[server.dbid][nrlind->num].table;

    list     *nrltoks = nrlind->l1;
    list     *nrlcols = nrlind->l2;
    listIter *li1     = listGetIterator(nrltoks, AL_START_HEAD);
    listNode *ln1     = listNext(li1);
    listIter *li2     = listGetIterator(nrlcols, AL_START_HEAD);
    listNode *ln2     = listNext(li2);
    sds       cmd     = sdsnewlen("\"", 1); /* has to be one arg */
    while (ln1 || ln2) {
        if (ln1) { 
            sds token  = ln1->value;
            cmd        = sdscatlen(cmd, token, sdslen(token));
            ln1 = listNext(li1);
        }
        if (ln2) {
            int cmatch = (int)(long)ln2->value;
            cmatch--; /* because (0 != NULL) */
            sds cname  = Tbl[server.dbid][tmatch].col_name[cmatch]->ptr;
            cmd        = sdscatlen(cmd, "$", 1); /* "$" variable delim */
            cmd        = sdscatlen(cmd, cname, sdslen(cname));
            ln2 = listNext(li2);
        }
    }
    /*TODO destroy both listIter's */
    cmd = sdscatlen(cmd, "\"", 1); /* has to be one arg */
    return cmd;
}

static void indexCommit(redisClient *c,
                        char        *iname,
                        char        *trgt,
                        bool        nrl,
                        char       *nrltbl,
                        char       *nrladd,
                        char       *nrldel,
                        bool        build) {
    if (Num_indx[server.dbid] >= MAX_NUM_INDICES) {
        addReply(c, shared.toomanyindices);
        return;
    }

    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames); 
        return;
    }

    sds    o_target = NULL; /* must come before first GOTO */
    d_l_t *nrlind   = NULL;
    int    cmatch   = - 1;
    int    tmatch   = - 1;

    if (!nrl) {
        // parse tablename.columnname
        o_target     = sdsdup(trgt);
        int   len    = sdslen(o_target);
        char *target = o_target;
        if (target[len - 1] == ')') target[len - 1] = '\0';
        if (*target         == '(') target++;
        char *column = strchr(target, '.');
        if (!column) {
            addReply(c, shared.indextargetinvalid);
            goto ind_commit_err;
        }
        *column = '\0';
        column++;
        tmatch  = find_table(target);
        if (tmatch == -1) {
            addReply(c, shared.nonexistenttable);
            goto ind_commit_err;
        }
    
        cmatch = find_column(tmatch, column);
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
    } else {
        tmatch = find_table(nrltbl);
        if (tmatch == -1) {
            addReply(c, shared.nonexistenttable);
            goto ind_commit_err;
        }

        nrlind = malloc(sizeof(d_l_t)); /* freed in freeNrlIndexObject */
        nrlind->l1 = listCreate();
        nrlind->l2 = listCreate();
        if (!parseNRLcmd(nrladd, nrlind->l1, nrlind->l2, tmatch)) {
            addReply(c, shared.index_nonrel_decl_fmt);
            free(nrlind);
            goto ind_commit_err;
        }
    }

    newIndex(c, iname, tmatch, cmatch, 0, nrlind);
    addReply(c, shared.ok);

    if (build) {
        /* IF table has rows - loop thru and populate index */
        robj *o   = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
        bt   *btr = (bt *)o->ptr;
        if (btr->numkeys > 0) {
            robj *ind  = Index[server.dbid][Num_indx[server.dbid] - 1].obj;
            robj *ibt  = lookupKey(c->db, ind);
            buildIndex(btr, btr->root, ibt->ptr, cmatch, tmatch, nrl);
        }
    }

ind_commit_err:
    if (o_target) sdsfree(o_target);
}

void createIndex(redisClient *c) {
    if (c->argc < 6) {
        addReply(c, shared.index_wrong_num_args);
        return;
    }

    char *nrldel = NULL;

    if (*((char *)c->argv[5]->ptr) != '(') { /* legacyIndex */
        nrldel  = (c->argc > 6) ? c->argv[6]->ptr : NULL;
        indexCommit(c, c->argv[2]->ptr, NULL, 1,
                    c->argv[4]->ptr, c->argv[5]->ptr, nrldel, 1);
    } else {
        /* TODO lazy programming, change legacyIndex syntax */
        /* NOTE: no free needed for sdstrim() */
        sds leg_col      = sdstrim(sdsdup(c->argv[5]->ptr), "()");
        sds leg_ind_sntx = sdscatprintf(sdsempty(), "%s.%s",
                                        (char *)c->argv[4]->ptr,
                                        (char *)leg_col);
        indexCommit(c, c->argv[2]->ptr, leg_ind_sntx, 0, NULL, NULL, NULL, 1);
        sdsfree(leg_ind_sntx);
        sdsfree(leg_col);
    }
}

void legacyIndexCommand(redisClient *c) {
    bool nrl     = 0;
    char *trgt   = NULL;
    char *nrltbl = NULL;
    char *nrladd = NULL;
    char *nrldel = NULL;
    if (c->argc > 3) {
        nrl = 1;
        nrltbl = c->argv[2]->ptr;
        nrladd = (c->argc > 3) ? c->argv[3]->ptr : NULL;
        nrldel = (c->argc > 4) ? c->argv[4]->ptr : NULL;
    } else {
        trgt = c->argv[2]->ptr;
    }
    /* the final argument means -> if(nrl) dont build index */
    indexCommit(c, c->argv[1]->ptr, trgt, nrl, nrltbl, nrladd, nrldel, !nrl);
}


void emptyIndex(redisDb *db, int inum) {
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


/* RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS */
/* RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS RANGE_OPS */

#define ISELECT_OPERATION(Q)                                            \
    if (!cstar) {                                                       \
        robj *r = outputRow(row, qcols, cmatchs, key, tmatch, 0);       \
        if (Q) addORowToRQList(ll, r, row, w->obc, key, tmatch, ctype); \
        else   addReplyBulk(c, r);                                      \
        decrRefCount(r);                                                \
    }

void iselectAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch,
                   int          cmatchs[MAX_COLUMN_PER_TABLE],
                   int          qcols,
                   bool         cstar) {
    list *ll    = NULL;
    uchar ctype = COL_TYPE_NONE;
    if (w->obc != -1) {
        ll    = listCreate();
        ctype = Tbl[server.dbid][tmatch].col_type[w->obc];
    }

    bool     qed = 0;
    btSIter *bi  = NULL;
    btSIter *nbi = NULL;
    LEN_OBJ
    if (w->low) { /* RANGE QUERY */
        RANGE_QUERY_LOOKUP_START
            ISELECT_OPERATION(q_pk)
        RANGE_QUERY_LOOKUP_MIDDLE
                ISELECT_OPERATION(q_fk)
        RANGE_QUERY_LOOKUP_END
    } else {    /* IN () QUERY */
        IN_QUERY_LOOKUP_START
            ISELECT_OPERATION(q_pk)
        IN_QUERY_LOOKUP_MIDDLE
                ISELECT_OPERATION(q_fk)
        IN_QUERY_LOOKUP_END
    }

    int sent = 0;
    if (qed && card) {
        obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
        for (int k = 0; k < (int)listLength(ll); k++) {
            if (w->lim != -1 && sent == w->lim) break;
            if (w->ofst > 0) {
                w->ofst--;
            } else {
                sent++;
                obsl_t *ob = vector[k];
                addReplyBulk(c, ob->row);
            }
        }
        sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
        free(vector);
    }
    if (ll) listRelease(ll);

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    if (cstar) {
        lenobj->ptr = sdscatprintf(sdsempty(), ":%lu\r\n", card);
    } else {
        lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
    }
}

static void addPKtoRQList(list *ll,
                          robj *pko,
                          robj *row,
                          int   obc,
                          int   tmatch,
                          bool  ctype) {
    addORowToRQList(ll, pko, row, obc, pko, tmatch, ctype);
}

#define BUILD_RQ_OPERATION(Q)                                    \
    if (Q) {                                                     \
        addPKtoRQList(ll, key, row, w->obc, tmatch, ctype);      \
    } else {                                                     \
        robj *cln  = cloneRobj(key); /* clone orig is BtRobj */  \
        listAddNodeTail(ll, cln);                                \
    }


#define BUILD_RANGE_QUERY_LIST                                                \
    list *ll    = listCreate();                                               \
    uchar ctype = COL_TYPE_NONE;                                              \
    if (w->obc != -1) {                                                       \
        ctype = Tbl[server.dbid][tmatch].col_type[w->obc];                    \
    }                                                                         \
                                                                              \
    bool     cstar = 0;                                                       \
    bool     qed   = 0;                                                       \
    ulong    card  = 0;                                                       \
    btSIter *bi    = NULL;                                                    \
    btSIter *nbi   = NULL;                                                    \
    if (w->low) { /* RANGE QUERY */                                           \
        RANGE_QUERY_LOOKUP_START                                              \
            BUILD_RQ_OPERATION(q_pk)                                          \
        RANGE_QUERY_LOOKUP_MIDDLE                                             \
                BUILD_RQ_OPERATION(q_fk)                                      \
        RANGE_QUERY_LOOKUP_END                                                \
    } else {    /* IN () QUERY */                                             \
        IN_QUERY_LOOKUP_START                                                 \
            BUILD_RQ_OPERATION(q_pk)                                          \
        IN_QUERY_LOOKUP_MIDDLE                                                \
                BUILD_RQ_OPERATION(q_fk)                                      \
        IN_QUERY_LOOKUP_END                                                   \
    }

void ideleteAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch) {
    BUILD_RANGE_QUERY_LIST

    MATCH_INDICES(tmatch)

    int sent = 0;
    if (card) {
        if (qed) {
            obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = vector[k];
                    robj *nkey = ob->row;
                    deleteRow(c, tmatch, nkey, matches, indices);
                }
            }
            sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
            free(vector);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                deleteRow(c, tmatch, nkey, matches, indices);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
}

void iupdateAction(redisClient *c,
                   cswc_t      *w,
                   int          tmatch,
                   int          ncols,
                   int          matches,
                   int          indices[],
                   char        *vals[],
                   uint32       vlens[],
                   uchar        cmiss[]) {
    BUILD_RANGE_QUERY_LIST

    bool pktype = Tbl[server.dbid][tmatch].col_type[0];
    int  sent   = 0;
    if (card) {
        robj *o = lookupKeyRead(c->db, Tbl[server.dbid][tmatch].name);
        if (qed) {
            obsl_t **vector = sortOrderByToVector(ll, ctype, w->asc);
            for (int k = 0; k < (int)listLength(ll); k++) {
                if (w->lim != -1 && sent == w->lim) break;
                if (w->ofst > 0) {
                    w->ofst--;
                } else {
                    sent++;
                    obsl_t *ob = vector[k];
                    robj *nkey = ob->row;
                    robj *row  = btFindVal(o, nkey, pktype);
                    updateRow(c, o, nkey, row, tmatch, ncols,
                              matches, indices, vals, vlens, cmiss);
                }
            }
            sortedOrderByCleanup(vector, listLength(ll), ctype, 1);
            free(vector);
        } else {
            listNode  *ln;
            listIter  *li = listGetIterator(ll, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                robj *row  = btFindVal(o, nkey, pktype);
                updateRow(c, o, nkey, row, tmatch, ncols, matches, indices,
                          vals, vlens, cmiss);
                decrRefCount(nkey); /* from cloneRobj in BUILD_RQ_OPERATION */
            }
            listReleaseIterator(li);
        }
    }

    if (w->lim != -1 && (uint32)sent < card) card = sent;
    addReplyLongLong(c, card);

    listRelease(ll);
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

    bool bdum;
    int  cmatchs[MAX_COLUMN_PER_TABLE];
    int  qcols = 0;
    parseCommaSpaceListReply(c, "*", 1, 0, 0, tmatch, cmatchs,
                             0, NULL, NULL, NULL, &qcols, &bdum);
    bt   *btr   = (bt *)o->ptr;
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
            snprintf(buf, 191, "DROP TABLE IF EXISTS `%s`;", m_tname);
            buf[191] = '\0';
            ADD_REPLY_BULK(r, buf)
            snprintf(buf, 191, "CREATE TABLE `%s` ( ", m_tname);
            buf[191] = '\0';
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
            snprintf(buf, 191, "LOCK TABLES `%s` WRITE;", m_tname);
            buf[191] = '\0';
            ADD_REPLY_BULK(r, buf)
        } else if (!strcasecmp(c->argv[2]->ptr, "RETURN") &&
                   !strcasecmp(c->argv[3]->ptr, "SIZE")      ) {
            ret_size = 1;
            snprintf(buf, 191, "KEYS: %d BT-DATA: %lld BT-MALLOC: %lld",
                          btr->numkeys, btr->data_size, btr->malloc_size);
            buf[191] = '\0';
            robj *r = createStringObject(buf, strlen(buf));
            addReplyBulk(c, r);
            decrRefCount(r);
            card++;
        }
    }

    if (btr->numkeys) {
        btEntry    *be;
        btSIter *bi = btGetFullRangeIterator(o, 0, 1);
        while ((be = btRangeNext(bi, 0)) != NULL) {      // iterate btree
            robj *pko = be->key;
            robj *row = be->val;
            robj *r   = outputRow(row, qcols, cmatchs, pko, tmatch, to_mysql);
            if (!to_mysql) {
                addReplyBulk(c, r);
                decrRefCount(r);
            } else {
                snprintf(buf, 191, "INSERT INTO `%s` VALUES (", m_tname);
                buf[191] = '\0';
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
        snprintf(buf, 191, "UNLOCK TABLES;");
        buf[191] = '\0'; /* not necessary, rule -> no sprintf's */
        ADD_REPLY_BULK(r, buf)
    }
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}
 
ull get_sum_all_index_size_for_table(redisClient *c, int tmatch) {
    ull isize = 0;
    for (int i = 0; i < Num_indx[server.dbid]; i++) {
        if (!Index[server.dbid][i].virt &&
             Index[server.dbid][i].table == tmatch) {
            robj *ind   = Index[server.dbid][i].obj;
            robj *ibt   = lookupKey(c->db, ind);
            if (ibt->type != REDIS_NRL_INDEX) { /*TODO: desc info on NRL inds */
                bt   *ibtr  = (bt *)(ibt->ptr);
                isize      += ibtr->malloc_size;
            }
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
        snprintf(buf, 255, "INFO: KEYS: [NUM: %d MIN: %s MAX: %s]"\
                          " BYTES: [BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
                btr->numkeys, (char *)minkey.ptr, (char *)maxkey.ptr,
                btr->data_size, btr->malloc_size, index_size);
        buf[255] = '\0';
    } else {
        snprintf(buf, 255, "INFO: KEYS: [NUM: %d MIN: %u MAX: %u]"\
                          " BYTES: [BT-DATA: %lld BT-TOTAL: %lld INDEX: %lld]",
            btr->numkeys, (uint32)(long)minkey.ptr, (uint32)(long)maxkey.ptr,
            btr->data_size, btr->malloc_size, index_size);
        buf[255] = '\0';
    }
    robj *r = createStringObject(buf, strlen(buf));
    addReplyBulk(c, r);
    decrRefCount(r);
    card++;
    lenobj->ptr = sdscatprintf(sdsempty(), "*%lu\r\n", card);
}

void freeNrlIndexObject(robj *o) {
    listNode *ln;
    d_l_t    *nrlind = (d_l_t *)o->ptr;
    listIter *li     = listGetIterator(nrlind->l1, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sdsfree(ln->value); /* free sds* from parseNRLcmd() */
    }
    listRelease(nrlind->l1);
    listRelease(nrlind->l2);
    free(nrlind);
}

#if 0
/* LEGACY CODE - the ROOTS */
void iselectCommand(redisClient *c) {
    int imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    int tmatch = Index[server.dbid][imatch].table;

    iselectAction(c, c->argv[2], tmatch, imatch, c->argv[3]->ptr);
}

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

void ideleteCommand(redisClient *c) {
    int   imatch = checkIndexedColumnOrReply(c, c->argv[1]->ptr);
    if (imatch == -1) return;
    int   tmatch = Index[server.dbid][imatch].table;
    ideleteAction(c, c->argv[2]->ptr, tmatch, imatch);
}
#endif

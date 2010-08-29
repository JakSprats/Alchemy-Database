/*
 * Implements istore and iselect
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

#include "alsosql.h"
#include "redis.h"
#include "bt.h"
#include "bt_iterator.h"
#include "row.h"
#include "index.h"
#include "zmalloc.h"
#include "common.h"
#include "store.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

// GLOBALS
extern int Num_tbls;

extern char  CCOMMA;
extern char  CEQUALS;
extern char  CMINUS;
extern char *EQUALS;
extern char *COLON;
extern char *COMMA;
extern char *PERIOD;

extern char *Col_type_defs[];

extern robj          *Tbl_name     [MAX_NUM_TABLES];
extern int            Tbl_col_count[MAX_NUM_TABLES];
extern robj          *Tbl_col_name [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];
extern unsigned char  Tbl_col_type [MAX_NUM_TABLES][MAX_COLUMN_PER_TABLE];

extern robj          *Index_obj     [MAX_NUM_INDICES];
extern bool           Index_virt    [MAX_NUM_INDICES];

#define MAX_TBL_DEF_SIZE     1024

stor_cmd StorageCommands[NUM_STORAGE_TYPES];

// STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS
// STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS STORE_COMMANDS

void legacyInsertCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    int ncols = Tbl_col_count[tmatch];
    MATCH_INDICES(tmatch)

    char *vals   = c->argv[2]->ptr;
    insertCommitReply(c, vals, ncols, tmatch, matches, indices);
}

void legacyTableCommand(redisClient *c) {
    if (Num_tbls >= MAX_NUM_TABLES) {
        addReply(c,shared.toomanytables);
        return;
    }

    char *tname = c->argv[1]->ptr;
    if (find_table(tname) != -1) {
        addReply(c,shared.nonuniquetablenames);
        return;
    }

    // parse column definitions
    char  col_names[MAX_COLUMN_PER_TABLE][MAX_COLUMN_NAME_SIZE];
    char *cname     = c->argv[2]->ptr;
    int   col_count = 0;
    while (1) {
        char *type  = strchr(cname, CEQUALS);
        char *nextc = strchr(cname, CCOMMA);
        if (type) {
            *type = '\0';
            type++;
        } else {
            addReply(c,shared.missingcolumntype);
            return;
        }
        if (nextc) {
            *nextc = '\0';
            nextc++;
        }
        unsigned char miss = 1;
        for (unsigned char j = 0; j < NUM_COL_TYPES; j++) {
            if (!strcmp(type, Col_type_defs[j])) {
                Tbl_col_type[Num_tbls][col_count] = j;
                miss = 0;
                break;
            }
        }
        if (miss) {
            addReply(c,shared.undefinedcolumntype);
            return;
        }
        if (strlen(cname) >= MAX_COLUMN_NAME_SIZE) {
            addReply(c,shared.columnnametoobig);
            return;
        }
        strcpy(col_names[col_count], cname);
        col_count++;
        if (!nextc) {
            break;
        } else if (col_count == MAX_COLUMN_PER_TABLE) {
            addReply(c,shared.toomanycolumns);
            return;
        }
        cname = nextc;
    }
    createTableCommitReply(c, col_names, col_count, tname);
}

unsigned char respOk(redisClient *c) {
    listNode *ln = listFirst(c->reply);
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    if (!strcmp(s, shared.ok->ptr)) return 1;
    else                            return 0;
}

static unsigned char respNotErr(redisClient *c) {
    listNode *ln = listFirst(c->reply);
    robj     *o  = ln->value;
    char     *s  = o->ptr;
    if (!strncmp(s, "-ERR ", 5)) return 0;
    else                         return 1;
}

static void cpyColDef(char *cdefs,
                      int  *slot,
                      int   tmatch,
                      int   cmatch,
                      int   qcols,
                      int   loop,
                      bool  has_conflicts,
                      bool  cname_cflix[]) {
    robj *col = Tbl_col_name[tmatch][cmatch];
    if (has_conflicts && cname_cflix[loop]) { // prepend tbl_name
        robj *tbl = Tbl_name[tmatch];
        memcpy(cdefs + *slot, tbl->ptr, sdslen(tbl->ptr));  
        *slot        += sdslen(tbl->ptr);        // tblname
        memcpy(cdefs + *slot, PERIOD, 1);
        *slot = *slot + 1;
    }
    memcpy(cdefs + *slot, col->ptr, sdslen(col->ptr));  
    *slot        += sdslen(col->ptr);            // colname
    memcpy(cdefs + *slot, EQUALS, 1);
    *slot = *slot + 1;
    char *ctype   = Col_type_defs[Tbl_col_type[tmatch][cmatch]];
    int   ctlen   = strlen(ctype);               // [INT,STRING]
    memcpy(cdefs + *slot, ctype, ctlen);
    *slot        += ctlen;
    if (loop != (qcols - 1)) {
        memcpy(cdefs + *slot, COMMA, 1);
        *slot = *slot + 1;                       // ,
    }
}

static bool _internalCreateTable(redisClient *c,
                                 redisClient *fc,
                                 int          qcols,
                                 int          cmatchs[],
                                 int          tmatch,
                                 int          j_tbls[],
                                 int          j_cols[],
                                 bool         cname_cflix[]) {
    if (find_table(c->argv[2]->ptr) > 0) return 1;

    char cdefs[MAX_TBL_DEF_SIZE];
    int  slot  = 0;
    for (int i = 0; i < qcols; i++) {
        if (tmatch != -1) {
            cpyColDef(cdefs, &slot, tmatch, cmatchs[i], qcols, i, 
                      0, cname_cflix);
        } else {
            cpyColDef(cdefs, &slot, j_tbls[i], j_cols[i], qcols, i,
                      1, cname_cflix);
        }
    }
    fc->argc    = 3;
    fc->argv[2] = createStringObject(cdefs, slot);
    legacyTableCommand(fc);
    if (!respOk(fc)) {
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    return 1;
}

bool internalCreateTable(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          cmatchs[],
                         int          tmatch) {
    int  idum[1];
    bool bdum[1];
    return _internalCreateTable(c, fc, qcols, cmatchs, tmatch,
                                idum, idum, bdum);
}

bool createTableFromJoin(redisClient *c,
                         redisClient *fc,
                         int          qcols,
                         int          j_tbls [],
                         int          j_cols[]) {
    bool cname_cflix[MAX_JOIN_INDXS];
    for (int i = 0; i < qcols; i++) {
        for (int j = 0; j < qcols; j++) {
            if (i == j) continue;
            if (!strcmp(Tbl_col_name[j_tbls[i]][j_cols[i]]->ptr,
                        Tbl_col_name[j_tbls[j]][j_cols[j]]->ptr)) {
                cname_cflix[i] = 1;
                break;
            } else {
                cname_cflix[i] = 0;
            }
        }
    }

    int idum[1];
    return _internalCreateTable(c, fc, qcols, idum, -1,
                                j_tbls, j_cols, cname_cflix);
}

bool performStoreCmdOrReply(redisClient *c, redisClient *fc, int sto) {
    (*StorageCommands[sto].func)(fc);
    if (!respNotErr(fc)) {
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    return 1;
}

static bool istoreAction(redisClient *c,
                         redisClient *fc,
                         int tmatch,
                         int cmatchs[],
                         int qcols,
                         int sto,
                         robj *pko,
                         robj *row,
                         robj *newname) {
    aobj  cols[MAX_COLUMN_PER_TABLE];
    int   totlen = 0;
    for (int i = 0; i < qcols; i++) {
        cols[i]  = getColStr(row, cmatchs[i], pko, tmatch);
        totlen  += cols[i].len;
    }

    char *newrow = NULL;
    int   rowlen = 0;
    fc->argc     = qcols + 1;
    //argv[0] NOT NEEDED
    if (StorageCommands[sto].argc) { // not INSERT
        robj **argv = fc->argv;
        if (StorageCommands[sto].argc < 0) { // pk =argv[1]:cols[0]
            char *pre    = c->argv[2]->ptr;
            argv[1]      = createStringObject(pre, sdslen(pre));
            argv[1]->ptr = sdscatlen(argv[1]->ptr, COLON,   1);
            argv[1]->ptr = sdscatlen(argv[1]->ptr, cols[0].s, cols[0].len);
            argv[2]      = createStringObject(cols[1].s, cols[1].len);
            if (StorageCommands[sto].argc == -3) {
                argv[3]  = createStringObject(cols[2].s, cols[2].len);
            }
        } else {
            argv[2] = createStringObject(cols[0].s, cols[0].len);
            if (StorageCommands[sto].argc == 2) {
                argv[3] = createStringObject(cols[1].s, cols[1].len);
            }
        }
        for (int i = 0; i < qcols; i++) {
            if (cols[i].sixbit) free(cols[i].s);
        }
    } else {                         // INSERT
        fc->argv[1] = newname;
        //TODO this can be done simpler w/ sdscatlen()
        int len = totlen + qcols -1;
        if (len > rowlen) {
            rowlen = len;
            if (newrow) newrow = zrealloc(newrow, rowlen); /* sds */
            else        newrow = zmalloc(         rowlen); /* sds */
        }
        int slot = 0;
        for (int i = 0; i < qcols; i++) {
            memcpy(newrow + slot, cols[i].s, cols[i].len);
            slot += cols[i].len;
            if (i != (qcols - 1)) {
                memcpy(newrow + slot, COMMA, 1);
                slot++;
            }
            if (cols[i].sixbit) free(cols[i].s);
        }
        fc->argv[2] = createStringObject(newrow, len);
    }

    if (newrow) zfree(newrow);
    return performStoreCmdOrReply(c, fc, sto);
}

void istoreCommit(redisClient *c,
                  int          tmatch,
                  int          imatch,
                  char        *sto_type,
                  char        *col_list,
                  char        *range,
                  robj        *newname) {
    int sto;
    CHECK_STORE_TYPE_OR_REPLY(sto_type,sto,)
    int cmatchs[MAX_COLUMN_PER_TABLE];
    int qcols = parseColListOrReply(c, tmatch, col_list, cmatchs);
    if (!qcols) {
        addReply(c, shared.nullbulk);
        return;
    }
    if (StorageCommands[sto].argc &&
        abs(StorageCommands[sto].argc) != qcols) {
        addReply(c, shared.storagenumargsmismatch);
        return;
    }
    RANGE_CHECK_OR_REPLY(range)
    robj *o = lookupKeyRead(c->db, Tbl_name[tmatch]);

    robj               *argv[STORAGE_MAX_ARGC + 1];
    struct redisClient *fc = createFakeClient();
    fc->argv               = argv;
    fc->argv[1]            = newname; /* the NEW Stored Objects NAME */
    if (!StorageCommands[sto].argc) { // create table first if needed
        if (!internalCreateTable(c, fc, qcols, cmatchs, tmatch)) {
            freeFakeClient(fc);
            decrRefCount(low);
            decrRefCount(high);
            return;
        }
    }

    btEntry      *be, *nbe;
    unsigned int  stored = 0;
    robj         *ind    = Index_obj [imatch];
    bool          virt   = Index_virt[imatch];
    robj         *bt     = virt ? o : lookupKey(c->db, ind);
    btIterator   *bi     = btGetRangeIterator(bt, low, high, virt);
    while ((be = btRangeNext(bi, 1)) != NULL) {                // iterate btree
        if (virt) {
            robj *pko = be->key;
            robj *row = be->val;
            if (!istoreAction(c, fc, tmatch, cmatchs, qcols, sto,
                              pko, row, newname))
                goto istore_err;
            stored++;
        } else {
            robj       *val = be->val;
            btIterator *nbi = btGetFullRangeIterator(val, 0, 0);
            while ((nbe = btRangeNext(nbi, 1)) != NULL) {     // iterate NodeBT
                robj *pko = nbe->key;
                robj *row = btFindVal(o, pko, Tbl_col_type[tmatch][0]);
                if (!istoreAction(c, fc, tmatch, cmatchs, qcols, sto,
                                  pko, row, newname)) {
                    btReleaseRangeIterator(nbi);
                    goto istore_err;
                }
                stored++;
            }
            btReleaseRangeIterator(nbi);
        }
    }

istore_err:
    btReleaseRangeIterator(bi);
    decrRefCount(low);
    decrRefCount(high);
    freeFakeClient(fc);

    addReplyLongLong(c, stored);
}

#if 0
void istoreCommand(redisClient *c) {
    int imatch = checkIndexedColumnOrReply(c, c->argv[3]->ptr);
    if (imatch == -1) return;
    int tmatch = Index_on_table[imatch];
    istoreCommit(c, tmatch, imatch, c->argv[1]->ptr, c->argv[5]->ptr,
                 c->argv[4]->ptr, c->argv[2]);
}
#endif

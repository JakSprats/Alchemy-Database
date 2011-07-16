/*
 * This file implements the indexing logic of Alsosql
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
#include "lua_integration.h"
#include "colparse.h"
#include "stream.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "index.h"

// GLOBALS
extern char    *Col_type_defs[];
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern ulong    CurrCard;

int     Num_indx;
r_ind_t Index[MAX_NUM_INDICES];

/* HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS HELPER_COMMANDS */
int find_index(int tmatch, int cmatch) {
    for (int i = 0; i < Num_indx; i++) {
        r_ind_t *ri = &Index[i];
        if (ri->obj && ri->table  == tmatch && ri->column == cmatch) return i;
    }
    return -1;
}
int find_next_index(int tmatch, int cmatch, int imatch) {
    for (int i = imatch + 1; i < Num_indx; i++) {
        r_ind_t *ri = &Index[i];
        if (ri->obj && ri->table  == tmatch && ri->column == cmatch) return i;
    }
    return -1;
}
int match_index(int tmatch, int inds[]) {
    int matches =  0;
    int uniq    = -1;
    for (int i = 0; i < Num_indx; i++) {
        r_ind_t *ri = &Index[i];
        if (ri->obj && ri->table == tmatch) { /* NOT deleted and Tmatch */
            if UNIQ(ri->cnstr) uniq = matches;
            inds[matches] = i; matches++;
        }
    }
    if (uniq != -1) { /* UNIQUE MCI must be first as it can FAIL */
        int imatch0 = inds[0]; inds[0] = inds[uniq]; inds[uniq] = imatch0;
    }
    return matches;
}
int match_index_name(char *iname) {
    for (int i = 0; i < Num_indx; i++) {
        if (Index[i].obj) {
            if (!strcmp(iname, (char *)Index[i].obj->ptr)) {
                return i;
            }
        }
    }
    return -1;
}

/* INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE INDEX_MAINTENANCE */
#define DEBUG_IADDMCI_UNIQ \
  aobj c_fcol   = getCol(btr, rrow, ri->bclist[final], apk, ri->table); \
  printf("fcol: "); dumpAobj(printf, &c_fcol);                          \
  printf("nbtr\n"); bt_dumptree(printf, nbtr, 0);

static void replyUniqConstrViol(cli *c) {
    sds resp = sdscatprintf(sdsempty(),
               "-ERR VIOLATION: UNIQUE INDEX CONSTRAINT - " \
               "FAILED AFTER %ld SUCCESSFUL ROWS\r\n", CurrCard);
    addReplySds(c, resp);
}
typedef struct delete_pair_t { /* used in iRemMCI() */
    bt   *ibtr;
    bt   *nbtr;
    aobj  acol;
} dp_t;
static dp_t init_dp(bt *ibtr, aobj *acol, bt *nbtr) {
    dp_t dp;
    dp.ibtr = ibtr;
    dp.acol = *acol; /* NOTE original acol should not be released/destroyed */
    dp.nbtr = nbtr;
    return dp;
}

static void iAdd(bt *ibtr, aobj *acol, aobj *apk, uchar pktyp) {
    bt *nbtr = btIndFind(ibtr, acol);
    if (!nbtr) {
        nbtr         = createIndexNode(pktyp);
        btIndAdd(ibtr, acol, nbtr);
        ibtr->msize += nbtr->msize;       /* ibtr inherits nbtr */
    }
    ulong size1  = nbtr->msize;
    btIndNodeAdd(nbtr, apk);
    ibtr->msize += (nbtr->msize - size1); /* ibtr inherits nbtr */
}
void destroy_index(bt *ibtr, bt_n *n) {
    for (int i = 0; i < n->n; i++) {
        void *be   = KEYS(ibtr, n, i);
        bt   *nbtr = (bt *)parseStream(be, ibtr);
        if (nbtr) bt_destroy(nbtr);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            destroy_index(ibtr, NODES(ibtr, n)[i]);
        }
    }
}
static bool iRem(bt *ibtr, aobj *acol, aobj *apk) {
    bt  *nbtr    = btIndFind(ibtr, acol);
    ulong  size1 = nbtr->msize;
    int  nkeys   = btIndNodeDelete(nbtr, apk);
    ibtr->msize -= (size1 - nbtr->msize);
    if (!nkeys) {
        btIndDelete(ibtr, acol);
        ibtr->msize -=  nbtr->msize;
        bt_destroy(nbtr);
        return 1;
    }
    return 0;
}
static bool _iAddMCI(cli *c,      bt   *btr,  aobj *apk,     uchar  pktyp,
                     int  imatch, void *rrow, bool  destroy, int    rec_ret) {
    bt      *nbtr  = NULL;                 /* compiler warning */
    r_ind_t *ri    = &Index[imatch];
    dp_t     dpl[ri->nclist];
    bt      *ibl[ri->nclist];
    int      trgr  = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
    int      final = ri->nclist - 1;
    int      depth = UNIQ(ri->cnstr) ? ri->nclist - 1 : ri->nclist;
    bt      *ibtr  = getIBtr(imatch);      /* get MCI */
    int      ndstr = 0;
    bool     ret   = 1; /* assume success */
    for (int i = 0; i < depth; i++) {
        ibl[i]     = ibtr;
        aobj acol  = getCol(btr, rrow, ri->bclist[i], apk, ri->table);
        if (acol.empty) goto i_add_mci_err;
        nbtr       = btIndFind(ibtr, &acol);
        if (!nbtr) {
            if (i == final) {                     /* final  MID -> NODE*/
                nbtr = createIndexNode(pktyp);
            } else {                              /* middle MID -> MID */
                uchar ntype = Tbl[ri->table].col_type[ri->bclist[i + 1]];
                if (i == trgr) {
                    if (C_IS_I(ntype)) {
                        nbtr = C_IS_I(pktyp) ? createUUBT(imatch, BT_MCI_UNIQ) :
                                               createULBT(imatch, BT_MCI_UNIQ);
                    } else {
                        nbtr = C_IS_I(pktyp) ? createLUBT(imatch, BT_MCI_UNIQ) :
                                               createLLBT(imatch, BT_MCI_UNIQ);
                    }
                } else nbtr = createMCI_MIDBT(ntype, imatch);
            }
            ulong isize1 = ibtr->msize;
            btIndAdd(ibtr, &acol, nbtr);  /* add MID to ibtr */
            ulong idiff  = nbtr->msize + (ibtr->msize - isize1);
            for (int j = i; j >= 0; j--) ibl[j]->msize += idiff; /*mem-bookeep*/
        }
        if (destroy) dpl[ndstr] = init_dp(ibtr, &acol, nbtr);
        ndstr++;
        ibtr = nbtr; /* releaseAobj(&acol); NOTE: MCI is [I,L] so NOT needed */
    }
    if (destroy) goto i_add_mci_err;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {                                   //DEBUG_IADDMCI_UNIQ
        aobj fcol = getCol(btr, rrow, ri->bclist[final], apk, ri->table);
        if (fcol.empty) goto i_add_mci_err;
        if (btFind(nbtr, &fcol)) {
            if (c) replyUniqConstrViol(c); { ret = 0; goto i_add_mci_err; }
        } /* Next ADD (FinFK|PK) 2 UUBT */
        if C_IS_I(pktyp) btAdd(nbtr, &fcol, (void *)(long)apk->i);
        else             btAdd(nbtr, &fcol, (void *)      apk->l);
        /* releaseAobj(&fcol); NOTE: MCI is [I,L] so NOT needed */
    } else {
        btIndNodeAdd(nbtr, apk); /* ADD PK to NODEBT */ 
    }
    ulong diff  = (nbtr->msize - size1);     /* memory bookeeping trickles up */
    if (diff) for (int i = 0; i < depth; i++) ibl[i]->msize += diff;
    return 1;

i_add_mci_err: /* NOTE: a destroy pass is done to UNDO what was done */
    if (!ndstr)   return 1; /* first MCI COL was empty - nothing happened */
    if (!destroy) return _iAddMCI(c, btr, apk, pktyp, imatch, rrow, 1, ret);
    else { /* destroy information was collected, if nkeys ==1, its invalid */
        for (int j = ndstr - 1; j>= 0; j--) {
            if (dpl[j].ibtr->numkeys == 1) {
                btIndDelete(dpl[j].ibtr, &dpl[j].acol); bt_destroy(dpl[j].nbtr);
            }
        }
        return rec_ret;
    }
}
static inline bool iAddMCI(cli   *c,     bt  *btr,    aobj *apk, 
                           uchar  pktyp, int  imatch, void *rrow) {
    return _iAddMCI(c, btr, apk, pktyp, imatch, rrow, 0, 0);
}
void destroy_mci(bt *ibtr, bt_n *n, int imatch, int lvl) {
    r_ind_t *ri     = &Index[imatch];
    int      trgr   = UNIQ(ri->cnstr) ? ri->nclist - 2 : -1;
    int      final  = ri->nclist - 1;
    if (lvl == final) return;
    for (int i = 0; i < n->n; i++) {
        void *be   = KEYS(ibtr, n, i);
        bt   *nbtr = (bt *)parseStream(be, ibtr);
        if (lvl == final || lvl == trgr) bt_destroy(nbtr);
        else { /* INNER RECURSE -> LEVEL INCREASE */
            destroy_mci(nbtr, nbtr->root, imatch, lvl + 1);
        }
    }
    if (!n->leaf) { /* NORMAL RECURSE -> SAME LEVEL */
        for (int i = 0; i <= n->n; i++) {
            destroy_mci(ibtr, NODES(ibtr, n)[i], imatch, lvl);
        }
    }
}
static void iRemMCI(bt *btr, aobj *apk, int imatch, void *rrow) {
    bt      *nbtr  = NULL; /* compiler warning */
    r_ind_t *ri    = &Index[imatch];
    dp_t     dpl[ri->nclist];
    int      final = ri->nclist - 1;
    int      depth = UNIQ(ri->cnstr) ? ri->nclist - 1 : ri->nclist;
    bt      *ibtr  = getIBtr(imatch);
    for (int i = 0; i < depth; i++) { /* find NODEBT, build DEL list */
        aobj acol = getCol(btr, rrow, ri->bclist[i], apk, ri->table);
        if (acol.empty) return; /* NOTE: no rollback, iAddMCI does rollback */
        nbtr      = btIndFind(ibtr, &acol);
        dpl[i]    = init_dp(ibtr, &acol, nbtr);
        ibtr      = nbtr; /* NOTE: DO NOT release acol - it is used later */
    }                     /* NOTE: DO NOT reuse nbtr   - it is used later */
    int nkeys;
    ulong size1 = nbtr->msize;
    if UNIQ(ri->cnstr) {
        aobj dcol = getCol(btr, rrow, ri->bclist[final], apk, ri->table);
        nkeys     = btIndNodeDelete(nbtr, &dcol); /* delete FinalCol from UBT */
        releaseAobj(&dcol); /* NOTE: I or L so not really needed */
    } else {
        nkeys     = btIndNodeDelete(nbtr, apk); /* delete PK from NODEBT */
    }
    ulong diff  = (size1 - nbtr->msize);      /* mem-bookeeping trickles up */
    if (diff) for (int i = 0; i < depth; i++) dpl[i].ibtr->msize -= diff;
    int i = depth - 1;         /* start at end */
    while (!nkeys && i >= 0) { /*previous DEL emptied BT->destroyBT,trickle-up*/
        ibtr         = dpl[i].ibtr;
        ulong isize1 = ibtr->msize;
        nkeys        = btIndDelete(ibtr, &dpl[i].acol);
        ulong idiff  = nbtr->msize + (isize1 - ibtr->msize);
        bt_destroy(nbtr);
        for (int j = i; j >= 0; j--) dpl[j].ibtr->msize -= idiff;/*trickle-up*/
        { i--; nbtr = ibtr; } /* go one step HIGHER in dpl[] - trickle-up */
    }
}
static bool iAddStream(cli *c, bt *btr, uchar *stream, int imatch) {
    aobj apk;
    convertStream2Key(stream, &apk, btr);
    void    *rrow = parseStream(stream, btr);
    bool     ret  = addToIndex(c, btr, &apk, rrow, imatch);
    releaseAobj(&apk);
    return ret;
}
bool addToIndex(cli *c, bt *btr, aobj *apk, void *rrow, int imatch) {
    r_ind_t *ri    = &Index[imatch];
    if (ri->virt)                                                    return 1;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->luat) { luatAdd(btr, (luat_t *)ibtr, apk, imatch, rrow); return 1; }
    int      pktyp = Tbl[ri->table].col_type[0];
    if (ri->clist) { if (!iAddMCI(c, btr, apk, pktyp, imatch, rrow)) return 0; }
    else {
        aobj acol = getCol(btr, rrow, ri->column, apk, ri->table);
        if (!acol.empty) iAdd(ibtr, &acol, apk, pktyp);
        releaseAobj(&acol);
    }
    return 1;
}
void delFromIndex(bt *btr, aobj *apk, void *rrow, int imatch) {
    r_ind_t *ri    = &Index[imatch];
    if (ri->virt)                                                     return;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->luat) { luatDel(btr,  (luat_t *)ibtr, apk, imatch, rrow); return; }
    if (ri->clist) iRemMCI(btr, apk, imatch, rrow);
    else {
        aobj  acol = getCol(btr, rrow, ri->column, apk, ri->table);
        if (!acol.empty) iRem(ibtr, &acol, apk);
        releaseAobj(&acol);
    }
}
void upIndex(bt *ibtr, aobj *aopk, aobj *ocol,
                       aobj *anpk, aobj *ncol, int pktyp) {
    //TODO if equivalent do not update -> aobjEQ()
    iAdd(ibtr, ncol, anpk, pktyp);                 /* ADD 1st, mimic MCI */
    if (!ocol->empty) iRem(ibtr, ocol, aopk);
}
bool updateIndex(cli *c, bt *btr, aobj *aopk, void *orow, 
                                  aobj *anpk, void *nrow, int imatch) {
    r_ind_t *ri    = &Index[imatch];
    if (ri->virt)                                          return 1;
    bt      *ibtr  = getIBtr(imatch);
    if (ri->luat) {
        luatAdd(btr, (luat_t *)ibtr, anpk, imatch, nrow);  return 1;
        luatDel(btr, (luat_t *)ibtr, aopk, imatch, orow);  return 1;
    }
    int      pktyp = Tbl[ri->table].col_type[0];
    if (ri->clist) {                                      /*ADD 1st can FAIL*/
        if (!iAddMCI(c, btr, anpk, pktyp, imatch, nrow))   return 0;
        iRemMCI(btr, aopk, imatch, orow);
    } else {
        aobj  ocol = getCol(btr, orow, ri->column, aopk, ri->table);
        aobj  ncol = getCol(btr, nrow, ri->column, anpk, ri->table);
        upIndex(ibtr, aopk, &ocol, anpk, &ncol, pktyp);
        releaseAobj(&ncol);
        releaseAobj(&ocol);
    }
    return 1;
}

/* CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX  CREATE_INDEX */
bool newIndex(cli    *c,     char   *iname, int  tmatch, int   cmatch,
              list   *clist, uchar   cnstr, bool virt,   bool  lru,
              luat_t *luat) {
    if (Num_indx == MAX_NUM_INDICES) {
        if (c) addReply(c, shared.toomanyindices); return 0;
    }
    r_tbl_t *rt     = &Tbl[tmatch];
    int      imatch = Num_indx;
    r_ind_t *ri     = &Index[imatch];
    bzero(ri, sizeof(r_ind_t));
    ri->obj         = _createStringObject(iname);        /* DESTROY ME 055 */
    ri->table       = tmatch; ri->column = cmatch; ri->clist = clist;
    ri->virt        = virt;   ri->lru    = lru;
    ri->luat        = luat ? 1: 0;
    ri->cnstr       = cnstr;
    if (ri->luat) rt->nltrgr++; /* this table now has LUA TRIGGERS */
    if (ri->clist) {
        listNode *ln;
        rt->nmci++;           /* this table now has MCI */
        ri->nclist   = listLength(ri->clist);
        ri->bclist   = malloc(ri->nclist * sizeof(int)); /* FREE ME 053 */
        int       i  = 0;
        listIter *li = listGetIterator(ri->clist, AL_START_HEAD);
        while((ln = listNext(li)) != NULL) { /* convert clist to bclist */
           ri->bclist[i]                = (int)(long)ln->value;
           rt->col_indxd[ri->bclist[i]] = 1; /* used in updateRow OVRWR */
           i++;
        }
    } else {
       rt->col_indxd[ri->column] = 1; ri->nclist  = 0; ri->bclist  = NULL;
    }
    if (virt) {
        ri->btr     = NULL;
        rt->vimatch = imatch;
    } else if (ri->luat) {
        ri->btr     = (bt *)luat;
        luat->num   = imatch;
    } else {
        ri->btr = (ri->clist) ? createMCIndexBT(ri->clist,          imatch) :
        /* normal & lru */      createIndexBT(rt->col_type[cmatch], imatch);
    }
    if (!virt && !lru && !luat) {
        bt *btr = getBtr(tmatch);
        if (btr->numkeys > 0) { /* IF table has rows - populate index */
            if (!buildIndex(c, btr, btr->root, ri->btr, imatch, 0)) {
                emptyIndex(imatch); return 0;
            }
        }
    } //printf("New index: %d\n", Num_indx);
    Num_indx++;
    return 1;
}
bool buildIndex(cli *c, bt *btr, bt_n *x, bt *ibtr, int imatch, bool trgr) {
    for (int i = 0; i < x->n; i++) {
        uchar *stream = (uchar *)KEYS(btr, x, i);
        if (!iAddStream(c, btr, stream, imatch))       return 0;
    }
    if (!x->leaf) {
        for (int i = 0; i <= x->n; i++) {
            bt_n *y = NODES(btr, x)[i];
            if (!buildIndex(c, btr, y, ibtr, imatch, trgr)) return 0;
        }
    }
    return 1;
}

sds getMCIlist(list *clist, int tmatch) {
    listNode *ln;
    sds       mci = sdsnewlen("(", 1);                   /* DESTORY ME 051 */
    int       i   = 0;
    listIter *li  = listGetIterator(clist, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
       if (i) mci = sdscatlen(mci, ", ", 2);
       int cmatch = (int)(long)ln->value;
       mci        = sdscatprintf(mci, "%s",
                               (char *)Tbl[tmatch].col_name[cmatch]->ptr);
       i++;
    }
    listReleaseIterator(li);
    mci = sdscatlen(mci, ")", 1);
    return mci;
}
bool addC2MCI(cli *c, int cmatch, list *clist) {
    if (cmatch == -1) {
        listRelease(clist);
        if (c) addReply(c, shared.indextargetinvalid);
        return 0;
    }
    if (!cmatch) {
        listRelease(clist);
        if (c) addReply(c, shared.mci_on_pk);
        return 0;
    }
    listAddNodeTail(clist, (void *)(long)cmatch);
    return 1;
}
static bool ICommit(cli *c, sds iname, char *tname, char *cname, uchar cnstr) {
    int    cmatch = -1;
    list  *clist  = NULL;
    int    tmatch = -1;
    tmatch  = find_table(tname);
    if (tmatch == -1) {
        addReply(c, shared.nonexistenttable);
        return 0;
    }
    SKIP_SPACES(cname);
    char *nextc = strchr(cname, ',');
    if (nextc) {    /* Multiple Column Index */
        if UNIQ(cnstr) {
            if (Tbl[tmatch].nmci) {
                addReply(c, shared.two_uniq_mci); return 0;
            } else if (!C_IS_NUM(Tbl[tmatch].col_type[0])) {/*INT & LONG*/
                addReply(c, shared.uniq_mci_pk_notint); return 0;
            }
        }
        int ocmatch = -1; /* first column can be used as normal index */
        clist       = listCreate();                  /* DESTROY ME 054 */
        while (1) {
            char *end = nextc - 1;
            REV_SKIP_SPACES(end)
            cmatch    = find_column_n(tmatch, cname, (end + 1 - cname));
            if (!addC2MCI(c, cmatch, clist)) return 0;
            if (ocmatch == -1) ocmatch = cmatch;
            nextc++;
            SKIP_SPACES(nextc);
            cname     = nextc;
            nextc     = strchr(nextc, ',');
            if (!nextc) {
                cmatch = find_column(tmatch, cname);
                if (!addC2MCI(c, cmatch, clist)) return 0;
                break;
            }
        }
        if UNIQ(cnstr) {/*NOTE: RESTRICTION: UNIQUE MCI final col must be INT */
            listNode *ln = listLast(clist);
            int       fcmatch = (int)(long)ln->value;
            if (!C_IS_NUM(Tbl[tmatch].col_type[fcmatch])) {/*INT & LONG */
                addReply(c, shared.uniq_mci_notint); return 0;
            }
        }
        cmatch = ocmatch;
    } else {
        if UNIQ(cnstr) { addReply(c, shared.UI_SC); return 0; }
        cmatch = find_column(tmatch, cname);
        if (cmatch == -1) { addReply(c, shared.indextargetinvalid); return 0; }
        for (int i = 0; i < Num_indx; i++) { /* already indxd? */
            r_ind_t *ri = &Index[i];
            if (ri->table == tmatch && ri->column == cmatch) {
                addReply(c, shared.indexedalready);
                return 0;
            }
        }
    }
    if (!newIndex(c, iname, tmatch, cmatch, clist, cnstr, 0, 0, NULL)) {
        return 0;
    }
    addReply(c, shared.ok);
    return 1;
}
void createIndex(redisClient *c) {
    if (c->argc < 6) { addReply(c, shared.index_wrong_nargs); return; }
    bool cnstr; uint32 coln;
    if (!strcasecmp(c->argv[1]->ptr, "UNIQUE")) {
        cnstr = CONSTRAINT_UNIQUE;
        coln  = 6;
        if (c->argc < 7) { addReply(c, shared.index_wrong_nargs); return; }
    } else {
        cnstr = CONSTRAINT_NONE;
        coln  = 5;
    }
    if (strcasecmp(c->argv[coln - 2]->ptr, "ON")) {
        addReply(c, shared.createsyntax); return;
    }
    char *token = c->argv[coln]->ptr;
    char *end   = strchr(token, ')');
    sds   iname = c->argv[coln - 3]->ptr;
    if (match_index_name(iname) != -1) {
        addReply(c, shared.nonuniqueindexnames); return;
    }
    if (!end || (*token != '(')) { addReply(c, shared.createsyntax); return; }

    STACK_STRDUP(cname, (token + 1), (end - token - 1))
    ICommit(c, iname, c->argv[coln - 1]->ptr, cname, cnstr);
}
void emptyIndex(int imatch) {
    r_ind_t *ri  = &Index[imatch];
    robj    *ind = ri->obj;
    if (!ind) return; /* previously deleted */
    r_tbl_t *rt = &Tbl[ri->table];
    if (ri->luat) if (rt->nltrgr) rt->nltrgr--;
    if (ri->clist) {
        if (Tbl[ri->table].nmci) Tbl[ri->table].nmci--;
        listRelease(ri->clist);                          /* DESTROYED 054 */
        free(ri->bclist);                                /* FREED 053 */
    }
    decrRefCount(ri->obj);                                 /* DESTROYED 055 */
    if (ri->btr) destroy_index(ri->btr, ri->btr->root);
    bzero(ri, sizeof(r_ind_t));
    ri->table    = -1;
    ri->column   = -1;
    ri->cnstr    =  CONSTRAINT_NONE;
    server.dirty++; //TODO shuffle indices to make space for deleted indices
}
void dropIndex(redisClient *c) {
    char *iname = c->argv[2]->ptr;
    int   imatch  = match_index_name(iname);
    if (imatch == -1) {
        addReply(c, shared.nullbulk);           return;
    }
    r_ind_t *ri = &Index[imatch];
    if (ri->virt) {
        addReply(c, shared.drop_virtual_index); return;
    }
    if (ri->lru) {
        addReply(c, shared.drop_lru);           return;
    }
    r_tbl_t *rt = &Tbl[ri->table];
    if (rt->sk == ri->column) {
        addReply(c, shared.drop_ind_on_sk);     return;
    }
    emptyIndex(imatch);
    addReply(c, shared.cone);
}

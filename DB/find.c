/*
 * This file implements find_table,index,column logic
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
#include <ctype.h>
#include <limits.h>

#include "sds.h"
#include "dict.h"
#include "adlist.h"
#include "redis.h"

#include "query.h"
#include "bt.h"
#include "common.h"
#include "find.h"

// GLOBALS
extern r_tbl_t *Tbl;   extern dict *TblD;
extern r_ind_t *Index; extern dict *IndD;

extern cli  *CurrClient;
extern ja_t  JTAlias[MAX_JOIN_COLS];

#define SPECIAL_COL_IMATCH_ADD_NUM 100
inline int setOCmatchFromImatch(int imatch) {
    if (!SIMP_UNIQ(Index[imatch].btr)) return -1;
    Index[imatch].iposon = 1; Index[imatch].cipos = 1; 
    return (imatch * -1) - 2 - SPECIAL_COL_IMATCH_ADD_NUM;
}
inline int getImatchFromOCmatch(int cmatch) {
    return (cmatch * -1) - 2 - SPECIAL_COL_IMATCH_ADD_NUM;
}
inline void resetIndexPosOn(int qcols, int *cmatchs) {
    for (int i = 0; i < qcols; i++) { // Turn Index[].iposon -> OFF
        if (cmatchs[i] < -1 && !IS_LSF(cmatchs[i])) {
            Index[getImatchFromOCmatch(cmatchs[i])].iposon = 0;
        }
    }   
}   

// INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX
static int _find_index(int tmatch, int cmatch, bool prtl) {
    if (cmatch < -1) return getImatchFromOCmatch(cmatch);
    int imatch = Tbl[tmatch].col[cmatch].imatch;
    if      (imatch == -1) return -1;
    else if (!prtl)        return Index[imatch].done ? imatch : - 1;
    else                   return imatch;
}
int find_index(int tmatch, int cmatch) {
    return _find_index(tmatch, cmatch, 0);
}
int find_partial_index(int tmatch, int cmatch) { // Used by INDEX CURSORs
    return _find_index(tmatch, cmatch, 1);
}

int _match_index(int tmatch, list *indl, bool prtl) {
    listNode *ln; 
    r_tbl_t  *rt      = &Tbl[tmatch];
    if (!rt->ilist) return 0;
    int       matches =  0;
    listIter *li      = listGetIterator(rt->ilist, AL_START_HEAD);
    while((ln = listNext(li))) {
        int      imatch = (int)(long)ln->value;
        r_ind_t *ri     = &Index[imatch];
        if (!prtl && !ri->done) continue; // \/ UNIQ can fail, must be 1st
        if (UNIQ(ri->cnstr) && !prtl) listAddNodeHead(indl, VOIDINT imatch);
        else                          listAddNodeTail(indl, VOIDINT imatch);
        matches++;
    } listReleaseIterator(li);
    return matches;
}
int match_index(int tmatch, list *indl) {
    return _match_index(tmatch, indl, 0);
}
int match_partial_index(int tmatch, list *indl) { // RDBSAVE partial indexes
    return _match_index(tmatch, indl, 1);
}

int match_partial_index_name(sds iname) { // Used by DROP INDEX|LUATRIGGER
    void *ptr = dictFetchValue(IndD, iname);
    return ptr ? ((int)(long)ptr) - 1 : -1;
}
int match_index_name(sds iname) { // Used by DROP LUATRIGGER
    int imatch = match_partial_index_name(iname);
    if (imatch == -1) return -1;
    else              return Index[imatch].done ? imatch : -1; // completed?
}

// TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL TBL
int find_table(sds tname) { /* NOTE does not use JTAlias[] */
    void *ptr = dictFetchValue(TblD, tname);
    return ptr ? ((int)(long)ptr) - 1 : -1;
}
int find_table_n(char *tname, int len) {
    CurrClient->LastJTAmatch = -1;
    for (int i = 0; i < CurrClient->NumJTAlias; i++) {// 1st check Join Aliases
        sds x = JTAlias[i].alias;
        if (((int)sdslen(x) == len) && !strncmp(tname, x, len)) {
            CurrClient->LastJTAmatch = i; return JTAlias[i].tmatch;
        }
    }
    sds stname = sdsnewlen(tname, len);                // DEST  087
    int tmatch = find_table(stname); sdsfree(stname);  // DESTD 087
    if (tmatch == -1) return -1;
    CurrClient->LastJTAmatch = tmatch + USHRT_MAX; // DONT collide w/ NumJTAlias
    return tmatch;
}
sds getJoinAlias(int jan) {
    if (jan == -1) return NULL;
    return (jan < CurrClient->NumJTAlias) ? JTAlias[jan].alias :
                                            Tbl[(jan - USHRT_MAX)].name;
}

// COL COL COL COL COL COL COL COL COL COL COL COL COL COL COL COL COL COL COL
static int check_special_column(int tmatch, sds cname) {
    char *sd = strchr(cname, '.');
    if (sd) {
         if (!strcmp(sd, ".pos()")) {
             sds iname = sdsnewlen(cname, sd - cname); // FREE ME 109
             int imatch = match_index_name(iname);
             sdsfree(iname);                           // FREED   109
             if (Index[imatch].table != tmatch) return -1;
             if (imatch != -1) return setOCmatchFromImatch(imatch);
        }
    }
    return -1; // MISS on special also
}
static int find_column_sds(int tmatch, sds cname) {
    r_tbl_t *rt    = &Tbl[tmatch];
    void    *ptr   = dictFetchValue(rt->cdict, cname);
    return ptr ? ((int)(long)ptr) - 1 : check_special_column(tmatch, cname);
}
int find_column(int tmatch, char *c) {
    sds      cname = sdsnew(c);                                     // DEST 091
    int      ret   = find_column_sds(tmatch, cname); sdsfree(cname);// DESTD 091
    return ret;
}
int find_column_n(int tmatch, char *c, int len) {
    sds      cname = sdsnewlen(c, len);                             // DEST 092
    int      ret   = find_column_sds(tmatch, cname); sdsfree(cname);// DESTD 092
    return ret;
}
int get_all_cols(int tmatch, list *cs, bool lru2, bool lfu2) {
    r_tbl_t *rt = &Tbl[tmatch];
    for (int i = 0; i < rt->col_count; i++) {
        if (!lru2 && rt->lrud && rt->lruc == i) continue; /* DONT PRINT LRU */
        if (!lfu2 && rt->lfu &&  rt->lfuc == i) continue; /* DONT PRINT LFU */
        listAddNodeTail(cs, VOIDINT i);
    }
    int ret = rt->col_count;
    if (!lru2 && rt->lrud) ret--;
    if (!lfu2 && rt->lfu)  ret--;
    return ret;
}

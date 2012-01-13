/*
 * This file implements ind_table,index,column logic
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

#ifndef __FIND__H
#define __FIND__H

#include "sds.h"
#include "adlist.h"
#include "redis.h"

#include "common.h"

int  setOCmatchFromImatch(int imatch);
int  getImatchFromOCmatch(int cmatch);
void resetIndexPosOn(int qcols, int *cmatchs);

int find_index      (int tmatch, int cmatch);
int match_index     (int tmatch, list *indl);
int match_index_name(sds iname);

int find_partial_index      (int tmatch, int cmatch); // Used by INDEX CURSORs
int match_partial_index     (int tmatch, list *indl);//RDBSAVE partial indexes 2
int match_partial_index_name(sds iname); // Used by DROP INDEX|LUATRIGGER

//TODO turn MACROS into functions
#define INDS_FROM_INDL                                    \
    int  inds[indl->len];                                 \
    listNode *lni;                                        \
    int       i  = 0;                                     \
    listIter *lii = listGetIterator(indl, AL_START_HEAD); \
    while((lni = listNext(lii))) {                        \
        inds[i] = (int)(long)lni->value; i++;             \
    } listReleaseIterator(lii);
#define MATCH_INDICES(tmatch)                  \
    list *indl    = listCreate();              \
    int   matches = match_index(tmatch, indl); \
    INDS_FROM_INDL                             \
    listRelease(indl);
#define MATCH_PARTIAL_INDICES(tmatch)                  \
    list *indl    = listCreate();                      \
    int   matches = match_partial_index(tmatch, indl); \
    INDS_FROM_INDL                                     \
    listRelease(indl);

typedef struct join_alias {
    sds alias; int tmatch;
} ja_t;

int find_table  (sds   tname);
int find_table_n(char *tname, int len);
sds getJoinAlias(int jan);

int find_column_sds(int tmatch, sds cname);
int find_column    (int tmatch, char *column);
int find_column_n  (int tmatch, char *column, int len);

#define TABLE_CHECK_OR_REPLY(TBL, RET)        \
    int tmatch = find_table(TBL);             \
    if (tmatch == -1) {                       \
        addReply(c, shared.nonexistenttable); \
        return RET;                           \
    }

#define COLUMN_CHECK_OR_REPLY(cargv2ptr, retval)   \
    char *cname  = cargv2ptr;                      \
    int   cmatch = find_column(tmatch, cname);     \
    if (cmatch == -1) {                            \
        addReply(c,shared.nonexistentcolumn);      \
        return retval;                             \
    }

int get_all_cols(int tmatch, list *cmatchs, bool lru2, bool lfu2);

#endif /* __FIND__H */ 

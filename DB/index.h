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

#ifndef __INDEX__H
#define __INDEX__H

#include "redis.h"

#include "btreepriv.h"
#include "parser.h"
#include "row.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

int find_index(     int tmatch, int cmatch);
int find_next_index(int tmatch, int cmatch, int j);
int match_index(    int tmatch, int inds[]);
int match_index_name(char *iname);

/* MATCH_INDICES(tmatch)
     creates (int inds[], int matches)     */
#define MATCH_INDICES(tmatch)                 \
    int   inds[MAX_COLUMN_PER_TABLE];         \
    int   matches = match_index(tmatch, inds);

sds  getMCIlist(list *clist, int tmatch);
bool addC2MCI(cli *c, int cmatch, list *clist);
bool newIndex(cli    *c,     char   *iname, int  tmatch, int   cmatch,
              list   *clist, uchar   cnstr, bool virt,   bool  lru,
              luat_t *luat);
void createIndex(cli *c);

bool buildIndex(cli *c, bt *btr, bt_n *x, bt *ibtr, int imatch, bool nri);

bool addToIndex (cli *c, bt *btr, aobj *apk,  void *rrow, int imatch);
void delFromIndex       (bt *btr, aobj *apk,  void *rrow, int imatch);
void upIndex            (bt *btr, aobj *aopk, aobj *ocol, 
                                  aobj *anpk, aobj *ncol, int pktyp) ;
bool updateIndex(cli *c, bt *btr, aobj *aopk, void *orow,
                                  aobj *anpk, void *nrow, int imatch);

void destroy_index(bt *btr, bt_n *n);
void destroy_mci  (bt *btr, bt_n *n, int imatch, int lvl);

void emptyIndex(int inum);
void dropIndex(redisClient *c);

#endif /* __INDEX__H */ 

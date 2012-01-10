/*
 * This file implements LRU Indexes
 *

AAGPL License

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

#ifndef __ALCHEMY_LRU__H
#define __ALCHEMY_LRU__H

#include "adlist.h"
#include "redis.h"

#include "row.h"
#include "query.h"
#include "aobj.h"

#define SERVER_BEGINNING_OF_TIME 1317473183 /* oldest LRU time possible */

uint32 getLru(int tmatch);

void createLruIndex(cli *c);
void updateLru     (cli *c, int tmatch, aobj *apk, uchar *lruc, bool lrud);

bool initLRUCS  (int tmatch, int   cmatchs[], int qcols);
bool initL_LRUCS(int tmatch, list *cs);
bool initLRUCS_J(jb_t *jb);

#define GET_LRUC                                                \
  bool   lrud = Tbl[tmatch].lrud;                               \
  uchar *lruc = NULL;                                           \
  if (lrud) {                                                   \
      uint32 clen; uchar rflag;                                 \
      lruc = getColData(rrow, Tbl[tmatch].lruc, &clen, &rflag); \
      if (!clen) lruc = NULL;                                   \
  }

#endif /* __ALCHEMY_LRU__H */ 

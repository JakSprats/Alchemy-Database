/*
 * This file implements LFU Indexes
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

#ifndef __ALCHEMY_LFU__H
#define __ALCHEMY_LFU__H

#include "adlist.h"
#include "redis.h"

#include "aobj.h"

ulong getLfu(ulong num);

void createLfuIndex(cli *c);
void updateLfu     (cli *c, int tmatch, aobj *apk, uchar *lfuc, bool lfu);

bool initLFUCS  (int tmatch, int   cmatchs[], int qcols);
bool initL_LFUCS(int tmatch, list *cs);
bool initLFUCS_J(jb_t *jb);

#define GET_LFUC                                                \
  bool   lfu = Tbl[tmatch].lfu;                                 \
  uchar *lfuc = NULL;                                           \
  if (lfu) {                                                    \
      uint32 clen; uchar rflag;                                 \
      lfuc = getColData(rrow, Tbl[tmatch].lfuc, &clen, &rflag); \
      if (!clen) lfuc = NULL;                                   \
  }


#endif /* __ALCHEMY_LFU__H */ 

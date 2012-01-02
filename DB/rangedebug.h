/*
 * This file implements Alchemy's RANGE DEBUG #ifdefs
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

#ifndef __A_RANGEDEBUG__H
#define __A_RANGEDEBUG__H

#define DEBUG_SINGLE_PK                                                    \
  printf("singleOpPK: tmatch: %d\n", g->co.w->wf.tmatch);
#define DEBUG_RANGE_PK                                                     \
  printf("rangeOpPK: imatch: %d\n", g->co.w->wf.imatch);
#define DEBUG_UBT_OP                                                       \
  printf("uBT_Op: btr: %p UniqueIndexVal: ", d->g->co.btr);                \
  dumpAobj(printf, &UniqueIndexVal);
#define DEBUG_NBT_OP                                                       \
  printf("nBT_Op: nbi: %p [iss: %d isd: %d isu: %d] empty: %d\n",          \
         (void *)nbi, iss, isd, isu, nbi->empty);
#define DEBUG_NBT_OP_GOT_ITER                                              \
  printf("GOTITER: dr: %d missed: %d\n", nbi->be.dr, nbi->missed);
#define DEBUG_NBT_OP_LOOP                                                  \
  printf("LOOP: nbi.missed: %d dr: %u nasc: %d\n",                         \
         nbi->missed, nbe->dr, nasc);
#define DEBUG_NBT_OP_POST_LOOP                                             \
  printf("POST LOOP: lim: %ld card: %ld dr: %d\n",                         \
         wb->lim, *d->card, nbi->be.dr);
#define DEBUG_NODE_BT                                                      \
  printf("nodeBT_Op: nbtr->numkeys: %d\n", d->nbtr->numkeys);
#define DEBUG_NODE_BT_OBC_1                                                \
  printf("nodeBT_Op OBYI_1: key: ");                                       \
  printf("nbe_key: "); dumpAobj(printf, nbe->key);                         \
  DEBUG_BT_TYPE(printf, nbtr)
#define DEBUG_NODE_BT_OBC_2                                                \
  printf("nodeBT_Op OBYI_2: nbtr: %p ctype: %d obc: %d val: %p key: ",     \
         nbtr, ctype, d->obc, nbe->val); dumpAobj(printf, &akey);          \
  printf("nbe_key: "); dumpAobj(printf, nbe->key);
#define DEBUG_MCI_FIND                                                     \
  printf("in btMCIFindVal: trgr: %d\n", trgr);
#define DEBUG_MCI_FIND_MID                                                 \
  dumpFilter(printf, flt, "\t");
#define DEBUG_RUN_ON_NODE                                                  \
  printf("in runOnNode: ibtr: %p still: %u nop: %p\n", ibtr, still, nop);  \
  bt_dumptree(printf, ibtr, 0, 0);
#define DEBUG_SINGLE_FK                                                    \
  printf("singleOpFK: imatch: %d key: ", g->co.w->wf.imatch);              \
  dumpAobj(printf, &g->co.w->wf.akey);
#define DEBUG_RANGE_FK                                                     \
  printf("rangeOpFK: imatch: %d\n", g->co.w->wf.imatch);
#define DEBUG_PASS_FILT_INL                                                \
printf("PF: ret: %d a2: ", ret); dumpAobj(printf, a2);                     \
printf("a: ");dumpAobj(printf, &a);
#define DEBUG_PASS_FILT_LOW                                                \
  printf("PassF: low:  "); dumpAobj(printf, &flt->alow);                   \
  printf("PassF: high: "); dumpAobj(printf, &flt->ahigh);                  \
  printf("PassF: a:    "); dumpAobj(printf, &a);                           \
  printf("ret: %d\n", ret);
#define DEBUG_PASS_FILT_KEY                                                \
  printf("PassF: key: "); dumpAobj(printf, &flt->akey);                    \
  printf("PassF: a:   "); dumpAobj(printf, &a);                            \
  printf("ret: %d\n", ret);

#endif /*__A_RANGEDEBUG__H */ 

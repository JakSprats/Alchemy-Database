/*
  *
  * This file implements Alchemy's EVICT & RECACHE commands
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

#include "redis.h"

#include "query.h"
#include "bt.h"
#include "btree.h"
#include "parser.h"
#include "stream.h"
#include "index.h"
#include "find.h"
#include "alsosql.h"
#include "common.h"
#include "cache.h"

extern r_tbl_t  *Tbl;
extern r_ind_t  *Index;

void evictCommand(cli *c) {
    int   len   = sdslen(c->argv[1]->ptr);
    char *tname = rem_backticks(c->argv[1]->ptr, &len); // Mysql compliant
    TABLE_CHECK_OR_REPLY(tname,)
    //TODO must be NUM PK & auto-inc
    bt   *btr   = getBtr(tmatch);
    if OTHER_BT(btr) { addReply(c, shared.evict_other); return; }
    if (!(C_IS_NUM(btr->s.ktype))) { addReply(c, shared.evict_other); return; }
    long  card  = 0;
    for (int i = 2; i < c->argc; i++) {
        sds    pk   = c->argv[i]->ptr;
        printf("\n\nEVICT: tbl: %s[%d] PK: %s\n", tname, tmatch, pk);
        aobj apk; initAobjFromStr(&apk, pk, sdslen(pk), btr->s.ktype);
        dwm_t  dwm  = btFindD(btr, &apk); releaseAobj(&apk);
        void  *rrow = dwm.k;
        bool   gost = !UU(btr) && rrow && !(*(uchar *)rrow);
        printf("EVICT: K: %p MISS: %d gost: %d\n", dwm.k, dwm.miss, gost);
        if (!rrow || dwm.miss || gost) continue;
        MATCH_INDICES(tmatch)
        if (matches) { // EVICT indexes
            for (int i = 0; i < matches; i++) {
                evictFromIndex(btr, &apk, rrow, inds[i]);
            }}
        printf("EVICT indexes done\n");
        btEvict(btr, &apk); card++; releaseAobj(&apk);
        printf("\n\n"); fflush(NULL);
    }
    printf("\n\n");
    addReplyLongLong(c, card);
}

/*
 * This file implements LFU Indexes
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

#include "sds.h"
#include "redis.h"

#include "find.h"
#include "bt.h"
#include "ddl.h"
#include "index.h"
#include "stream.h"
#include "aobj.h"
#include "query.h"
#include "common.h"
#include "lfu.h"

/* TODO LIST LFU
  1.) LFU indexes should be UNIQUE U128 indexes [lfu|pk]
*/

// GLOBALS
extern r_tbl_t *Tbl;
extern int      Num_indx; extern r_ind_t *Index;

/* NOTE:
    the LFUINDEX was done quickly, it indexes using log2(numrowaccess)
    which means numrowaccess of log2()=4 [16-31] are equivalent
    The better solution is to have a single BTREE using {KEY=[LFU|PK], VAL=PK}
    Since LFU is a ULONG and PK can be [UINT|ULONG] and VAL can be [UINT|ULONG]
    the BTREE would need to support [128,160,192] bit IN-TREE rows
*/

inline ulong getLfu(ulong num) { return num + 1; }

void createLfuIndex(cli *c) {
    if (strcasecmp(c->argv[2]->ptr, "ON")) {
        addReply(c, shared.createsyntax); return;
    }
    int    len   = sdslen(c->argv[3]->ptr);
    char  *tname = rem_backticks(c->argv[3]->ptr, &len); /* Mysql compliant */
    TABLE_CHECK_OR_REPLY(tname,)
    if (OTHER_BT(getBtr(tmatch))) { addReply(c, shared.lfu_other); return; }
    r_tbl_t *rt  = &Tbl[tmatch];
    if (rt->lfu) { addReply(c, shared.lfu_repeat); return; }
    rt->lfu      = 1;
    rt->lfuc     = rt->col_count; // -> new Column
    addColumn(tmatch, "LFU", COL_TYPE_LONG);

    sds  iname   = P_SDS_EMT "%s_%s", LFUINDEX_DELIM, tname);  // FREE ME 108
    rt->lfui     = newIndex(c, iname, tmatch, rt->lfuc, NULL, CONSTRAINT_NONE,
                            0, 0, NULL, -1, 0, 1); // Can not fail
    sdsfree(iname);                                            // FREED 108
    addReply(c, shared.ok);
}

#define DEBUG_UPDATE_LFU \
  printf("updateLfu: tmatch: %d lfuc: %p lfu: %d LfuColInSelect: %d apk: ", \
         tmatch, lfuc, lfu, c->LfuColInSelect);                             \
  if (apk) dumpAobj(printf, apk); else printf("\n");

void updateLfu(cli *c, int tmatch, aobj *apk, uchar *lfuc, bool lfu) {
    //DEBUG_UPDATE_LFU
    if (!lfu)                 return;
    if (tmatch == -1)         return; /* from JOIN opSelectSort */
    if (c->LfuColInSelect)    return; /* NOTE: otherwise TOO cyclical */
    r_tbl_t *rt     = &Tbl[tmatch];
    int      imatch = rt->lfui;
    if (lfuc) {
        ulong   num   = streamLFUToULong(lfuc);
        ulong   nnum  = num + 1;
        overwriteLFUcol(lfuc, nnum);
        bt     *ibtr  = getIBtr(imatch);
        int     pktyp = rt->col[0].type;
        aobj ocol; initAobjLong(&ocol, num);
        aobj ncol; initAobjLong(&ncol, nnum);
        upIndex(c, ibtr, apk, &ocol, apk, &ncol, pktyp, NULL, NULL, imatch);
        releaseAobj(&ocol); releaseAobj(&ncol);
    } else { /* LFU empty -> run "UPDATE tbl SET LFU = 1 WHERE PK = apk" */
        char    *LfuBuf = "1";
        MATCH_INDICES(tmatch)
        int      ncols  = rt->col_count;
        uchar    cmiss[ncols]; ue_t    ue   [ncols]; lue_t le[ncols];
        char    *vals [ncols]; uint32  vlens[ncols];
        for (int i = 0; i < ncols; i++) {
            ue[i].yes = 0; le[i].yes = 0;
            if (i == rt->lfuc) {
                cmiss[i] = 0; vals [i] = LfuBuf; vlens[i] = strlen(LfuBuf);
            } else cmiss[i] = 1;
        }
        bt      *btr   = getBtr(tmatch);
        void    *rrow  = btFind(btr, apk); // apk has NOT been evicted
        uc_t uc;
        init_uc(&uc, btr, tmatch, ncols, matches, inds, vals, vlens, cmiss,
                ue,  le);
        updateRow(c, &uc, apk, rrow); /* NOTE: ALWAYS succeeds */
        //NOTE: rrow is no longer valid, updateRow() can change it
        release_uc(&uc);
    }
}

inline bool initLFUCS(int tmatch, int cmatchs[], int qcols) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->lfu) {
        for (int i = 0; i < qcols; i++) if (cmatchs[i] == rt->lfuc) return 1;
    }
    return 0;
}
inline bool initL_LFUCS(int tmatch, list *cs) {
    r_tbl_t *rt = &Tbl[tmatch];
    if (rt->lfu) { listNode *ln;
        listIter *li = listGetIterator(cs, AL_START_HEAD);
        while((ln = listNext(li))) {
            int cm = (int)(long)ln->value;
            if (cm == rt->lfuc) return 1;
        } listReleaseIterator(li);
    }
    return 0;
}   
inline bool initLFUCS_J(jb_t *jb) {
    for (int i = 0; i < jb->qcols; i++) {
        if (Tbl[jb->js[i].t].lfuc == jb->js[i].c) return 1;
    }
    return 0;
}

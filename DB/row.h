/*
 * This file implements the rows of Alsosql
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

#ifndef __ALSOSQL_ROW__H
#define __ALSOSQL_ROW__H

#include "redis.h"

#include "btreepriv.h"
#include "query.h"
#include "common.h"

void *createRow(cli    *c,    bt     *btr,      int tmatch, int  ncols,
                char   *vals, twoint  cofsts[]);
uint32 getRowMallocSize(uchar *stream);

uchar *getColData(         uchar *orow, int cmatch, uint32 *clen, uchar *rflag);
aobj   getCol    (bt *btr, uchar *rrow, int cmatch, aobj *apk, int tmatch);
aobj   getSCol   (bt *btr, uchar *rrow, int cmatch, aobj *apk, int tmatch);

robj *cloneRobjErow(robj *r);   // EMBEDDED
void decrRefCountErow(robj *r); // EMBEDDED

bool addReplyRow(cli   *c,    robj *r,    int    tmatch, aobj *apk,
                 uchar *lruc, bool  lrud, uchar *lfuc,   bool  lfu);

int   output_start    (char *buf, uint32 blen, int qcols);
robj *write_output_row(int   qcols,   uint32  prelen, char *pbuf,
                       uint32 totlen, sl_t   *outs);
robj *outputRow(bt   *btr,       void *row,  int qcols,
                int   cmatchs[], aobj *aopk, int tmatch);

int   deleteRow(int tmatch, aobj *apk, int matches, int inds[], bool ispk);

typedef struct update_ctl {
    bt      *btr;
    aobj    *apk;
    void    *orow;
    int      tmatch;
    int      ncols;
    int      matches;
    int     *inds;
    char   **vals;
    uint32  *vlens;
    uchar   *cmiss;
    ue_t    *ue;
    lue_t   *le;
} uc_t;

void init_uc(uc_t  *uc,     bt     *btr,
             int    tmatch, int     ncols,   int    matches, int   inds[],
             char  *vals[], uint32  vlens[], uchar  cmiss[], ue_t  ue[],
             lue_t *le);
void release_uc(uc_t *uc);

int updateRow(cli *c, uc_t *uc, aobj *apk, void *orow);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpRow(printer *prn, bt *btr, void *rrow, aobj *apk, int tmatch);

#endif /* __ALSOSQL_ROW__H */

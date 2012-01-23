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
#include "alsosql.h"
#include "common.h"

void pushColumnLua(bt *btr, uchar *orow, int tmatch, aobj *a, aobj *apk);

void *createRow(cli *c,     aobj *apk,  bt     *btr,      int tmatch,
                int  ncols, char *vals, twoint  cofsts[]);
uint32 getRowMallocSize(uchar *stream);

uchar *getColData(uchar *orow, int cmatch, uint32 *clen, uchar *rflag);
aobj   getCol    (bt   *btr, uchar *rrow, icol_t ic, aobj *apk, int tmatch, 
                  lfca_t *lfca);
aobj   getSCol   (bt   *btr, uchar *rrow, icol_t ic, aobj *apk, int tmatch,
                  lfca_t *lfca);

robj *cloneRobjErow(robj *r);   // EMBEDDED
void decrRefCountErow(robj *r); // EMBEDDED

bool addReplyRow(cli   *c,    robj *r,    int    tmatch, aobj *apk,
                 uchar *lruc, bool  lrud, uchar *lfuc,   bool  lfu);

int   output_start    (char *buf, uint32 blen, int qcols);
robj *write_output_row(int   qcols,   uint32  prelen, char *pbuf,
                       uint32 totlen, sl_t   *outs);

#define OR_NONE      0
#define OR_ALLB_OK   1
#define OR_ALLB_NO   2
#define OR_LUA_FAIL  3
robj *outputRow(bt   *btr, void *rrow,   int     qcols, icol_t *ics,
                aobj *apk, int   tmatch, lfca_t *lfca,  bool   *ostt);
void outputColumnNames(cli *c, int tmatch, bool cstar, icol_t *ics, int qcols);

int   deleteRow(int tmatch, aobj *apk, int matches, int inds[]);

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

void deleteLuaObj(int tmatch, int cmatch, aobj *apk);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpRow(printer *prn, bt *btr, void *rrow, aobj *apk, int tmatch);

#endif /* __ALSOSQL_ROW__H */

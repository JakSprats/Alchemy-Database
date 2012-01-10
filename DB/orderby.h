/*
 * This file implements "SELECT ... ORDER BY col LIMIT X OFFSET Y" helper funcs
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

#ifndef __ORDER_BY__H
#define __ORDER_BY__H

#include "adlist.h"

#include "query.h"
#include "common.h"

list *initOBsort   (bool qed, wob_t *wb, bool rcrud);
void  releaseOBsort(list *ll);

obsl_t *create_obsl  (void *row, uint32 nob);
void    destroy_obsl(obsl_t *ob, bool ofree);
obsl_t *cloneOb     (obsl_t *ob, uint32 nob);

void assignObEmptyKey(obsl_t *ob, uchar ctype, int i);
bool assignObKey(wob_t  *wb, bt *btr, void *rrow, aobj *apk, int i,
                 obsl_t *ob, int tmatch);
bool addRow2OBList(list *ll,      wob_t *wb,   bt   *btr, void *r,
                   bool  is_robj, void  *rrow, aobj *apk);

obsl_t **sortOB2Vector(list *ll);
void     sortOBCleanup(obsl_t **vector, int vlen, bool decr_row);

//DEBUG
void    dumpObKey(printer *prn, int i, void *key, uchar ctype);
void    dumpOb(printer *prn, obsl_t *ob);

#endif /* __ORDER_BY__H */ 

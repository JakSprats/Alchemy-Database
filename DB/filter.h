/*
 * This file implements the filtering of columns in SELECTS
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

#ifndef __SQL_FILTERS__H
#define __SQL_FILTERS__H

#include "redis.h"

#include "query.h"
#include "range.h"
#include "common.h"

void  initFilter(f_t *flt);
f_t  *newEmptyFilter();
void  releaseFilterD_KL(f_t *flt); /* RANGE_QUERY */
void  releaseFilterR_KL(f_t *flt); /* JOIN */
void  destroyFilter(void *v);

f_t  *cloneFilter( f_t  *oflt);
void *vcloneFilter(void *oflt);

void convertFilterListToAobj(list *flist);

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpFilter(printer *prn, f_t *flt, char *prfx);

#endif /*___SQL_FILTERS__H */ 

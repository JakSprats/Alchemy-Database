/*
 * Implements ALCHEMY_DATABASE StarSchema table joins
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

#ifndef __ALCHEMYDB_JOIN__H
#define __ALCHEMYDB_JOIN__H

#include "adlist.h"
#include "redis.h"

#include "wc.h"
#include "range.h"
#include "common.h"

bool joinGeneric        (cli *c, jb_t *jb);
bool validateJoinOrderBy(cli *c, jb_t *jb);

void setupFirstJoinStep(cswc_t *w, jb_t *jb, qr_t *q);

void init_ijp(ijp_t *ij);
void switchIJ(ijp_t *ij);


#endif /* __ALCHEMYDB_JOIN__H */ 

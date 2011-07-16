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

void joinGeneric(cli *c, jb_t *jb);

void init_ijp(ijp_t *ij);
void switchIJ(ijp_t *ij);

bool validateJoinOrderBy(cli *c, jb_t *jb);

void explainJoin(cli *c, jb_t *jb);

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpIJ(cli *c, printer *prn, int i, ijp_t *ij, ijp_t *nij);
void dumpJB(cli *c, printer *prn, jb_t *jb);

#endif /* __ALCHEMYDB_JOIN__H */ 

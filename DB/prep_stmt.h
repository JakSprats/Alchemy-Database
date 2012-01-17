/*
 * Implements ALCHEMY_DATABASE PREPARE & EXECUTE commands
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

#ifndef __ALCHEMYDB_PREP_STMT__H
#define __ALCHEMYDB_PREP_STMT__H

#include "adlist.h"
#include "redis.h"

#include "query.h"
#include "common.h"

void prepareRQ     (cli *c,     cswc_t *w, wob_t *wb, bool cstar,
                    int  qcols, icol_t *ic);
bool prepareJoin   (cli *c, jb_t *jb);
void prepareCommand(cli *c);

bool executeCommandBinary (cli *c, uchar *x); // EMBEDDED
bool executeCommandInnards(cli *c);           // EMBEDDED
void executeCommand       (cli *c);

#endif /* __ALCHEMYDB_PREP_STMT__H */ 

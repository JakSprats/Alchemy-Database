/*
 * This file implements ALCHEMY_DATABASE's AOF writing
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

#ifndef __ALSQSQL_AOF_H
#define __ALSQSQL_AOF_H

#include "redis.h"

#include "bt.h"
#include "common.h"

bool appendOnlyDumpIndices(FILE *fp, int tmatch);
bool appendOnlyDumpTable  (FILE *fp, bt *btr, int tmatch);

#endif /* __ALSQSQL_AOF_H */

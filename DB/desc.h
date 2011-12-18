/*
 * This file implements ALCHEMY_DATABASEs DESC & DUMP commands
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

#ifndef __DESC__H
#define __DESC__H

#include "redis.h"

#include "query.h"

sds createAlterTableFulltext(r_tbl_t *rt, r_ind_t *ri, int cmatch, bool nl);
sds dumpSQL_Index(char *mtname, r_tbl_t *rt, r_ind_t *ri, int tmatch, bool nl);

void sqlDumpCommand(redisClient *c);

ull get_sum_all_index_size_for_table(int tmatch);
void print_mem_usage(int tmatch);

void descCommand(redisClient *c);

void showCommand(redisClient *c);

#endif /* __DESC__H */ 

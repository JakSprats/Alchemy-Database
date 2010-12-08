/*
 *
 * This file implements "CREATE TABLE x AS redis_datastructure"
 *

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __CR8TBLAS__H
#define __CR8TBLAS__H

#include "common.h"
#include "redis.h"

#define NUM_ACCESS_TYPES              15
#define ACCESS_SELECT_COMMAND_NUM      0
//#define ACCESS_SCANSELECT_COMMAND_NUM  1
int getAccessCommNum(char *as_cmd);

void createTableAsObject(redisClient *c);

#endif /* __CR8TBLAS__H */

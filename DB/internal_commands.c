/*
 * This file implements ALCHEMY_DATABASE's internal commands for
 *  CREATE TABLE AS SELECT
 *  SELECT ... WHERE x IN (SELECT ...)
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
#include <stdio.h>
#include <stdlib.h> // NOTE: MUST come before redis.h ... to define BYTE_ORDER

#include "redis.h"

#include "parser.h"
#include "alsosql.h"
#include "common.h"
#include "internal_commands.h"

stor_cmd AccessCommands[NUM_ACCESS_TYPES];

void initAccessCommands() { //printf("initAccessCommands\n");
    bzero(&AccessCommands, sizeof(stor_cmd) * NUM_ACCESS_TYPES);
    AccessCommands[0].func  = sqlSelectCommand;
    AccessCommands[0].name  = "SELECT";
    AccessCommands[0].argc  = 6;
    AccessCommands[0].parse = parseScanCmdToArgv;

    AccessCommands[1].func  = tscanCommand;
    AccessCommands[1].name  = "SCAN";
    AccessCommands[1].argc  = 4;
    AccessCommands[1].parse = parseScanCmdToArgv;
}

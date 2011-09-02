/*
 * This file implements ALCHEMY_DATABASE's redis-server hooks
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

#ifndef DXDB_HOOKS_H
#define DXDB_HOOKS_H

#include "redis.h"
#include "dict.h"

#include "xdb_common.h"


void  DXDB_populateCommandTable(dict *server_commands);

void  DXDB_createSharedObjects();

void  DXDB_initServerConfig();

void  DXDB_initServer();

void  DXDB_main();

void  DXDB_createClient(redisClient *c);

void  DXDB_emptyDb();

rcommand *DXDB_lookupCommand(sds name);

void      DXDB_call(struct redisCommand *cmd, long long *dirty);

int           DXDB_processCommand(redisClient *c);
unsigned char DXDB_processInputBuffer_begin(redisClient *c);
void          DXDB_processInputBuffer_ZeroArgs(redisClient *c);

int           DXDB_loadServerConfig(int argc, sds *argv);
int           DXDB_configSetCommand(redisClient *c, robj *o);
unsigned char DXDB_configCommand(redisClient *c);
void          DXDB_configGetCommand(redisClient *c, char *pattern, int *matchs);

int   DXDB_rdbSave(FILE *fp);
int   DXDB_rdbLoad(FILE *fp);

void  DXDB_flushdbCommand();

int   DXDB_rewriteAppendOnlyFile(FILE *fp);

void  DBXD_genRedisInfoString(sds info);

void DXDB_setClientSA(redisClient *c);

int *DXDB_getKeysFromCommand(rcommand *cmd, robj **argv, int argc,
                             int *numkeys, int flags, sds *override_key,
                             unsigned char *err);

void DXDB_syncCommand(redisClient *c);

unsigned char isWhiteListedIp(redisClient *c); //TODO move to another file?

// PROTOTYPES
// from redis.c
unsigned int dictSdsCaseHash(const void *key);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);

#endif /* DXDB_HOOKS_H */

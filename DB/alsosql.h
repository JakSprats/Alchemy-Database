/*
 * This file implements basic SQL commands of Alchemy Database (single row ops)
 *  and calls the range-query and join ops
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

#ifndef __ALSOSQL__H
#define __ALSOSQL__H

#include "adlist.h"
#include "redis.h"

#include "aobj.h"
#include "common.h"

#define getBtr(tmatch) Tbl[tmatch].btr
#define getIBtr(imatch) Index[imatch].btr;

#define EMPTY_LEN_OBJ    \
    long  card   = 0;    \
    robj *lenobj = NULL;

#define INIT_LEN_OBJ                           \
    lenobj = createObject(REDIS_STRING, NULL); \
    addReply(c, lenobj);                       \
    decrRefCount(lenobj);

#define LEN_OBJ     \
    long  card = 0; \
    robj *lenobj;   \
    INIT_LEN_OBJ

bool leftoverParsingReply(redisClient *c, char *x);

int parseUpdateOrReply(redisClient  *c,
                       int           tmatch,
                       char         *cname,
                       int           cmatchs[],
                       char         *vals   [],
                       unsigned int  vlens  []);

bool initLRUCS(int tmatch, int cmatchs[], int qcols);
bool initLRUCS_J(jb_t *jb);

void insertParse(cli *c, robj **argv, bool repl, int tmatch,
                 bool parse, sds *key);
void insertCommand   (redisClient *c);
void replaceCommand  (redisClient *c);
void sqlSelectCommand(redisClient *c);
void updateCommand   (redisClient *c);
void deleteCommand   (redisClient *c);

void addReplyRow(cli *c, robj *r, int tmatch, aobj *apk, uchar *lruc);

/* FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS */
void tscanCommand(redisClient *c);  /* scan.h does not exist */

void setDeferredMultiBulkLong(redisClient *c, void *node, long card);
void setDeferredMultiBulkError(redisClient *c, void *node, sds error);

#endif /*__ALSOSQL__H */ 

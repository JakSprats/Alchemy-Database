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

// LUA_SEL_FUNCS LUA_SEL_FUNCS LUA_SEL_FUNCS LUA_SEL_FUNCS LUA_SEL_FUNCS
typedef struct lfca {
    int n;     lue_t *l;  int  curr;
} lfca_t;

//USED for PREPARE/EXECUTE
int    getSizeWB    (          wob_t *wb);
uchar *serialiseWB  (          wob_t *wb);
int    deserialiseWB(uchar *x, wob_t *wb);

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

bool initLRUCS(int tmatch, int cmatchs[], int qcols);
bool initLRUCS_J(jb_t *jb);

void initLFCA   (lfca_t *lfca, list *ls);
void releaseLFCA(lfca_t *lfca);

void insertParse(cli *c,     robj **argv, bool repl, int tmatch,
                 bool parse, sds   *key);
void insertCommand   (redisClient *c);
void replaceCommand  (redisClient *c);
void sqlSelectCommand(redisClient *c);
void updateCommand   (redisClient *c);
void deleteCommand   (redisClient *c);

// FAST_EMBEDDED FAST_EMBEDDED FAST_EMBEDDED FAST_EMBEDDED FAST_EMBEDDED
#define INS_ERR 0
#define INS_INS 1
#define INS_UP  2
uchar insertCommit(cli  *c,      sds     uset,   sds     vals,  
                   int   ncols,  int     tmatch, int     matches,
                   int   inds[], int     pcols,  list   *cmatchl,
                   bool  repl,   uint32  upd,    uint32 *tsize,
                   bool  parse,  sds    *key);

bool sqlSelectBinary (cli   *c, int tmatch, bool cstar, int *cmatchs, int qcols,
                      cswc_t *w, wob_t *wb, bool need_cn, lfca_t *lfca);
bool sqlSelectInnards(cli *c, sds clist, sds from, sds tbl_list, sds where,
                      sds  wclause, bool chk, bool need_cn);

bool deleteInnards   (cli *c, sds tlist, sds wclause);
int  updateInnards   (cli *c, int tmatch, sds vallist, sds wclause,
                      bool fromup, aobj *u_apk);

/* FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS FILLERS */
void tscanCommand(redisClient *c);  /* scan.h does not exist */

#endif /*__ALSOSQL__H */ 

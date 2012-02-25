/*
 * This file implements the indexing logic of Alsosql
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

#ifndef __INDEX__H
#define __INDEX__H

#include "redis.h"

#include "btreepriv.h"
#include "parser.h"
#include "row.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"

void iAddUniq(bt *ibtr, uchar pktyp, aobj *apk, aobj *acol); // OBYI uses also

sds  getMCIlist(list *clist, int tmatch);        //NOTE: Used in DESC command
bool addC2MCI  (cli *c, icol_t ic, list *clist); //NOTE: Used in rdbLoad()

bool runLuaFunctionIndexFunc(cli *c, sds iconstrct, sds tname, sds  iname);
int  newIndex(cli    *c,     sds    iname, int  tmatch,    icol_t ic,
              list   *clist, uchar  cnstr, bool virt,      bool   lru,
              luat_t *luat,  icol_t obc,   bool prtl,      bool   lfu,
              uchar   dtype, sds    fname, sds  iconstrct, sds idestrct);
void createIndex(cli *c);

long buildIndex (cli *c, bt *btr, int imatch, long limit);

bool addToIndex (cli *c, bt *btr, aobj *apk,  void *rrow,   int imatch);

void delFromIndex       (bt *btr, aobj *apk,  void *rrow,   int imatch,
                                                                     bool gost);
void evictFromIndex     (bt *btr, aobj *apk,  void *rrow,   int imatch);

bool upIndex    (cli *c, bt *btr, aobj *aopk,  aobj *ocol, 
                                  aobj *anpk,  aobj *ncol,  int pktyp,
                                  aobj *oocol, aobj *nocol, int imatch);

void emptyIndex(cli *c, int inum);
void dropIndex (cli *c);

bool runFailableInsertIndexes  (cli *c,       bt  *btr, aobj *npk, void *nrow,
                                int  matches, int  inds[]);
void runLuaTriggerInsertIndexes(cli *c,       bt  *btr, aobj *npk, void *nrow,
                                int  matches, int  inds[]);
void runPreUpdateLuatriggers   (              bt  *btr, aobj *opk, void *orow,
                                int  matches, int  inds[]);
void runPostUpdateLuatriggers  (              bt  *btr, aobj *npk, void *nrow,
                                int  matches, int  inds[]);
void runDeleteIndexes          (              bt  *btr, aobj *opk, void *orow,
                                int matches,  int inds[], bool  wgost);
// LUA_INDEX_CALLBACKS
int luaAlchemySetIndex         (lua_State *lua);
int luaAlchemyUpdateIndex      (lua_State *lua);
int luaAlchemyDeleteIndex      (lua_State *lua);
int luaAlchemySetIndexByName   (lua_State *lua);
int luaAlchemyUpdateIndexByName(lua_State *lua);
int luaAlchemyDeleteIndexByName(lua_State *lua);
#endif /* __INDEX__H */ 

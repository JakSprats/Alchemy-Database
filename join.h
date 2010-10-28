/*
 * Implements jstore and join
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __JOINSTORE__H
#define __JOINSTORE__H

#include "adlist.h"
#include "redis.h"

#include "common.h"

void freeJoinRowObject(robj *o);
void freeAppendSetObject(robj *o);
void freeValSetObject(robj *o);

int multiColCheckOrReply(redisClient *c,
                         char        *clist,
                         int          j_tbls[],
                         int          j_cols[],
                         bool        *cntstr);

int parseIndexedColumnListOrReply(redisClient *c, char *ilist, int j_indxs[]);

void joinGeneric(redisClient *c,
                 redisClient *fc,
                 int          j_indxs[],
                 int          j_tbls [],
                 int          j_cols[],
                 int          n_ind, 
                 int          qcols, 
                 robj        *low,  
                 robj        *high, 
                 int          sto,
                 bool         sub_pk,
                 int          nargc,
                 robj        *nname,
                 int          obt,
                 int          obc,
                 bool         asc,
                 int          lim,
                 list        *inl,
                 bool         cntstr);


void jstoreCommit(redisClient *c,
                  int          sto,
                  robj        *low,  
                  robj        *high, 
                  robj        *nname,
                  int          j_indxs[MAX_JOIN_INDXS],
                  int          j_tbls [MAX_JOIN_INDXS],
                  int          j_cols [MAX_JOIN_INDXS],
                  int          n_ind,
                  int          qcols,
                  int          obt,
                  int          obc,
                  bool         asc,
                  int          lim,
                  list        *inl);

#endif /* __JOINSTORE__H */ 

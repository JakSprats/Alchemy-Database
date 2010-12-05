/*
 * Implements jstore and join
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

#ifndef __JOINSTORE__H
#define __JOINSTORE__H

#include "adlist.h"
#include "redis.h"

#include "sql.h"
#include "common.h"

typedef struct jqo {
    int t;
    int i;
    int c;
    int n;
} jqo_t;

typedef struct jrow_reply {
    redisClient  *c;
    redisClient  *fc;
    int          *jind_ncols;
    int           qcols;
    int           sto;
    bool          sub_pk;
    int           nargc;
    robj         *nname;
    jqo_t        *csort_order;
    bool          reordered;
    char         *reply;
    int           obt;
    int           obc;
    list         *ll;
    bool          ctype;
    bool          cstar;
} jrow_reply_t;

typedef struct build_jrow_reply {
    jrow_reply_t  j;
    int           n_ind;
    robj         *jk;
    ulong        *card;
    int          *j_indxs;
} build_jrow_reply_t;

void freeJoinRowObject(robj *o);
void freeAppendSetObject(robj *o);
void freeValSetObject(robj *o);

void joinGeneric(redisClient *c,
                 redisClient *fc,
                 jb_t        *jb,
                 bool         sub_pk,
                 int          nargc);

void jstoreCommit(redisClient *c, jb_t *jb);

#endif /* __JOINSTORE__H */ 

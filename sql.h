/*
 * This file implements the sql parsing routines for Alsosql
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ALSOSQL_SQL__H
#define __ALSOSQL_SQL__H

#include "redis.h"
#include "common.h"

#define CHECK_WHERE_CLAUSE_ERROR_REPLY(ret)                    \
    if      (which == 0) addReply(c, shared.selectsyntax);     \
    else if (which == 1) addReply(c, shared.deletesyntax);     \
    else if (which == 2) addReply(c, shared.updatesyntax);     \
    else                 addReply(c, shared.scanselectsyntax); \
    return ret;

#define ARGN_OVERFLOW                    \
    argn++;                              \
    if (argn == c->argc) {               \
        CHECK_WHERE_CLAUSE_ERROR_REPLY() \
    }

unsigned char checkSQLWhereClauseOrReply(redisClient  *c,
                                          robj       **key, 
                                          robj       **range,
                                          int         *imatch,
                                          int         *cmatch,
                                          int         *argn, 
                                          int          tmatch,
                                          bool         which);

void joinParseReply(redisClient *c, sds clist, int argn);

#endif /*__ALSOSQL_SQL__H */ 

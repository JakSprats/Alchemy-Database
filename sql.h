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

char *rem_backticks(char *token, int *len);

bool parseCreateTable(redisClient *c,
                      char          cnames[][MAX_COLUMN_NAME_SIZE],
                      int          *ccount,
                      int          *parsed_argn,
                      char         *o_token[]);

#define WHERE_CLAUSE_ERROR_REPLY(ret)                        \
    if      (sop == 0) addReply(c, shared.selectsyntax);     \
    else if (sop == 1) addReply(c, shared.deletesyntax);     \
    else if (sop == 2) addReply(c, shared.updatesyntax);     \
    else               addReply(c, shared.scanselectsyntax); \
    return ret;

#define ARGN_OVERFLOW(ret)            \
    argn++;                           \
    if (argn == c->argc) {            \
        WHERE_CLAUSE_ERROR_REPLY(ret) \
    }

unsigned char checkSQLWhereClauseReply(redisClient  *c,
                                       robj       **key, 
                                       robj       **range,
                                       int         *imatch,
                                       int         *cmatch,
                                       int         *argn, 
                                       int          tmatch,
                                       bool         sop,
                                       bool         just_parse,
                                       int         *oba,
                                       bool        *asc,
                                       int         *lim,
                                       bool        *store);

bool joinParseReply(redisClient  *c,
                    sds           clist,
                    int           argn,
                    int           j_indxs[],
                    int           j_tbls [],
                    int           j_cols [],
                    int          *qcols,
                    int          *sto,
                    robj        **newname,
                    robj        **range,
                    int          *n_ind,
                    int          *obt,
                    int          *obc,
                    bool         *asc,
                    int          *lim);

void joinReply(redisClient *c, sds clist, int argn);

#endif /*__ALSOSQL_SQL__H */ 

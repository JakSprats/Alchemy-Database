/*
COPYRIGHT: RUSS
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

void joinParseReply(redisClient *c, sds clist, int argn, int which);

#endif /*__ALSOSQL_SQL__H */ 

/*
 * This file implements the parsing of columns in SELECT and UPDATE statements
 * and "CREATE TABLE" parsing

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

#ifndef __ALSOSQL_COLPARSE__H
#define __ALSOSQL_COLPARSE__H

#include "redis.h"

#include "btreepriv.h"
#include "query.h"
#include "common.h"

void incrOffsetVar(redisClient *c, wob_t *wb, long incr);

bool parseU128 (char *s,             uint128 *x);
bool parseU128n(char *s, uint32 len, uint128 *x);

char *parseRowVals(sds vals,  char   **pk,        int    *pklen,
                   int ncols, twoint   cofsts[],  int     tmatch,
                   int pcols, int      cmatchs[], int     lncols);

bool parseCommaSpaceList(cli  *c,         char  *tkn,
                         bool  col_check, bool   tbl_check, bool  join_check,
                         bool  exact,     bool   isi,
        /* COL or TBL */ int   tmatch,    list  *cs,
        /* JOIN */       list *ts,        list *jans,       list *js,
                         int  *qcols,     bool  *cstar);

#define CMATCHS_FROM_CMATCHL                                                   \
    int  cmatchs[cmatchl->len];                                                \
    listNode *lnc;                                                             \
    int       ic  = 0;                                                         \
    listIter *lic = listGetIterator(cmatchl, AL_START_HEAD);                   \
    while((lnc = listNext(lic))) { cmatchs[ic] = (int)(long)lnc->value; ic++; }\
    listReleaseIterator(lic);

#define UPDATES_FROM_UPDATEL                                       \
    CMATCHS_FROM_CMATCHL                                           \
    char   *mvals  [mvalsl->len];                                  \
    ic = 0; lic = listGetIterator(mvalsl, AL_START_HEAD);          \
    while((lnc = listNext(lic))) { mvals[ic] = lnc->value; ic++; } \
    listReleaseIterator(lic);                                      \
    uint32  mvlens [mvlensl->len];                                 \
    ic = 0; lic = listGetIterator(mvlensl, AL_START_HEAD);         \
    while((lnc = listNext(lic))) {                                 \
        mvlens[ic] = (uint32)(long)lnc->value; ic++;               \
    }                                                              \
    listReleaseIterator(lic);                                      \
    listRelease(cmatchl); listRelease(mvalsl); listRelease(mvlensl);


bool parseSelect(cli  *c,  bool  is_scan, bool *no_wc, int  *tmatch,
                 list *cs, int  *qcols,   bool *join,  bool *cstar,
                 char *cl, char *from,    char *tlist, char *where, bool chk);

int parseUpdateColListReply(cli  *c,  int   tmatch, char *vallist,
                            list *cs, list *vals,   list *vlens);

uchar getExprType(char *pred, int plen);
int parseExpr(cli *c, int tmatch, int cmatch, char *val, uint32 vlen, ue_t *ue);
bool parseLuaExpr(int tmatch, int cmatch, char *val, uint32 vlen, lue_t *le);

bool parseColType(cli *c, sds type, uchar *col_type);
bool parseCreateTable(cli  *c,      list *ctypes, list  *cnames,
                      int  *ccount, sds   as_line);

// PREPARE_EXECUTE PREPARE_EXECUTE PREPARE_EXECUTE PREPARE_EXECUTE
int    getJTASize();
uchar *serialiseJTA  (int jtsize);
int    deserialiseJTA(uchar *x);

#endif /*__ALSOSQL_COLPARSE__H */ 

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
#include "alsosql.h"
#include "query.h"
#include "common.h"

void luasellistRelease(list *v);

void incrOffsetVar(redisClient *c, wob_t *wb, long incr);

bool parseU128 (char *s,             uint128 *x);
bool parseU128n(char *s, uint32 len, uint128 *x);

// INSERT
char *parseRowVals(sds vals,  char   **pk,        int  *pklen,
                   int ncols, twoint   cofsts[],  int   tmatch,
                   int pcols, icol_t  *ics,       int   lncols, bool *ai);

// SELECT
bool parseCSLSelect(cli  *c,         char  *tkn, 
                    bool  exact,     bool   isi, 
                    int   tmatch,    list  *cs,    list   *ls,
                    int  *qcols,     bool  *cstar);

bool parseCSLJoinTable(cli *c, char *tkn, list *ts, list  *jans);

bool parseCSLJoinColumns(cli  *c,     char  *tkn,  bool  exact,
                         list *ts,    list  *jans, list *js,
                         int  *qcols, bool  *cstar);

void init_ics(icol_t *ics, list *cmatchl);
#define CMATCHS_FROM_CMATCHL                          \
    icol_t ics[cmatchl->len]; init_ics(ics, cmatchl);

void init_mvals_mvlens(char   **mvals,  list *mvalsl,
                       uint32  *mvlens, list *mvlensl);
#define UPDATES_FROM_UPDATEL                                         \
    CMATCHS_FROM_CMATCHL                                             \
    char   *mvals  [mvalsl->len];                                    \
    uint32  mvlens [mvlensl->len];                                   \
    init_mvals_mvlens(mvals, mvalsl, mvlens, mvlensl);               \
    listRelease(cmatchl); listRelease(mvalsl); listRelease(mvlensl);

bool parseSelect(cli  *c,     bool    is_scan, bool *no_wc, int  *tmatch,
                 list *cs,    list   *ls,    int  *qcols, bool *join,
                 bool *cstar, char   *cl,      char *from,  char *tlist, 
                 char *where, bool    chk);

// UPDATE
int parseUpdateColListReply(cli  *c,  int   tmatch, char *vallist,
                            list *cs, list *vals,   list *vlens);

uchar getExprType(char *pred, int plen);
int parseExpr(cli *c, int tmatch, int cmatch, char *val, uint32 vlen, ue_t *ue);

// LUA_UPDATE
void initLUE   (lue_t *le, sds fname, list *lcs);
void releaseLUE(lue_t *le);
bool parseCommaListToAobjs(char *tkn, int tmatch, list *as);
bool checkOrCr8LFunc(int tmatch, lue_t *le, sds expr, bool cln);
bool parseLuaExpr(int tmatch, char *val, uint32 vlen, lue_t *le);

// CREATE_TABLE
bool parseColType(cli *c, sds type, uchar *col_type);
bool parseCreateTable(cli  *c,      list *ctypes, list  *cnames,
                      int  *ccount, sds   as_line);

// PREPARE_EXECUTE
int    getJTASize();
uchar *serialiseJTA  (int jtsize);
int    deserialiseJTA(uchar *x);

// REPLY
sds  getQueriedCnames(int tmatch, icol_t *ics, int qcols, lfca_t *lfca);
void setDMBcard_cnames(cli  *c,    cswc_t *w,    icol_t *ics, int qcols,
                       long  card, void   *rlen, lfca_t *lfca);
void setDMB_Join_card_cnames(cli *c, jb_t *jb, long card, void *rlen);

#endif /*__ALSOSQL_COLPARSE__H */ 

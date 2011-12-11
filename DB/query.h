/*
 * This file implements structures for query parsing
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

#ifndef __ALC_QUERY__H
#define __ALC_QUERY__H

#include "sds.h"
#include "adlist.h"
#include "dict.h"
#include "redis.h"

#include "btreepriv.h"
#include "xdb_common.h"
#include "common.h"

typedef struct r_col {
    sds   name; uchar type; bool  indxd; int imatch;
} r_col_t;

//TODO many of r_tbl's elements are optional -> bitmap + malloc(elements)
typedef struct r_tbl { // 114 bytes -> 120B
    sds      name;
    bt      *btr;
    int      vimatch;
    uint128  ainc;       // PK AUTO INCREMEMNT VALUE
    int      col_count;
    r_col_t *col;
    list    *ilist;      // USAGE: list of this table's imatch's
    dict    *cdict;      // USAGE: maps cname to cmatch
    uint32   n_intr;     /* LRU: num ACCESSES in current LRU interval */
    uint32   lastts;     /* LRU: HIGH-LOAD: last timestamp of lru interval */
    uint32   nextts;     /* LRU: HIGH-LOAD: next timestamp of lru interval */
    uint32   lrud;       /* LRU: timestamp & bool */
    int      lruc;       /* LRU: column containing LRU */
    int      lrui;       /* LRU: index containing LRU */
    uint32   nmci;       /* MCI: number of MultipleColumnIndexes */ //TODO bool?
    uint32   nltrgr;     /* LAUT: number of LuaTriggers */          //TODO bool?
    int      sk;         /* SK: index of shard-key column */
    int      fk_cmatch;  /* SK: Foreign-key local column */
    int      fk_otmatch; /* SK: Foreign-key other table's table */
    int      fk_ocmatch; /* SK: Foreign-key other table's column */
    bool     hashy;      /* HASH: adds "HASHABILITY" to the table */
    uint32   tcols;      /* HASH: on INSERT num new columns */
    sds     *tcnames;    /* HASH: on INSERT new column names */
    uint32   ctcol;      /* HASH: on INSERT current new columns */
    bool     lfu;        /* LFU: indexing on/off */
    int      lfuc;       /* LFU: column containing LFU */
    int      lfui;       /* LFU: index containing LFU */
} r_tbl_t;

//TODO bool's can all be in a bitmap
typedef struct r_ind { // 67 bytes -> 72B
    bt     *btr;     /* Btree of index                                     */
    sds     name;    /* Name of index                                      */
    int     table;   /* table index is ON                                  */
    int     column;  /* single column OR 1st MCI column                    */
    list   *clist;   /* MultipleColumnIndex(mci) list                      */
    int     nclist;  /* MCI: num columns                                   */
    int    *bclist;  /* MCI: array representation (for speed)              */
    bool    virt;    /* virtual                      - i.e. on primary key */
    uchar   cnstr;   /* CONSTRAINTS: [UNIQUE,,,]                           */
    bool    lru;     /* LRUINDEX                                           */
    bool    luat;    /* LUATRIGGER - call lua function per CRUD            */
    int     obc;     /* ORDER BY col                                       */
    bool    done;    /* CREATE INDEX OFFSET -> not done until finished     */
    long    ofst;    /* CREATE INDEX OFFSET partial indexes current offset */
    bool    lfu;     /* LFUINDEX                                           */
    bool    iposon;  /* Index Position On (i.e. SELECT "index.pos()"       */
    uint32  cipos;   /* Current Index position, when iposon                */
} r_ind_t;

typedef struct update_expression {
    bool  yes;
    char  op;
    int   c1match;
    int   type;
    char *pred;
    int   plen;
} ue_t;

typedef struct lua_update_expression {
    bool  yes;
    sds   fname;
    int   ncols;
    int  *cmatchs;
} lue_t;

typedef struct filter {
    int      jan;    /* JoinAliasNumber filter runs on (for JOINS)        */
    int      imatch; /* index  filter runs on (for JOINS)                 */
    int      tmatch; /* table  filter runs on (for JOINS)                 */
    int      cmatch; /* column filter runs on                             */
    enum OP  op;     /* operation filter applies [,=,!=]                  */

    bool     iss;    /* is string, WHERE fk = 'fk' (1st iss=0, 2nd iss=1) */
    sds      key;    /* RHS of filter (e.g. AND x < 7 ... key=7)          */
    aobj     akey;   /* value of KEY [sds="7",int=7,float=7.0]            */

    sds      low;    /* LHS of Range filter (AND x BETWEEN 3 AND 4 -> 3)  */
    aobj     alow;   /* value of LOW  [sds="3",int=3,float=3.0]           */
    sds      high;   /* RHS of Range filter (AND x BETWEEN 3 AND 4 -> 4)  */
    aobj     ahigh;  /* value of HIGH [sds="4",int=4,float=4.0]           */

    list    *inl;    /* WHERE ..... AND x IN (1,2,3)                      */
    list    *klist;  /* MCI list of matching (ordered) keys (as f_t) */
} f_t;

typedef struct lua_trigger_command {
    sds   fname;
    int   ncols;
    int  *cmatchs;
    bool  tblarg;
} ltc_t;
typedef struct lua_trigger {
    ltc_t     add;
    ltc_t     del;
    ushort16  num; /* Index[num] */
} luat_t;

typedef struct where_clause_order_by {
    uint32  nob;                       /* number ORDER BY columns             */
    int     obc[MAX_ORDER_BY_COLS];    /* ORDER BY col                        */
    int     obt[MAX_ORDER_BY_COLS];    /* ORDER BY tbl -> JOINS               */
    bool    asc[MAX_ORDER_BY_COLS];    /* ORDER BY ASC/DESC                   */
    long    lim;                       /* ORDER BY LIMIT                      */
    long    ofst;                      /* ORDER BY OFFSET                     */
    sds     ovar;                      /* OFFSET varname - used by cursors    */
} wob_t;

typedef struct check_sql_where_clause {
    uchar   wtype;
    sds     token;
    sds     lvr;     /* Leftover AFTER parse                    */
    f_t     wf;      /* WhereClause Filter (i.e. i,c,t,low,inl) */
    list   *flist;   /* FILTER list (nonindexed cols in WC)     */
} cswc_t;

typedef struct order_by_sort_element {
    void   *row;
    void  **keys;
    aobj   *apk;
    uchar  *lruc;
    bool    lrud;
    uchar  *lfuc;
    bool    lfu;
} obsl_t;

typedef struct join_column {
    int t; int c; int jan;
} jc_t;
typedef struct index_join_pair {
    enum OP  op;
    f_t      lhs;     /* LeftHandSide  table,column,index */
    f_t      rhs;     /* RIGHTHandSide table,column,index */
    list    *flist;   /* Filter-lists are per JOIN LEVEL */
    uint32   nrows;   /* number of rows */
    int      kimatch; /* MCI imatch */
} ijp_t;
typedef struct jb_t {
    bool    cstar;
    int     qcols;
    sds     lvr;                /* Leftover AFTER parse                       */
    jc_t   *js;                 /* Queried tables & columns & join-aliases    */
    uint32  n_jind;             /* num 2ndary Join-Indexes                    */
    int     hw;                 /* "highwater" line JIndexes become Filters   */
    ijp_t  *ij;                 /* list of Join-Indexes                       */
    list   *mciflist;           /* missing MCI indexes as FILTERS             */
    wob_t   wb;                 /* ORDER BY [c1,c2 DESC] LIMIT x OFFSET y     */
    list   *fflist;             /* Deepest Join-Level FILTER list on RHS      */
    list   *fklist;             /* Deepest Join-Level Keylist on RHS          */
    int     fkimatch;           /* Deepest Join-Level MCI imatch              */
    uint32  fnrows;             /* Deepest Join-Level FILTER's number of rows */
    obsl_t *ob;                 /* ORDER BY values                            */
} jb_t;

void init_wob(wob_t *wb);
void destroy_wob(wob_t *wb);
void init_check_sql_where_clause(cswc_t *w, int tmatch, sds token);
void destroyINLlist(list **inl);
void releaseFlist(list **flist);
void destroyFlist(list **flist);
void destroy_check_sql_where_clause(cswc_t *w);

void init_join_block(jb_t *jb);
void destroy_join_block(cli *c, jb_t *jb);

typedef struct string_and_length {
    char *s;
    int   len;
    bool  freeme;
    uchar type;
} sl_t;
void release_sl(sl_t sl);

#endif /*__ALC_QUERY__H */ 

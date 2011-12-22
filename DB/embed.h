/*
 * This file implements the API for embedding ALCHEMY_DATABASE
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

#ifndef ALCHEMY_EMBED__H
#define ALCHEMY_EMBED__H

#include "redis.h"

#include "xdb_common.h"

struct aobj;

typedef struct embedded_response_t {
    int           retcode; /* REDIS_OK, REDIS_ERR */
    int           ncols;   /* SELECTed column named count */
    char        **cnames;  /* SELECTed column names */
    int           nobj;    /* number of returned objects */
    struct aobj **objs;    /* returned objects */
} eresp_t;

void initEmbeddedAlchemy(); // NOTE: used for UNSAFE access
void embedded_exit();

void embeddedSaveSelectedColumnNames(int tmatch, int cmatchs[], int qcols);
struct jb_t;
void embeddedSaveJoinedColumnNames(struct jb_t *jb);

eresp_t *e_alchemy_raw    (char *sql,             select_callback *scb);
eresp_t *e_alchemy        (int argc, robj **argv, select_callback *scb);
eresp_t *e_alchemy_no_free(int argc, robj **argv, select_callback *scb);

// VERSION_2 VERSION_2 VERSION_2 VERSION_2 VERSION_2 VERSION_2 VERSION_2
enum E_OP {INSERT, UPDATE, DELETE, SELECT, SCAN, EXECUTE, EXEC_BIN}; // SQL
enum R_OP {SET, GET, DEL};                                           // REDIS

typedef struct ereq_t {
    enum            E_OP op;                   // SQL
    enum            R_OP rop;                  // REDIS
    // SQL
    sds             tablelist;
    sds             insert_value_string;
    sds             select_column_list;
    sds             update_set_list;
    sds             where_clause;
    bool            save_queried_column_names;
    // REDIS
    sds             redis_key;                 // REDIS GET/DET/DEL
    sds             redis_value;               // REDIS SET
    // EXECUTE
    sds             plan_name;                 // EXECUTE
    int             eargc;                     // EXECUTE
    sds            *eargv;                     // EXECUTE
    uchar          *exec_bin;                  // EXECUTE
    // SELECT/SCAN CALLBACK
    select_callback *scb;
} ereq_t;
void init_ereq   (ereq_t *ereq);
void release_ereq(ereq_t *ereq);

eresp_t *e_alchemy_sql_fast(ereq_t *ereq);

eresp_t *e_alchemy_thin_select(uchar qtype,  int tmatch, int cmatch, int imatch,
                               enum OP op,   int qcols,
                               uint128 keyx, long keyl,  int keyi,
                               int *cmatchs, bool cstar, select_callback *scb,
                               bool save_cnames);

eresp_t *e_alchemy_redis(ereq_t *ereq);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void printEmbedResp(eresp_t *ersp);

#endif /* ALCHEMY_EMBED__H */

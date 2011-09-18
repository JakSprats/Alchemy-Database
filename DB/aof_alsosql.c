/*
 * This file implements ALCHEMY_DATABASE's AOF writing
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "redis.h"

#include "luatrigger.h"
#include "bt_iterator.h"
#include "index.h"
#include "colparse.h"
#include "alsosql.h"
#include "common.h"
#include "aof_alsosql.h"

extern int      Num_tbls     [MAX_NUM_TABLES];
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern int      Num_indx;
extern r_ind_t  Index[MAX_NUM_INDICES];

extern char    *Col_type_defs[];
extern uchar    OutputMode;

bool appendOnlyDumpIndices(FILE *fp, int tmatch) {
    //printf("appendOnlyDumpIndices: fp: %p tmatch: %d\n", fp, tmatch);
    char cmd_INDEX[]  = "*6\r\n$6\r\nCREATE\r\n$5\r\nINDEX\r\n";
    char cmd_UINDEX[] = "*7\r\n$6\r\nCREATE\r\n$6\r\nUNIQUE\r\n$5\r\nINDEX\r\n";
    char cmd_LUAT[]   = "*6\r\n$6\r\nCREATE\r\n$10\r\nLUATRIGGER\r\n";
    char cmd_LUAT_D[] = "*7\r\n$6\r\nCREATE\r\n$10\r\nLUATRIGGER\r\n";
    char c_on[]       = "$2\r\nON\r\n";
    sds  tname        = Tbl[tmatch].name->ptr;
    MATCH_INDICES(tmatch)
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri   = &Index[inds[i]];
        if (ri->virt) continue;
        luat_t  *luat = (luat_t *)ri->btr;/* NOTE: only used for LUATRIGGER */

        char *cmd;
        if (ri->luat) cmd = (luat->del.ncols) ? cmd_LUAT_D : cmd_LUAT;
        else          cmd = UNIQ(ri->cnstr)   ? cmd_UINDEX : cmd_INDEX;
        if (fwrite(cmd, strlen(cmd), 1, fp) == 0)                 return 0;

        sds s = ri->obj->ptr;
        if (fwriteBulkString(fp, s, sdslen(s)) == -1)                 return 0;
        if (fwrite(c_on, sizeof(c_on) - 1, 1, fp) == 0)               return 0;
        s     = tname;
        if (fwriteBulkString(fp, s, sdslen(s)) == -1)                 return 0;

         if (ri->luat) {        /* LUA_TRIGGER */
            luat_t *luat  = (luat_t *)ri->btr;
            sds     alist = getLUATlist(&luat->add, tmatch);/* DESTROY ME 075 */
            if (fwriteBulkString(fp, alist, sdslen(alist)) == -1)     return 0;
            sdsfree(alist);                              /* DESTROYED 075 */
            if (luat->del.ncols) {
                sds dlist = getLUATlist(&luat->del, tmatch); /* DESTROY ME 076*/
                if (fwriteBulkString(fp, dlist, sdslen(dlist)) == -1) return 0;
                sdsfree(dlist);                              /* DESTROYED 076 */
            }
        } else if (ri->clist) { /* MCI */
            sds mlist = getMCIlist(ri->clist, tmatch);   /* DESTROY ME 051*/
            if (fwriteBulkString(fp, mlist, sdslen(mlist)) == -1)     return 0;
            sdsfree(mlist);                              /* DESTROYED 051 */
        } else {                /* NORMAL INDEX */
            sds cname = Tbl[tmatch].col_name[ri->column]->ptr;
            sds c_w_p = sdscatprintf(sdsempty(), "(%s)", cname); //DEST 074
            if (fwriteBulkString(fp, c_w_p, sdslen(c_w_p)) == -1)     return 0;
            sdsfree(c_w_p);                              /* DESTROYED 074 */
        }
    }
    return 1;
}

bool appendOnlyDumpTable(FILE *fp, bt *btr, int tmatch) {
    //printf("appendOnlyDumpTable: tmatch: %d\n", tmatch);
    sds tname  = Tbl[tmatch].name->ptr;
    /* Dump Table definition */
    char cmd[] = "*4\r\n$6\r\nCREATE\r\n$5\r\nTABLE\r\n";
    if (fwrite(cmd,sizeof(cmd)-1,1,fp) == 0)                          return 0;
    if (fwriteBulkString(fp, tname, sdslen(tname)) == -1)             return 0;
    /* create single column_def string in format (col type,,,,) */
    sds s = sdsnewlen("(", 1);
    for (int i = 0; i < Tbl[tmatch].col_count; i++) {
        if (i) s = sdscatlen(s, ",", 1);
        sds cname = Tbl[tmatch].col_name[i]->ptr;
        s = sdscatprintf(s, "%s %s", cname,
                           Col_type_defs[Tbl[tmatch].col_type[i]]);
    }
    s = sdscatlen(s, ")", 1);
    if (fwriteBulkString(fp, s, sdslen(s)) == -1)                     return 0;
    sdsfree(s);

    uchar orig_OutputMode = OutputMode;
    OutputMode            = OUTPUT_NORMAL; /* REDIS output not OK here */
    if (btr->numkeys) { /* Dump Table DATA */
        int  cmatchs[MAX_COLUMN_PER_TABLE];
        btEntry *be;
        char cmd2[]  = "*5\r\n$6\r\nINSERT\r\n$4\r\nINTO\r\n";
        char cvals[] = "$6\r\nVALUES\r\n";
        int  qcols   = get_all_cols(tmatch, cmatchs, 1);
        btSIter *bi  = btGetFullRangeIter(btr, 1);
        while ((be = btRangeNext(bi, 1)) != NULL) {
            if (fwrite(cmd2 ,sizeof(cmd2) - 1, 1, fp) == 0)       goto aoft_err;
            if (fwriteBulkString(fp, tname, sdslen(tname)) == -1) goto aoft_err;
            if (fwrite(cvals ,sizeof(cvals) - 1, 1, fp) == 0)     goto aoft_err;
            aobj *apk  = be->key;
            void *rrow = be->val;
            robj *r    = outputRow(btr, rrow, qcols, cmatchs, apk, tmatch);
            sds   srow = sdscatprintf(sdsempty(), "(%s)", (char *)r->ptr);
            if (fwriteBulkString(fp, srow, sdslen(srow)) == -1)   goto aoft_err;
            sdsfree(srow);
            decrRefCount(r);
        }
        btReleaseRangeIterator(bi);
    }
    OutputMode = orig_OutputMode; /* set orig_OutputMode BACK */
    return 1;

aoft_err:
    OutputMode = orig_OutputMode; /* set orig_OutputMode BACK */
    return 0;
}

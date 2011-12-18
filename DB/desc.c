/*
 * This file implements ALCHEMY_DATABASEs DESC & DUMP commands
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
#include <unistd.h>
#include <errno.h>

#include "redis.h"

#include "debug.h"
#include "luatrigger.h"
#include "bt.h"
#include "bt_iterator.h"
#include "index.h"
#include "colparse.h"
#include "find.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "desc.h"

extern char    *Col_type_defs[];

extern int      Num_tbls;
extern r_tbl_t *Tbl;
extern int      Num_indx;
extern r_ind_t *Index;

extern uchar    OutputMode;

#define ADD_REPLY_BULK(r, buf)    \
    r = _createStringObject(buf); \
    addReplyBulk(c, r);           \
    decrRefCount(r);              \
    card++;

#define FILE_DUMP_SUCCESS "SUCCESS: DUMPED: %llu bytes in %ld rows to file: %s"
#define FILE_DUMP_FAILURE "FAILURE: DUMPED: %llu bytes in %ld rows to file: %s"
#define SINGLE_LINE       "*1\r\n$%lu\r\n%s\r\n"
#define EMPTY_DUMP2FILE   "-ERR: table is empty\r\n"

sds createAlterTableFulltext(r_tbl_t *rt, r_ind_t *ri, int cmatch, bool nl) {
    sds tname = rt->name;
    sds cname = rt->col[cmatch].name;
    sds iname = !cmatch ? P_SDS_EMT "%s_%s_%s", tname, cname, INDEX_DELIM) :
                          ri->name;
    sds s     = sdscatprintf(sdsempty(),
                                "ALTER TABLE `%s` ADD FULLTEXT %s (%s);%s",
                                  tname, iname, cname, nl ? "\n" : "");
    if (!cmatch) sdsfree(iname);
    return s;
}
sds dumpSQL_Index(char *mtname, r_tbl_t *rt, r_ind_t *ri, int tmatch, bool nl) {
   bool text_ind = C_IS_S(rt->col[ri->column].type);
    if (text_ind) return createAlterTableFulltext(rt, ri, ri->column, nl);
    else {
        char *cmd   = UNIQ(ri->cnstr) ? "CREATE UNIQUE INDEX" : "CREATE INDEX";
        char *tname = mtname ? mtname : rt->name;
        sds   s     = sdscatprintf(sdsempty(), "%s %s ON %s ",
                                              cmd, ri->name, tname);
        if (ri->clist) { /* MCI */
            sds mlist = getMCIlist(ri->clist, tmatch);   /* DESTROY ME 051*/
            s = sdscatlen(s, mlist, sdslen(mlist));
            sdsfree(mlist);                              /* DESTROYED 051 */
        } else {         /* NORMAL INDEX */
            sds cname = rt->col[ri->column].name;
            sds c_w_p = sdscatprintf(sdsempty(), "(%s)", cname); //DEST 074
            s = sdscatlen(s, c_w_p, sdslen(c_w_p));
            sdsfree(c_w_p);                              /* DESTROYED 074 */
        }
        s = nl ? sdscatlen(s, ";\n", 2) : sdscatlen(s, ";", 1) ;
        return s;
    }
}
/* SYNTAX:
     1.) DUMP table
     2.) DUMP table TO MYSQL [mysql_table]
     3.) DUMP table TO FILE filename        */
void sqlDumpCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    bool err = 0;
    if (c->argc > 5 || c->argc == 3) err = 1;
    else if ((c->argc > 3)) {
        sds arg3 = c->argv[3]->ptr;
        if ((strcasecmp(c->argv[2]->ptr, "TO") ||
            (strcasecmp(arg3, "MYSQL") && strcasecmp(arg3, "FILE")))) err = 1;
        else if (!strcasecmp(arg3, "FILE") && (c->argc < 5)) err = 1;
    }
    if (err) { addReply(c, shared.dump_syntax); return; }

    bt       *btr     = getBtr(tmatch);
    r_tbl_t *rt       = &Tbl[tmatch];
    char     *tname   = rt->name;
    void     *rlen    = addDeferredMultiBulkLength(c);
    long      card    = 0;
    bool      toms    = 0;
    bool      tof     = 0;
    char     *mtname = NULL;
    char     *fname   = NULL;
    if (c->argc > 3 && !strcasecmp(c->argv[3]->ptr, "FILE")) {
        tof   = 1;
        fname = c->argv[4]->ptr;
    } else if (c->argc > 3 && !strcasecmp(c->argv[3]->ptr, "MYSQL")) {
        robj *r;
        toms = 1;
        if (c->argc > 4) mtname = c->argv[4]->ptr;
        else             mtname = tname;
        bool text_pk = C_IS_S(rt->col[0].type);
        snprintf(buf, 191, "DROP TABLE IF EXISTS `%s`;", mtname);
        buf[191] = '\0';
        ADD_REPLY_BULK(r, buf)
        snprintf(buf, 191, "CREATE TABLE `%s` ( ", mtname);
        buf[191] = '\0';
        r = _createStringObject(buf);
        for (int i = 0; i < rt->col_count; i++) {
            r->ptr = sdscatprintf(r->ptr, "%s %s %s%s",
                                        (i) ? "," : "",
                                        rt->col[i].name,
                                        Col_type_defs[rt->col[i].type],
                                        (!i && !text_pk) ? " PRIMARY KEY" : "");
        }
        r->ptr = sdscat(r->ptr, ");");
        addReplyBulk(c, r); decrRefCount(r); card++;
        if (text_pk) {// MYSQL can not index "TEXT" columns directly w/o length
            sds s = createAlterTableFulltext(rt, NULL, 0, 0);
            r     = createObject(REDIS_STRING, s);
            addReplyBulk(c, r); decrRefCount(r); card++;
        }
        snprintf(buf, 191, "LOCK TABLES `%s` WRITE;", mtname);
        buf[191] = '\0';
        ADD_REPLY_BULK(r, buf)
    }

    FILE *fp    = NULL;
    ull   bytes = 0;
    if (btr->numkeys) {
        if (tof) {
            if((fp = fopen(fname, "w")) == NULL) {
                sds err = sdscatprintf(sdsempty(),
                                  "-ERR failed to open: %s\r\n", fname);
                setDeferredMultiBulkError(c, rlen, err); return;
            }
        }
        btEntry *be;
        bool   ok       = 1;
        uchar  o_out   = OutputMode;
        OutputMode     = OUTPUT_NORMAL; /* REDIS output not OK here */
        list  *cmatchl = listCreate();
        int    qcols   = get_all_cols(tmatch, cmatchl, 1, 1);
        CMATCHS_FROM_CMATCHL
        btSIter *bi    = btGetFullRangeIter(btr, 1);
        while ((be = btRangeNext(bi, 1))) {
            aobj *apk  = be->key;
            void *rrow = be->val;
            robj *r    = outputRow(btr, rrow, qcols, cmatchs, apk, tmatch);
            if (toms) {
                snprintf(buf, 191, "INSERT INTO `%s` VALUES (", mtname);
                buf[191]  = '\0';
                robj *ins = _createStringObject(buf);
                ins->ptr  = sdscatlen(ins->ptr, r->ptr, sdslen(r->ptr));
                ins->ptr  = sdscatlen(ins->ptr, ");", 2);
                addReplyBulk(c, ins);
                decrRefCount(ins);
            } else if (tof) {
                if ((fwrite(r->ptr, sdslen(r->ptr), 1, fp) == 0) ||
                    ((fwrite("\n", 1, 1, fp) == 0))) {
                    sds s   = sdscatprintf(sdsempty(), FILE_DUMP_FAILURE,
                                                            bytes, card, fname);
                    sds err = sdscatprintf(sdsempty(), SINGLE_LINE,
                                                            sdslen(s), s);
                    setDeferredMultiBulkError(c, rlen, err); 
                    sdsfree(s); fclose(fp); decrRefCount(r);
                    ok = 0; break;
                }
                bytes += sdslen(r->ptr) + 1;
            } else addReplyBulk(c, r);
            decrRefCount(r); card++;
        } btReleaseRangeIterator(bi);
        OutputMode = o_out; listRelease(cmatchl);
        if (!ok) return;
    }

    if (toms) { // for MYSQL: Dump the table's indexes also
        robj *r;
        MATCH_INDICES(tmatch)
        for (int i = 0; i < matches; i++) {
            r_ind_t *ri = &Index[inds[i]];
            if (ri->virt || ri->luat) continue;
            sds      s  = dumpSQL_Index(mtname, rt, ri, tmatch, 0);
            r           = createObject(REDIS_STRING, s);
            addReplyBulk(c, r); decrRefCount(r); card++;
        }
        char *buf2 = "UNLOCK TABLES;"; ADD_REPLY_BULK(r, buf2)
    }
    if (tof) {
        if (fp) {
            sds s   = sdscatprintf(sdsempty(), FILE_DUMP_SUCCESS,
                                                bytes, card, fname);
            sds err = sdscatprintf(sdsempty(), SINGLE_LINE, sdslen(s), s);
            setDeferredMultiBulkError(c, rlen, err); 
            sdsfree(s); fclose(fp);
        } else {
            sds err = sdsnewlen(EMPTY_DUMP2FILE, strlen(EMPTY_DUMP2FILE));
            setDeferredMultiBulkError(c, rlen, err); 
        }
    } else setDeferredMultiBulkLength(c, rlen, card);
}
 
ull get_sum_all_index_size_for_table(int tmatch) {
    ull isize = 0;
    MATCH_INDICES(tmatch)
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri = &Index[inds[i]];
        if (!ri->virt && !ri->luat) {
            bt *ibtr = getIBtr(inds[i]); isize += ibtr->msize;
        }
    }
    return isize;
}

static void outputAdvancedIndexInfo(redisClient *c, int tmatch, long *card) {
    MATCH_INDICES(tmatch)
    if (!matches) return;
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri = &Index[inds[i]];
        if (ri->luat) {
            luat_t *luat = (luat_t *)ri->btr;
            sds     acmd = getLUATlist(&luat->add, tmatch);/* DESTROY ME 077 */
            sds     desc = sdscatprintf(sdsempty(), "LUATRIGGER: %s [ADD: %s]",
                                         ri->name, acmd);
            sdsfree(acmd);                               /* DESTROYED 077 */
            if (luat->del.ncols) {
                sds dcmd = getLUATlist(&luat->del, tmatch); /* DESTROY ME 078*/
                desc     = sdscatprintf(desc, " [DEL: %s]", dcmd);
                sdsfree(dcmd);                           /* DESTROYED 078 */
            }
            robj *r    = createObject(REDIS_STRING, desc);
            addReplyBulk(c, r); decrRefCount(r); INCR(*card)
        }
    }
}
static void outputPartialIndex(int tmatch, int imatch, robj *r) {
    r_tbl_t *rt = &Tbl  [tmatch];
    r_ind_t *ri = &Index[imatch];
    double perc = (((double)ri->ofst / (double)rt->btr->numkeys) * 100.00);
    r->ptr      = sdscatprintf(r->ptr, " [completed: %ld/%d -> %.4g%%]",
                                       ri->ofst, rt->btr->numkeys, perc);
}

void descCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    void    *rlen = addDeferredMultiBulkLength(c);
    long     card = 0;
    r_tbl_t *rt   = &Tbl[tmatch];
    for (int j = 0; j < rt->col_count; j++) {
        robj *r      = createObject(REDIS_STRING, NULL);
        r->ptr       = sdscatprintf(sdsempty(), "%s | %s",
                                    rt->col[j].name,
                                    Col_type_defs[rt->col[j].type]);
        if (rt->sk == j) r->ptr = sdscat(r->ptr, " - SHARDKEY");
        if (rt->fk_cmatch == j) {
            r_tbl_t *ort = &Tbl[rt->fk_otmatch];
            r->ptr = sdscatprintf(r->ptr, " - FOREIGN KEY REFERENCES %s (%s)",
                                  ort->name, ort->col[rt->fk_ocmatch].name);
        }
        if (rt->ilist) {
            listNode *ln; 
            int       loops = 0;
            listIter *li    = listGetIterator(rt->ilist, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                int imatch = (int)(long)ln->value;
                r_ind_t *ri      = &Index[imatch];
                if (ri->column != j) continue;
                ull      isize   = 0;
                if (!ri->virt && !ri->luat) {
                    bt *ibtr = getIBtr(imatch); isize = ibtr->msize;
                }
                sds idesc = NULL;
                if (ri->clist) idesc = getMCIlist(ri->clist, tmatch);/*DEST051*/
                r->ptr = sdscatprintf(r->ptr, 
                                      "%s%s%sINDEX: %s%s%s [BYTES: %lld]",
                                        loops     ? ", "       : " - ", 
                                        ri->cnstr ? " UNIQUE " : "",
                                        ri->done  ? ""         : " PARTIAL ",
                                        ri->name,
                                        idesc     ? " "        : "",
                                        idesc     ? idesc      : "",
                                        isize);
                if (idesc) sdsfree(idesc);               /* DESTROYED 051 */
                if (ri->lru) r->ptr = sdscatprintf(r->ptr, " - LRUINDEX");
                if (ri->lfu) r->ptr = sdscatprintf(r->ptr, " - LFUINDEX");
                if (ri->obc != -1) {
                    r->ptr = sdscatprintf(r->ptr, " - ORDER BY %s",
                                          rt->col[ri->obc].name);
                }
                if (!ri->done) outputPartialIndex(tmatch, imatch, r);
                loops++;
            } listReleaseIterator(li);
        }
        addReplyBulk(c, r);
        decrRefCount(r);
	card++;
    }

    outputAdvancedIndexInfo(c, tmatch, &card);

    aobj mink, maxk;
    ull   index_size = get_sum_all_index_size_for_table(tmatch);
    bt   *btr        = getBtr(tmatch);
    if (btr->numkeys) { assignMinKey(btr, &mink); assignMaxKey(btr, &maxk); }
    else              { initAobj(&mink);          initAobj(&maxk);          }

    sds s = sdsempty();                                  // FREEME 102(1)
    if        (C_IS_S(mink.type)) {
        sds mins = sdsnewlen(mink.s, (mink.len > 64) ? 64 : mink.len); //FREE103
        sds maxs = sdsnewlen(maxk.s, (mink.len > 64) ? 64 : mink.len); //FREE104
        s = sdscatprintf(s, "INFO: KEYS: [NUM: %d MIN: %s MAX: %s]",
                             btr->numkeys, mins, maxs);
        sdsfree(mins); sdsfree(maxs);                       // FREED 103 & 104
    } else if (C_IS_F(mink.type)) {
        s = sdscatprintf(s, "INFO: KEYS: [NUM: %d MIN: %f MAX: %f]",
                             btr->numkeys, mink.f, maxk.f);
    } else {
        uchar   kt  = mink.type;
        uint128 min = C_IS_I(kt) ? mink.i : C_IS_L(kt) ? mink.l : mink.x;
        uint128 max = C_IS_I(kt) ? maxk.i : C_IS_L(kt) ? maxk.l : maxk.x;
        if C_IS_X(kt) {
            char c_xmin[64]; char c_xmax[64]; 
            SPRINTF_128(c_xmin, 64, min) SPRINTF_128(c_xmax, 64, max)
            s = sdscatprintf(s, "INFO: KEYS: [NUM: %d MIN: %s MAX: %s]",
                               btr->numkeys, c_xmin, c_xmax);
        } else s = sdscatprintf(s, "INFO: KEYS: [NUM: %d MIN: %lu MAX: %lu]",
                               btr->numkeys, (ulong)min, (ulong)max);
    }
    s = sdscatprintf(s, " BYTES: [BT-TOTAL: %ld [BT-DATA: %ld] INDEX: %lld]]%s",
                        btr->msize, btr->dsize, index_size,
                        rt->hashy ? " - HASHABILITY" : "");
    robj *r = createObject(REDIS_STRING, s);             // FREEME 102(2)
    addReplyBulk(c, r); decrRefCount(r);                 // FREED 102
    card++;

    setDeferredMultiBulkLength(c, rlen, card);
    dump_bt_mem_profile(btr);
}

void print_mem_usage(int tmatch) {
    ull  index_size = get_sum_all_index_size_for_table(tmatch);
    bt  *btr        = getBtr(tmatch);
    ull  tot_data   = btr->msize;
    ull  pure_data  = btr->dsize;
    ull  bt_ovrhd   = tot_data - pure_data;
    ull  tot_mem    = btr->msize + index_size;
    sds  tname      = Tbl[tmatch].name;
    printf("Table: %s TotMem: %llu Components: " \
           "[RowData: %llu BtreeOverhead: %llu Indexes: %llu]\n",
           tname, tot_mem, pure_data, bt_ovrhd, index_size);
}

void showCommand(redisClient *c) {
    if        (!strcasecmp("TABLES",  c->argv[1]->ptr)) {
        void *rlen = addDeferredMultiBulkLength(c);
        long  card = 0;
        for (int i = 0; i < Num_tbls; i++) {
            r_tbl_t *rt = &Tbl[i];
            if (!rt->name) continue;
            card++;
            robj *r = _createStringObject(rt->name);
            if (rt->sk != -1) {
                r->ptr = sdscatprintf(r->ptr, " - SHARDKEY: %s",
                                      rt->col[rt->sk].name);
            }
            if (rt->fk_cmatch != -1) {
                r_tbl_t *ort = &Tbl[rt->fk_otmatch];
                r->ptr = sdscatprintf(r->ptr, " - FK_REF: %s (%s)",
                                   ort->name, ort->col[rt->fk_ocmatch].name);
            }
            addReplyBulk(c, r);
            decrRefCount(r);
        }
        setDeferredMultiBulkLength(c, rlen, card);
    } else if (!strcasecmp("INDEXES", c->argv[1]->ptr)) {
        void *rlen = addDeferredMultiBulkLength(c);
        long  card = 0;
        for (int i = 0; i < Num_indx; i++) {
            r_ind_t *ri = &Index[i];
            if (!ri->name) continue;
            r_tbl_t *rt = &Tbl[ri->table];
            card++;
            sds s = sdscatprintf(sdsempty(), "%s ->(%s.%s)",
                                 ri->name, rt->name, rt->col[ri->column].name);
            robj *r = createObject(REDIS_STRING, s);
            addReplyBulk(c, r);
            decrRefCount(r);
        }
        setDeferredMultiBulkLength(c, rlen, card);
    } else {
        addReply(c, shared.show_syntax);
    }
}

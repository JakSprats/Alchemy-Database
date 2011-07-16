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

#include "lua_integration.h"
#include "bt.h"
#include "bt_iterator.h"
#include "index.h"
#include "colparse.h"
#include "alsosql.h"
#include "aobj.h"
#include "common.h"
#include "desc.h"

extern char    *Col_type_defs[];

extern int      Num_tbls;
extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern int      Num_indx;
extern r_ind_t  Index[MAX_NUM_INDICES];

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

/* SYNTAX:
     1.) DUMP table
     2.) DUMP table TO MYSQL [mysql_table]
     3.) DUMP table TO FILE filename        */
void sqlDumpCommand(redisClient *c) {
    char buf[192];
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)
    bool err = 0;
    if (c->argc > 5 || c->argc == 3) {
        err = 1;
    } else if ((c->argc > 3)) {
        if ((strcasecmp(c->argv[2]->ptr, "TO") ||
               (strcasecmp(c->argv[3]->ptr, "MYSQL") &&
                strcasecmp(c->argv[3]->ptr, "FILE")))) {
            err = 1;
        } else if (!strcasecmp(c->argv[3]->ptr, "FILE") && (c->argc < 5)) {
            err = 1;
        }
    }
    if (err) { addReply(c, shared.dump_syntax); return; }

    bt   *btr      = getBtr(tmatch);
    char *tname    = Tbl[tmatch].name->ptr;
    void *rlen     = addDeferredMultiBulkLength(c);
    long  card     = 0;
    bool  toms     = 0;
    bool  tof      = 0;
    char *m_tname  = NULL;
    char *fname    = NULL;
    if (c->argc > 3 && !strcasecmp(c->argv[3]->ptr, "FILE")) {
        tof   = 1;
        fname = c->argv[4]->ptr;
    } else if (c->argc > 3 && !strcasecmp(c->argv[3]->ptr, "MYSQL")) {
        robj *r;
        toms = 1;
        if (c->argc > 4) m_tname = c->argv[4]->ptr;
        else             m_tname = tname;
        snprintf(buf, 191, "DROP TABLE IF EXISTS `%s`;", m_tname);
        buf[191] = '\0';
        ADD_REPLY_BULK(r, buf)
        snprintf(buf, 191, "CREATE TABLE `%s` ( ", m_tname);
        buf[191] = '\0';
        r = _createStringObject(buf);
        for (int i = 0; i < Tbl[tmatch].col_count; i++) {
            bool b = (C_IS_I(Tbl[tmatch].col_type[i]));
            r->ptr = sdscatprintf(r->ptr, "%s %s %s%s",
                              (!i) ? "" : ",",
                              (char *)Tbl[tmatch].col_name[i]->ptr,
                              b ? "INT" : (i == 0) ? "VARCHAR(512)" : "TEXT",
                              (i == 0) ? " PRIMARY KEY" : "");
        }
        r->ptr = sdscat(r->ptr, ");");
        addReplyBulk(c, r);
        decrRefCount(r);
        card++;
        snprintf(buf, 191, "LOCK TABLES `%s` WRITE;", m_tname);
        buf[191] = '\0';
        ADD_REPLY_BULK(r, buf)
    }

    FILE *fp    = NULL;
    ull   bytes = 0;
    if (btr->numkeys) {
        int cmatchs[MAX_COLUMN_PER_TABLE];
        if (tof) {
            if((fp = fopen(fname, "w")) == NULL) {
                sds err = sdscatprintf(sdsempty(),
                                  "-ERR failed to open: %s\r\n", fname);
                setDeferredMultiBulkError(c, rlen, err);
                return;
            }
        }
        uchar orig_OutputMode = OutputMode;
        OutputMode            = OUTPUT_NORMAL; /* REDIS output not OK here */
        btEntry *be;
        int      qcols = get_all_cols(tmatch, cmatchs, 1);
        btSIter *bi    = btGetFullRangeIter(btr);
        while ((be = btRangeNext(bi)) != NULL) {      // iterate btree
            aobj *apk  = be->key;
            void *rrow = be->val;
            robj *r    = outputRow(btr, rrow, qcols, cmatchs, apk, tmatch);
            if (toms) {
                snprintf(buf, 191, "INSERT INTO `%s` VALUES (", m_tname);
                buf[191] = '\0';
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
                    sdsfree(s);
                    fclose(fp);
                    OutputMode = orig_OutputMode; /* set orig_OutputMode BACK */
                    return;
                }
                bytes += sdslen(r->ptr) + 1;
            } else {
                addReplyBulk(c, r);
            }
            decrRefCount(r);
            card++;
        }
        btReleaseRangeIterator(bi);
        OutputMode = orig_OutputMode; /* set orig_OutputMode BACK */
    }

    if (toms) {
        robj *r;
        char *buf2 = "UNLOCK TABLES;";
        ADD_REPLY_BULK(r, buf2)
    }
    if (tof) {
        if (fp) {
            sds s       = sdscatprintf(sdsempty(), FILE_DUMP_SUCCESS,
                                                    bytes, card, fname);
            sds err = sdscatprintf(sdsempty(), SINGLE_LINE, sdslen(s), s);
            setDeferredMultiBulkError(c, rlen, err); 
            sdsfree(s);
            fclose(fp);
        } else {
            sds err = sdsnewlen(EMPTY_DUMP2FILE, strlen(EMPTY_DUMP2FILE));
            setDeferredMultiBulkError(c, rlen, err); 
        }
    } else {
        setDeferredMultiBulkLength(c, rlen, card);
    }
}
 
ull get_sum_all_index_size_for_table(int tmatch) {
    ull isize = 0;
    for (int i = 0; i < Num_indx; i++) {
        r_ind_t *ri = &Index[i];
        if (!ri->virt && !ri->luat && ri->table == tmatch) {
            bt   *ibtr  = getIBtr(i);
            isize      += ibtr->msize;
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
                                         (char *)ri->obj->ptr, acmd);
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

void descCommand(redisClient *c) {
    TABLE_CHECK_OR_REPLY(c->argv[1]->ptr,)

    void    *rlen = addDeferredMultiBulkLength(c);
    long     card = 0;
    r_tbl_t *rt   = &Tbl[tmatch];
    for (int j = 0; j < rt->col_count; j++) {
        robj *r      = createObject(REDIS_STRING, NULL);
        r->ptr       = sdscatprintf(sdsempty(), "%s | %s",
                                    (char *)rt->col_name[j]->ptr,
                                    Col_type_defs[rt->col_type[j]]);
        if (rt->sk == j) r->ptr = sdscat(r->ptr, " - SHARDKEY");
        if (rt->fk_cmatch == j) {
            r_tbl_t *ort = &Tbl[rt->fk_otmatch];
            r->ptr = sdscatprintf(r->ptr, " - FOREIGN KEY REFERENCES %s (%s)",
                                  (char *)ort->name->ptr,
                                  (char *)ort->col_name[rt->fk_ocmatch]->ptr);
        }
        int   imatch = find_index(tmatch, j);
        if (imatch != -1) {
            int loops = 0;
            while (1) {
                r_ind_t *ri      = &Index[imatch];
                int      nimatch = find_next_index(tmatch, j, imatch);
                ull      isize   = 0;
                if (!ri->virt && !ri->luat) {
                    bt *ibtr = getIBtr(imatch);
                    isize    = ibtr->msize;
                }
                sds idesc = NULL;
                if (ri->clist) idesc = getMCIlist(ri->clist, tmatch);/*DEST051*/
                r->ptr = sdscatprintf(r->ptr, "%s%sINDEX: %s%s%s [BYTES: %lld]",
                                      loops ? ", " : " - ", 
                                      ri->cnstr ? " UNIQUE " : "",
                                      (char *)ri->obj->ptr,
                                      idesc ? " " : "", idesc ? idesc : "",
                                      isize);
                if (idesc) sdsfree(idesc);               /* DESTROYED 051 */
                if (ri->lru) r->ptr = sdscatprintf(r->ptr, " - LRUINDEX");
                if (nimatch == -1) break;
                else               imatch = nimatch;
                loops++;
            }
        }
        addReplyBulk(c, r);
        decrRefCount(r);
	card++;
    }

    outputAdvancedIndexInfo(c, tmatch, &card);

    aobj minkey, maxkey;
    ull   index_size = get_sum_all_index_size_for_table(tmatch);
    bt   *btr        = getBtr(tmatch);
    if (btr->numkeys) {
        assignMinKey(btr, &minkey);
        assignMaxKey(btr, &maxkey);
    } else {
        initAobj(&minkey);
        initAobj(&maxkey);
    }

    char buf[256];
    if (C_IS_S(minkey.type)) {
        sds mins = sdsnewlen(minkey.s, (minkey.len > 64) ? 64 : minkey.len);
        sds maxs = sdsnewlen(maxkey.s, (minkey.len > 64) ? 64 : minkey.len);
        snprintf(buf, 255, "INFO: KEYS: [NUM: %d MIN: %s MAX: %s]" \
                          " BYTES: [BT-TOTAL: %ld [BT-DATA: %ld] INDEX: %lld]]",
                btr->numkeys, mins, maxs, btr->msize, btr->dsize, index_size);
        buf[255] = '\0';
        sdsfree(mins);
        sdsfree(maxs);
    } else {
        ulong min = C_IS_I(minkey.type) ? minkey.i : minkey.l; //TODO FLOAT
        ulong max = C_IS_I(minkey.type) ? maxkey.i : maxkey.l; //TODO FLOAT
        snprintf(buf, 255, "INFO: KEYS: [NUM: %d MIN: %lu MAX: %lu]" \
                          " BYTES: [BT-TOTAL: %ld [BT-DATA: %ld] INDEX: %lld]]",
               btr->numkeys, min, max, btr->msize,
               btr->dsize, index_size);
        buf[255] = '\0';
    }
    robj *r = _createStringObject(buf); addReplyBulk(c, r); decrRefCount(r);
    card++;

    setDeferredMultiBulkLength(c, rlen, card);
    dump_bt_mem_profile(btr);
}

void showCommand(redisClient *c) {
    if        (!strcmp("TABLES",  c->argv[1]->ptr)) {
        void *rlen = addDeferredMultiBulkLength(c);
        long  card = 0;
        for (int i = 0; i < Num_tbls; i++) {
            r_tbl_t *rt = &Tbl[i];
            if (!rt->name) continue;
            card++;
            robj *r = _createStringObject(Tbl[i].name->ptr);
            if (rt->sk != -1) {
                r->ptr = sdscatprintf(r->ptr, " - SHARDKEY: %s",
                                      (char *)rt->col_name[rt->sk]->ptr);
            }
            if (rt->fk_cmatch != -1) {
                r_tbl_t *ort = &Tbl[rt->fk_otmatch];
                r->ptr = sdscatprintf(r->ptr, " - FK_REF: %s (%s)",
                                  (char *)ort->name->ptr,
                                  (char *)ort->col_name[rt->fk_ocmatch]->ptr);
            }
            addReplyBulk(c, r);
            decrRefCount(r);
        }
        setDeferredMultiBulkLength(c, rlen, card);
    } else if (!strcmp("INDEXES", c->argv[1]->ptr)) {
        void *rlen = addDeferredMultiBulkLength(c);
        long  card = 0;
        for (int i = 0; i < Num_indx; i++) {
            r_ind_t *ri = &Index[i];
            if (!ri->obj) continue;
            r_tbl_t *rt = &Tbl[ri->table];
            card++;
            sds s = sdscatprintf(sdsempty(), "%s ->(%s.%s)",
                                 (char *)ri->obj->ptr, (char *)rt->name->ptr, 
                                 (char *)rt->col_name[ri->column]->ptr);
            robj *r = createObject(REDIS_STRING, s);
            addReplyBulk(c, r);
            decrRefCount(r);
        }
        setDeferredMultiBulkLength(c, rlen, card);
    } else {
        addReply(c, shared.show_syntax);
    }
}

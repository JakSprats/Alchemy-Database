/* B-tree Implementation.
 *
 * Implements in memory b-tree tables with insert/del/replace/find/ ops

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.

 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <strings.h>

#include "redis.h"

#include "row.h"
#include "btree.h"
#include "bt_iterator.h"
#include "bt.h"
#include "parser.h"
#include "stream.h"
#include "aobj.h"
#include "common.h"

/* Abstract-BTREE Prototypes */
static bt   *abt_create(uchar ktype, int num, uchar btype);
static void  abt_destroy(bt *nbtr, bt *btr);

static int intCmp(void *s1, void *s2) {
    return (long)s1 - (long)s2;
}

bt *btCreate(uchar ktype, int num, uchar btype) {
    return abt_create(ktype, num, btype);
}
robj *createBtreeObject(uchar ktype, int num, uchar btype) { /* Data & Index */
    bt *btr = btCreate(ktype, num, btype);
    return createObject(REDIS_BTREE, btr);
}
robj *createEmptyBtreeObject() {                           /* Virtual indices */
    return createObject(REDIS_BTREE, NULL);
}
static bt *createInodeIntBt() {
    bt *btr    = bt_create(intCmp, TRANSITION_ONE_BTREE_BYTES);
    btr->ktype = COL_TYPE_INT;
    btr->btype = BTREE_INDEX_NODE;
    btr->num   = -1;
    return btr;
}
bt *createIndexNode(uchar pktype) {                          /* INODE_BT */
    if (pktype == COL_TYPE_INT) return createInodeIntBt();
    else                        return btCreate(pktype, -1, BTREE_INDEX_NODE);
}

void btDestroy(bt *nbtr, bt *btr) {
    abt_destroy(nbtr, btr);
}
void freeBtreeObject(robj *o) {
    bt *btr = (bt *)(o->ptr);
    if (!btr) return; /* virtual indices have a NULL here */
    btDestroy(btr, NULL);
}

//TODO the following 3 functions should go into bt_code.c
static void destroy_bt_node(bt *btr, bt_n *n) {
    for (int i = 0; i < n->n; i++) {
        void *be    = KEYS(btr, n)[i];
        int   ssize = getStreamMallocSize(be, btr);
        if (btr->btype == BTREE_INDEX) { /* Index is BT of IndexNodeBTs */
            uchar *stream = be;
            skipToVal(&stream);
            bt    **nbtr  = (bt **)stream;
            destroy_bt_node(*nbtr, (*nbtr)->root);
            bt_free_btree(*nbtr, btr);      /* memory management in btr(Index)*/
        }
        if (!INODE(btr)) bt_free(be, btr, ssize);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            destroy_bt_node(btr, NODES(btr, n)[i]);
        }
    }
    bt_free_btreenode(n, btr); /* memory management in btr */
}

/* bt_release -> dont destroy data, just btree */
static void bt_release(bt *btr, bt_n *n) {
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            bt_release(btr, NODES(btr, n)[i]);
        }
    }
    bt_free_btreenode(n, btr); /* memory management in btr */
}

static void bt_to_bt_insert(bt *nbtr, bt *obtr, bt_n *n) {
    for (int i = 0; i < n->n; i++) {
        char *be = KEYS(obtr, n)[i];
        bt_insert(nbtr, be);
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            bt_to_bt_insert(nbtr, obtr, NODES(obtr, n)[i]);
        }
    }
}
          
/* ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE ABSTRACT-BTREE */
static bt *abt_create(uchar ktype, int num, uchar btype) {
    bt *btr    = bt_create(btStreamCmp, TRANSITION_ONE_BTREE_BYTES);
    btr->ktype = ktype;
    btr->btype = btype;
    btr->num   = num;
    return btr;
}

static void abt_destroy(bt *nbtr, bt *btr) {
    if (nbtr->root) {
        destroy_bt_node(nbtr, nbtr->root);
        nbtr->root  = NULL;
    }
    bt_free_btree(nbtr, btr); /* memory management in btr */
}

#define DECLARE_BT_KEY \
    bool  med; uchar sflag; uint32 ksize;                                    \
    char *btkey = createBTKey(akey, &med, &sflag, &ksize, btr); /*FREE 026*/ \
    if (!btkey) return 0;
    
static bool abt_replace(bt *btr, aobj *akey, void *val) {
    uint32 ssize;
    DECLARE_BT_KEY
    char  *nstream = createStream(btr, val, btkey, ksize, &ssize);
    uchar *ostream = bt_replace(btr, btkey, nstream);
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return destroyStream(btr, ostream);
}

static void *abt_find_val(bt *btr, aobj *akey) {
    DECLARE_BT_KEY
    uchar *stream = bt_find(btr, btkey);
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return parseStream(stream, btr);
}

static bool abt_del(bt *btr, aobj *akey) {
    DECLARE_BT_KEY
    uchar *stream = bt_delete(btr, btkey);               /* FREED 028 */
    destroyBTKey(btkey, med);                            /* FREED 026 */
    return destroyStream(btr, stream);                   /* DESTROYED 027 */
}

static uint32 abt_insert(bt *btr, aobj *akey, void *val) {
    if (btr->numkeys == TRANSITION_ONE_MAX) {
        btr = abt_resize(btr, TRANSITION_TWO_BTREE_BYTES);
    }
    uint32 ssize;
    DECLARE_BT_KEY
    char *stream = createStream(btr, val, btkey, ksize, &ssize); /*DESTROY 027*/
    destroyBTKey(btkey, med);                            /* FREED 026 */
    bt_insert(btr, stream);                              /* FREE ME 028 */
    return ssize;
}

bt *abt_resize(bt *obtr, int new_size) {
     bt *nbtr         = bt_create(obtr->cmp, new_size);
     nbtr->ktype      = obtr->ktype;
     nbtr->btype      = obtr->btype;
     nbtr->num        = obtr->num;
     nbtr->data_size  = obtr->data_size;
    if (obtr->root) {
        bt_to_bt_insert(nbtr, obtr, obtr->root); /* 1.) copy from old to new */
        bt_release(obtr, obtr->root);            /* 2.) release old */
        memcpy(obtr, nbtr, sizeof(bt));          /* 3.) overwrite old w/ new */
        free(nbtr);                              /* 4.) free new */
    } //bt_dump_info(obtr, obtr->ktype);
    return obtr;
}

/* DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA */
int   btAdd(bt *btr, aobj *apk, void *val) { return abt_insert(btr, apk, val); }
void  btReplace(bt *btr, aobj *apk, void *val) { abt_replace(btr, apk, val); }
void *btFindVal(bt *btr, aobj *apk) { return abt_find_val(btr, apk); }
int   btDelete( bt *btr, aobj *apk) { return abt_del(btr, apk); }

/* INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX INDEX */
void btIndAdd(bt *ibtr, aobj *akey, bt *nbtr) { abt_insert(ibtr, akey, nbtr); }
bt *btIndFindVal(bt *ibtr, aobj *akey) { return abt_find_val(ibtr, akey); }
int btIndDelete(bt *ibtr, aobj *akey) {
    abt_del(ibtr, akey);
    return ibtr->numkeys;
}

/* INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE INDEX_NODE */
void btIndNodeAdd(   bt *nbtr, aobj *apk) { abt_insert(nbtr, apk, NULL); }
int  btIndNodeDelete(bt *nbtr, aobj *apk) {
    abt_del(nbtr, apk);
    return nbtr->numkeys;
}

/* JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT */
/* JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT JOIN_BT */
#define INIT_JOIN_BTREE_BYTES 1024
void btReleaseJoinRangeIterator(btIterator *iter);

static int intJoinRowCmp(void *a, void *b) {
    joinRowEntry *ja = (joinRowEntry *)a;
    joinRowEntry *jb = (joinRowEntry *)b;
    robj         *ra = ja->key;
    robj         *rb = jb->key;
    int ia, ib;
    if (ra->encoding == REDIS_ENCODING_RAW) ia = atoi(ra->ptr);
    else                                    ia = (int)(long)ra->ptr;
    if (rb->encoding == REDIS_ENCODING_RAW) ib = atoi(rb->ptr);
    else                                    ib = (int)(long)rb->ptr;
    return (ia == ib) ? 0 : (ia < ib) ? -1 : 1;
}

static int strJoinRowCmp(void *a, void *b) {
    joinRowEntry *ja = (joinRowEntry *)a;
    joinRowEntry *jb = (joinRowEntry *)b;
    robj         *ra = ja->key;
    robj         *rb = jb->key;
    return strcmp(ra->ptr, rb->ptr);
}

static int floatJoinRowCmp(void *a, void *b) {
    joinRowEntry *ja = (joinRowEntry *)a;
    joinRowEntry *jb = (joinRowEntry *)b;
    robj         *ra = ja->key;
    robj         *rb = jb->key;
    float         fa = atof(ra->ptr);
    float         fb = atof(rb->ptr);
    float         f  = fa - fb;
    return (f == 0.0) ? 0 : ((f > 0.0) ? 1: -1);
}

bt *createJoinResultSet(uchar pkt) {
    bt *btr = NULL; /* compiler warning */
    if (       pkt == COL_TYPE_INT) {
        btr = bt_create(intJoinRowCmp,   INIT_JOIN_BTREE_BYTES);
    } else if (pkt == COL_TYPE_STRING) {
        btr = bt_create(strJoinRowCmp,   INIT_JOIN_BTREE_BYTES);
    } else if (pkt == COL_TYPE_FLOAT) {
        btr = bt_create(floatJoinRowCmp, INIT_JOIN_BTREE_BYTES);
    }
    return btr;
}

void *btJoinFindVal(bt *jbtr, joinRowEntry *key) {
    return bt_find(jbtr, key);
}
int btJoinAddRow(bt *jbtr, joinRowEntry *key) {
    if (bt_find(jbtr, key)) return DICT_ERR;
    bt_insert(jbtr, key);
    return DICT_OK;
}

int btJoinDeleteRow(bt *jbtr, joinRowEntry *key) {
    bt_delete(jbtr, key);
    return jbtr->numkeys;
}

static void emptyJoinBtNode(bt   *jbtr,
                            bt_n *n,
                            int   ncols,
                            bool  is_ob,
                            void (*freer)(list *s, int ncols, bool is_ob)) {
    for (int i = 0; i < n->n; i++) {
        joinRowEntry *be  = KEYS(jbtr, n)[i];
        list         *val = be->val;
        freer(val, ncols, is_ob);      /* free list of ind_rows (cols,sizes) */
        decrRefCount(be->key);         /* free jk */
        free(be);                      /* free jre */
    }
    if (!n->leaf) {
        for (int i = 0; i <= n->n; i++) {
            emptyJoinBtNode(jbtr, NODES(jbtr, n)[i], ncols, is_ob, freer);
        }
    }
    bt_free_btreenode(n, jbtr); /* memory management in btr */
}

void btJoinRelease(bt   *jbtr,
                   int   ncols,
                   bool  is_ob,
                   void (*freer)(list *s, int ncols, bool is_ob)) {
    if (jbtr->root) {
        emptyJoinBtNode(jbtr, jbtr->root, ncols, is_ob, freer);
        jbtr->root = NULL;
        bt_free_btree(jbtr, NULL);
    }
}

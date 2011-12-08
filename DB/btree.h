/*-
 * Copyright 1997, 1998, 2001 John-Mark Gurney.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

#ifndef _BTREE_H_
#define _BTREE_H_

#include "adlist.h" 

#include "common.h" 

struct btree;
struct btreenode;

#define UINTSIZE   4
#define VOIDSIZE   8 /* force to 8, otherwise UU would not work on 32bit */
#define ULONGSIZE  8
#define U128SIZE  16

typedef struct btree_specification { /* size 7B */
    unsigned char   ktype;    /* [STRING,INT,FLOAT,LONG]--------------------| */
    unsigned char   btype;    /* [data,index,node]                          | */
    unsigned char   ksize;    /* INODE_I(4), UU&INDEX(8), UL&LU(12), LL(16) | */
    unsigned short  bflag;    /* [OTHER_BT + BTFLAG_*_INDEX]                | */
    unsigned short  num;      /*--------------------------------------------| */
} __attribute__ ((packed)) bts_t;

typedef void * bt_data_t;
typedef int (*bt_cmp_t)(bt_data_t k1, bt_data_t k2);

void bt_incr_dsize(struct btree *ibtr, size_t size);
void bt_decr_dsize(struct btree *ibtr, size_t size);

void *bt_malloc(        struct btree *btr,                       int size);
void  bt_free(          struct btree *btr, void *v,              int size);
void  bt_free_btreenode(struct btree *btr, struct btreenode *x);
void  bt_free_btree(    struct btree *btr);

struct btree *bt_create(bt_cmp_t cmp, unsigned char trans, bts_t *s);
void      bt_insert (struct btree *btr, bt_data_t k);
bt_data_t bt_delete (struct btree *btr, bt_data_t k);
bt_data_t bt_replace(struct btree *btr, bt_data_t k, bt_data_t val);

void  bt_dump_info(printer *prn, struct btree *btr);
void  bt_dumptree(printer *prn, struct btree *btr, unsigned char is_index);

bt_data_t  bt_max    (struct btree *btr);
bt_data_t  bt_min    (struct btree *btr);
bt_data_t  bt_find    (struct btree *btr, bt_data_t k);
bt_data_t *bt_find_loc(struct btree *btr, bt_data_t k);

struct btIterator;
struct btreenode;
int  bt_init_iterator(struct btree *bre, bt_data_t k, struct btIterator *iter);
int  bt_find_closest_slot(struct btree *btr, struct btreenode *x, bt_data_t k);

void bt_to_bt_insert(struct btree *nbtr,
                     struct btree *obtr, struct btreenode *x);

void bt_destroy   (struct btree *btr);
void bt_release   (struct btree *btr, struct btreenode *x);

void dump_bt_mem_profile(struct btree *btr);

int bt_checktree(struct btree *btr, void *kmin, void *kmax);
#endif /* _BTREE_H_ */

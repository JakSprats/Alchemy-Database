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
 *	$Id: btree.h,v 1.4.2.1 2001/03/28 06:17:12 jmg Exp $
 *
 */

#ifndef _BTREE_H_
#define _BTREE_H_

struct btree;

#define	BT_SIZEDEF	128

typedef void * bt_data_t;
typedef int (*bt_cmp_t)(bt_data_t, bt_data_t);

struct btree *bt_create(bt_cmp_t, int nodesize);
void *bt_malloc(       int,    struct btree *);
void  bt_free(         void *, struct btree *, int);
void bt_free_btreenode(void *, struct btree *);
void bt_free_btree(    void *, struct btree *);

void  bt_insert(struct btree *, bt_data_t);
void *bt_delete(struct btree *, bt_data_t);

void  bt_dumptree(struct btree *, int, int);
void  bt_treestats(struct btree *);
int   bt_checktree(struct btree *, bt_data_t, bt_data_t );

void *bt_max(struct btree *);
void *bt_min(struct btree *);
void *bt_find(struct btree *, bt_data_t);

struct btIterator;
struct btreenode;
int  bt_init_iterator(struct btree *, bt_data_t, struct btIterator *);
int  bt_find_closest_slot(struct btree *, struct btreenode *, bt_data_t );

#endif /* _BTREE_H_ */

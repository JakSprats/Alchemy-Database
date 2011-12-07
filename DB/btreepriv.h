/*-
 * Copyright 1997-1999, 2001 John-Mark Gurney.
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
 */

#ifndef _BTREEPRIV_H_
#define _BTREEPRIV_H_

#include "btree.h"

#define TRANS_ONE 1
#define TRANS_TWO 2
#define TRANS_ONE_MAX 64

#define BTFLAG_NONE          0
#define BTFLAG_AUTO_INC      1
#define BTFLAG_UINT_UINT     2
#define BTFLAG_UINT_ULONG    4
#define BTFLAG_ULONG_UINT    8
#define BTFLAG_ULONG_ULONG  16
#define BTFLAG_UINT_PTR     32 /* UINT  Index */
#define BTFLAG_ULONG_PTR    64 /* ULONG Index */
#define BTFLAG_OBC         128 /* ORDER BY Index */

typedef struct btree { // 60 Bytes -> 64B
    struct btreenode  *root;
    bt_cmp_t           cmp;
 
    unsigned long      msize;
    unsigned long      dsize;

    unsigned int       numkeys;  /* --- 8 bytes | */
    unsigned int       numnodes; /* ------------| */

    unsigned short     keyofst;  /* --- 8 bytes | */ //TODO can be computed
    unsigned short     nodeofst; /*             | */ //TODO can be computed
    unsigned short     nbyte;    /*             | */
    unsigned short     kbyte;    /* ------------| */

    unsigned char      t;        /* --------------------------- 8 bytes | */
    unsigned char      nbits;    /*                                     | */
    bts_t              s;        /*-------------------------------------| */

    unsigned int       dirty_left; // 4 bytes
    unsigned char      dirty;      // NOTE: if ANY btn in btr is dirty
} __attribute__ ((packed)) bt;

//#define BTREE_DEBUG
typedef struct btreenode { // 8 bytes
    unsigned int  scion;       /* 4 billion scion possible */
    int           n     : 30;  /* 1 billion entries (per bt_n)*/
    int           leaf  : 1;
    int           dirty : 1;
#ifdef BTREE_DEBUG
    unsigned long num;
#endif
} bt_n;

#define KEYS_PER_BTN 128
#define DIRTY_BITMAP_BYTES_PER_BTN ((int)(KEYS_PER_BTN/8))
typedef struct btreenode_dirty_stream_section { // 24 Bytes
    void *ds;
    char  btmp[DIRTY_BITMAP_BYTES_PER_BTN];
} __attribute__ ((packed)) bds_t;

void *KEYS(bt *btr, bt_n *x, int i);
#define NODES(btr, x) ((bt_n **)((char *)x + btr->nodeofst))

#define GET_BTN_SIZE(leaf)   \
  size_t size  = leaf  ? btr->kbyte           : btr->nbyte; 
#define GET_BTN_MSIZE(dirty) \
  size_t msize = dirty ? size + sizeof(bds_t) : size;
#define GET_BTN_SIZES(leaf, dirty) \
    GET_BTN_SIZE(leaf) \
    GET_BTN_MSIZE(dirty)
#define GET_DS(x, size) (*((void **)((char *)x + size)))
#define GET_DS_FROM_BTN(x) \
    GET_BTN_SIZE(x->leaf) \
    uint32 *ds = GET_DS(x, size);

bt_n *findminnode(bt *btr, bt_n *x);

#endif /* _BTREEPRIV_H_ */

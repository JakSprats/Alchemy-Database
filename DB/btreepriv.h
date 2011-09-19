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

/* SIZE: 2ptr(16), 2L(16), 2INT(8), 4Ushort(8), 2uchar(2) 1BTS(6)-> 56B */
typedef struct btree {
    struct btreenode  *root;
    bt_cmp_t           cmp;
 
    unsigned long      msize;
    unsigned long      dsize;

    unsigned int       numkeys;  /* --- 8 bytes | */
    unsigned int       numnodes; /* ------------| */

    unsigned short     keyofst;  /* --- 8 bytes | */
    unsigned short     nodeofst; /*             | */
    unsigned short     nbyte;    /*             | */
    unsigned short     kbyte;    /* ------------| */

    unsigned char      t;        /* --------------------------- 8 bytes | */
    unsigned char      nbits;    /*                                     | */
    bts_t              s;        /*-------------------------------------| */
} __attribute__ ((packed)) bt;

//#define BTREE_DEBUG
typedef struct btreenode { /* 8 bytes */
    unsigned int scion;       /* 4 billion scion possible */
    int          n    : 31;   /* 2 billion entries (per bt_n)*/
    int          leaf : 1;
#ifdef BTREE_DEBUG
    unsigned long num;
#endif
} bt_n;

void *KEYS(bt *btr, bt_n *x, int i);
#define NODES(btr, x) ((bt_n **)((char *)x + btr->nodeofst))

#endif /* _BTREEPRIV_H_ */

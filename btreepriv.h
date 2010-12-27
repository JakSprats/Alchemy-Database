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

//TODO: benchmark HOW 4096 outperforms 128 as IndexNodeBT size
//       also does 32 make sense as TRANSITION_ONE_MAX
#define TRANSITION_ONE_MAX            31
#define TRANSITION_ONE_BTREE_BYTES   128
#define TRANSITION_TWO_BTREE_BYTES  4096

/* SIZE: 2ptr(16), 5shrt(10), 3uchar(3), 2LL(24) + 2int(8) -> 53bytes */
typedef struct btree {
    struct btreenode *root;
    bt_cmp_t           cmp;
 
    unsigned short     keyoff;
    unsigned short     nodeptroff;
    unsigned short     t;
    unsigned short     textra;
    unsigned char      nbits;

    unsigned char      ktype; /* [STRING,INT,FLOAT] */
    unsigned char      btype; /* [data,index,node] */
    short              num;

    unsigned long long malloc_size;
    unsigned long long data_size;

    unsigned int       numkeys;
    unsigned int       numnodes;
} __attribute__ ((packed)) bt;

typedef struct btreenode { /* 2 bytes */
    int    n    : 31;      /* 2 billion entries */
    int    leaf : 1;
} bt_n;

#define KEYS(btr, x)     ((void **)((char *)x + btr->keyoff))
#define NODES(btr, x)    ((struct btreenode **)((char *)x + btr->nodeptroff))

#endif /* _BTREEPRIV_H_ */

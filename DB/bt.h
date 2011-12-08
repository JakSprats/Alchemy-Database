/* B-tree Implementation.
 *
 * This file implements in memory b-tree with insert/del/replace/find/ ops
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

#ifndef __ALSO_SQL_BT_H
#define __ALSO_SQL_BT_H

#include "row.h"
#include "btreepriv.h"
#include "aobj.h"
#include "common.h"

bt   *abt_resize(bt *obtr, uchar trans);

bt   *createUUBT(int num, uchar btype);
bt   *createLUBT(int num, uchar btype);
bt   *createULBT(int num, uchar btype);
bt   *createLLBT(int num, uchar btype);

bt   *createMCI_MIDBT(uchar ktype, int imatch);
bt   *createIndexBT  (uchar ktype, int imatch);
bt   *createMCIndexBT(list *clist, int imatch);
bt   *createDBT      (uchar ktype, int tmatch);

/* different Btree types */
#define BTREE_TABLE    0
#define BTREE_INDEX    1
#define BTREE_INODE    2
#define BTREE_MCI      3
#define BTREE_MCI_MID  4
#define BT_MCI_UNIQ    5

/* INT Inodes have been optimised */
#define INODE_I(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_I(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC)) 
/* LONG Inodes have been optimised */
#define INODE_L(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_L(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC)) 
/* U128 Inodes have been optimised */
#define INODE_X(btr) \
  (btr->s.btype == BTREE_INODE && C_IS_X(btr->s.ktype) && \
   !(btr->s.bflag & BTFLAG_OBC)) 
#define INODE(btr) (INODE_I(btr) || INODE_L(btr) || INODE_X(btr))

/* UU tables containing ONLY [PK=INT,col1=INT]  have been optimised */
#define UU(btr) (btr->s.bflag == BTFLAG_UINT_UINT)
#define UU_SIZE 8

/* UL tables containing ONLY [PK=INT,col1=LONG] have been optimised */
typedef struct uint_ulong_key {
    uint32 key; ulong  val;
}  __attribute__ ((packed)) ulk;
#define UL(btr) (btr->s.bflag & BTFLAG_UINT_ULONG)
#define UL_SIZE 12

/* LU tables containing ONLY [PK=LONG,col1=INT] have been optimised */
typedef struct ulong_uint_key {
    ulong  key; uint32 val;
}  __attribute__ ((packed)) luk;
#define LU(btr) (btr->s.bflag & BTFLAG_ULONG_UINT)
#define LU_SIZE 12

/* LL tables containing ONLY [PK=LONG,col1=LONG] have been optimised */
typedef struct ulong_ulong_key {
    ulong key; ulong val;
}  __attribute__ ((packed)) llk;
#define LL(btr) (btr->s.bflag & BTFLAG_ULONG_ULONG)
#define LL_SIZE 16

// START: 128 bit (16 byte) OTHER_BTs - 128_128_128_128_128_128_128_128_128_128
/* UX tables containing ONLY [PK=INT,col1=U128] have been optimised */
typedef struct uint_u128_key {
    uint32  key; uint128 val;
}  __attribute__ ((packed)) uxk;
#define UX(btr) (btr->s.bflag & BTFLAG_UINT_U128)
#define UX_SIZE 20

/* XU tables containing ONLY [PK=U128,col1=INT] have been optimised */
typedef struct u128_uint_key {
    uint128 key; uint32  val;
}  __attribute__ ((packed)) xuk;
#define XU(btr) (btr->s.bflag & BTFLAG_U128_UINT)
#define XU_SIZE 20

/* LX tables containing ONLY [PK=LONG,col1=U128] have been optimised */
typedef struct ulong_u128_key {
    ulong   key; uint128 val;
}  __attribute__ ((packed)) lxk;
#define LX(btr) (btr->s.bflag & BTFLAG_ULONG_U128)
#define LX_SIZE 24

/* XL tables containing ONLY [PK=U128,col1=LONG] have been optimised */
typedef struct u128_ulong_key {
    uint128 key; ulong   val;
}  __attribute__ ((packed)) xlk;
#define XL(btr) (btr->s.bflag & BTFLAG_U128_ULONG)
#define XL_SIZE 24

/* XX tables containing ONLY [PK=U128,col1=U128] have been optimised */
typedef struct u128_u128_key {
    uint128 key; uint128 val;
}  __attribute__ ((packed)) xxk;
#define XX(btr) (btr->s.bflag & BTFLAG_U128_U128)
#define XX_SIZE 32

// END: 128 bit (16 byte) OTHER_BTs - 128_128_128_128_128_128_128_128_128_128

/* Indexes containing INTs AND LONGs have been optimised */
#define UP(btr)  (btr->s.bflag & BTFLAG_UINT_INDEX)

#define LUP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_ULONG_UINT)
#define LLP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_ULONG_ULONG)

#define XUP(btr) (btr->s.bflag & BTFLAG_UINT_INDEX && \
                  btr->s.bflag & BTFLAG_U128_UINT)
#define XLP(btr) (btr->s.bflag & BTFLAG_ULONG_INDEX && \
                  btr->s.bflag & BTFLAG_U128_ULONG)
#define XXP(btr) (btr->s.bflag & BTFLAG_U128_INDEX && \
                  btr->s.bflag & BTFLAG_U128_ULONG)

#define UKEY(btr) (UU(btr) || LU(btr) || XU(btr))
#define LKEY(btr) (UL(btr) || LL(btr) || XL(btr))
#define XKEY(btr) (UX(btr) || LX(btr) || XX(btr))

/* NOTE OTHER_BT covers *P as they are [UL,LL,XL] respectively */
#define OTHER_BT(btr) (btr->s.bflag >= BTFLAG_UINT_UINT)
/* NOTE: BIG_BT means the KEYS are bigger than 8 bytes */
#define BIG_BT(btr)   (btr->s.bflag >  BTFLAG_UINT_UINT)
#define NORM_BT(btr)  (btr->s.bflag == BTFLAG_NONE)

int   btAdd    (bt *btr, aobj *apk, void *val);
void *btFind   (bt *btr, aobj *apk);
int   btReplace(bt *btr, aobj *apk, void *val);
int   btDelete (bt *btr, aobj *apk);

void  btIndAdd   (bt *btr, aobj *akey, bt  *nbtr);
bt   *btIndFind  (bt *btr, aobj *akey);
int   btIndDelete(bt *btr, aobj *akey);

bt   *createIndexNode(uchar pktyp, bool hasobc);
void  btIndNodeAdd   (bt *btr, aobj *apk);
void *btIndNodeFind  (bt *btr, aobj *apk);
int   btIndNodeDelete(bt *btr, aobj *apk);

bool  btIndNodeOBCAdd   (cli *c, bt *nbtr, aobj *apk, aobj *ocol);
int   btIndNodeOBCDelete(        bt *nbtr,            aobj *ocol);

#endif /* __ALSO_SQL_BT_H */

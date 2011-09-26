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

/* INT Indexes have been optimised */
#define INODE_I(btr) \
  (btr->s.btype == BTREE_INODE && \
   C_IS_I(btr->s.ktype) &&        \
   !(btr->s.bflag & BTFLAG_OBC)) 
/* LONG Indexes have been optimised */
#define INODE_L(btr) \
  (btr->s.btype == BTREE_INODE && \
   C_IS_L(btr->s.ktype) &&        \
   !(btr->s.bflag & BTFLAG_OBC)) 
#define INODE(btr) (INODE_I(btr) || INODE_L(btr))

/* UU tables containing ONLY [PK=INT,col1=INT]  have been optimised */
#define UU(btr) (btr->s.bflag & BTFLAG_UINT_UINT)
#define UU_SIZE 8

/* LU tables containing ONLY [PK=LONG,col1=INT] have been optimised */
typedef struct ulong_uint_key {
    ulong  key;
    uint32 val;
}  __attribute__ ((packed)) luk;
#define LU(btr) (btr->s.bflag & BTFLAG_ULONG_UINT)
#define LU_SIZE 12

/* UL tables containing ONLY [PK=INT,col1=LONG] have been optimised */
typedef struct uint_ulong_key {
    uint32 key;
    ulong  val;
}  __attribute__ ((packed)) ulk;
#define UL(btr) (btr->s.bflag & BTFLAG_UINT_ULONG)
#define UL_SIZE 12

/* LL tables containing ONLY [PK=LONG,col1=LONG] have been optimised */
typedef struct ulong_ulong_key {
    ulong key;
    ulong val;
}  __attribute__ ((packed)) llk;
#define LL(btr) (btr->s.bflag & BTFLAG_ULONG_ULONG)
#define LL_SIZE 16

/* Indexes containing INTs AND LONGs have been optimised */
#define UP(btr)  (btr->s.bflag & BTFLAG_UINT_PTR)
#define LUP(btr) (btr->s.bflag & BTFLAG_ULONG_PTR && \
                  btr->s.bflag & BTFLAG_ULONG_UINT)
#define LP(btr)  (btr->s.bflag & BTFLAG_ULONG_PTR)

/* NOTE OTHER_BT covers UP & LP as they are [UL & LL] respectively */
#define OTHER_BT(ibtr) (UU(ibtr) || UL(ibtr) || LU(ibtr) || LL(ibtr))
/* NOTE: BIG_BT means the KEYS are bigger than 8 bytes */
#define BIG_BT(ibtr)   (UL(ibtr) || LU(ibtr) || LL(ibtr))
/* NOTE: NORM_BT has a dependency on order of flags */
#define NORM_BT(btr) (btr->s.bflag <= BTFLAG_UINT_UINT)

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

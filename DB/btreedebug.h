/*
 * This file implements Alchemy's BTREE DEBUG #ifdefs
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

#ifndef __A_BTREEDEBUG__H
#define __A_BTREEDEBUG__H

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
//#define BT_MEM_PROFILE
#ifdef BT_MEM_PROFILE
static ulong tot_bt_data     = 0; static ulong tot_bt_data_mem = 0;
static ulong tot_num_bt_ns   = 0; static ulong tnbtnmem        = 0;
static ulong tot_num_bts     = 0; static ulong tot_num_bt_mem  = 0;
void dump_bt_mem_profile(bt *btr) {
    printf("BT: num: %d\n", btr->s.num);
    printf("tot_bt_data:     %lu\n", tot_bt_data);
    printf("tot_bt_data_mem: %lu\n", tot_bt_data_mem);
    printf("tot_num_bts:     %lu\n", tot_num_bts);
    printf("tot_bt_mem:      %lu\n", tot_num_bt_mem);
    printf("tot_num_btn:     %lu\n", tot_num_bt_ns);
    printf("tbtn_mem:        %lu\n", tnbtnmem);
    fflush(NULL);
}
  #define BT_MEM_PROFILE_BT   {tot_num_bts++; tot_num_bt_mem += size;}
  #define BT_MEM_PROFILE_MLC  {tot_bt_data++; tot_bt_data_mem += size;}
  #define BT_MEM_PROFILE_NODE {tot_num_bt_ns++; tnbtnmem += size;}
#else
void dump_bt_mem_profile(bt *btr) { btr = NULL; return; }
  #define BT_MEM_PROFILE_BT
  #define BT_MEM_PROFILE_MLC
  #define BT_MEM_PROFILE_NODE
#endif
#ifdef BTREE_DEBUG
  unsigned long BtreeNodeNum = 0;
  #define BT_ADD_NODE_NUM btn->num = BtreeNodeNum++;
#endif

// MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT MEMORY_MANAGEMENT
#define DEBUG_INCR_MEM \
    printf("INCR MEM: osize: %ld plus: %lu nsize: %ld\n", btr->msize, size, (btr->msize + size));
#define DEBUG_DECR_MEM \
    printf("DECR MEM: osize: %ld minus: %lu nsize: %ld\n", btr->msize, size, (btr->msize - size));
#define DEBUG_BT_MALLOC \
    printf("bt_MALLOC: %p size: %d\n", (void *)btr, size);
#define DEBUG_ALLOC_BTN \
    printf("allocbtreeNODE: %p leaf: %d size: %d\n", (void *)btr, leaf, size);
#define DEBUG_ALLOC_BTREE \
    printf("allocBTREE: %p size: %d\n", (void *)btr, size);
#define DEBUG_BT_FREE \
    printf("bt_FREE: %p size: %d\n", (void *)btr, size);
#define DEBUG_FREE_BTN \
    printf("bt_free_btreeNODE: %p leaf: %d size: %lu\n", (void *)btr, x->leaf, size);
#define DEBUG_ALLOC_DS \
  printf("alloc_ds: leaf: %d n: %d t: %d\n", x->leaf, btr->t * 2, btr->t); \
  printf("alloc_ds: x: %p dsp: %p ds: %p dssize: %lu\n",                   \
         (void *)x, (void *)dsp, (void *)ds, dssize);


// INDEX.POS() INDEX.POS() INDEX.POS() INDEX.POS() INDEX.POS() INDEX.POS()
#define DEBUG_ADD_TO_CIPOS \
  if (x->leaf) printf("LEAF: i: %d CurrPos: %d\n", i, Index[btr->s.num].cipos);\
  else printf("NODE: i: %d CurrPos: %d\n", i, Index[btr->s.num].cipos);


// KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS KEYS
#define DEBUG_KEY_OTHER                                                        \
  if UU(btr) { uint32 key = (long)v / UINT_MAX;                                \
               uint32 val = (long)v % UINT_MAX;                                \
               printf("\t\tUU: v: %d KEY: %lu VAL: %lu\n", v, key, val); }     \
  else if LU(btr) { luk *lu = (luk *)v; printf("\t\tLU: KEY: %lu VAL: %lu\n",  \
                                           lu->key, lu->val); }                \
  else if UL(btr) { ulk *ul = (ulk *)v; printf("\t\tUL: KEY: %u  VAL: %lu\n",  \
                                               ul->key, ul->val); }            \
  else if LL(btr) { llk *ll = (llk *)v; printf("\t\tLL: KEY: %lu VAL: %lu\n",  \
                                               ll->key, ll->val); }            \
  else if UX(btr) { uxk *ux = (uxk *)v; printf("\t\tUX: KEY: %u ", ux->key);   \
                                        printf(" VAL: ");                      \
                                   DEBUG_U128(printf, ux->val); printf("\n"); }\
  else if XU(btr) { xuk *xu = (xuk *)v; printf("\t\tXU: KEY: ");               \
                                   DEBUG_U128(printf, xu->key);                \
                                        printf(" VAL: %u\n", xu->val); }       \
  else if LX(btr) { lxk *lx = (lxk *)v; printf("\t\tLX: KEY: %llu ", lx->key); \
                                        printf(" VAL: ");                      \
                                   DEBUG_U128(printf, lx->val); printf("\n"); }\
  else if XL(btr) { xlk *xl = (xlk *)v; printf("\t\tXL: KEY: ");               \
                                   DEBUG_U128(printf, xl->key);                \
                                   printf(" VAL: %lu\n", xl->val); }           \
  else if XX(btr) { xxk *xx = (xxk *)v; printf("\t\tXX: KEY: ");               \
                                   DEBUG_U128(printf, xx->key);                \
                                   printf(" VAL: ");                           \
                                   DEBUG_U128(printf, xx->val); printf("\n"); }\
  if ISVOID(btr) printf("\t\tVOID: p: %p lu: %lu\n", (void *)v, v);            \
  if INODE_X(btr) {                                                            \
      uint128 *pbu = v; printf("\t\tINODE_X: ");                               \
      DEBUG_U128(printf, *pbu); printf("\n"); }

#define DEBUG_AKEYS                                                            \
  printf("AKEYS: i: %d ofst: %d v: %p uint: %d uu: %d lu: %d ul: %d ll: %d\n", \
          i, ofst, (void *)v, ISUINT(btr), UU(btr), LU(btr), UL(btr), LL(btr));\
  DEBUG_KEY_OTHER

#define DEBUG_KEYS                                                        \
  printf("KEYS: uint: %d void: %d i: %d\n", ISUINT(btr), ISVOID(btr), i);

#define DEBUG_SETBTKEY_OBT \
  if (p) printf("setBTKey: memcpy to v: %p\n", (void *)v);



// DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR DR
#define DEBUG_INCR_PREV                                                        \
  printf("tbg.p.x: %p tbg.p.i: %d tbg.c.x: %p tbg.c.i: %d\n",                  \
          (void *)tbg.p.x, tbg.p.i, (void *)tbg.c.x, tbg.c.i);
#define DEBUG_INCR_CASE2B                                                      \
  printf("incrCase2B dr: %d\n", dr);
#define DEBUG_ADD_DS_TO_BTN                                                    \
  printf("MMMMMMMMMMMM: addDStoBTN: to x: %p returning y: %p - p: %p pi: %d\n",\
          (void *)x, (void *)y, (void *)p, pi);
#define DEBUG_GET_DR                                                           \
  printf("getDR: x: %p i: %d ds: %p -> dr: %u\n", (void *)x, i, (void *)ds, dr);
#define DEBUG_ZERO_DR                                                          \
  printf("zeroDR: dirty: %d x: %p i: %d p: %p, pi: %d x.n: %d key: \n",        \
         x->dirty, (void *)x, i, (void *)p, pi, x->n); printKey(btr, x, i);
#define DEBUG_SET_DR_1                                                         \
  printf("============setDR: dirty: %d x: %p i: %d dr: %u p: %p, pi: %d key: ",\
         x->dirty, (void *)x, i, dr, (void *)p, pi); printKey(btr, x, i);
#define DEBUG_SET_DR_2                                                         \
  printf("ds: %p i: %d dr: %d ds[i]: %d\n", (void *)ds, i, dr, ds[i]);
#define DEBUG_INCR_DR_1                                                        \
  printf("++++++++++++incrDR: dirty: %d x: %p i: %d dr: %u p: %p, pi: %d key: ",          x->dirty, (void *)x, i, dr, (void *)p, pi); printKey(btr, x, i);
#define DEBUG_INCR_DR_2                                                        \
  uint32 odr = ds[i];
#define DEBUG_INCR_DR_3                                                        \
  printf("ds: %p i: %d dr: %d ds[i]: %d odr: %d\n",                            \
         (void *)ds, i, dr, ds[i], odr);


// GET_CHILD_RECURSE GET_CHILD_RECURSE GET_CHILD_RECURSE GET_CHILD_RECURSE
#define DEBUG_GET_C_REC_1                                                     \
printf("get_prev_child_recurse: x: %p i: %d xp: %p xp->leaf: %d xp->n: %d\n", \
        (void *)x, i, (void *)xp, xp->leaf, xp->n);
#define DEBUG_GET_C_REC_2                                                     \
  printf("get_prev_child_recurse: tbg.c.i: %d\n", tbg.c.i);
#define DEBUG_INCR_PREV_DR \
  printf("incrPrevDR: x: %p i: %d dr: %d key: ", (void *)x, i, dr);           \
  printKey(btr, x, i);

// SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY SET_BT_KEY
#define DEBUG_SET_KEY \
  printf("setBTKey: ksize: %d btr: %p v: %p p: %p uint: %d void: %d uu: %d " \
         "lu: %d ul: %d ll: %d\n",                                           \
          btr->s.ksize, (void *)btr, v, (void *)p,                           \
          ISUINT(btr), ISVOID(btr), UU(btr),                                 \
          LU(btr), UL(btr), LL(btr));                                        \
  DEBUG_KEY_OTHER
#define DEBUG_SET_BTKEY_2A                                             \
  printf("2A return: dr: %d\n", dwd.dr);                               \
  printf("22222AAAAAA: setBTKeyCase2_A: x: %p i: %d\n", (void *)x, i);
#define DEBUG_SET_BTKEY_2B                                             \
  printf("2B return: dr: %d\n", dwd.dr);                               \
  printf("22222BBBBBB: setBTKeyCase2_B: x: %p i: %d\n", (void *)x, i);
#define DEBUG_SET_BTKEY_2C \
  printf("22222CCCCCC: setBTKeyCase2_C: y: %p y->n: %d\n", (void *)y, y->n);
#define DEBUG_SET_BTKEY_INS \
  printf("IIIIIIIIIII: setBTKeyInsert\n");
#define DEBUG_SET_BTKEY \
  printf("KKKKKKKKKKK: setBTKey dx: %p di: %d sx: %p si: %d dr: %u skey: ", \
        (void *)dx, di, (void *)sx, si, dr); printKey(btr, sx, si);
#define DEBUG_MV_X_KEYS_1                                                \
  printf("VVVVVVVVVVV: drt: mvXKeys: x2x: %d dx: %p dii: %d sx: %p sii: %d " \
         "drs: %d\n", x2x, (void *)*dx, dii, (void *)*sx, sii, drs);         \
  printf("drd: %u dkey: ", drd); printKey(btr, *dx, dii);                    \
  printf("drs: %u skey: ", drs); printKey(btr, *sx, sii);
#define DEBUG_MV_X_KEYS_2 \
  printf("ZERO DEST: dii: %d\n", dii);
#define DEBUG_TRIM_BTN \
  printf("trimBTN: x: %p n: %d\n", (void *)x, x->n);

// DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
#define DEBUG_DEL_START \
  printf("START: ndk\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_POST_S \
  printf("POSTS: s: %d i: %d r: %d leaf: %d x.n: %d\n", \
          s, i, r, x->leaf, x->n);                      \
  printf("NDK: x: %p i: %d p: %p pi: %d key: ",         \
         (void *)x, i, (void *)p, pi); printKey(btr, x, i);
#define DEBUG_DEL_CASE_1 \
  printf("ndk CASE_1    s: %d i: %d x->n: %d\n", s, i, x->n); \
  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_1_DIRTY \
  printf("CASE1 drt: %d i: %d s: %d dr: %u gost: %d key: ", \
          drt, i, s, dwd.dr, gst); printKey(btr, x, i);
#define DEBUG_DEL_CASE_2 \
  printf("ndk CASE_2 x[i].n: %d x[i+1].n: %d t: %d\n", \
          NODES(btr, x)[i]->n, NODES(btr, x)[i + 1]->n, btr->t); \
  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2a \
  printf("ndk CASE_2a\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2b \
  printf("ndk CASE_2b\n");  //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_2c \
  printf("ndk CASE_2c\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3a1 \
  printf("ndk CASE_3a1: x: %p y: %p xp: %p\n", \
          (void *)x, (void *)y, (void *)xp); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3a2 \
  printf("ndk CASE_3a2: x: %p y: %p xp: %p\n", \
          (void *)x, (void *)y, (void *)xp); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3b1 \
  printf("ndk CASE_3b1: x: %p y: %p xp: %p\n", \
          (void *)x, (void *)y, (void *)xp); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_CASE_3b2 \
  printf("ndk CASE_3b2: x: %p y: %p xp: %p\n", \
          (void *)x, (void *)y, (void *)xp); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_END \
  printf("END: ndk\n"); //bt_dumptree(printf, btr, 0);
#define DEBUG_DEL_POST_CASE_3 \
  printf("POST CASE3: xp: %p x: %p i: %d s: %d key: ",      \
         (void *)xp, (void *)x, i, s); printKey(btr, x, i);

#define DEBUG_BT_DELETE_D \
    printf("bt_delete_d: qkey: %lu mkey: %lu dpdr: %lu ndr: %lu\n", \
            qkey, mkey, dpdr, ndr);


// ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS ACCESSORS
#define DEBUG_FIND_NODE_KEY                                     \
  if (x->leaf) printf("LEAF: findnodekey: i: %d r: %d x: %p\n", \
                      i ,r, (void *)x);                         \
  else         printf("NODE: findnodekey: i: %d r: %d x: %p\n", \
                      i ,r, (void *)x);
#define DEBUG_CURRKEY_MISS \
  printf("key_covers_miss: mkey: %lu qkey: %lu span: %lu -> hit: %d\n", \
          mkey, qkey, span, (qkey >= mkey && qkey <= span));
#define DEBUG_BT_II \
  printf("r: %d i: %d x->n: %d miss: %d\n", r, i, x->n, miss);

#endif /*__A_BTREEDEBUG__H */ 

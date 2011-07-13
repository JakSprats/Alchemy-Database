/*
 * This file implements the Query Optimiser for ALCHEMY_DATABASE
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

#include "redis.h"

#include "join.h"
#include "bt.h"
#include "filter.h"
#include "alsosql.h"
#include "index.h"
#include "common.h"
#include "qo.h"

// FROM redis.c
extern struct sharedObjectsStruct shared;


extern r_tbl_t  Tbl[MAX_NUM_TABLES];
extern r_ind_t  Index[MAX_NUM_INDICES];
extern bool     Explain;

static int Idum; /* dummy variable */

typedef struct mci_hits {
    lolo  hitmap; /* fist MCI key hits -> higher hitmap */
    int   imatch;
    int   clen;   /* length of MCI KLIST match of clist */
    f_t  *flt;
    bool  jmatch; /* filter shares index.cmatch, lowest prio sorting criteria */
} mh_t;
void dumpMCIHits(printer *prn, int i, mh_t *m);

#define DEBUG_MCMCI_MID                                                        \
  for (int j = 0; j < ni; j++) dumpMCIHits(printf, j, &MciHits[j]);
#define DEBUG_MCMCI_END                                                        \
  printf("0: imatch: %d (%s) clen: %d hitmap: %lld flist:(%p)\n",              \
       flt->imatch, ri->obj->ptr, MciHits[0].clen, MciHits[0].hitmap, *flist); \
  if (klist) dumpFL(printf, "\t\t", "KLIST", *klist);                          \
  dumpFL(printf, "\t\t", "FLIST", *flist);
#define DEBUG_MID_assignFiltersToJoinPairs                                     \
printf("MID assignFiltersToJoinPairs\n"); dumpJB(printf, jb);

#define DEBUG_START_SORT_JP                                             \
  printf("PRE JOINPLAN\n"); dumpJB(printf, jb);
#define DEBUG_POST_LHSLESS_SORT                                         \
  printf("POST LHSLESS JOINPLAN\n"); dumpJB(printf, jb);
#define DEBUG_JPLEFTJAN                                                 \
  printf("POST LTUPSORT JOINPLAN\n"); dumpJB(printf, jb);
#define DEBUG_JINDTOP                                                   \
  printf("POST JINDSORT JOINPLAN\n"); dumpJB(printf, jb);
#define DEBUG_BUILD_JCHAIN                                              \
  printf("POST buildJoinChain: ord: %d\n", oed); dumpJB(printf, jb);
#define DEBUG_MCI_JI_ADD                                                \
  printf("post addMCIJoinedIndexAsFilter\n"); dumpJB(printf, jb);
#define DEBUG_ASSIGN_FILTERS                                            \
  printf("POST assignFiltersToJoinPairs\n"); dumpJB(printf, jb);
#define DEBUG_DET_CHEAD                                                 \
  printf("POST determineChainHead\n"); dumpJB(printf, jb);
#define DEBUG_OPT_SELFJ                                                 \
  printf("FINAL\n"); dumpJB(printf, jb);
#define DEBUG_VALIDATE_CHAIN                                            \
  printf("POST validateChain\n"); dumpJB(printf, jb);
#define DEBUG_VAL_KLIST                                                 \
  printf("START: validateKlist\n");                                     \
  dumpFL(printf, "\t", "KLIST", *klist); dumpFL(printf, "\t", "FLIST", *flist);
#define DEBUG_VAL_KLIST_END                                             \
  printf("END: validateKlist\n");                                       \
  dumpFL(printf, "\t", "KLIST", *klist); dumpFL(printf, "\t", "FLIST", *flist);

/* HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS */
static void replaceHead(list *flist, listNode *ln, f_t *flt) {
    listDelNode(    flist, ln); /* delete from curr position */
    listAddNodeHead(flist, flt);  /* reADD @ head */
}
static void addFltKey(list **flist, f_t *flt) {
    if (!*flist) *flist = listCreate();                  /* DESTROY ME 044 */
    listAddNodeTail(*flist, flt);
}

static int jpIndexesTopSort(const void *s1, const void *s2) {
    ijp_t *ij1 = (ijp_t *)s1;
    ijp_t *ij2 = (ijp_t *)s2;
    if      (ij1->rhs.tmatch == -1) return  1;
    else if (ij2->rhs.tmatch == -1) return -1;
    else                            return  0;
}
static int jpLeftJanSort(const void *s1, const void *s2) {
    ijp_t *ij1 = (ijp_t *)s1;
    ijp_t *ij2 = (ijp_t *)s2;
    return ij1->lhs.jan - ij2->lhs.jan;
}

static void alignChainToJans(jb_t *jb) {
    for (int j = 0; j < jb->hw - 1; j++) { /* chain links via jans */
        int j1 = j + 1;
        if (jb->ij[j].rhs.jan != jb->ij[j1].lhs.jan) {
            if (jb->ij[j].lhs.jan == jb->ij[j1].lhs.jan) switchIJ(&jb->ij[j]);
            if (jb->ij[j].rhs.jan == jb->ij[j1].rhs.jan) switchIJ(&jb->ij[j1]);
            if (jb->ij[j].lhs.jan == jb->ij[j1].rhs.jan) {
                switchIJ(&jb->ij[j]);
                switchIJ(&jb->ij[j1]);
             }
         }
    }
}
static void revChain(jb_t *jb) {
    ijp_t   newij[jb->hw];
    int i = 0;
    for (int j = jb->hw - 1; j >= 0; j--) {    /* start from end */
        switchIJ(&jb->ij[j]);                         /* switch LHS & RHS */
        memcpy(&newij[i], &jb->ij[j], sizeof(ijp_t)); /* order to beginning */
        i++;
    }
    memcpy(&jb->ij, &newij, sizeof(ijp_t) * i);
}
static bool onFilterlessHeadRevChain(jb_t *jb) {
    int    ljan0  = jb->ij[         0].lhs.jan;
    int    rjanF  = jb->ij[jb->hw - 1].rhs.jan;
    uint32 l0hits = 0;
    uint32 rFhits = 0;
    for (int k = jb->hw; k < (int)jb->n_jind; k++) {
        if (jb->ij[k].lhs.jan == ljan0) l0hits++;
        if (jb->ij[k].lhs.jan == rjanF) rFhits++;
    }
    if (!l0hits && rFhits) { revChain(jb); return 1; }
    else                                   return 0;
}
static bool buildJoinChain(jb_t *jb) {
    listNode *ln;
    listIter *li;
    list     *cl   = listCreate();
    for (int i = 0; i < jb->hw; i++) listAddNodeTail(cl, &jb->ij[i]);
    bool      loop = 1;
    while (loop) {
        list *leftl  = listCreate(); list *rightl = listCreate();
        li           = listGetIterator(cl, AL_START_HEAD);
        ln           = listNext(li);                  /* start at HEAD */
        int lj0 = ((ijp_t *)ln->value)->lhs.jan;
        while ((ln = listNext(li)) != NULL) {
            ijp_t *ij = ln->value;
            if        (ij->lhs.jan == lj0) {
                listAddNodeTail(leftl, ij);
                lj0 = ij->rhs.jan;
                listDelNode(cl, ln);
            } else if (ij->rhs.jan == lj0) {
                listAddNodeTail(leftl, ij);
                lj0 = ij->lhs.jan;
                listDelNode(cl, ln);
            }
        } listReleaseIterator(li); 
        li      = listGetIterator(cl, AL_START_HEAD);
        ln      = listNext(li);                  /* start at HEAD */
        int rj0 = ((ijp_t *)ln->value)->rhs.jan;
        if (listLength(leftl)) listAddNodeTail(leftl,  ln->value);
        else                   listAddNodeTail(rightl, ln->value);
        listDelNode(cl, ln);                     /* cl HEAD just added */
        while ((ln = listNext(li)) != NULL) {
            ijp_t *ij = ln->value;
            if        (ij->lhs.jan == rj0) {
                listAddNodeTail(rightl, ij);
                rj0 = ij->rhs.jan;
                listDelNode(cl, ln);
            } else if (ij->rhs.jan == rj0) {
                listAddNodeTail(rightl, ij);
                rj0 = ij->lhs.jan;
                listDelNode(cl, ln);
            }
        } listReleaseIterator(li); 
        loop = (listLength(cl)) ? loop + 1 : 0;
        li = listGetIterator(leftl, AL_START_HEAD);/*prepend LEFT normal order*/
        while ((ln = listNext(li)) != NULL) listAddNodeHead(cl, ln->value);
        listReleaseIterator(li); 
        li = listGetIterator(rightl, AL_START_TAIL);/* prepend RIGHT reversed */
        while ((ln = listNext(li)) != NULL) listAddNodeHead(cl, ln->value);
        listReleaseIterator(li); 
        listRelease(leftl); listRelease(rightl);
    }
    ijp_t newij[cl->len];
    int   i = 0;
    li      = listGetIterator(cl, AL_START_HEAD);
    while ((ln = listNext(li)) != NULL) {
        ijp_t *ij = ln->value;
        memcpy(&newij[i], ij, sizeof(ijp_t)); //dumpIJ(printf, i, ln->value); 
        i++;
    } listReleaseIterator(li); 
    listRelease(cl);
    memcpy(&jb->ij, &newij, sizeof(ijp_t) * i);
    alignChainToJans(jb);
    return onFilterlessHeadRevChain(jb); /* head no filter, reverse chain */
}

static f_t *cr8MCIFlt(jb_t *jb, ijp_t *ij, int cmatch, bool rhs) {
    f_t *flt    = newEmptyFilter();                      /* DESTROY ME 069 */
    if (!jb->mciflist) jb->mciflist = listCreate();      /* DESTROY ME 069 */
    listAddNodeTail(jb->mciflist, flt);
    f_t *jflt   = rhs ? &ij->rhs : &ij->lhs;
    flt->jan    = jflt->jan;
    flt->imatch = jflt->imatch;
    flt->tmatch = jflt->tmatch;
    flt->cmatch = cmatch;
    flt->op     = NONE;
    flt->iss    = jflt->iss;//printf("cr8MCIFlt\n");dumpFilter(printf,flt,"\t");
    return flt;
}
static void addNewFltKey(list **flist, f_t *nflt) {
    if (*flist) { /* see if the flt is already in flist */
        listNode *ln;
        bool      hit = 0;
        listIter *li  = listGetIterator(*flist, AL_START_HEAD);
        while ((ln = listNext(li)) != NULL) {
            f_t *flt = ln->value;
            if (nflt->tmatch == flt->tmatch && nflt->cmatch == flt->cmatch) {
                hit = 1; break;
            }
        } listReleaseIterator(li);
        if (hit) return;
    }
    addFltKey(flist, nflt);
}
static void addMCI(jb_t *jb,  ijp_t *ij, int imatch, list **flist,
                   bool  rhs, int    strt) {
    r_ind_t *ri   = &Index[imatch];
    f_t     *iflt = rhs ? &ij->rhs : &ij->lhs;
    for (int m = strt; m < ri->nclist; m++) {/*KLIST must contain ENTIRE CLIST*/
        bool hit = 0;
        for (int k = jb->hw; k < (int)jb->n_jind; k++) {
            f_t *ijl = &jb->ij[k].lhs;
            if (ijl->jan == iflt->jan && ijl->cmatch == ri->bclist[m]) {
                hit = 1; break; /* this cmatch already in KLIST */
            }
        }
        if (!hit) addNewFltKey(flist, cr8MCIFlt(jb, ij, ri->bclist[m], rhs));
    }
}
static void checkMissnMCI(jb_t *jb, ijp_t *ij, list **flist, int ii, bool rhs) {
    f_t *iflt = rhs ? &ij->rhs : &ij->lhs;
    MATCH_INDICES(iflt->tmatch)
    for (int i = 0; i < matches; i++) {
        r_ind_t *ri = &Index[inds[i]];
        if (ri->clist) { /* check ALL MCIs for this table */
            listNode *ln;
            int       strt = 0;
            if (ii) { /* secondary joins can join on MCI clist[0] */
                if (iflt->tmatch == ri->table && iflt->cmatch == ri->column) {
                    addNewFltKey(flist, cr8MCIFlt(jb, ij, iflt->cmatch, rhs));
                    strt = 1;
                }
            }
            listIter *li = listGetIterator(*flist, AL_START_HEAD);
            while ((ln = listNext(li)) != NULL) {
                f_t *flt = ln->value;
                if (flt->tmatch == ri->table && flt->cmatch == ri->column) {
                    addMCI(jb, ij, inds[i], flist, rhs, strt); break;
                }
            } listReleaseIterator(li);
        }
    }
}
static void addMCIJoinedIndexAsFilter(jb_t *jb) { /* JoinIndex can be in MCI */
    for (int j = 0; j < jb->hw; j++) {
        r_tbl_t *rt = &Tbl[jb->ij[j].lhs.tmatch];
        if (rt->nmci) checkMissnMCI(jb, &jb->ij[j], &jb->ij[j].flist, j, 0);
    }
}
static void addJoinIndexToFFlist(jb_t *jb, list **fflist, ijp_t * ij) {
    if (Tbl[ij->rhs.tmatch].nmci) checkMissnMCI(jb, ij, fflist, 1, 1);
}

static bool sortFlist2Clist(list *flist, list *clist, int tmatch) {
    if (!clist || !flist) return 0;
    listNode *ln;
    bool      khit = 0;
    listIter *li   = listGetIterator(clist, AL_START_TAIL);
    while ((ln = listNext(li)) != NULL) { /* find flt's that match clist */
        int       cmatch = (int)(long)ln->value;
        listNode *fln;
        bool      hit    = 0;
        f_t      *hitf   = NULL;
        listNode *hitln  = NULL;
        listIter *fli    = listGetIterator(flist, AL_START_HEAD);
        while ((fln = listNext(fli)) != NULL) {
            f_t *flt = fln->value;
            if (flt->tmatch == tmatch && flt->cmatch == cmatch) {
                khit  = 1;
                hit   = 1;
                hitf  = flt;
                hitln = fln;
                break;
            }
        } listReleaseIterator(fli);
        if (hitln) replaceHead(flist, hitln, hitf);
    } listReleaseIterator(li);
    return khit;
}
static void createKList(list **flist, list *clist, int tmatch, list **klist) {
    if (!*flist || !clist) return;
    listNode *cln;
    listIter *cli = listGetIterator(clist, AL_START_HEAD);
    listNode *fln;
    listIter *fli = listGetIterator(*flist, AL_START_HEAD);
    while (1) { /* symmetric clist and flist walk and compare */
        cln = listNext(cli);
        fln = listNext(fli);
        if (!cln || !fln) break;
        int  cmatch = (int)(long)cln->value;
        f_t *flt    = fln->value;
        if (flt->tmatch == tmatch && flt->cmatch == cmatch) {
            if (!*klist) *klist = listCreate();      /* DESTROY ME 056 */
            listAddNodeTail(*klist, flt);
        }
    } listReleaseIterator(fli); listReleaseIterator(cli);
    if (*klist) { /* NOTE: safe to delete First as flist was sorted2clist */
        for (uint32 i = 0; i < listLength(*klist); i++) { /* delete repeats */
            listDelNode(*flist, listFirst(*flist));
        }
        if (!(*flist)->len) releaseFlist(flist);
    }
}

static mh_t MciHits[MAX_JOIN_INDXS];
static int mciHitSort(const void *s1, const void *s2) {
    mh_t *m1   = (mh_t *)s1;
    mh_t *m2   = (mh_t *)s2;
    lolo  diff = (m2->hitmap - m1->hitmap); /* most hits wins */
    if (diff) return diff;
    diff       = (m1->clen   - m2->clen);   /* on tie least nclist wins */
    if (diff) return diff;
    return  (int)(m2->jmatch - m1->jmatch); /* on 2X tie, jmatch wins */
}
static uint32 getMCIKeys(list *klist, uchar cnstr) {
    listIter *fli  = listGetIterator(klist, AL_START_HEAD);
    listNode *fln  = listNext(fli);
    f_t      *flt  = fln->value;
    if (flt->op == NONE) { listReleaseIterator(fli);     return UINT_MAX; }
    bt       *ibtr = getIBtr(flt->imatch);
    bt       *nbtr = btIndFind(ibtr, &flt->akey);
    if (!nbtr) {                                         return UINT_MAX; }
    while ((fln = listNext(fli)) != NULL) {
        if (UNIQ(cnstr) && fln == klist->tail) {
            listReleaseIterator(fli);
            return nbtr->numkeys;
        }
        f_t *flt  = fln->value;
        if (flt->op == NONE) { listReleaseIterator(fli); return UINT_MAX; }
        bt  *xbtr = btIndFind(nbtr, &flt->akey);
        if (!xbtr) return 0;
        nbtr      = xbtr;
    } listReleaseIterator(fli);
    return nbtr->numkeys;
}
static uint32 matchCheapestMCI(list **flist,   list **klist,
                               int   *kimatch, int    jcmatch) {
    listNode *fln, *cln, *dln;
    bool      mci = 0;
    int       ni  = 0;
    listIter *fli = listGetIterator(*flist, AL_START_HEAD);
    while ((fln = listNext(fli)) != NULL) {
        f_t  *flt    = fln->value;
        if (!Tbl[flt->tmatch].nmci) break; /* not MCIs - skip check */
        MciHits[ni].clen = 0;
        lolo hitmap      = 0; /* NOTE: more clist matchs, higher hitmap */
        bool jmatch      = 0;
        if (flt->imatch != -1) {
            r_ind_t *ri = &Index[flt->imatch];
            if (jcmatch == ri->column) jmatch = 1;
            if (ri->clist) { /* count hits in clist from flist */
                mci          = 1;
                int       i  = MAX_JOIN_INDXS;
                listIter *li = listGetIterator(ri->clist, AL_START_HEAD);
                while ((cln = listNext(li)) != NULL) {
                    int       cmatch = (int)(long)cln->value;
                    listIter *dli    = listGetIterator(*flist, AL_START_HEAD);
                    while ((dln = listNext(dli)) != NULL) {
                        f_t *dflt = dln->value;
                        if (dflt->op != EQ) continue;
                        if (cmatch == dflt->cmatch) {
                            hitmap += (1 << i);
                            MciHits[ni].clen++;
                        }
                    } listReleaseIterator(dli);
                    i--;
                } listReleaseIterator(li);
            } else {
                if (jcmatch == ri->column) {
                    hitmap = (ull)(1 << MAX_JOIN_INDXS);
                    MciHits[ni].clen = 1;
                }
            }
        }
        MciHits[ni].imatch = flt->imatch;
        MciHits[ni].hitmap = hitmap;
        MciHits[ni].flt    = flt;
        MciHits[ni].jmatch = jmatch;
        ni++;
    } listReleaseIterator(fli);
    if (!mci) return -1; /* means ignore MCI, go to regular indexes */

    qsort(&MciHits, ni, sizeof(mh_t), mciHitSort);
    listRelease(*flist); *flist = listCreate();                //DEBUG_MCMCI_MID
    for (int j = 0; j < ni; j++) listAddNodeTail(*flist, MciHits[j].flt);

    listNode *ln = listFirst(*flist); /* first flt will be fist join-step */
    f_t *flt     = ln->value;
    if (flt->imatch != -1) {
        *kimatch     = flt->imatch;
        r_ind_t  *ri = &Index[flt->imatch];
        sortFlist2Clist(*flist, ri->clist, flt->tmatch);
        createKList(flist, ri->clist, flt->tmatch, klist);     //DEBUG_MCMCI_END
        if (!ri->clist) return -1; /* best match is NOT MCI */
        int expected = UNIQ(ri->cnstr) ? ri->nclist -1 : ri->nclist;
        if (MciHits[0].clen == expected) return getMCIKeys(*klist, ri->cnstr);
    }
    return UINT_MAX;
}

#define CNT_INDXD (UINT_MAX - 1) /* indexed columns before non-indexed */
#define QOP_MAX_NUM_CHECK 10     /* if [range,inl] bigger, dont estimate cost */
static uint32 numRows4INL(f_t *flt) {
    int ctype = Tbl[flt->tmatch].col_type[flt->cmatch];
    if (!C_IS_NUM(ctype))              return CNT_INDXD; /* only NUMs */
    int num = listLength(flt->inl);
    if (Index[flt->imatch].virt) return num;
    if (num > QOP_MAX_NUM_CHECK)       return CNT_INDXD;
    listNode *ln;
    aobj      afk; initAobjZeroNum(&afk, ctype);
    bt       *ibtr = getIBtr(flt->imatch);
    uint32    cnt  = 0;
    listIter *li   = listGetIterator(flt->inl, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        aobj *a  = ln->value;
        if C_IS_I(ctype) afk.i = a->i;
        else             afk.l = a->l;
        bt *nbtr = btIndFind(ibtr, &afk);
        if (nbtr) cnt += nbtr->numkeys;
    } listReleaseIterator(li);
    releaseAobj(&afk);
    return cnt;
}
static uint32 numRows4Range(f_t *flt) {
    int ctype = Tbl[flt->tmatch].col_type[flt->cmatch];
    if (!C_IS_NUM(ctype))              return CNT_INDXD; /* only NUMs */
    int range = C_IS_I(ctype) ? flt->ahigh.i - flt->alow.i :
                                flt->ahigh.l - flt->alow.l;
    if (Index[flt->imatch].virt) return range;
    if (range > QOP_MAX_NUM_CHECK)     return CNT_INDXD;
    aobj    afk; initAobjZeroNum(&afk, ctype);
    bt     *ibtr = getIBtr(flt->imatch);
    uint32  cnt  = 0;
    ulong   low  = C_IS_I(ctype) ? flt->alow.i  : flt->alow.l;
    ulong   high = C_IS_I(ctype) ? flt->ahigh.i : flt->ahigh.l;
    for (ulong i = low; i <= high; i++) {
        if C_IS_I(ctype) afk.i = i;
        else             afk.l = i;
        bt *nbtr  = btIndFind(ibtr, &afk); /* blindly lookup - no Iter8r*/
        if (nbtr) cnt += nbtr->numkeys;
    }
    releaseAobj(&afk);
    return cnt;
}
static uint32 numRows4Key(f_t *flt) {
    if (Index[flt->imatch].virt) return 1;
    bt *ibtr = getIBtr(flt->imatch);
    bt *nbtr = btIndFind(ibtr, &flt->akey);
    return nbtr ? nbtr->numkeys : 0;
}
static uint32 getNumRow4Filter(f_t *flt) {
    if      (flt->key)   return numRows4Key(  flt);
    else if (flt->low)   return numRows4Range(flt);
    else /*  flt->inl */ return numRows4INL(  flt);
}
static uint32 sortFLCheap(list **flist,   list **klist, 
                          int   *kimatch, int    jcmatch) {
    if (!*flist)  return UINT_MAX;
    convertFilterListToAobj(*flist);
    int nrows = matchCheapestMCI(flist, klist, kimatch, jcmatch);
    if (nrows != -1) return nrows;
    if (!*flist)     return UINT_MAX; /* flist MAYBE converted to klist */
    listNode *ln,                        *lowln = NULL;
    uint32    lowc  = UINT_MAX; f_t      *lowf  = NULL;
    listIter *li    = listGetIterator(*flist, AL_START_HEAD);
    while ((ln = listNext(li)) != NULL) {
        f_t *flt = ln->value;
        if (flt->imatch != -1) {
            r_ind_t *ri = &Index[flt->imatch];
            if (ri->clist) continue;                /* logic here for NON-MCI */
            if (jcmatch != -1 && flt->cmatch != jcmatch) continue;
            if (flt->op == EQ || flt->op == RQ || flt->op == IN) {/*KEY,RQ,INL*/
                uint32 cnt = getNumRow4Filter(flt);
                if (cnt < lowc) {
                    *kimatch = flt->imatch; lowc     = cnt;
                    lowf     = flt;         lowln    = ln;
                }
            }
        }
    } listReleaseIterator(li);
    if ((*flist)->len > 1 && lowln) replaceHead(*flist, lowln, lowf);
    return lowc;
}
static bool assignFiltersToJoinPairs(redisClient *c, jb_t *jb) {
    ijp_t *lastij = &jb->ij[jb->hw - 1];
    for (int k = jb->hw; k < (int)jb->n_jind; k++) {
        ijp_t *ij  = &jb->ij[k];
        f_t   *flt = &ij->lhs;
        bool   hit = 0;
        for (int j = 0; j < jb->hw; j++) { /* IndPair w/ same JAN */
            if (jb->ij[j].lhs.jan == ij->lhs.jan) {
                addFltKey(&jb->ij[j].flist, flt); hit = 1; break;
            }
        }
        if (!hit) {
            if (lastij->rhs.jan == ij->lhs.jan) addFltKey(&jb->fflist, flt);
            else { addReply(c, shared.joindanglingfilter); return 0; }
        }
    }                                       //DEBUG_MID_assignFiltersToJoinPairs
    addMCIJoinedIndexAsFilter(jb);                            //DEBUG_MCI_JI_ADD
    for (int j = 0; j < jb->hw; j++) {
        ijp_t *ij = &jb->ij[j];
        ij->nrows = sortFLCheap(&ij->flist,   &ij->lhs.klist,
                                &ij->kimatch, ij->lhs.cmatch);
        if (ij->kimatch != -1 ) { /* NOTE: kimatch outranks ij.*.imatch */
            ij->lhs.imatch = ij->kimatch;                  /* current lhs */
            if (j) jb->ij[j - 1].rhs.imatch = ij->kimatch; /* previous rhs */
        }
    }
    addJoinIndexToFFlist(jb, &jb->fflist, &jb->ij[jb->hw - 1]);
    jb->fnrows = sortFLCheap(&jb->fflist,   &jb->fklist,
                             &jb->fkimatch, jb->ij[jb->hw - 1].rhs.cmatch);
    if (jb->fkimatch != -1) lastij->rhs.imatch = jb->fkimatch;/* previous rhs */
    return 1;
}
/* NOTE: CODE BELOW must have "kimatch" defined (ij.kimatch & fkimatch) */
static void listUnset(list **flist) {
    listRelease(*flist);
    *flist = NULL;
}
static bool revChainReassign(redisClient *c, jb_t *jb) {
    revChain(jb);
    if (jb->fflist) listUnset(&jb->fflist);
    if (jb->fklist) listUnset(&jb->fklist);
    for (int j = 0; j < jb->hw; j++) {
        if (jb->ij[j].flist)     listUnset(&jb->ij[j].flist);
        if (jb->ij[j].lhs.klist) listUnset(&jb->ij[j].lhs.klist);
    }
    return assignFiltersToJoinPairs(c, jb); /* reassign flt w/ new LHS,RHS */
}
static bool determineChainHead(redisClient *c, jb_t *jb) { /* is end cheaper? */
    if (jb->ij[0].nrows > jb->fnrows) return revChainReassign(c, jb);
    else                              return 1;
}

typedef struct equivalent_join_pairs {
    uint32  nrows;
    list   *flist;
    list   *klist;
} eqjp_t;
static eqjp_t EqJoinPair[MAX_JOIN_INDXS];

static int jpSeqSelfJoinSort(const void *s1, const void *s2) {
    eqjp_t *ej1 = (eqjp_t *)s1;
    eqjp_t *ej2 = (eqjp_t *)s2;
    return (int)(ej1->nrows > ej2->nrows);
}
static void smallestSelfJoin(jb_t *jb) {
    int equiv = 1;
    for (int j = 1; j < jb->hw; j++) { /* count sequential selfjoins */
        if (jb->ij[j].lhs.imatch != jb->ij[0].lhs.imatch ||
            jb->ij[j].lhs.imatch != jb->ij[0].rhs.imatch) break;
        equiv++;
    }
    if (equiv > 1) { /* equivalent steps can swap flists - sort to cheapest */
        for (int i = 0; i < equiv; i++) {
            EqJoinPair[i].nrows = jb->ij[i].nrows;
            EqJoinPair[i].flist = jb->ij[i].flist;
            EqJoinPair[i].klist = jb->ij[i].lhs.klist;
        }
        qsort(&EqJoinPair, equiv, sizeof(eqjp_t), jpSeqSelfJoinSort);
        for (int i = 0; i < equiv; i++) { /* overwrite w/ sorted flists */
            jb->ij[i].nrows     = EqJoinPair[i].nrows;
            jb->ij[i].flist     = EqJoinPair[i].flist;
            jb->ij[i].lhs.klist = EqJoinPair[i].klist;
        }
    }
}

bool sortJoinPlan(cli *c, jb_t *jb) {                      //DEBUG_START_SORT_JP
    for (uint32 i = 0; i < jb->n_jind; i++) { /* LHS gets lower [index,jan] */
        ijp_t *ij = &jb->ij[i];
        if (ij->rhs.tmatch != -1) {
            if (ij->rhs.tmatch < ij->lhs.tmatch)         switchIJ(ij);
            else if (ij->rhs.tmatch == ij->lhs.tmatch &&
                     ij->rhs.jan    <  ij->lhs.jan)      switchIJ(ij);
        }
    }                                                  //DEBUG_POST_LHSLESS_SORT
    qsort(&jb->ij, jb->n_jind, sizeof(ijp_t), jpLeftJanSort);  //DEBUG_JPLEFTJAN
    qsort(&jb->ij, jb->n_jind, sizeof(ijp_t), jpIndexesTopSort); //DEBUG_JINDTOP
    for (uint32 i = 0; i < jb->n_jind; i++) {
        if (jb->ij[i].rhs.tmatch == -1) { jb->hw = i; break; }
    }
    if (jb->hw < 1) { addReply(c, shared.fulltablejoin); return 0; }
    return 1;
}
bool optimiseJoinPlan(cli *c, jb_t *jb) {
    if (!sortJoinPlan(c, jb))               return 0;
    bool oed = buildJoinChain(jb);                        //DEBUG_BUILD_JCHAIN
    if (!assignFiltersToJoinPairs(c, jb))   return 0;     //DEBUG_ASSIGN_FILTERS
    if (!oed && !determineChainHead(c, jb)) return 0;     //DEBUG_DET_CHEAD
    smallestSelfJoin(jb);                                 //DEBUG_OPT_SELFJ
    return 1;
}

static void reduceFlist(list **flist) {
    if (!*flist) return;
    listNode *ln, *delLn = NULL;
    listIter *li = listGetIterator(*flist, AL_START_TAIL);
    while ((ln = listNext(li)) != NULL) { /* find flt's that match clist */
        if (delLn) listDelNode(*flist, delLn);
        f_t *flt = ln->value;
        if (flt->op == NONE) delLn = ln;
        else { delLn = NULL; break; }
    } listReleaseIterator(li);
    if (delLn) listDelNode(*flist, delLn);
    if (!(*flist)->len) releaseFlist(flist);
}
static void validateKlist(list **klist, list **flist, ijp_t *ij, bool rhs) {
    if (!*klist) return;                                       //DEBUG_VAL_KLIST
    listNode *ln;
    int       ojcmatch = rhs ? ij->rhs.cmatch : ij->lhs.cmatch;
    bool      hitj     = 0; bool kbrk = 0;
    listIter *li       = listGetIterator(*klist, AL_START_HEAD);
    while ((ln = listNext(li)) != NULL) { /* keylist's work ONLY w/ KEYS */
        f_t *flt = ln->value;
        if (hitj && flt->op != EQ) { kbrk = 1; break; }
        if (flt->cmatch == ojcmatch) hitj = 1;
    }
    if (kbrk) { /* !NOOP -> FLIST */
        listNode *delLn = NULL;
        do {
            if (delLn) listDelNode(*klist, delLn);
            delLn    = ln;
            f_t *flt = ln->value;
            if (flt->op != NONE) addFltKey(flist, flt);
        } while ((ln = listNext(li)) != NULL);
        if (delLn) listDelNode(*klist, delLn);
    }
    listReleaseIterator(li);                               //DEBUG_VAL_KLIST_END
    if (!(*klist)->len) releaseFlist(klist);
    return;
}
bool validateChain(cli *c, jb_t *jb) {
    int  rjan;
    bool ok   = 1;
    bool revd = 0;
    while (1) {
start:
        rjan = jb->ij[0].lhs.jan;
        for (int j = 0; j < jb->hw; j++) { /* validate index-chain */
            if (jb->ij[j].lhs.jan != rjan)       { ok = 0; break; }
            else if (jb->ij[j].lhs.imatch == -1) { ok = 0; break; }
            else if (jb->ij[j].rhs.imatch == -1) { ok = 0; break; }
            else if (j && Index[jb->ij[j].lhs.imatch].clist &&
                     !jb->ij[j - 1].lhs.klist) {
                if (!revd) {
                    revd = 1;
                    if (!revChainReassign(c, jb)) return 0; 
                    else                          goto start; /* try again */
                } else                           { ok = 0; break; }
            }
            rjan = jb->ij[j].rhs.jan;
        }
        if (ok) break;
        else    { addReply(c, shared.join_chain); return 0; }
    }                                                     //DEBUG_VALIDATE_CHAIN
    for (int j = 0; j < jb->hw; j++) {
        validateKlist(&jb->ij[j].lhs.klist, &jb->ij[j].flist, &jb->ij[j], 0);
    }
    validateKlist(&jb->fklist, &jb->fflist, &jb->ij[jb->hw - 1], 1);
    for (int j = 0; j < jb->hw; j++) reduceFlist(&jb->ij[j].flist);
    reduceFlist(&jb->fflist);                             //DEBUG_VALIDATE_CHAIN
    return 1;
}

bool promoteKLorFLtoW(cswc_t *w, list **klist, list **flist, bool freeme) {
    list     **fl = *klist ? klist : flist; /* NOTE Keylist has precedence */
    if (!*fl)               return 0;
    listNode *ln  = listFirst(*fl); /* take FIRST flt as w */
    f_t *flt      = ln->value;
    memcpy(&w->wf, flt, sizeof(f_t));
    if (freeme) free(flt); /* FREED, destroyed happens later*/
    listDelNode(*fl, ln);
    if (!(*fl)->len) releaseFlist(fl);
    if (w->wf.imatch == -1) return 0;
    r_ind_t  *ri  = &Index[w->wf.imatch];
    w->wtype      = (w->wf.akey.type != COL_TYPE_NONE) ? 
                     (ri->virt  ? SQL_SINGLE_LKP : SQL_SINGLE_FK_LKP) :
                     (w->wf.low ? SQL_RANGE_LKP  : SQL_IN_LKP);
    if (fl && *klist) w->wf.klist = *klist; /* klist exists & not released */
    convertFilterSDStoAobj(&w->wf);
    return 0;
}
static void rangeQuerySortFLCheap(list **flist, list **klist) {
    sortFLCheap(flist, klist, &Idum, -1);
}
bool optimiseRangeQueryPlan(cli *c, cswc_t *w) {
    if (!w->flist) return 0;
    list     *kl  = NULL;
    rangeQuerySortFLCheap(&w->flist, &kl);
    promoteKLorFLtoW(w, &kl, &w->flist, 1);
    if (w->wf.imatch == -1) {
        addReply(c, shared.whereclause_col_not_indxd);
        return 0;
    }
    if (w->wf.op != EQ && w->wf.op != RQ && w->wf.op != IN) {
        addReply(c, shared.key_query_mustbe_eq);
        return 0;
    }
    return 1;
}

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpMCIHits(printer *prn, int i, mh_t *m) {
    (*prn)("MciHits[%d]: imatch: %d hitmap: %lld clen: %d jmatch: %d\n",
            i, m->imatch, m->hitmap, m->clen, m->jmatch);
    (*prn)("filter:\n");
    dumpFilter(prn, m->flt, "\t\t");
}

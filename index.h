/*
COPYRIGHT: RUSS
 */

#ifndef __INDEX__H
#define __INDEX__H

#include "redis.h"
#include "btreepriv.h"
#include "common.h"

int find_index( int tmatch, int cmatch);
int match_index(int tmatch, int indices[]);
int match_index_name(char *iname);
int checkIndexedColumnOrReply(redisClient *c, char *curr_tname);

/* MATCH_INDICES(tmatch)
     creates (int indices[], int matches)     */
#define MATCH_INDICES(tmatch)                      \
    int   indices[REDIS_DEFAULT_DBNUM];            \
    int   matches = match_index(tmatch, indices);

void newIndex(redisClient *c, char *iname, int tmatch, int cmatch, bool virt);
void createIndex(redisClient *c);

void iAdd(bt *btr, robj *i_key, robj *i_val, unsigned char pktype);

void addToIndex(redisDb      *db,
                robj         *pko,
                char         *vals,
                unsigned int  col_ofsts[],
                int           inum);
void delFromIndex(redisClient *c, 
                  robj        *old_pk,
                  robj        *row,  
                  int          inum,  
                  int          tmatch);
void updateIndex(redisClient   *c,
                 robj          *old_pk,
                 robj          *new_pk,
                 robj          *new_val,
                 robj          *row,
                 int            inum,
                 unsigned char  pk_update,
                 int            tmatch);

/* RANGE_CHECK_OR_REPLY(char *cargv3ptr) -
     creates (robj *low, robj *high)     */
#define RANGE_CHECK_OR_REPLY(cargv3ptr)                              \
    robj *low, *high;                                                \
    {                                                                \
        char *local_range = cargv3ptr;                               \
        char *local_nextc = strchr(local_range, CMINUS);             \
        if (!local_nextc) {                                          \
            addReply(c, shared.invalidrange);                        \
            return;                                                  \
        }                                                            \
        *local_nextc = '\0';                                         \
        local_nextc++;                                               \
        low  = createStringObject(local_range, strlen(local_range)); \
        high = createStringObject(local_nextc, strlen(local_nextc)); \
    }

void dropIndex(redisClient *c);

void iselectAction(redisClient *c,
                   char        *range,
                   int          tmatch,
                   int          i_match,
                   char        *col_list);
void ideleteAction(redisClient *c, char *range, int tmatch, int imatch);
void iupdateAction(redisClient   *c,
                   char          *range,
                   int            tmatch,
                   int            imatch,
                   int            ncols,
                   int            matches,
                   int            indices[],
                   char          *vals[],
                   unsigned int   vlens[],
                   unsigned char  cmiss[]);

int get_sum_all_index_size_for_table(redisClient *c, int tmatch);

#endif /* __INDEX__H */ 

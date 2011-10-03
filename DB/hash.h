
#ifndef __ALC_HASH__
#define __ACL_HASH__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "common.h"

#define HASH_NULL_KEY 0
#define MAX_COLLISIONS 25

//TODO user_pointer should be a uint32 and struct -> packed
typedef struct alc_hash32_entry_t {
  uint32  key;
  void   *user_pointer;
  uint32  n_collisions; /* num_collisions now, always <= max_collisions */
} __attribute__ ((packed)) alc_hash32_entry_t;

typedef struct alc_hash32_t {
  uint32              nvalid_entries;
  uint32              nentries;
  uint32              max_collisions;
  alc_hash32_entry_t *entries;
} alc_hash32_t;

alc_hash32_t *alc_hash32_make(uint32 nentries);
void   alc_hash32_clear  (alc_hash32_t *ht);
void   alc_hash32_destroy(alc_hash32_t *ht);
int    alc_hash32_insert(uint32 key, void *user_pointer, alc_hash32_t *ht);
void   alc_hash32_delete(uint32 key,                     alc_hash32_t *ht);
void  *alc_hash32_fetch (uint32 key,               const alc_hash32_t *ht);

uint32 alc_hash32_size(alc_hash32_t *ht);

// interpret the user pointer as a 32bit unsigned int and increment by 'incr'
int alc_hash32_uincr(uint32 key, uint32 incr, alc_hash32_t *ht);

// free the memory for those nodes that eval( user_ptr, value_ptr )
void alc_hash32_thin(alc_hash32_t *ht, void *user_ptr, uint32 (*eval)(void *user_ptr, void *value_ptr) );

/* eval( user_ptr, key, value_ptr ) for all elements */
void alc_hash32_map(alc_hash32_t *ht, void *user_ptr,
                    void (*eval)(void *user_ptr, uint32 key, void *value_ptr));

#endif // ALC_HASH

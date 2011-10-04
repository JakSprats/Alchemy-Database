
#ifndef __ALC_HASH__
#define __ACL_HASH__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "common.h"

#define HASH_NULL       0
#define MAX_COLLISIONS 25

typedef struct ahash_entry { // 16 BYTES
  uint32 n_colijns; /* num_collisions now, always <= max_collisions */
  uint32 key;
  ulong  val;
} ahash_entry;

typedef struct ahash {
  uint32       nvalid_entries;
  uint32       nentries;
  uint32       max_collisions;
  ahash_entry *entries;
} ahash;

ahash *alc_hash32_make(uint32 nentries);
void   alc_hash32_destroy(ahash *ht);
uint32 alc_hash32_size   (ahash *ht);
int    alc_hash32_insert(uint32 key, ulong val, ahash *ht);
void   alc_hash32_delete(uint32 key,            ahash *ht);
ulong  alc_hash32_fetch (uint32 key,            ahash *ht);

#endif // ALC_HASH

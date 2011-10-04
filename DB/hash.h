
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

typedef struct ahash32_entry { // 16 BYTES
    uint32 n_colijns; /* num_collisions now, always <= max_colijns */
    uint32 key;
    ulong  val;
} ahash32_entry;

typedef struct ahash32 {
    uint32         nvalid;
    uint32         nentries;
    uint32         max_colijns;
    ahash32_entry *entries;
} ahash32;

ahash32 *alc_hash32_make(uint32 nentries);
void     alc_hash32_destroy(ahash32 *ht);
uint32   alc_hash32_size   (ahash32 *ht);
int      alc_hash32_insert(uint32 key, ulong val, ahash32 *ht);
void     alc_hash32_delete(uint32 key,            ahash32 *ht);
ulong    alc_hash32_fetch (uint32 key,            ahash32 *ht);


// NOTE: hash16 is a SHAMELESS COPY hash32
typedef struct ahash16_entry { // 7 BYTES
    uchar  n_colijns; /* num_collisions now, always <= max_colijns */
    ushort16 key;
    uint32 val;
} __attribute__ ((packed)) ahash16_entry;

typedef struct ahash16 {
    uint32         nvalid;
    uint32         nentries;
    uint32         max_colijns;
    ahash16_entry *entries;
} ahash16;

ahash16 *alc_hash16_make(uint32 nentries);
void     alc_hash16_destroy(ahash16 *ht);
uint32   alc_hash16_size   (ahash16 *ht);
int      alc_hash16_insert(ushort16 key, uint32 val, ahash16 *ht);
void     alc_hash16_delete(ushort16 key,             ahash16 *ht);
uint32   alc_hash16_fetch (ushort16 key,             ahash16 *ht);

#endif // ALC_HASH

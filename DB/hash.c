
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "hash.h"

#define ERR_OOM             -1
#define ERR_INVALID_HASHKEY -1

__inline__ static void init32(ahash32 *ht) {
    ht->nvalid = 0;
    /* only reset if NOT set */
    if (ht->max_colijns == 0) ht->max_colijns = MAX_COLLISIONS;
}

ahash32 *alc_hash32_make(uint32 nentries) {
    ahash32 *ht   = (ahash32 *)malloc(sizeof(ahash32)); if (!ht) return NULL;
    bzero(ht, sizeof(ahash32));
    if (!nentries) nentries = 2; /* need at least some */
    size_t   size = nentries * sizeof(ahash32_entry);
    ht->nentries  = nentries;
    ht->entries   = (ahash32_entry *)malloc(size);
    if (!ht->entries) { free(ht); ht = NULL; return NULL; }
    bzero(ht->entries, size);
    init32(ht);
    return ht;
}

uint32 alc_hash32_size(ahash32 *ht) {
    return (ht->nentries * sizeof(ahash32_entry)) + sizeof(ahash32);
}

void alc_hash32_destroy(ahash32 *ht) {
    if (ht) { if (ht->entries) free(ht->entries); free(ht); }
}

//NOTE: when a dict is serialised into a stream, it can not contain pointers
//      so the ht->entries pointer is "simulated" it always comes just below
#define SIMULATE_32HT_ENTRIES         \
  if (!ht->entries) {                 \
    uchar *v = (uchar *)ht;           \
    v += sizeof(ahash32);             \
    ht->entries = (ahash32_entry *)v; \
  }

__inline__ static ahash32_entry *lookup_32entry(const uint32 key, ahash32 *ht) {
    SIMULATE_32HT_ENTRIES
    uint32         nentries  = ht->nentries,
                   index     = key % nentries;
    ahash32_entry *entry     = ht->entries + index;
    uint32         entry_key = entry->key;
    if (!(entry_key ^ key))                                     return entry;
    if (entry->n_colijns > 0) { /* search collisions */
        uint32 nwrap = 0, last_index = index + entry->n_colijns;
        if (last_index >= nentries) { /* clip */
            nwrap = last_index - nentries + 1; last_index = nentries - 1;
        }
        for(index++,entry++; index <= last_index; index++, entry++) {
            entry_key = entry->key; if (!(entry_key ^ key))     return entry;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap ; nwrap--,entry++) {
                entry_key = entry->key; if (!(entry_key ^ key)) return entry;
            }
        }
    }
    return NULL;
}

__inline__ static ahash32_entry *lookup_32insert_entry(const uint32  key,
                                                       ahash32      *ht){
    SIMULATE_32HT_ENTRIES
    uint32         nentries  = ht->nentries,
                   index     = key % nentries;
    ahash32_entry *entry     = ht->entries + index;
    uint32         entry_key = entry->key;
    if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) return entry;
    { /* search collisions */
        uint32         n_colijns   = 1,
                       nwrap       = 0,
                       last_index  = index + ht->max_colijns;
        ahash32_entry *collision   = entry;
        if (last_index >= nentries) { /* clip */
          nwrap      = MIN(nentries, last_index - nentries + 1);
          last_index = nentries - 1;
        }
        for(index++,entry++; index<=last_index; index++,entry++,n_colijns++) {
            entry_key = entry->key;
            if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) goto done;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap; nwrap--, entry++, n_colijns++) {
                entry_key = entry->key;
                if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) goto done;
            }
        }
        return NULL;

    done: /* update collision count */
        if (n_colijns > collision->n_colijns) collision->n_colijns = n_colijns;
        return entry;
    }
}

static int hash32_insert(uint32 key, ulong val, uint32 rehash_lvl, ahash32 *ht);

#define DEBUG_REHASH                                   \
  printf("rehash: nvalid: %d from: %u to : %u\n",      \
          ht->nvalid, nentries, ht->nentries);

static int rehash32(ahash32 *ht, uint32 rehash_lvl) { /* add %25 more entries */
    SIMULATE_32HT_ENTRIES
    int            status   = 0;
    uint32         nentries = ht->nentries;
    ahash32_entry *entries  = ht->entries,
                  *dead     = entries;
    ht->nentries += MAX(2, nentries / 4); //DEBUG_REHASH
    ht->entries   =
        (ahash32_entry *)malloc(ht->nentries * sizeof(ahash32_entry));
    if (!ht->entries) return ERR_OOM;
    bzero(ht->entries, ht->nentries * sizeof(ahash32_entry));
    init32(ht);
    for(; nentries ; nentries--,entries++) {
        uint32 key = entries->key;
        if (key) {
            status = hash32_insert(key, entries->val, rehash_lvl, ht);
            if (status) return status;
        }
    }
    free(dead);
    return status;
}

int alc_hash32_insert(uint32 key, ulong val, ahash32 *ht) {
    return hash32_insert(key, val, 0, ht);
}

static int hash32_insert(uint32 key, ulong val, uint32 rehash_lvl, ahash32 *ht){
    int status = 0;
    if (!key) return ERR_INVALID_HASHKEY;
    ahash32_entry *v;
add:
    v = lookup_32insert_entry(key, ht);
    if (v) {
        if (v->key == key) v->val = val; // over-write
        else  { v->key = key; v->val = val; ht->nvalid++; } // set
    } else { /* indication of a poor hash function */
        if (rehash_lvl > 1) {
            while (!v) {
                if (ht->max_colijns == ht->nentries) break;    // full?
                ht->max_colijns +=  ht->max_colijns / 2;       // expand
                if (ht->max_colijns > ht->nentries) {
                    ht->max_colijns = ht->nentries;            // clip 
                }
                v = lookup_32insert_entry(key, ht);            // again
            }
        }
        if (rehash32(ht, rehash_lvl + 1) != 0) return ERR_OOM; // gotta rehash
        goto add;                                              // add
    }
    return status;
}

void alc_hash32_delete(uint32 key, ahash32 *ht) {
    if (key) {
        ahash32_entry *v = lookup_32entry(key, ht);
        if (v) { v->key = 0; ht->nvalid--; }
    }
}

ulong alc_hash32_fetch(uint32 key, ahash32 *ht) {
    if (key) {
        ahash32_entry *v = lookup_32entry(key, ht);
        if (v) return v->val;
    }
    return 0;
}

#ifndef ALCHEMY_DATABASE
#define DEBUG_HASH_TEST
#endif
#define SEQUENTIAL_TEST

#ifdef DEBUG_HASH_TEST
int main(int argc, char **argv) {
    if (argc != 3) { printf("Usage: %s num range\n", argv[0]); return -1; }
    int      num   = atoi(argv[1]);
    int      range = atoi(argv[2]);
    ahash32 *ht    = alc_hash32_make(2);
    printf("SIZE: %u\n", alc_hash32_size(ht));
    srand(1); // NOT a random seed
    for (int i = 0; i < num; i++) {
        int r = rand() % range;
#ifdef SEQUENTIAL_TEST
        r = i + 4;
#endif
        alc_hash32_insert(r, (long)r, ht);
    }
    printf("SIZE: %u nentries: %d\n", alc_hash32_size(ht), ht->nentries);

    srand(1); // NOT a random seed
    for (int i = 0; i < num; i++) {
        int  r = rand() % range;
#ifdef SEQUENTIAL_TEST
        r = i + 4;
#endif
        long j = alc_hash32_fetch(r, ht);
        if (r != j) {
            printf("ERROR: fetch: %d -> %ld\n", r, j); return -1;
        }
    }
    return 0;
}
#endif


// NOTE: hash16 is a SHAMELESS COPY of hash32
__inline__ static void init16(ahash16 *ht) {
    ht->nvalid = 0;
    /* only reset if NOT set */
    if (ht->max_colijns == 0) ht->max_colijns = MAX_COLLISIONS;
}

ahash16 *alc_hash16_make(uint32 nentries) {
    ahash16 *ht   = (ahash16 *)malloc(sizeof(ahash16)); if (!ht) return NULL;
    bzero(ht, sizeof(ahash16));
    if (!nentries) nentries = 2; /* need at least some */
    size_t   size = nentries * sizeof(ahash16_entry);
    ht->nentries  = nentries;
    ht->entries   = (ahash16_entry *)malloc(size);
    if (!ht->entries) { free(ht); ht = NULL; return NULL; }
    bzero(ht->entries, size);
    init16(ht);
    return ht;
}

uint32 alc_hash16_size(ahash16 *ht) {
    return (ht->nentries * sizeof(ahash16_entry)) + sizeof(ahash16);
}

void alc_hash16_destroy(ahash16 *ht) {
    if (ht) { if (ht->entries) free(ht->entries); free(ht); }
}

#define SIMULATE_16HT_ENTRIES         \
  if (!ht->entries) {                 \
    uchar *v = (uchar *)ht;           \
    v += sizeof(ahash16);             \
    ht->entries = (ahash16_entry *)v; \
  }

__inline__ static ahash16_entry *lookup_16entry(const ushort16  key,
                                                ahash16        *ht) {
    SIMULATE_16HT_ENTRIES
    uint32         nentries  = ht->nentries,
                   index     = key % nentries;
    ahash16_entry *entry     = ht->entries + index;
    ushort16       entry_key = entry->key;
    if (!(entry_key ^ key))                                     return entry;
    if (entry->n_colijns > 0) { /* search collisions */
        uint32 nwrap = 0, last_index = index + entry->n_colijns;
        if (last_index >= nentries) { /* clip */
            nwrap = last_index - nentries + 1; last_index = nentries - 1;
        }
        for(index++,entry++; index <= last_index; index++, entry++) {
            entry_key = entry->key; if (!(entry_key ^ key))     return entry;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap ; nwrap--,entry++) {
                entry_key = entry->key; if (!(entry_key ^ key)) return entry;
            }
        }
    }
    return NULL;
}

__inline__ static ahash16_entry *lookup_16insert_entry(const ushort16  key,
                                                       ahash16        *ht){
    SIMULATE_16HT_ENTRIES
    uint32         nentries  = ht->nentries,
                   index     = key % nentries;
    ahash16_entry *entry     = ht->entries + index;
    ushort16       entry_key = entry->key;
    if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) return entry;
    { /* search collisions */
        uint32         n_colijns   = 1,
                       nwrap       = 0,
                       last_index  = index + ht->max_colijns;
        ahash16_entry *collision   = entry;
        if (last_index >= nentries) { /* clip */
          nwrap      = MIN(nentries, last_index - nentries + 1);
          last_index = nentries - 1;
        }
        for(index++,entry++; index<=last_index; index++,entry++,n_colijns++) {
            entry_key = entry->key;
            if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) goto done;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap; nwrap--, entry++, n_colijns++) {
                entry_key = entry->key;
                if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) goto done;
            }
        }
        return NULL;

    done: /* update collision count */
        if (n_colijns > collision->n_colijns) collision->n_colijns = n_colijns;
        return entry;
    }
}

static int hash16_insert(ushort16  key, uint32 val, uint32 rehash_lvl,
                         ahash16  *ht);

#define DEBUG_REHASH                                   \
  printf("rehash: nvalid: %d from: %u to : %u\n",      \
          ht->nvalid, nentries, ht->nentries);

static int rehash16(ahash16 *ht, uint32 rehash_lvl) { /* add %25 more entries */
    SIMULATE_16HT_ENTRIES
    int            status   = 0;
    uint32         nentries = ht->nentries;
    ahash16_entry *entries  = ht->entries,
                  *dead     = entries;
    ht->nentries += MAX(2, nentries / 4); //DEBUG_REHASH
    ht->entries   =
        (ahash16_entry *)malloc(ht->nentries * sizeof(ahash16_entry));
    if (!ht->entries) return ERR_OOM;
    bzero(ht->entries, ht->nentries * sizeof(ahash16_entry));
    init16(ht);
    for(; nentries ; nentries--,entries++) {
        ushort16 key = entries->key;
        if (key) {
            status = hash16_insert(key, entries->val, rehash_lvl, ht);
            if (status) return status;
        }
    }
    free(dead);
    return status;
}

int alc_hash16_insert(ushort16 key, uint32 val, ahash16 *ht) {
    return hash16_insert(key, val, 0, ht);
}

static int hash16_insert(ushort16 key, uint32 val, uint32 rehash_lvl,
                         ahash16 *ht)                               {
    int status = 0;
    if (!key) return ERR_INVALID_HASHKEY;
    ahash16_entry *v;
add:
    v = lookup_16insert_entry(key, ht);
    if (v) {
        if (v->key == key) v->val = val; // over-write
        else  { v->key = key; v->val = val; ht->nvalid++; } // set
    } else { /* indication of a poor hash function */
        if (rehash_lvl > 1) {
            while (!v) {
                if (ht->max_colijns == ht->nentries) break;    // full?
                ht->max_colijns +=  ht->max_colijns / 2;       // expand
                if (ht->max_colijns > ht->nentries) {
                    ht->max_colijns = ht->nentries;            // clip 
                }
                v = lookup_16insert_entry(key, ht);            // again
            }
        }
        if (rehash16(ht, rehash_lvl + 1) != 0) return ERR_OOM; // gotta rehash
        goto add;                                              // add
    }
    return status;
}

void alc_hash16_delete(ushort16 key, ahash16 *ht) {
    if (key) {
        ahash16_entry *v = lookup_16entry(key, ht);
        if (v) { v->key = 0; ht->nvalid--; }
    }
}

uint32 alc_hash16_fetch(ushort16 key, ahash16 *ht) {
    if (key) {
        ahash16_entry *v = lookup_16entry(key, ht);
        if (v) return v->val;
    }
    return 0;
}

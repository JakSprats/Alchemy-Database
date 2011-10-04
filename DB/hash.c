
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "hash.h"

#define ERR_OOM             -1
#define ERR_INVALID_HASHKEY -1

__inline__ static void init32(ahash *ht) {
    ht->nvalid_entries = 0;
    /* only reset if NOT set */
    if (ht->max_collisions == 0) ht->max_collisions = MAX_COLLISIONS;
}

ahash *alc_hash32_make(uint32 nentries) {
    ahash  *ht   = (ahash *)malloc(sizeof(ahash));
    if (!ht) return NULL;
    bzero(ht, sizeof(ahash));
    if (!nentries) nentries = 2; /* need at least some */
    size_t  size = nentries * sizeof(ahash_entry);
    ht->nentries = nentries;
    ht->entries  = (ahash_entry *)malloc(size);
    if (!ht->entries) { free(ht); ht = NULL; return NULL; }
    bzero(ht->entries, size);
    init32(ht);
    return ht;
}

void alc_hash32_destroy(ahash *ht) {
    if (ht) { if (ht->entries) free(ht->entries); free(ht); }
}

//NOTE: when a dict is serialised into a stream, it can not contain pointers
//      so the ht->entries pointer is "simulated" it always comes just below
#define SIMULATE_HT_ENTRIES \
  if (!ht->entries) { \
    uchar *v = (uchar *)ht; v += sizeof(ahash); ht->entries = (ahash_entry *)v;\
  }

__inline__ static ahash_entry *lookup_32entry(const uint32 key, ahash *ht) {
    SIMULATE_HT_ENTRIES
    uint32       nentries  = ht->nentries,
                 index     = key % nentries;
    ahash_entry *entry     = ht->entries + index;
    uint32       entry_key = entry->key;
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

__inline__ static ahash_entry *lookup_insert_entry(const uint32 key, ahash *ht){
    SIMULATE_HT_ENTRIES
    uint32       nentries  = ht->nentries,
                 index     = key % nentries;
    ahash_entry *entry     = ht->entries + index;
    uint32       entry_key = entry->key;
    if (!(entry_key ^ key) || !(entry_key ^ HASH_NULL)) return entry;
    { /* search collisions */
        uint32       n_colijns   = 1,
                     nwrap       = 0,
                     last_index  = index + ht->max_collisions;
        ahash_entry *collision   = entry;
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

static int hash32_insert(uint32 key, ulong val, uint32 rehash_lvl, ahash *ht);

#define DEBUG_REHASH                                   \
  printf("rehash: nvalid: %d from: %u to : %u\n",      \
          ht->nvalid_entries, nentries, ht->nentries);

static int rehash32(ahash *ht, uint32 rehash_lvl) { /* add %25 more entries */
    SIMULATE_HT_ENTRIES
    int          status   = 0;
    uint32       nentries = ht->nentries;
    ahash_entry *entries  = ht->entries,
                *dead     = entries;
    ht->nentries += MAX(2, nentries / 4); //DEBUG_REHASH
    ht->entries   = (ahash_entry *)malloc(ht->nentries * sizeof(ahash_entry));
    if (!ht->entries) return ERR_OOM;
    bzero(ht->entries, ht->nentries * sizeof(ahash_entry));
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

int alc_hash32_insert(uint32 key, ulong val, ahash *ht) {
    return hash32_insert(key, val, 0, ht);
}

static int hash32_insert(uint32 key, ulong val, uint32 rehash_lvl, ahash *ht) {
    int status = 0;
    if (!key) {
        status = ERR_INVALID_HASHKEY;
    } else { 
        ahash_entry *v;
add:
        v = lookup_insert_entry(key, ht);
        if (v) {
            if (v->key == key) v->val = val; // over-write
            else  { v->key = key; v->val = val; ht->nvalid_entries++; } // set
        } else { /* indication of a poor hash function */
            if (rehash_lvl > 1) {
                while (!v) {
                    if (ht->max_collisions == ht->nentries) break;  // full?
                    ht->max_collisions +=  ht->max_collisions / 2;  // expand
                    if (ht->max_collisions > ht->nentries) {
                        ht->max_collisions =  ht->nentries;         // clip 
                    }
                    v = lookup_insert_entry(key, ht);               // again
                }
            }
            if (rehash32(ht, rehash_lvl + 1) != 0) {                // rehash
                status = ERR_OOM; goto done;
            }
            goto add; /* add */
        }
    }

done:
    return status ;
}

void alc_hash32_delete(uint32 key, ahash *ht) {
    if (key) {
        ahash_entry *v = lookup_32entry(key, ht);
        if (v) { v->key = 0; ht->nvalid_entries--; }
    }
}

ulong alc_hash32_fetch(uint32 key, ahash *ht) {
    if (key) {
        ahash_entry *v = lookup_32entry(key, ht);
        if (v) return v->val;
    }
    return 0;
}

uint32 alc_hash32_size(ahash *ht) {
    return (ht->nentries * sizeof(ahash_entry)) + sizeof(ahash);
}

#ifndef ALCHEMY_DATABASE
#define DEBUG_HASH_TEST
#endif
#define SEQUENTIAL_TEST

#ifdef DEBUG_HASH_TEST
int main(int argc, char **argv) {
    if (argc != 3) { printf("Usage: %s num range\n", argv[0]); return -1; }
    int    num   = atoi(argv[1]);
    int    range = atoi(argv[2]);
    ahash *ht    = alc_hash32_make(2);
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

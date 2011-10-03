
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "hash.h"

#define ERR_OOM             -1
#define ERR_INVALID_HASHKEY -1

__inline__ static void init32(alc_hash32_t *ht) {
    uint32 nentries = ht->nentries;
    alc_hash32_entry_t *entries = ht->entries;
    memset(entries, 0, nentries*sizeof(alc_hash32_entry_t));
    /* only reset if NOT set */
    if (ht->max_collisions == 0) ht->max_collisions = MAX_COLLISIONS;
    ht->nvalid_entries = 0;
}

alc_hash32_t *alc_hash32_make(uint32 nentries) {
    size_t        size;
    alc_hash32_t *ht = (alc_hash32_t *)malloc(sizeof(alc_hash32_t));
    if (!ht) return NULL;
    memset(ht, 0, sizeof(alc_hash32_t));
    if (!nentries) nentries = 2; /* need at least some */
    size         = nentries * sizeof(alc_hash32_entry_t);
    ht->nentries = nentries;
    ht->entries  = (alc_hash32_entry_t *)malloc(size);
    if (!ht->entries) { free(ht); ht = NULL; return NULL; }
    init32(ht);
    return ht;
}

void alc_hash32_clear(alc_hash32_t *ht) {
    init32(ht);
}

void alc_hash32_destroy(alc_hash32_t *ht) {
    if (ht != NULL) {
        if (ht->entries != NULL) free(ht->entries);
        free(ht);
    }
}

__inline__ static alc_hash32_entry_t *lookup_32entry(const uint32 key, 
                              const alc_hash32_t *ht) {
    uint32            nentries  = ht->nentries,
                        index     = key % nentries;
    alc_hash32_entry_t *entry     = ht->entries + index ;
    uint32            entry_key = entry->key;
    if ( (entry_key ^ key) == 0 ) goto done;
    if ( entry->n_collisions > 0 ) { /* search collisions */
        uint32 nwrap = 0, last_index = index + entry->n_collisions;
        if (last_index >= nentries) { /* clip */
            nwrap = last_index - nentries + 1;
            last_index = nentries - 1;
        }
        for(index++,entry++; index<=last_index; index++,entry++) {
            entry_key = entry->key;
            if ( (entry_key ^ key) == 0 ) goto done;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap ; nwrap--,entry++) {
                entry_key = entry->key;
                if ( (entry_key ^ key) == 0 ) goto done;
            }
        }
    }
    entry = NULL;

done:
    return entry;
}

__inline__ static alc_hash32_entry_t 
                            *lookup_insertion_32entry(const uint32      key,
                                                      const alc_hash32_t *ht) {
    uint32              nentries  = ht->nentries,
                        index     = key % nentries;
    alc_hash32_entry_t *entry     = ht->entries + index;
    uint32              entry_key = entry->key;
    if ((entry_key ^ key) == 0 || /* match */
        (entry_key ^ HASH_NULL_KEY) == 0 ) return entry;
    { /* search collisions */
        uint32 n_collisions           = 1,
                 nwrap                = 0,
                 last_index           = index + ht->max_collisions;
        alc_hash32_entry_t *collision = entry;
        if (last_index >= nentries) { /* clip */
          nwrap      = MIN(nentries, last_index - nentries + 1);
          last_index = nentries - 1;
        }
        for(index++,entry++; index<=last_index; 
                                               index++,entry++,n_collisions++) {
            entry_key = entry->key;
            if ((entry_key ^ key) == 0 ||
                (entry_key ^ HASH_NULL_KEY) == 0 ) goto done;
        }
        if (nwrap) { /* wrap */
            entry = ht->entries;
            for( ; nwrap; nwrap--, entry++, n_collisions++) {
                entry_key = entry->key;

                if ((entry_key ^ key) == 0 ||
                    (entry_key ^ HASH_NULL_KEY) == 0 ) goto done;
            }
        }
        return NULL;

    done: /* update collision count */
        if (n_collisions > collision->n_collisions ) {
            collision->n_collisions = n_collisions;
        }
        return entry;
    }
}

static int hash32_insert(uint32 key,          void         *user_pointer,
                             uint32 rehash_level, alc_hash32_t *ht);

/* add %50 more entries */
static int rehash32(alc_hash32_t *ht, uint32 rehash_level) {//printf("rehsh\n");
    int                 status   = 0;
    uint32            nentries = ht->nentries;
    alc_hash32_entry_t *entries  = ht->entries,
                       *dead     = entries;
    ht->nentries += nentries/2;
    ht->entries = 
      (alc_hash32_entry_t *)malloc(ht->nentries*sizeof(alc_hash32_entry_t));
    if (ht->entries == NULL) {
        status = ERR_OOM; goto done;
    }
    init32(ht);
    for(; nentries ; nentries--,entries++) {
        uint32 key = entries->key;
        if (key != HASH_NULL_KEY) {
            status = hash32_insert(key,          entries->user_pointer,
                                   rehash_level, ht);
            if (status != 0) goto done;
        }
    }
    free(dead);

done:
    return status ;
}

int alc_hash32_insert(uint32 key, void *user_pointer, alc_hash32_t *ht) {
    return hash32_insert(key, user_pointer, 0, ht);
}

static int hash32_insert(uint32 key,          void         *user_pointer,
                             uint32 rehash_level, alc_hash32_t *ht)          {
    int status = 0;
    if (key == HASH_NULL_KEY) {
        status = ERR_INVALID_HASHKEY;
    } else { 
        alc_hash32_entry_t *val;
add:
        val = lookup_insertion_32entry(key, ht);
        if (val != NULL) {
            if (val->key == key ) {        /* over write , don't incr count */
                val->user_pointer = user_pointer;
            } else  {                /* set */
                val->key = key;
                val->user_pointer = user_pointer;
                ht->nvalid_entries++;
            }
        } else {
            /* indication of a poor hash function */
            if (rehash_level > 1) {
                while (val == NULL) {
                    if (ht->max_collisions == ht->nentries) break;  /* full? */
                    ht->max_collisions +=  ht->max_collisions/2; /* expand */
                    if (ht->max_collisions > ht->nentries) {
                        ht->max_collisions =  ht->nentries; /* clip */
                    }
                    val = lookup_insertion_32entry(key, ht); /* again */
                }
            }
            if ( rehash32(ht, rehash_level + 1) != 0) { /* gotta rehash */
                status = ERR_OOM; goto done;
            }
            goto add; /* add */
        }
    }

done:
    return status ;
}

void alc_hash32_delete(uint32 key, alc_hash32_t *ht) {
    if (key != HASH_NULL_KEY) {
        alc_hash32_entry_t *val = lookup_32entry(key, ht);
        if (val != NULL) {
            val->key = HASH_NULL_KEY;
            ht->nvalid_entries--;
        }
    }
}

void  *alc_hash32_fetch(uint32 key, const alc_hash32_t *ht) {
    void *user_pointer = NULL;
    if (key != HASH_NULL_KEY) {
      alc_hash32_entry_t *val = lookup_32entry(key, ht);
      if (val != NULL ) user_pointer = val->user_pointer;
    }
    return user_pointer;
}

uint32 alc_hash32_size(alc_hash32_t *ht) {
    return (ht->nentries * sizeof(alc_hash32_entry_t)) +
           sizeof(alc_hash32_t) - 8;
}

// interpret the user pointer as a 32bit unsigned int and increment by 'incr'
int alc_hash32_uincr(uint32 key, uint32 incr, alc_hash32_t *ht) {
    int status = 0;
    if (key != HASH_NULL_KEY) {
        alc_hash32_entry_t *val = lookup_32entry(key, ht);
        if (val != NULL ) {
            uint32 count    = (uint32)(long)(val->user_pointer) + incr;
            val->user_pointer = (void *)(long)count;
        } else {
            status = alc_hash32_insert(key, (void *)(long)incr, ht);
        }
    }
    return status;
}

// free the memory for those nodes that eval( user_ptr, value_ptr )
void alc_hash32_thin(alc_hash32_t *ht,                void *user_ptr,
                     uint32 (*eval)(void *user_ptr, void *value_ptr) ) {
    uint32            i, n    = ht->nentries;
    alc_hash32_entry_t *entries = ht->entries;
    for(i = 0; i < n; i++) { // loop thru table, evaling all non-null entries
        alc_hash32_entry_t *e = entries + i;
        if (e->key != HASH_NULL_KEY) {
            uint32 val = (*eval)(user_ptr, e->user_pointer);
            if ( val > 0 ) { /* delete */
                e->key = HASH_NULL_KEY;
                ht->nvalid_entries--;
            }
        }
    }
}

// eval( user_ptr, key, value_ptr ) for all elements
void alc_hash32_map(alc_hash32_t *ht,            void *user_ptr,
                    void (*eval)(void *user_ptr, uint32 key, void *value_ptr)) {
    uint32            i, n    = ht->nentries;
    alc_hash32_entry_t *entries = ht->entries;
    for(i = 0; i < n; i++) { // loop thru table, evaling all non-null entries
        alc_hash32_entry_t *e = entries + i;
        if (e->key != HASH_NULL_KEY) (*eval)(user_ptr, e->key, e->user_pointer);
    }
}

//#define HASH_TEST
#ifdef HASH_TEST
int main(int argc, char **argv) {
    if (argc != 2) { printf("Usage: %s num\n", argv[0]); return -1; }
    int           num = atoi(argv[1]);
    alc_hash32_t *ht  = alc_hash32_make(2);
    printf("SIZE: %u\n", alc_hash32_size(ht));
    for (int i = 0; i < num; i++) alc_hash32_insert(i, (void*)(long)i, ht);
    printf("SIZE: %u\n", alc_hash32_size(ht));

    for (int i = 0; i < num; i++) {
        void *v = alc_hash32_fetch(i, ht);
        if (i != (int)(long)v) {
            printf("ERROR: fetch: %d -> %d\n", i, (int)(long)v); return -1;
        }
    }
    return 0;
}
#endif

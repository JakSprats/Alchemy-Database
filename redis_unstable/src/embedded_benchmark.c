
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <strings.h>
#include <sys/time.h>

#include "redis.h"

#include "embed.h"

// PROTOTYPES
typedef int printer(const char *format, ...);
struct aobj;
void dumpAobj(printer *prn, struct aobj *a);

//GLOBALS
extern eresp_t     *CurrEresp; // USED in callbacks to get "CurrEresp->cnames[]"
extern redisClient *EmbeddedCli; // USED for unsafe access

// HELPERS
static long long mstime(void) {
    struct timeval tv; gettimeofday(&tv, NULL);
    return ((long)tv.tv_sec)*1000 + tv.tv_usec/1000;
}
static void free_rargv(int rargc, robj **rargv) {
    for (int j = 0; j < rargc; j++) decrRefCount(rargv[j]);
    zfree(rargv);
}

// DEBUG
static bool print_callback_w_cnames(erow_t* er) {
    printf("\tROW:\n");
    for (int i = 0; i < er->ncols; i++) {
        printf("\t\t%s: ", CurrEresp->cnames[i]); dumpAobj(printf, er->cols[i]);
    }
    return 1;
}

// TESTS
static void insert_into_table(int nrows) {
    e_alchemy_raw("CREATE TABLE kv (pk INT, val TEXT)", NULL);
    long long beg = mstime();
    for (int i = 1; i < nrows; i++) {
        char buf[128];
        sprintf(buf, "INSERT INTO kv (val) VALUES ('random_text_%011d')", i);
        e_alchemy_raw(buf, NULL);
    }
    long long fin = mstime();
    printf("INSERT: %d rows, duration: %lld ms, %lldK TPS\n",
           nrows, (fin - beg), nrows / (fin - beg));
}
static void select_from_table(int nrows) {
    long long beg = mstime();
    for (int i = 1; i < nrows; i++) {
        char buf[128];
        sprintf(buf, "SELECT * FROM kv WHERE pk = %011d", i);
        //e_alchemy_raw(buf, print_callback_w_cnames);
        e_alchemy_raw(buf, NULL);
    }
    long long fin = mstime();
    printf("SELECT: %d rows, duration: %lld ms, %lldK TPS\n",
           nrows, (fin - beg), nrows / (fin - beg));
}

static void populate_kv(int nkeys) {
    int    rargc = 3;
    robj **rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0]     = createStringObject("SET", strlen("SET"));
    rargv[1]     = createStringObject("key_00000000000001",
                                      strlen("key_00000000000001"));
    long long beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        char buf[64];
        sprintf(rargv[1]->ptr, "key_%011d",         i);
        sprintf(buf,           "random_text_%011d", i);
        rargv[2] = createStringObject(buf, 26);
        e_alchemy_no_free(rargc, rargv, NULL);
        decrRefCount(rargv[2]);
    }
    long long fin = mstime();
    printf("SET KV: %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        char buf[64];
        sprintf(rargv[1]->ptr, "key_%011d",         i);
        sprintf(buf,           "random_text_%011d", i);
        rargv[2] = createStringObject(buf, 26);
        e_alchemy_no_free(rargc, rargv, NULL);
        decrRefCount(rargv[2]);
    }
    fin = mstime();
    printf("SET KV: OVERWRITE %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    beg = mstime();
    char buf[64];
    sprintf(buf, "random_text_%011d", 0);
    rargv[2] = createStringObject(buf, 26);

    for (int i = 0; i < nkeys; i++) e_alchemy_no_free(rargc, rargv, NULL);
    fin = mstime();
    printf("SET KV: SAME-KEY %d times, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    free_rargv(rargc, rargv);
}
static void get_kv(int nkeys) {
    int    rargc = 2;
    robj **rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0] = createStringObject("GET", strlen("GET"));
    rargv[1] = createStringObject("key_00000000000001", strlen("key_00000000000001"));
    long long beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        sprintf(rargv[1]->ptr, "key_%011d",         i);
        e_alchemy_no_free(rargc, rargv, NULL);
    }
    long long fin = mstime();
    printf("GET KV: %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));


    free_rargv(rargc, rargv);
}

static void populate_unsafe_kv(int nkeys) {
    initEmbeddedAlchemy();
    robj *key = createStringObject("key_00000000000001",
                                   strlen("key_00000000000001"));
    long long beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        sprintf(key->ptr, "key_%011d",         i);
        char buf[64]; sprintf(buf, "random_text_%011d", i);
        robj *val = createStringObject(buf, 26);
        setKey(EmbeddedCli->db, key, val);
    }
    long long fin = mstime();
    printf("UNSAFE: SET KV: %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        sprintf(key->ptr, "key_%011d",         i);
        char buf[64]; sprintf(buf, "random_text_%011d", i);
        robj *val = createStringObject(buf, 26);
        setKey(EmbeddedCli->db, key, val);
    }
    fin = mstime();
    printf("UNSAFE: SET KV: OVERWRITE: %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    beg = mstime();
    char buf[64]; sprintf(buf, "random_text_%011d", 0);
    robj *val = createStringObject(buf, 26);
    for (int i = 0; i < nkeys; i++) setKey(EmbeddedCli->db, key, val);
    fin = mstime();
    printf("UNSAFE: SET KV: SAME-KEY: %d times, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));

    decrRefCount(key); decrRefCount(val);
}
static void get_unsafe_kv(int nkeys) {
    initEmbeddedAlchemy();
    robj *key = createStringObject("key_00000000000001",
                                   strlen("key_00000000000001"));
    long long beg = mstime();
    for (int i = 0; i < nkeys; i++) {
        sprintf(key->ptr, "key_%011d",         i);
        robj *o = lookupKeyRead(EmbeddedCli->db, key);
    }
    long long fin = mstime();
    printf("UNSAFE: GET KV: %d keys, duration: %lld ms, %lldK TPS\n",
           nkeys, (fin - beg), nkeys / (fin - beg));
}


int main(int argc, char **argv) {
    argc = 0; argv = NULL; /* compiler warning */
    int nrows = 10000000;
    int nkeys = 10000000; //nrows = nkeys = 1000000; // DEBUG values

    populate_unsafe_kv(nkeys);
    get_unsafe_kv(nkeys);
    e_alchemy_raw("FLUSHALL", NULL);

    populate_kv(nkeys);
    get_kv(nkeys);
    e_alchemy_raw("FLUSHALL", NULL);

    insert_into_table(nrows);
    select_from_table(nrows);
    e_alchemy_raw("FLUSHALL", NULL);

    return 0;
}

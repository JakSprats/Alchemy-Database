
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <strings.h>

#include "redis.h"

#include "common.h"
#include "embed.h"

#define TEST_KV
//#define TEST_SB
#define TEST_REDIS
#define TEST_PREPARED_STATEMENT_KV

//#define DEBUG_DESC_TABLE
//#define DEBUG_INFO
//#define DEBUG_KEYS
//#define DEBUG_PRINT
//#define DEBUG_FEW_ROWS
//#define DEBUG_HIT_ENTER
//#define SHORT_INSERT_STRING

//GLOBALS
extern eresp_t *CurrEresp;       // USED in callbacks to get "CurrEresp->cnames[]"
extern bool     GlobalZipSwitch; // can GLOBALLY turn off [lzf] compression of rows
extern bool     GlobalNeedCn;    // turn print_cnames ON/OFF in PREPARED_STATMENTs

extern int   Num_tbls; // USED in thin_select
extern dict *StmtD;    // USED for EXEC_BIN

// PROTOTYPES
struct aobj;
void dumpAobj(printer *prn, struct aobj *a);      // from aobj.h
void bytesToHuman(char *s, unsigned long long n); // from redis.c
void print_mem_usage(int tmatch);                 // from desc.h

static void init_kv_table();
static void populate_kv_table(ulong prows);

// HELPERS
static long long mstime() {
    struct timeval tv; gettimeofday(&tv, NULL);
    return ((long)tv.tv_sec)*1000 + tv.tv_usec/1000;
}

// CALLBACKS
static bool print_cb_w_cnames(erow_t* er) {
    printf("\tROW:\n");
    for (int i = 0; i < er->ncols; i++) {
        printf("\t\t%s: ", CurrEresp->cnames[i]); dumpAobj(printf, er->cols[i]);
    }
    return 1;
}
static bool print_cb_key(erow_t* er) {
    printf("\tVAL: "); dumpAobj(printf, er->cols[0]); return 1;
}

// DEBUG
static void hit_return_to_continue() {
#ifdef DEBUG_HIT_ENTER
    char buff[80];
    printf("Hit Return To Continue:\n");
    if (!fgets(buff, sizeof(buff), stdin)) assert(!"fgets FAILED");
#endif
}
static void debug_rows(ulong beg, ulong end) {
    ereq_t ereq; init_ereq(&ereq);
    ereq.op                 = SELECT;
    ereq.tablelist          = sdsnew("kv");
    ereq.scb                = print_cb_w_cnames;
    ereq.select_column_list = sdsnew("*");
    for (ulong i = beg; i < end; i++) {
        char lbuf[32]; sprintf(lbuf, "pk = %lu", i);
        ereq.where_clause = sdsnew(lbuf);
        e_alchemy_sql_fast(&ereq);
        sdsfree(ereq.where_clause); ereq.where_clause = NULL;
    }
    hit_return_to_continue();
    release_ereq(&ereq);
}
static void desc_table(char *tname) {
#ifdef  DEBUG_DESC_TABLE
    char buf[64]; snprintf(buf, 63, "DESC %s", tname); buf[63] = '\0';
    printf("\n");
    eresp_t *eresp = e_alchemy_raw(buf, NULL);
    printEmbedResp(eresp); printf("\n");
#else
    tname = NULL; /* compiler warning */
#endif
}
static void info() {
#ifdef  DEBUG_INFO
    printf("\n");
    eresp_t *eresp = e_alchemy_raw("INFO", NULL);
    printEmbedResp(eresp); printf("\n");
#endif
}
static void keys() {
#ifdef  DEBUG_KEYS
    printf("KEYS: \n");
    eresp_t *eresp = e_alchemy_raw("KEYS *", NULL);
    printEmbedResp(eresp); printf("\n");
#endif
}
static void print_redis_mem_info() {
    char hmem[64]; bytesToHuman(hmem, zmalloc_used_memory());
    printf("used_memory: %zu used_memory_human: %s used_memory_rss: %zu "
           "mem_fragmentation_ratio: %.2f\n",
            zmalloc_used_memory(), hmem,
            zmalloc_get_rss(), zmalloc_get_fragmentation_ratio());
}

// POPULATE_KV
static void init_kv_table() {
    e_alchemy_raw("DROP TABLE kv", NULL);
    e_alchemy_raw("CREATE TABLE kv (pk LONG, val TEXT)", NULL);
}
static void populate_kv_table(ulong prows) {
    char buf[1024]; char nbuf[16];
#ifndef SHORT_INSERT_STRING
    char  *insert_string = "(00000000000001, '00000000000001 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')";
#else
    char  *insert_string = "(00000000000001, '00000000000001 not much text')";
#endif

    ereq_t ereq; init_ereq(&ereq);
    ereq.op        = INSERT;
    ereq.tablelist = sdsnew("kv");

    memcpy(buf, insert_string, strlen(insert_string) + 1);
    char *pkspot  = buf + 1;
    char *valspot = buf + 18;
#ifdef  DEBUG_DESC_TABLE
    printf("Populating table KV: prows: %lu zip: %d\n", prows, GlobalZipSwitch);
#endif
    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < prows; i++) {
        sprintf(nbuf, "%014lu", i); // Next 2 lines avoid a 420Byte sprintf()
        memcpy(pkspot, nbuf, 14); memcpy(valspot, nbuf, 14);
        ereq.insert_value_string = sdsnew(buf);
        e_alchemy_sql_fast(&ereq);
        sdsfree(ereq.insert_value_string); ereq.insert_value_string = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : prows / (fin - beg);
    printf("KV: INSERT%s%lu rows, duration: %lld ms, %lldK TPS\n",
           GlobalZipSwitch ? ":\t\t\t" : ": [NO ZIP]:\t\t",
           prows, (fin - beg), tps);
    print_mem_usage(Num_tbls - 1);
    desc_table("kv"); hit_return_to_continue();
    release_ereq(&ereq);
}

// SELECT_KV
static void test_kv_zip_select(ulong prows, ulong qrows) {
    ereq_t ereq; init_ereq(&ereq);
    init_kv_table(); populate_kv_table(prows);

    ereq.op                 = SELECT;
    ereq.tablelist          = sdsnew("kv");
#ifdef DEBUG_PRINT
    ereq.scb                = print_cb_w_cnames;
#endif
    ereq.select_column_list = sdsnew("val");

    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < qrows; i++) {
        char lbuf[32];
        uint32 index      = rand() % prows + 1;
        sprintf(lbuf, "pk = %u", index);
        ereq.where_clause = sdsnew(lbuf);
        e_alchemy_sql_fast(&ereq);
        sdsfree(ereq.where_clause); ereq.where_clause = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("KV: SELECT:\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           qrows, (fin - beg), tps);
    hit_return_to_continue();
    release_ereq(&ereq);
}

// DELETE_KV
static void test_kv_delete(ulong prows, ulong qrows) {
    ereq_t ereq; init_ereq(&ereq);
    init_kv_table(); populate_kv_table(prows);

    ereq.op                 = DELETE;
    ereq.tablelist          = sdsnew("kv");
#ifdef DEBUG_PRINT
    ereq.scb                = print_cb_w_cnames;
#endif

    ulong     iters = (prows / 2); // DELETE first HALF of table
    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < iters; i++) {
        char lbuf[32];
        sprintf(lbuf, "pk = %lu", i);
        ereq.where_clause = sdsnew(lbuf);
        e_alchemy_sql_fast(&ereq);
        sdsfree(ereq.where_clause); ereq.where_clause = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("KV: DELETE:\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           iters, (fin - beg), tps);
    hit_return_to_continue();
#ifdef  DEBUG_DESC_TABLE
    printf("AFTER DELETION\n"); desc_table("kv");
#endif
    release_ereq(&ereq);
}

// UPDATE_KV
static void test_kv_update(ulong prows, ulong qrows) {
    ereq_t ereq; init_ereq(&ereq);
    init_kv_table(); populate_kv_table(prows);

    ereq.op              = UPDATE;
    ereq.tablelist       = sdsnew("kv");
#ifdef DEBUG_PRINT
    ereq.scb             = print_cb_w_cnames;
#endif
    ereq.update_set_list = sdsnew("val='not much text'");

    ulong     iters = (prows / 2); // UPDATE first HALF of table
    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < iters; i++) {
        char lbuf[32];
        sprintf(lbuf, "pk = %lu", i);
        ereq.where_clause = sdsnew(lbuf);
        e_alchemy_sql_fast(&ereq);
        sdsfree(ereq.where_clause); ereq.where_clause = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("KV: UPDATE:\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           iters, (fin - beg), tps);
    hit_return_to_continue();
#ifdef  DEBUG_DESC_TABLE
    printf("AFTER UPDATE\n"); desc_table("kv");
#endif
    //debug_rows((prows / 2 - 2), (prows / 2 + 2));
    release_ereq(&ereq);
}

static void test_redis(ulong prows, ulong qrows) {
    char buf[1024];
#ifndef SHORT_INSERT_STRING
    char  *insert_string = "00000000000001 XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')";
#else
    char  *insert_string = "00000000000001 not much text')";
#endif

    ereq_t ereq; init_ereq(&ereq);
#ifdef DEBUG_PRINT
    ereq.scb = print_cb_key;
#endif

    memcpy(buf, insert_string, strlen(insert_string) + 1);

    ereq.rop = SET;
    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < prows; i++) {
        char lbuf[32];
        sprintf(lbuf, "KEY_%lu", i);   ereq.redis_key   = sdsnew(lbuf);
        sprintf(lbuf, "%014lu", i); memcpy(buf, lbuf, 14);
        ereq.redis_value = sdsnew(buf);
        e_alchemy_redis(&ereq);
    }
    fin = mstime(); tps = (fin == beg) ? 0 : prows / (fin - beg);
    printf("SET:\t\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n",
           prows, (fin - beg), tps);
    print_redis_mem_info(); printf("\n");

    ereq.rop = GET;
    beg      = mstime(), fin, tps;
    for (ulong i = 1; i < qrows; i++) {
        char lbuf[32];
        ulong index = (ulong)rand() % prows + 1;
        sprintf(lbuf, "KEY_%lu", index); ereq.redis_key = sdsnew(lbuf);
        e_alchemy_redis(&ereq);
        sdsfree(ereq.redis_key);   ereq.redis_key   = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("GET:\t\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           qrows, (fin - beg), tps);
    info(); keys(); hit_return_to_continue();

    ereq.rop    = DEL;
    ulong iters = (prows / 2); // DEL first HALF of keys
    beg         = mstime(), fin, tps;
    for (ulong i = 1; i < iters; i++) {
        char lbuf[32];
        sprintf(lbuf, "KEY_%lu", i); ereq.redis_key = sdsnew(lbuf);
        e_alchemy_redis(&ereq);
        sdsfree(ereq.redis_key);   ereq.redis_key   = NULL;
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("DEL:\t\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           iters, (fin - beg), tps);
    info(); keys(); hit_return_to_continue();

    release_ereq(&ereq);
}

//TEST_PREPARED_STATEMENT_KV
static void test_prepared_statement_kv(ulong prows, ulong qrows) {
    ereq_t ereq; init_ereq(&ereq);
    init_kv_table(); populate_kv_table(prows);

    eresp_t *eresp = e_alchemy_raw(
                      "PREPARE P_KV AS SELECT val FROM kv WHERE pk = $1", NULL);
#ifdef DEBUG_PRINT
    printEmbedResp(eresp); printf("\n");
#endif

    ereq.op        = EXECUTE;
    ereq.plan_name = sdsnew("P_KV");
#ifdef DEBUG_PRINT
    GlobalNeedCn   = 1;
    ereq.scb       = print_cb_w_cnames;
#endif
    ereq.eargc     = 1;
    ereq.eargv     = zmalloc(sizeof(sds) * ereq.eargc);        // FREEME 115

    long long beg = mstime(), fin, tps;
    for (ulong i = 1; i < qrows; i++) {
        uint32 index  = rand() % prows + 1;
        sds    arg    = sdscatprintf(sdsempty(), "%u", index); // FREE ME 116
        ereq.eargv[0] = arg;
#ifdef DEBUG_PRINT
        eresp_t *eresp = e_alchemy_sql_fast(&ereq);
        printEmbedResp(eresp); printf("\n");
#else
        e_alchemy_sql_fast(&ereq);
#endif
        if (i != qrows - 1) sdsfree(arg);
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("KV: EXECUTE:\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           qrows, (fin - beg), tps);

    sdsfree(ereq.plan_name);

    ereq.op        = EXEC_BIN;
    ereq.plan_name = sdsnew("P_KV");
    robj  *o       = dictFetchValue(StmtD, ereq.plan_name);
    if (!o) assert(!"PREPARED STATEMENT NOT FOUND");
    ereq.exec_bin  = o->ptr;

    beg = mstime();
    for (ulong i = 1; i < qrows; i++) {
        uint32 index  = rand() % prows + 1;
        sds    arg    = sdscatprintf(sdsempty(), "%u", index); // FREE ME 116
        ereq.eargv[0] = arg;
#ifdef DEBUG_PRINT
        eresp_t *eresp = e_alchemy_sql_fast(&ereq);
        printEmbedResp(eresp); printf("\n");
#else
        e_alchemy_sql_fast(&ereq);
#endif
        if (i != qrows - 1) sdsfree(arg);
    }
    fin = mstime(); tps = (fin == beg) ? 0 : qrows / (fin - beg);
    printf("KV: EXEC_BIN:\t\t\t%lu rows, duration: %lld ms, %lldK TPS\n\n",
           qrows, (fin - beg), tps);

#ifdef DEBUG_PRINT
    GlobalNeedCn   = 0;
#endif
    hit_return_to_continue();
    release_ereq(&ereq);
}

// MAIN
int main(int argc, char **argv) {
    argc = 0; argv = NULL; /* compiler warning */
    ulong  prows = 1000000;
    ulong  qrows = 2000000;
#ifdef DEBUG_FEW_ROWS
    prows = 5; qrows = 5;
#endif

#ifdef TEST_KV
    test_kv_zip_select              (prows, qrows);
    test_kv_delete                  (prows, qrows);
    test_kv_update                  (prows, qrows);
#endif

#ifdef TEST_REDIS
    test_redis                      (prows, qrows);
#endif

#ifdef TEST_PREPARED_STATEMENT_KV
    test_prepared_statement_kv      (prows, qrows);
#endif

    embedded_exit();
    printf("Exiting...\n");
    return 0;
}

/*
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __REDIS_H__
#define __REDIS_H__

#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <limits.h>
#include <pthread.h>

#include "dict.h"
#include "ae.h"
#include "anet.h"
#include "sds.h"    /* Dynamic safe strings */
#include "adlist.h" /* Linked lists */

enum
{
  REG_GS = 0,
# define REG_GS		REG_GS
  REG_FS,
# define REG_FS		REG_FS
  REG_ES,
# define REG_ES		REG_ES
  REG_DS,
# define REG_DS		REG_DS
  REG_EDI,
# define REG_EDI	REG_EDI
  REG_ESI,
# define REG_ESI	REG_ESI
  REG_EBP,
# define REG_EBP	REG_EBP
  REG_ESP,
# define REG_ESP	REG_ESP
  REG_EBX,
# define REG_EBX	REG_EBX
  REG_EDX,
# define REG_EDX	REG_EDX
  REG_ECX,
# define REG_ECX	REG_ECX
  REG_EAX,
# define REG_EAX	REG_EAX
  REG_TRAPNO,
# define REG_TRAPNO	REG_TRAPNO
  REG_ERR,
# define REG_ERR	REG_ERR
  REG_EIP,
# define REG_EIP	REG_EIP
  REG_CS,
# define REG_CS		REG_CS
  REG_EFL,
# define REG_EFL	REG_EFL
  REG_UESP,
# define REG_UESP	REG_UESP
  REG_SS
# define REG_SS	REG_SS
};

#include <sys/uio.h>

int ll2string(char *s, size_t len, long long value);

void redisLog(int level, const char *fmt, ...);

/* Log levels */
#define REDIS_DEBUG 0
#define REDIS_VERBOSE 1
#define REDIS_NOTICE 2
#define REDIS_WARNING 3

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define REDIS_ENCODING_RAW 0    /* Raw representation */
#define REDIS_ENCODING_INT 1    /* Encoded as integer */
#define REDIS_ENCODING_ZIPMAP 2 /* Encoded as zipmap */
#define REDIS_ENCODING_HT 3     /* Encoded as an hash table */

/* The VM object structure */
struct redisObjectVM {
    off_t page;         /* the page at witch the object is stored on disk */
    off_t usedpages;    /* number of pages used on disk */
    time_t atime;       /* Last access time */
} vm;

/* The actual Redis Object */
typedef struct redisObject {
    void *ptr;
    unsigned char type;
    unsigned char encoding;
    int           refcount;
    unsigned char storage;  /* If this object is a key, where is the value?
                             * REDIS_VM_MEMORY, REDIS_VM_SWAPPED, ... */
    unsigned char vtype; /* If this object is a key, and value is swapped out,
                          * this is the type of the swapped out object. */
    /* VM fields, this are only allocated if VM is active, otherwise the
     * object allocation function will just allocate
     * sizeof(redisObjct) minus sizeof(redisObjectVM), so using
     * Redis without VM active will not have any overhead. */
    struct redisObjectVM vm;
}  __attribute__ ((packed)) robj;

robj *createStringObject(char *ptr, size_t len);
robj *createStringObjectFromLongLong(long long value);
void dictRedisObjectDestructor(void *privdata, void *val);

/* Object types */
#define REDIS_STRING     0
#define REDIS_LIST       1
#define REDIS_SET        2
#define REDIS_ZSET       3
#define REDIS_HASH       4
#ifdef ALSOSQL
  #define REDIS_BTREE           5
  #define REDIS_ROW             6
  #define REDIS_JOINROW         7
  #define REDIS_APPEND_SET      8
  #define REDIS_VAL_SET         9
  #define REDIS_FULL_SET       10
  #define REDIS_INT            11
#endif

typedef struct redisDb {
    dict *dict;                 /* The keyspace for this DB */
    dict *expires;              /* Timeout of keys with a timeout set */
    dict *blockingkeys;         /* Keys with clients waiting for data (BLPOP) */
    dict *io_keys;              /* Keys with clients waiting for VM I/O */
    int id;
} redisDb;

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
} multiState;

typedef struct redisClient {
    int fd;
    redisDb *db;
    int dictid;
    sds querybuf;
    robj **argv, **mbargv;
    int argc, mbargc;
    int bulklen;            /* bulk read len. -1 if not in bulk read mode */
    int multibulk;          /* multi bulk command format active */
    list *reply;
    int sentlen;
    time_t lastinteraction; /* time of the last interaction, used for timeout */
    int flags;              /* REDIS_SLAVE | REDIS_MONITOR | REDIS_MULTI ... */
    int slaveseldb;         /* slave selected db, if this client is a slave */
    int authenticated;      /* when requirepass is non-NULL */
    int replstate;          /* replication state if this is a slave */
    int repldbfd;           /* replication DB file descriptor */
    long repldboff;         /* replication DB file offset */
    off_t repldbsize;       /* replication DB file size */
    multiState mstate;      /* MULTI/EXEC state */
    robj **blockingkeys;    /* The key we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    int blockingkeysnum;    /* Number of blocking keys */
    time_t blockingto;      /* Blocking operation timeout. If UNIX current time
                             * is >= blockingto then the operation timed out. */
    list *io_keys;          /* Keys this client is waiting to be loaded from the
                             * swap file in order to continue. */
    dict *pubsub_channels;  /* channels a client is interested in (SUBSCRIBE) */
    list *pubsub_patterns;  /* patterns a client is interested in (SUBSCRIBE) */
} redisClient;

robj *createObject(int type, void *ptr);
robj *createHashObject(void);
robj *createSetObject(void);
robj *getDecodedObject(robj *o);
void freeSetObject(robj *o);

void incrRefCount(robj *o);
void decrRefCount(void *o);

int htNeedsResize(dict *dict);
void setGenericCommand(redisClient *c, int nx, robj *key, robj *val,
                              robj *expire, unsigned char internal);

struct redisCommand *lookupCommand(char *name);
robj *lookupKey(redisDb *db, robj *key);
robj *lookupKeyWrite(redisDb *db, robj *key);
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply);
robj *lookupKeyRead(redisDb *db, robj *key);

int deleteKey(redisDb *db, robj *key);

void addReply(redisClient *c, robj *obj);
void addReplySds(redisClient *c, sds s);
void addReplyBulk(redisClient *c, robj *obj);
void addReplyLongLong(redisClient *c, long long ll);

int stringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase);

/* Error codes */
#define REDIS_OK                0
#define REDIS_ERR               -1

typedef struct zskiplistNode {
    struct zskiplistNode **forward;
    struct zskiplistNode *backward;
    unsigned int *span;
    double score;
    robj *obj;
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

#define REDIS_DEFAULT_DBNUM     16

#define REDIS_SHARED_INTEGERS 10000
struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,

#ifdef ALSOSQL /* ALSOSQL START */
    *toomanytables,       *undefinedcolumntype, *missingcolumntype,
    *toomanycolumns,      *columnnametoobig,    *insertcannotoverwrite,
    *uint_pk_too_big,     *uint_no_negative_values,
    *col_uint_too_big,    *col_uint_no_negative_values,
    *toofewcolumns,       *toomanyindices,      *nonuniquecolumns,
    *nonuniquetablenames, *nonuniqueindexnames, *indextargetinvalid,
    *indexedalready,      *index_wrong_num_args,
    *nonexistenttable,    *insertcolumnmismatch,
    *columntoolarge,      *nonexistentcolumn,   *nonexistentindex,
    *invalidrange,        *toofewindicesinjoin, *toomanyindicesinjoin,
    *invalidupdatestring, *badindexedcolumnsyntax,

    *joinindexedcolumnlisterror, *joincolumnlisterror, *join_on_multi_col,
    *join_requires_range,

    *storagetypeunkown, *storagenumargsmismatch, *erronstoretotable,

    *createsyntax, *dropsyntax,
    *insertsyntax,         *insertsyntax_no_into, *insertsyntax_col_declaration,
    *insertsyntax_no_values,
    *selectsyntax,         *selectsyntax_nofrom,
    *selectsyntax_nowhere, *selectsyntax_notpk,    *selectsyntax_noequals,
    *deletesyntax,
    *deletesyntax_nowhere, *deletesyntax_notpk,    *deletesyntax_noequals,
    *updatesyntax,         *update_pk_range_query, *update_pk_overwrite,
    *updatesyntax_nowhere, *updatesyntax_notpk,    *updatesyntax_noequals,
    *scanselectsyntax,     *scanselectsyntax_noequals,

    *whereclause_no_and,
    *selectsyntax_store_norange,
    *joinsyntax_no_tablename,
    *drop_virtual_index,

    *createtable_as_on_wrong_type, *createtable_as_index,
    *create_table_as_function_not_found, *create_table_as_dump_num_args,
    *create_table_as_access_num_args,

    *denorm_wildcard_no_star,
#endif /* ALSOSQL END */
    *outofrangeerr, *plus,
    *select0, *select1, *select2, *select3, *select4,
    *select5, *select6, *select7, *select8, *select9,
    *messagebulk, *pmessagebulk, *subscribebulk, *unsubscribebulk, *mbulk3,
    *mbulk4, *psubscribebulk, *punsubscribebulk,
    *integers[REDIS_SHARED_INTEGERS];
} shared;

struct redisServer {
    int port;
    int fd;
    redisDb *db;
    long long dirty;            /* changes to DB from the last save */
    list *clients;
    list *slaves, *monitors;
    char neterr[ANET_ERR_LEN];
    aeEventLoop *el;
    int cronloops;              /* number of times the cron function run */
    list *objfreelist;          /* A list of freed objects to avoid malloc() */
    time_t lastsave;            /* Unix time of last save succeeede */
    /* Fields used only for stats */
    time_t stat_starttime;         /* server start time */
    long long stat_numcommands;    /* number of processed commands */
    long long stat_numconnections; /* number of connections received */
    long long stat_expiredkeys;   /* number of expired keys */
    /* Configuration */
    int verbosity;
    int glueoutputbuf;
    int maxidletime;
    int dbnum;
    int daemonize;
    int appendonly;
    int appendfsync;
    int shutdown_asap;
    time_t lastfsync;
    int appendfd;
    int appendseldb;
    char *pidfile;
    pid_t bgsavechildpid;
    pid_t bgrewritechildpid;
    sds bgrewritebuf; /* buffer taken by parent during oppend only rewrite */
    sds aofbuf;       /* AOF buffer, written before entering the event loop */
    struct saveparam *saveparams;
    int saveparamslen;
    char *logfile;
    char *bindaddr;
    char *dbfilename;
    char *appendfilename;
    char *requirepass;
    int rdbcompression;
    int activerehashing;
    /* Replication related */
    int isslave;
    char *masterauth;
    char *masterhost;
    int masterport;
    redisClient *master;    /* client that is master for this slave */
    int replstate;
    unsigned int maxclients;
    unsigned long long maxmemory;
    unsigned int blpop_blocked_clients;
    unsigned int vm_blocked_clients;
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    /* Virtual memory configuration */
    int vm_enabled;
    char *vm_swap_file;
    off_t vm_page_size;
    off_t vm_pages;
    unsigned long long vm_max_memory;
    /* Hashes config */
    size_t hash_max_zipmap_entries;
    size_t hash_max_zipmap_value;
    /* Virtual memory state */
    FILE *vm_fp;
    int vm_fd;
    off_t vm_next_page; /* Next probably empty page */
    off_t vm_near_pages; /* Number of pages allocated sequentially */
    unsigned char *vm_bitmap; /* Bitmap of free/used pages */
    time_t unixtime;    /* Unix time sampled every second. */
    /* Virtual memory I/O threads stuff */
    /* An I/O thread process an element taken from the io_jobs queue and
     * put the result of the operation in the io_done list. While the
     * job is being processed, it's put on io_processing queue. */
    list *io_newjobs; /* List of VM I/O jobs yet to be processed */
    list *io_processing; /* List of VM I/O jobs being processed */
    list *io_processed; /* List of VM I/O jobs already processed */
    list *io_ready_clients; /* Clients ready to be unblocked. All keys loaded */
    pthread_mutex_t io_mutex; /* lock to access io_jobs/io_done/io_thread_job */
    pthread_mutex_t obj_freelist_mutex; /* safe redis objects creation/free */
    pthread_mutex_t io_swapfile_mutex; /* So we can lseek + write */
    pthread_attr_t io_threads_attr; /* attributes for threads creation */
    int io_active_threads; /* Number of running I/O threads */
    int vm_max_threads; /* Max number of I/O threads running at the same time */
    /* Our main thread is blocked on the event loop, locking for sockets ready
     * to be read or written, so when a threaded I/O operation is ready to be
     * processed by the main thread, the I/O thread will use a unix pipe to
     * awake the main thread. The followings are the two pipe FDs. */
    int io_ready_pipe_read;
    int io_ready_pipe_write;
    /* Virtual memory stats */
    unsigned long long vm_stats_used_pages;
    unsigned long long vm_stats_swapped_objects;
    unsigned long long vm_stats_swapouts;
    unsigned long long vm_stats_swapins;
    /* Pubsub */
    dict *pubsub_channels; /* Map channels to list of subscribed clients */
    list *pubsub_patterns; /* A list of pubsub_patterns */
    /* Misc */
    FILE *devnull;
    unsigned char big_endian;
    unsigned char psize;
#ifdef ALSOSQL
    int dbid;
#endif   
};

/* Structure to hold hash iteration abstration. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    int encoding;
    unsigned char *zi;
    unsigned char *zk, *zv;
    unsigned int zklen, zvlen;

    dictIterator *di;
    dictEntry *de;
} hashIterator;

#define REDIS_HASH_KEY 1
#define REDIS_HASH_VALUE 2

int hashSet(robj *o, robj *key, robj *value);
int hashDelete(robj *o, robj *key);
robj *hashGet(robj *o, robj *key);
hashIterator *hashInitIterator(robj *subject);
int hashNext(hashIterator *hi);
robj *hashCurrent(hashIterator *hi, int what); 
void hashReleaseIterator(hashIterator *hi);

int dictEncObjKeyCompare(void *privdata, const void *key1, const void *key2);
unsigned int dictEncObjHash(const void *key);

struct redisClient *createFakeClient(void);
void freeFakeClient(struct redisClient *c);

int rdbSaveLen(FILE *fp, unsigned int len);
unsigned int rdbLoadLen(FILE *fp, int *isencoded);
int rdbSaveStringObject(FILE *fp, robj *obj);
robj *rdbLoadStringObject(FILE *fp);

#define increment_used_memory(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    if (zmalloc_thread_safe) { \
        pthread_mutex_lock(&used_memory_mutex);  \
        used_memory += _n; \
        pthread_mutex_unlock(&used_memory_mutex); \
    } else { \
        used_memory += _n; \
    } \
} while(0)

#define decrement_used_memory(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    if (zmalloc_thread_safe) { \
        pthread_mutex_lock(&used_memory_mutex);  \
        used_memory -= _n; \
        pthread_mutex_unlock(&used_memory_mutex); \
    } else { \
        used_memory -= _n; \
    } \
} while(0)

#define REDIS_RDB_LENERR UINT_MAX

typedef void redisCommandProc(redisClient *c);
typedef void redisVmPreloadProc(redisClient *c, struct redisCommand *cmd, int argc, robj **argv);

struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    int flags;
    /* Use a function to determine which keys need to be loaded
     * in the background prior to executing this command. Takes precedence
     * over vm_firstkey and others, ignored when NULL */
    redisVmPreloadProc *vm_preload_proc;
    /* What keys should be loaded in background when calling this command? */
    int vm_firstkey; /* The first argument that's a key (0 = no keys) */
    int vm_lastkey;  /* THe last argument that's a key */
    int vm_keystep;  /* The step between first and last key */
    int alsosql; //TODO throwout
};

void hsetCommand(redisClient *c);
void selectCommand(redisClient *c);

/* ALSOSQL */
typedef struct storage_command {
    void (*func)(redisClient *c);
    char *name;
    int   argc;
} stor_cmd;

#ifdef ALSOSQL
struct redisClient *rsql_createFakeClient(void);
#endif

#endif

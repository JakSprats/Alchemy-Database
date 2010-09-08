/* Redis benchmark utility.
 *
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

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>

#include "ae.h"
#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"

#include <stdarg.h>

void redisLog(int level, const char *fmt, ...) {
    va_list ap;
    level = 0; /* compliler warning */

    va_start(ap, fmt);
    vprintf(fmt, ap);
    printf("\n");
    va_end(ap);

}
#define RL4 redisLog(4,

#define REPLY_INT 0
#define REPLY_RETCODE 1
#define REPLY_BULK 2
#define REPLY_MBULK 3

#define CLIENT_CONNECTING 0
#define CLIENT_SENDQUERY 1
#define CLIENT_READREPLY 2

#define MAX_LATENCY 5000

#define REDIS_NOTUSED(V) ((void) V)

#define bool unsigned char

static struct config {
    int debug;
    int numclients;
    int requests;
    int liveclients;
    int donerequests;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    aeEventLoop *el;
    char *hostip;
    int hostport;
    int keepalive;
    long long start;
    long long totlatency;
    int *latency;
    list *clients;
    int quiet;
    int loop;
    int idlemode;

    bool perform_range_query;
    bool perform_join_test;
    bool perform_3way_join_test;
    bool perform_10way_join_test;
    bool perform_fk_test;
    bool perform_fk_join_test;
    bool perform_address_test;
    bool perform_test_table_test;
    bool perform_bigrow_table_test;
    bool perform_bigrow_sel_test;

    bool populate_test_table;
    bool populate_join_table;
    bool populate_3way_join_table;
    bool populate_10way_join_table;
    bool populate_fk_table;
    bool populate_fk2_table;

    long range_query_len;
    long second_random_modulo;

    bool random_force;
    long not_rand;
    long second_not_rand;

    long dump_every_x_reqs;
} config;

typedef struct _client {
    int state;
    int fd;
    sds obuf;
    sds ibuf;
    int mbulk;          /* Number of elements in an mbulk reply */
    int readlen;        /* readlen == -1 means read a single line */
    int totreceived;
    unsigned int written;        /* bytes of 'obuf' already written */
    int replytype;
    long long start;    /* start time in milliseconds */
} *client;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);

/* Implementation */
static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void freeClient(client c) {
    listNode *ln;

    aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->fd,AE_READABLE);
    sdsfree(c->ibuf);
    sdsfree(c->obuf);
    close(c->fd);
    zfree(c);
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->fd,AE_READABLE);
    aeCreateFileEvent(config.el,c->fd, AE_WRITABLE,writeHandler,c);
    sdsfree(c->ibuf);
    c->ibuf = sdsempty();
    c->readlen = (c->replytype == REPLY_BULK ||
                  c->replytype == REPLY_MBULK) ? -1 : 0;
    c->mbulk = -1;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_SENDQUERY;
    c->start = mstime();
    createMissingClients(c);
}

long start_first_id = 100000001;

bool randomize_range = 0;

long default_second_random_modulo = 50;
long default_range_query_len      = 5;

long start_range_query      = 100000001;
long orig_start_range_query = 100000001;

long not_rand_reset_val        = 1;
long second_not_rand_reset_val = 1;
static void reset_non_rand() {
    config.not_rand        = not_rand_reset_val;
    config.second_not_rand = second_not_rand_reset_val;
}

static void randomizeClientKey(client c) {
    char *p = c->obuf;
    char buf[64];
    long r;

    if (!randomize_range) {
        char   *x = strstr(p, " 000");
        if (!x) x = strstr(p, "(000");
        if (x) {
            bool nested_rand = 0;
            char *y = strstr(x, ",0001");
            if (y) nested_rand = 1;
            if (config.random_force) {
                r = start_first_id + random() % config.randomkeys_keyspacelen;
            } else {
                r = start_first_id + config.not_rand++;
            }
            sprintf(buf, "%ld", r);
            int diff = 9 - strlen(buf);
            memcpy(x + 4 + diff, buf, strlen(buf));
            if (nested_rand) {
                if ((config.not_rand % config.second_random_modulo) == 0)
                    config.second_not_rand++;
                sprintf(buf, "%ld", config.second_not_rand);
                diff = 9 - strlen(buf);
                memcpy(x + 17 + diff, buf, strlen(buf));
            }
            //if ((r % 1000000) == 0) RL4 "%ld: buf: %s", r, c->obuf);
        } else {
            while ((p = strstr(p, "_rand"))) {
                if (!p) return;
                p += 5;
                r = random() % config.randomkeys_keyspacelen;
                sprintf(buf, "%ld", r);
                memcpy(p, buf, strlen(buf));
            }
        }
    } else {
        char *z = strstr(p, "BETWEEN 00010");
        if (z) {
            z += 8;
            sprintf(buf, "000%ld AND 000%ld", start_range_query,
                               start_range_query + config.range_query_len);
            memcpy(z, buf, strlen(buf));
            start_range_query += config.range_query_len;
            if (start_range_query >= 
                orig_start_range_query + config.randomkeys_keyspacelen)
                     start_range_query = orig_start_range_query;
        }
    }
    //RL4 "buf: %s", c->obuf);
}

static void prepareClientForReply(client c, int type) {
    if (type == REPLY_BULK) {
        c->replytype = REPLY_BULK;
        c->readlen = -1;
    } else if (type == REPLY_MBULK) {
        c->replytype = REPLY_MBULK;
        c->readlen = -1;
        c->mbulk = -1;
    } else {
        c->replytype = type;
        c->readlen = 0;
    }
}

// reqs per interval
long long last_call_time = 0;
int       last_num_reqs                   = 0;
static void dump_req_per_sec() {
    if (last_call_time == 0) last_call_time = config.start;

    long long now        = mstime();
    long long time_delta = now                 - last_call_time;
    int       reqs_delta = config.donerequests - last_num_reqs;
    float     reqpersec  = 1000*(float)reqs_delta/((float)time_delta);
    printf("%.2f req-per-sec tot_reqs: %d actual_time: %lld\n",
           reqpersec, config.donerequests, now);
    last_call_time = now;
    last_num_reqs = config.donerequests;
}

static void clientDone(client c) {
    static int last_tot_received = 1;

    long long latency;
    config.donerequests ++;
    latency = mstime() - c->start;
    if (latency > MAX_LATENCY) latency = MAX_LATENCY;
    config.latency[latency]++;

    if (config.debug && last_tot_received != c->totreceived) {
        printf("Tot bytes received: %d\n", c->totreceived);
        last_tot_received = c->totreceived;
    }
    if (config.donerequests == config.requests) {
        freeClient(c);
        aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
        if (config.randomkeys) randomizeClientKey(c);
    } else {
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        freeClient(c);
    }
    if (config.dump_every_x_reqs &&
        (config.donerequests % config.dump_every_x_reqs == 0))
          dump_req_per_sec();
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    char buf[1024];
    int nread;
    client c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    nread = read(c->fd, buf, 1024);
    if (nread == -1) {
        fprintf(stderr, "Reading from socket: %s\n", strerror(errno));
        freeClient(c);
        return;
    }
    if (nread == 0) {
        fprintf(stderr, "EOF from client\n");
        freeClient(c);
        return;
    }
    c->totreceived += nread;
    c->ibuf = sdscatlen(c->ibuf,buf,nread);

processdata:
    /* Are we waiting for the first line of the command of for  sdf 
     * count in bulk or multi bulk operations? */
    if (c->replytype == REPLY_INT ||
        c->replytype == REPLY_RETCODE ||
        (c->replytype == REPLY_BULK && c->readlen == -1) ||
        (c->replytype == REPLY_MBULK && c->readlen == -1) ||
        (c->replytype == REPLY_MBULK && c->mbulk == -1)) {
        char *p;

        /* Check if the first line is complete. This is only true if
         * there is a newline inside the buffer. */
        if ((p = strchr(c->ibuf,'\n')) != NULL) {
            if (c->replytype == REPLY_BULK ||
                (c->replytype == REPLY_MBULK && c->mbulk != -1))
            {
                /* Read the count of a bulk reply (being it a single bulk or
                 * a multi bulk reply). "$<count>" for the protocol spec. */
                *p = '\0';
                *(p-1) = '\0';
                c->readlen = atoi(c->ibuf+1)+2;
                // printf("BULK ATOI: %s\n", c->ibuf+1);
                /* Handle null bulk reply "$-1" */
                if (c->readlen-2 == -1) {
                    clientDone(c);
                    return;
                }
                /* Leave all the rest in the input buffer */
                c->ibuf = sdsrange(c->ibuf,(p-c->ibuf)+1,-1);
                /* fall through to reach the point where the code will try
                 * to check if the bulk reply is complete. */
            } else if (c->replytype == REPLY_MBULK && c->mbulk == -1) {
                /* Read the count of a multi bulk reply. That is, how many
                 * bulk replies we have to read next. "*<count>" protocol. */
                *p = '\0';
                *(p-1) = '\0';
                c->mbulk = atoi(c->ibuf+1);
                /* Handle null bulk reply "*-1" */
                if (c->mbulk == -1) {
                    clientDone(c);
                    return;
                }
                // printf("%p) %d elements list\n", c, c->mbulk);
                /* Leave all the rest in the input buffer */
                c->ibuf = sdsrange(c->ibuf,(p-c->ibuf)+1,-1);

                /* JAKSPRATS: I thnk this is fixed in redis-2.0.0 */
                if (!c->mbulk) {
                    clientDone(c);
                    return;
                }

                goto processdata;
            } else {
                c->ibuf = sdstrim(c->ibuf,"\r\n");
                clientDone(c);
                return;
            }
        }
    }
    /* bulk read, did we read everything? */
    if (((c->replytype == REPLY_MBULK && c->mbulk != -1) || 
         (c->replytype == REPLY_BULK)) && c->readlen != -1 &&
          (unsigned)c->readlen <= sdslen(c->ibuf))
    {
        // printf("BULKSTATUS mbulk:%d readlen:%d sdslen:%d\n",
        //    c->mbulk,c->readlen,sdslen(c->ibuf));
        if (c->replytype == REPLY_BULK) {
            clientDone(c);
        } else if (c->replytype == REPLY_MBULK) {
            // printf("%p) %d (%d)) ",c, c->mbulk, c->readlen);
            // fwrite(c->ibuf,c->readlen,1,stdout);
            // printf("\n");
            if (--c->mbulk == 0) {
                clientDone(c);
            } else {
                c->ibuf = sdsrange(c->ibuf,c->readlen,-1);
                c->readlen = -1;
                goto processdata;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    client c = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(fd);
    REDIS_NOTUSED(mask);

    if (c->state == CLIENT_CONNECTING) {
        c->state = CLIENT_SENDQUERY;
        c->start = mstime();
    }
    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        int len = sdslen(c->obuf) - c->written;
        int nwritten = write(c->fd, ptr, len);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(config.el,c->fd,AE_WRITABLE);
            aeCreateFileEvent(config.el,c->fd,AE_READABLE,readHandler,c);
            c->state = CLIENT_READREPLY;
        }
    }
}

static client createClient(void) {
    client c = zmalloc(sizeof(struct _client));
    char err[ANET_ERR_LEN];

    c->fd = anetTcpNonBlockConnect(err,config.hostip,config.hostport);
    if (c->fd == ANET_ERR) {
        zfree(c);
        fprintf(stderr,"Connect: %s\n",err);
        return NULL;
    }
    anetTcpNoDelay(NULL,c->fd);
    c->obuf = sdsempty();
    c->ibuf = sdsempty();
    c->mbulk = -1;
    c->readlen = 0;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_CONNECTING;
    aeCreateFileEvent(config.el, c->fd, AE_WRITABLE, writeHandler, c);
    config.liveclients++;
    listAddNodeTail(config.clients,c);
    return c;
}

static void createMissingClients(client c) {
    while(config.liveclients < config.numclients) {
        client new = createClient();
        if (!new) continue;
        sdsfree(new->obuf);
        new->obuf = sdsdup(c->obuf);
        if (config.randomkeys) randomizeClientKey(c);
        prepareClientForReply(new,c->replytype);
    }
}

static void showLatencyReport(char *title) {
    int j, seen = 0;
    float perc, reqpersec;

    reqpersec = (float)config.donerequests/((float)config.totlatency/1000);
    if (!config.quiet) {
        printf("====== %s ======\n", title);
        printf("  %d requests completed in %.2f seconds\n", config.donerequests,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");
        for (j = 0; j <= MAX_LATENCY; j++) {
            if (config.latency[j]) {
                seen += config.latency[j];
                perc = ((float)seen*100)/config.donerequests;
                printf("%.2f%% <= %d milliseconds\n", perc, j);
            }
        }
        printf("%.2f requests per second\n\n", reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", title, reqpersec);
    }
}

static void prepareForBenchmark(void)
{
    memset(config.latency,0,sizeof(int)*(MAX_LATENCY+1));
    config.start = mstime();
    config.donerequests = 0;
}

static void endBenchmark(char *title) {
    config.totlatency = mstime()-config.start;
    showLatencyReport(title);
    freeAllClients();
}

void parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;
        
        if (!strcmp(argv[i],"-c") && !lastarg) {
            config.numclients = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-n") && !lastarg) {
            config.requests = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-k") && !lastarg) {
            config.keepalive = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-h") && !lastarg) {
            char *ip = zmalloc(32);
            if (anetResolve(NULL,argv[i+1],ip) == ANET_ERR) {
                printf("Can't resolve %s\n", argv[i]);
                exit(1);
            }
            config.hostip = ip;
            i++;
        } else if (!strcmp(argv[i],"-p") && !lastarg) {
            config.hostport = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-d") && !lastarg) {
            config.datasize = atoi(argv[i+1]);
            i++;
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024) config.datasize = 1024*1024;
        } else if (!strcmp(argv[i],"-r") && !lastarg) {
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[i+1]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
            i++;

        /* START: ALSOSQL */
        } else if (!strcmp(argv[i],"-R")) {
            config.perform_range_query       = 1;
        } else if (!strcmp(argv[i],"-A")) {
            config.perform_address_test      = 1;
        } else if (!strcmp(argv[i],"-T")) {
            config.perform_test_table_test   = 1;
        } else if (!strcmp(argv[i],"-J")) {
            config.perform_join_test         = 1;
        } else if (!strcmp(argv[i],"-J3")) {
            config.perform_3way_join_test    = 1;
        } else if (!strcmp(argv[i],"-J10")) {
            config.perform_10way_join_test   = 1;
        } else if (!strcmp(argv[i],"-F")) {
            config.perform_fk_test           = 1;
        } else if (!strcmp(argv[i],"-FJ")) {
            config.perform_fk_join_test      = 1;
        } else if (!strcmp(argv[i],"-B")) {
            config.perform_bigrow_table_test = 1;
        } else if (!strcmp(argv[i],"-BS")) {
            config.perform_bigrow_sel_test   = 1;

        } else if (!strcmp(argv[i],"-PT")) {
            config.populate_test_table       = 1;
        } else if (!strcmp(argv[i],"-PJ")) {
            config.populate_join_table       = 1;
        } else if (!strcmp(argv[i],"-PJ3")) {
            config.populate_3way_join_table  = 1;
        } else if (!strcmp(argv[i],"-PJ10")) {
            config.populate_10way_join_table = 1;
        } else if (!strcmp(argv[i],"-PF")) {
            config.populate_fk_table         = 1;
        } else if (!strcmp(argv[i],"-PF2")) {
            config.populate_fk2_table        = 1;
        } else if (!strcmp(argv[i],"-RF")) {
            config.random_force              = 1;

        } else if (!strcmp(argv[i],"-Q") && !lastarg) {
            config.range_query_len = default_range_query_len;
            config.range_query_len = atoi(argv[i+1]);
            if (config.range_query_len < 0)
                config.range_query_len = default_range_query_len;
            i++;
        } else if (!strcmp(argv[i],"-M") && !lastarg) {
            config.second_random_modulo = default_second_random_modulo;
            config.second_random_modulo = atoi(argv[i+1]);
            if (config.second_random_modulo < 0)
                config.second_random_modulo = default_second_random_modulo;
            i++;
        } else if (!strcmp(argv[i],"-NR") && !lastarg) {
            int val = atoi(argv[i+1]);
            if (val > 0) not_rand_reset_val = val; /* will set not_rand */
            i++;
        } else if (!strcmp(argv[i],"-SNR") && !lastarg) {
            int val = atoi(argv[i+1]);
            if (val > 0) second_not_rand_reset_val = val;
            i++;
        } else if (!strcmp(argv[i],"-X")) {
            long val = atol(argv[i+1]);
            if (val > 0) config.dump_every_x_reqs = val;
            i++;
        /* END: ALSOSQL */

        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-D")) {
            config.debug = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else {
            printf("Wrong option '%s' or option argument missing\n\n",argv[i]);
            printf("Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n");
            printf(" -h <hostname>        Server hostname (default 127.0.0.1)\n");
            printf(" -p <hostname>        Server port (default 6379)\n");
            printf(" -c <clients>         Number of parallel connections (default 50)\n");
            printf(" -n <requests>        Total number of requests (default 10000)\n");
            printf(" -d <size>            Data size of SET/GET value in bytes (default 2)\n");
            printf(" -k <boolean>         1=keep alive 0=reconnect (default 1)\n");
            printf(" -r <keyspacelen>     Use random keys for SET/GET/INCR, random values for SADD\n");
            printf("    Using this option the benchmark will get/set keys\n");
            printf("    in the form mykey_rand000000012456 instead of constant\n");
            printf("    keys, the <keyspacelen> argument determines the max\n");
            printf("    number of values for the random number. For instance\n");
            printf("    if set to 10 only rand000000000000 - rand000000000009\n");
            printf("    range will be allowed.\n");
            printf(" -q                   Quiet. Just show query/sec values\n");
            printf(" -l                   Loop. Run the tests forever\n");
            printf(" -I                   Idle mode. Just open N idle connections and wait.\n");
            printf(" -D                   Debug mode. more verbose.\n");
            printf(" ---------- ALSOSQL OPTIONS --------------.\n");
            printf(" -A                   Perform Address Memory Test (INSERT)\n");
            printf(" -T                   test table: INSERT/SELECT/UPDATE/DELETE\n");
            printf(" -R                   SELECT * BETWEEN X AND Y\n");
            printf(" -J                   Join tables (test & join) w/ range-query\n");
            printf(" -J3                  Join tables (test & join & third_join) w/ range-query\n");
            printf(" -J10                 Join tables (test & join & third_join) w/ range-query\n");
            printf(" -PT                  populate(INSERT) into test table\n");
            printf(" -PJ                  populate(INSERT) into join table\n");
            printf(" -PJ3                 populate(INSERT) into third_join table\n");
            printf(" -PF                  populate(INSERT) into fk table\n");
            printf(" -B                   populate(INSERT) into bigrow table\n");
            printf(" -BS                  SELECT FROM into bigrow table\n");
            printf(" -Q <range-query-len> SELECT * BETWEEN X AND (X + range-query-len)\n");
            printf(" -M <modulo-for-fk>   INSERT of FKs modulo\n");
            exit(1);
        }
    }

    if (config.perform_range_query    || config.perform_join_test        ||
        config.perform_3way_join_test || config.perform_fk_test          ||
        config.perform_fk_join_test   || config.perform_10way_join_test)
            randomize_range = 1; 
}

void test_denormalised_address() {
    reset_non_rand();
    int datasize = config.datasize;

    prepareForBenchmark();
    config.datasize = 18;
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,"SET 000100000001:street %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'s',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
    }
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SET STREET");

    prepareForBenchmark();
    config.datasize = 10;
    c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,"SET 000100000001:city %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'c',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
    }
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SET CITY");

    prepareForBenchmark();
    config.datasize = 2;
    c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,"SET 000100000001:state %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'2',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
    }
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SET STATE");

    prepareForBenchmark();
    config.datasize = 5;
    c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,"SET 000100000001:zipcode %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'1',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
    }
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);

    endBenchmark("SET ZIPCODE");
    config.datasize = datasize;
}

void test_insert_address_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
//create table address \(id int, street TEXT, city TEXT, state int, zip int\)
    reset_non_rand();
        prepareForBenchmark();
        client c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"INSERT INTO address VALUES (000100000001,ABCDEFGHIJKLMNO,abcdefghi,22,11111)\r\n");
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("INSERT ADDRESS");
}

void test_select_address_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
//create table address \(id int, street TEXT, city TEXT, state int, zip int\)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT * FROM address WHERE id = 000100000001\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SELECT ADDRESS");
}


void test_insert_test_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// create table test (id int, field TEXT, name TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO test VALUES (000100000001,abcd,efgh)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT TEST");
}

void test_select_test_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT * FROM test WHERE id = 000100000001\n");
    prepareClientForReply(c,REPLY_BULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SELECT TEST");
}

void test_update_test_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"UPDATE test SET field=F,name=N WHERE id = 000100000001\n");
    prepareClientForReply(c,REPLY_INT);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("UPDATE TEST");
}

void test_delete_test_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"DELETE FROM test WHERE id = 000100000001\n");
    prepareClientForReply(c,REPLY_INT);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("DELETE TEST");
}

void denorm_set_for_compare_w_test_table() {
    reset_non_rand();
    int datasize = config.datasize;
    config.datasize = 8;
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,"SET 000100000001 %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'x',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
    }
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SET DENORM");
    config.datasize = datasize;

    reset_non_rand();
    prepareForBenchmark();
    c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"GET 000100000001\r\n");
    prepareClientForReply(c,REPLY_BULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("GET DENORM");

    reset_non_rand();
    prepareForBenchmark();
    c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"DEL 000100000001\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("DEL DENORM");
}

void test_Iselect_test_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT * FROM test WHERE id BETWEEN 000100000001 AND 000100000002\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("ISELECT TEST");
}

void test_insert_join_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// create table join (id int, field TEXT, name TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO join VALUES (000100000001,stuv,wxyz)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT JOIN");
}

void test_join_test_with_join_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT test.id,test.field,join.name FROM test,join WHERE test.id=join.id AND test.id BETWEEN 000100000001 AND 000100000002\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("ISELECT JOIN");
}

void test_insert_third_join_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// create table third_join (id int, field TEXT, name TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO third_join VALUES (000100000001,lmno,pqrs)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT THIRD_JOIN");
}

void test_3way_join() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT test.id,test.field,join.name,third_join.field FROM test,join,third_join WHERE test.id=join.id AND test.id=third_join.id AND test.id BETWEEN 000100000001 AND 000100000002\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("3-WAY JOIN");
}

void test_insert_10way_join_table(int i) {
// THE FOLLOWING COMMAND MUST BE RUN using 10 times varying _0 from (_0 to _9)
// ./redis-cli CREATE TABLE join_0 (id INT, field TEXT, name TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscatprintf(c->obuf,
                "INSERT INTO join_%d VALUES (000100000001,%d,%d)\r\n", i, i, i);
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT 10WAY_JOIN");
}

void test_10way_join() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT join_0.id, join_0.field, join_1.field, join_2.field, join_3.field, join_4.field, join_5.field, join_6.field, join_7.field, join_8.field, join_9.field FROM join_0, join_1, join_2, join_3, join_4, join_5, join_6, join_7, join_8, join_9 WHERE join_0.id BETWEEN 000100000001 AND 000100000002 AND join_0.id = join_1.id AND join_0.id = join_2.id AND join_0.id = join_3.id AND join_0.id = join_4.id AND join_0.id = join_5.id AND join_0.id = join_6.id AND join_0.id = join_7.id AND join_0.id = join_8.id AND join_0.id = join_9.id\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("10-WAY JOIN");
}

void test_insert_FK_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// CREATE TABLE FK (id int, fk int, value TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO FK VALUES (000100000001,000100000001,ZZZZZZZ)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT FK");
}

void test_Iselect_FK_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT * FROM FK WHERE fk BETWEEN 000100000001 AND 000100000002\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("ISELECT FK");
}

void test_insert_FK2_table() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// CREATE TABLE FK2 (id int, fk int, value TEXT)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO FK2 VALUES (000100000001,000100000001,YYYYYYY)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT FK2");
}

void test_join_FK_table() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT FK.value, FK2.value FROM FK, FK2 WHERE FK.fk = FK2.fk AND FK.fk BETWEEN 000100000001 AND 000100000002\n");
    prepareClientForReply(c,REPLY_MBULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("ISELECT FK");
}

void perform_bigrow_table_test() {
// THE FOLLOWING COMMAND MUST BE RUN using ./redis-cli
// CREATE TABLE bigrow \(id int primary key, field TEXT\)
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"INSERT INTO bigrow VALUES (000100000001,abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrs)\r\n");
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("INSERT TEST");
}

void perform_bigrow_sel_test() {
    reset_non_rand();
    prepareForBenchmark();
    client c = createClient();
    if (!c) exit(1);
    c->obuf = sdscat(c->obuf,"SELECT * FROM bigrow WHERE id = 000100000001\n");
    prepareClientForReply(c,REPLY_BULK);
    createMissingClients(c);
    aeMain(config.el);
    endBenchmark("SELECT TEST");
}


int main(int argc, char **argv) {
    client c;

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.debug = 0;
    config.numclients = 50;
    config.requests = 10000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop();
    config.keepalive = 1;
    config.donerequests = 0;
    config.datasize = 3;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.clients = listCreate();
    config.latency = zmalloc(sizeof(int)*(MAX_LATENCY+1));

    config.hostip = "127.0.0.1";
    config.hostport = 6379;

    /* ALSOSQL */
    config.range_query_len          = default_range_query_len;
    config.second_random_modulo     = default_second_random_modulo;

    config.perform_address_test      = 0;
    config.perform_test_table_test   = 0;
    config.perform_range_query       = 0;
    config.perform_join_test         = 0;
    config.perform_3way_join_test    = 0;
    config.perform_10way_join_test   = 0;
    config.perform_fk_test           = 0;
    config.perform_fk_join_test      = 0;

    config.populate_test_table       = 0;
    config.populate_join_table       = 0;
    config.populate_3way_join_table  = 0;
    config.populate_10way_join_table = 0;
    config.populate_fk_table         = 0;
    config.populate_fk2_table        = 0;
    config.perform_bigrow_table_test = 0;
    config.perform_bigrow_sel_test   = 0;

    config.random_force              = 0;
    config.dump_every_x_reqs         = 0;
    reset_non_rand();

    parseOptions(argc,argv);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdsempty();
        prepareClientForReply(c,REPLY_RETCODE); /* will never receive it */
        createMissingClients(c);
        aeMain(config.el);
        /* and will wait for every */
    }

    do {
        if (config.perform_address_test) {
            printf("test_insert_address_table\n");
            test_insert_address_table(c);
            printf("test_select_address_table\n");
            test_select_address_table(c);

            printf("sleep 10\n");
            sleep(10);

            printf("test_denormalised_address\n");
            test_denormalised_address(c);
            return 0;
        }

        if (config.populate_test_table) {
            printf("test_insert_test_table\n");
            test_insert_test_table();
            return 0;
        }

        if (config.populate_join_table) {
            printf("test_insert_join_table\n");
            test_insert_join_table();
            return 0;
        }

        if (config.populate_3way_join_table) {
            printf("test_insert_third_join_table\n");
            test_insert_third_join_table();
            return 0;
        }

        if (config.populate_10way_join_table) {
            printf("test_insert_third_join_table\n");
            for (int i = 0; i < 10; i++) {
                test_insert_10way_join_table(i);
            }
            return 0;
        }

        if (config.perform_range_query) {
            printf("Range Query Test: length: %ld\n", config.range_query_len);
            printf("test_Iselect_test_table\n");
            test_Iselect_test_table();
            return 0;
        }

        if (config.perform_join_test) {
            printf("JoinTest: length: %ld\n", config.range_query_len);
            printf("test_join_test_with_join_table\n");
            test_join_test_with_join_table();
            return 0;
        }

        if (config.perform_3way_join_test) {
            printf("JoinTest: length: %ld\n", config.range_query_len);
            printf("test_3way_join\n");
            test_3way_join();
            return 0;
        }

        if (config.perform_10way_join_test) {
            printf("JoinTest: length: %ld\n", config.range_query_len);
            printf("test_10way_join\n");
            test_10way_join();
            return 0;
        }

        if (config.populate_fk_table) {
            printf("test_insert_FK_table: %ld\n", config.second_random_modulo);
            test_insert_FK_table();
            printf("max FK: %ld\n", config.second_not_rand);
            return 0;
        }

        if (config.perform_fk_test) {
            printf("FK-Test: length: %ld\n", config.range_query_len);
            printf("test_Iselect_FK_table\n");
            test_Iselect_FK_table();
            return 0;
        }

        if (config.perform_fk_join_test) {
            printf("FK-JOIN-Test: length: %ld\n", config.range_query_len);
            printf("test_join_FK_table\n");
            test_join_FK_table();
            return 0;
        }

        if (config.populate_fk2_table) {
            printf("test_insert_FK2_table: %ld\n", config.second_random_modulo);
            test_insert_FK2_table();
            printf("max FK: %ld\n", config.second_not_rand);
            return 0;
        }

        if (config.perform_test_table_test) {
            printf("test_insert_test_table\n");
            test_insert_test_table();

            printf("test_select_test_table\n");
            test_select_test_table();

            printf("test_update_test_table\n");
            test_update_test_table();

            printf("test_delete_test_table\n");
            test_delete_test_table();

            printf("denorm_set_for_compare_w_test_table\n");
            denorm_set_for_compare_w_test_table();

            return 0;
        }

        if (config.perform_bigrow_table_test) {
            printf("perform_bigrow_table_test\n");
            perform_bigrow_table_test();
            return 0;
        }

        if (config.perform_bigrow_sel_test) {
            printf("perform_bigrow_sel_test\n");
            perform_bigrow_sel_test();
            return 0;
        }

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"PING\r\n");
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("PING");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"*1\r\n$4\r\nPING\r\n");
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("PING (multi bulk)");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscatprintf(c->obuf,"SET foo_rand000000000000 %d\r\n",config.datasize);
        {
            char *data = zmalloc(config.datasize+2);
            memset(data,'x',config.datasize);
            data[config.datasize] = '\r';
            data[config.datasize+1] = '\n';
            c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
        }
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("SET");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"GET foo_rand000000000000\r\n");
        prepareClientForReply(c,REPLY_BULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("GET");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"INCR counter_rand000000000000\r\n");
        prepareClientForReply(c,REPLY_INT);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("INCR");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LPUSH mylist 3\r\nbar\r\n");
        prepareClientForReply(c,REPLY_INT);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LPUSH");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LPOP mylist\r\n");
        prepareClientForReply(c,REPLY_BULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LPOP");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"SADD myset 24\r\ncounter_rand000000000000\r\n");
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("SADD");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"SPOP myset\r\n");
        prepareClientForReply(c,REPLY_BULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("SPOP");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LPUSH mylist 3\r\nbar\r\n");
        prepareClientForReply(c,REPLY_RETCODE);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LPUSH (again, in order to bench LRANGE)");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LRANGE mylist 0 99\r\n");
        prepareClientForReply(c,REPLY_MBULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LRANGE (first 100 elements)");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LRANGE mylist 0 299\r\n");
        prepareClientForReply(c,REPLY_MBULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LRANGE (first 300 elements)");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LRANGE mylist 0 449\r\n");
        prepareClientForReply(c,REPLY_MBULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LRANGE (first 450 elements)");

        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf,"LRANGE mylist 0 599\r\n");
        prepareClientForReply(c,REPLY_MBULK);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark("LRANGE (first 600 elements)");

        printf("\n");
    } while(config.loop);

    return 0;
}

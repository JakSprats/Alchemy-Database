/* ALCHEMY_DATABASE's generic benchmark utility.
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

#define REPLY_INT 0
#define REPLY_RETCODE 1
#define REPLY_BULK 2
#define REPLY_MBULK 3

#define CLIENT_CONNECTING 0
#define CLIENT_SENDQUERY 1
#define CLIENT_READREPLY 2

#define MAX_LATENCY 5000

#define REDIS_NOTUSED(V) ((void) V)

#define MAX_NUM_COLUMNS 64

static struct config {
    int          numclients;
    int          requests;
    int          liveclients;
    int          donerequests;
    int          keysize;
    int          randomkeys;
    int          randomkeys_keyspacelen;
    aeEventLoop *el;
    char        *hostip;
    int          hostport;
    long long    start;
    long long    totlatency;
    int         *latency;
    list        *clients;
    int          quiet;
    int          loop;

    int          sequential;
    int          num_modulo;
    int          modulo[MAX_NUM_COLUMNS];
    int          incr_seq;

    sds          query;
    int          qargc;
    int          reply_type;
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

static void randomizeClientKey(client c);

static void clientDone(client c) {
    long long latency;
    config.donerequests ++;
    latency = mstime() - c->start;
    if (latency > MAX_LATENCY) latency = MAX_LATENCY;
    config.latency[latency]++;

    if (config.donerequests == config.requests) {
        freeClient(c);
        aeStop(config.el);
        return;
    }
    resetClient(c);
    if (config.randomkeys || config.sequential || config.incr_seq)
        randomizeClientKey(c);
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
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

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
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
        if (config.randomkeys || config.sequential || config.incr_seq)
            randomizeClientKey(c);
        prepareClientForReply(new,c->replytype);
    }
}

static void showLatencyReport() {
    int j, seen = 0;
    float perc, reqpersec;

    reqpersec = (float)config.donerequests/((float)config.totlatency/1000);
    if (!config.quiet) {
        printf("  %d requests completed in %.2f seconds\n", config.donerequests,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
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
        printf("%.2f requests per second\n", reqpersec);
    }
}

static void prepareForBenchmark(void) {
    memset(config.latency,0,sizeof(int)*(MAX_LATENCY+1));
    config.start = mstime();
    config.donerequests = 0;
}

static void endBenchmark() {
    config.totlatency = mstime()-config.start;
    showLatencyReport();
    freeAllClients();
}

static void usage(char *arg);

long Sequence = 1;
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
        } else if (!strcmp(argv[i],"-r") && !lastarg) {
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[i+1]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
            i++;
        } else if (!strcmp(argv[i],"-Q") && !lastarg) {
            char **x = malloc((argc - i + 1) * sizeof(char *));
            int    k = 0;
            for (int j = i + 1; j < argc; j++) {
                x[k] = malloc(strlen(argv[j]) + 1);
                strcpy(x[k], argv[j]);
                k++;
            }
            config.query = sdscatprintf(sdsempty(),"*%d\r\n", k);
            for (int j = 0; j < k; j++) {
                config.query = sdscatprintf(config.query,"$%lu\r\n",
                    (unsigned long)strlen(x[j]));
                config.query = sdscatlen(config.query, x[j], strlen(x[j]));
                config.query = sdscatlen(config.query, "\r\n", 2);
            }
            for (int j = 0; j < k; j++) {
                free(x[k]);
            }
            free(x);
            break;
        } else if (!strcmp(argv[i],"-INLINE") && !lastarg) {
            config.query  = sdsnewlen(argv[i + 1], strlen(argv[i + 1]));
            i++;
            if (config.query[sdslen(config.query) - 3] != '\r' &&
                config.query[sdslen(config.query) - 2] != '\n')
                config.query = sdscatlen(config.query, "\r\n\r\n", 4);
        } else if (!strcmp(argv[i],"-A") && !lastarg) {
            char *rtype = argv[i + 1];
            if (!strcmp(rtype, "INT")) {
                config.reply_type = REPLY_INT;
            } else if (!strcmp(rtype, "OK")) {
                config.reply_type = REPLY_RETCODE;
            } else if (!strcmp(rtype, "LINE")) {
                config.reply_type = REPLY_BULK;
            } else if (!strcmp(rtype, "MULTI")) {
                config.reply_type = REPLY_MBULK;
            } else {
                usage(rtype);
            }
            i++;
        } else if (!strcmp(argv[i],"-m") && !lastarg) {
            char *arg         = argv[i + 1];
            config.modulo[0]  = atoi(arg); /* NOTE will stop at ',' */
            char *nextc       = arg;
            config.num_modulo = 1;
            while ((nextc = strchr(nextc, ',')) != NULL) {
                nextc++;
                config.modulo[config.num_modulo] = atoi(nextc);
                config.num_modulo++;
            }
            i++;
        } else if (!strcmp(argv[i],"-i") && !lastarg) {
            config.incr_seq = atoi(argv[i+1]);
            if (config.incr_seq < 0) config.incr_seq = 0;
            i++;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet      = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop       = 1;
        } else if (!strcmp(argv[i],"-s")) {
            config.sequential = 1;
            Sequence          = atoi(argv[i+1]);
            if (Sequence < 0) Sequence = 1;
            i++;
        } else {
            usage(argv[i]);
        }
    }
}

static void usage(char *arg) {
    if (arg) printf("Wrong option '%s' or option argument missing\n\n", arg);
    printf("Usage: gen-benchmark [-h <host>] [-p <port>] [-c <concurrency>] [-n <requests]> -A [OK,INT,LINE,MULTI] -Q query_arg1 query_arg2 ...\n\n");
    printf(" -Q \"QUERY\"            QUERY to be sent to server each command line arg passed to redis line protocol as a seperate arg [-Q MUST COME LAST]\n");
    printf(" -A [OK,INT,LINE,MULTI] response type must be one of them\n");
    printf(" -h <hostname>           Server hostname (default 127.0.0.1)\n");
    printf(" -p <hostname>           Server port (default 6379)\n");
    printf(" -c <clients>            Num parallel connections (default 50)\n");
    printf(" -n <requests>           Total num requests (default 10000)\n");
    printf(" -r <keyspacelen>        Use random keys\n");
    printf(" -s <sequence_start>     Use sequential keys\n");
    printf(" -i <incr_num>           Use incremental sequential keys\n");
    printf(" -m <modulo,,,,,,>       Modulo for foreign keys (2nd instance of \"0000\")\n");
    printf("  Using this option the benchmark will string replace queries\n");
    printf("  in the form 000012345678 instead of constant 000000000001\n");
    printf("  The <keyspacelen> argument determines the max\n");
    printf("  number of values for the random number. For instance\n");
    printf("  if set to 10 only (000000000000 - 000000000009) range will be generated.\n");
    printf(" -q                      Quiet. Just show query/sec values\n");
    printf(" -l                      Loop. Run the tests forever\n");
    exit(1);
}

static char *rand_replace(char *p, long r) {
    char buf[32];
    p += 3;
    sprintf(buf, "%011ld", r);
    memcpy(p, buf, strlen(buf));
    return p;
}

#define MIN(A,B) ((A > B) ? B : A)
static void randomizeClientKey(client c) {
    char *p = c->obuf;
    long  r;
    if (config.sequential)    r = Sequence++;
    else if (config.incr_seq) r = Sequence += config.incr_seq;
    else                      r = random() % config.randomkeys_keyspacelen;
    int  hits   = 0;
    long orig_r = r;
    while ((p = strstr(p, "0000"))) {
        r      = orig_r;
        char x = *(p - 1);
        if (x == '(' || x == ',' || x == '_' || x == '=') {
            if (hits > 0 && config.num_modulo) {
                int m = MIN((hits - 1), (config.num_modulo - 1));
                //printf("m: %d r: %ld c.m: %d\n", m, r, config.modulo[m]);
                r %= config.modulo[m];
            }
            if (!r) r = 1; /* 0 as a FK is BAD */
            p  = rand_replace(p, r);
            hits++;
        } else {
            p +=4;
        }
    }
    //printf("hits: %d buf: %s\n", hits, c->obuf);
}

int main(int argc, char **argv) {
    client c;
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.numclients             = 50;
    config.requests               = 10000;
    config.liveclients            = 0;
    config.el                     = aeCreateEventLoop();
    config.donerequests           = 0;
    config.randomkeys             = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet                  = 0;
    config.loop                   = 0;
    config.latency                = NULL;
    config.clients                = listCreate();
    config.latency                = zmalloc(sizeof(int)*(MAX_LATENCY+1));
    config.hostip                 = "127.0.0.1";
    config.hostport               = 6379;

    config.sequential             = 0;
    config.incr_seq               = 0;
    config.num_modulo             = 0;
    config.query                  = NULL;
    config.reply_type             = -1;

    parseOptions(argc,argv);

    if (!config.query || (config.reply_type == -1)) {
        usage(NULL);
    }

    do {
        prepareForBenchmark();
        c = createClient();
        if (!c) exit(1);
        c->obuf = sdscat(c->obuf, config.query);
        prepareClientForReply(c, config.reply_type);
        createMissingClients(c);
        aeMain(config.el);
        endBenchmark();
        printf("\n");
    } while(config.loop);

    return 0;
}

#include "redis.h"
#include "sha1.h"   /* SHA1 is used for DEBUG DIGEST */

#include <arpa/inet.h>
#include <signal.h>

#ifdef HAVE_BACKTRACE
#include <execinfo.h>
#include <ucontext.h>
#endif /* HAVE_BACKTRACE */

/* ================================= Debugging ============================== */

/* Compute the sha1 of string at 's' with 'len' bytes long.
 * The SHA1 is then xored againt the string pointed by digest.
 * Since xor is commutative, this operation is used in order to
 * "add" digests relative to unordered elements.
 *
 * So digest(a,b,c,d) will be the same of digest(b,a,c,d) */
void xorDigest(unsigned char *digest, void *ptr, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20], *s = ptr;
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx,s,len);
    SHA1Final(hash,&ctx);

    for (j = 0; j < 20; j++)
        digest[j] ^= hash[j];
}

void xorObjectDigest(unsigned char *digest, robj *o) {
    o = getDecodedObject(o);
    xorDigest(digest,o->ptr,sdslen(o->ptr));
    decrRefCount(o);
}

/* This function instead of just computing the SHA1 and xoring it
 * against diget, also perform the digest of "digest" itself and
 * replace the old value with the new one.
 *
 * So the final digest will be:
 *
 * digest = SHA1(digest xor SHA1(data))
 *
 * This function is used every time we want to preserve the order so
 * that digest(a,b,c,d) will be different than digest(b,c,d,a)
 *
 * Also note that mixdigest("foo") followed by mixdigest("bar")
 * will lead to a different digest compared to "fo", "obar".
 */
void mixDigest(unsigned char *digest, void *ptr, size_t len) {
    SHA1_CTX ctx;
    char *s = ptr;

    xorDigest(digest,s,len);
    SHA1Init(&ctx);
    SHA1Update(&ctx,digest,20);
    SHA1Final(digest,&ctx);
}

void mixObjectDigest(unsigned char *digest, robj *o) {
    o = getDecodedObject(o);
    mixDigest(digest,o->ptr,sdslen(o->ptr));
    decrRefCount(o);
}

/* Compute the dataset digest. Since keys, sets elements, hashes elements
 * are not ordered, we use a trick: every aggregate digest is the xor
 * of the digests of their elements. This way the order will not change
 * the result. For list instead we use a feedback entering the output digest
 * as input in order to ensure that a different ordered list will result in
 * a different digest. */
void computeDatasetDigest(unsigned char *final) {
    unsigned char digest[20];
    char buf[128];
    dictIterator *di = NULL;
    dictEntry *de;
    int j;
    uint32_t aux;

    memset(final,0,20); /* Start with a clean result */

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;

        if (dictSize(db->dict) == 0) continue;
        di = dictGetIterator(db->dict);

        /* hash the DB id, so the same dataset moved in a different
         * DB will lead to a different digest */
        aux = htonl(j);
        mixDigest(final,&aux,sizeof(aux));

        /* Iterate this DB writing every entry */
        while((de = dictNext(di)) != NULL) {
            sds key;
            robj *keyobj, *o;
            long long expiretime;

            memset(digest,0,20); /* This key-val digest */
            key = dictGetKey(de);
            keyobj = createStringObject(key,sdslen(key));

            mixDigest(digest,key,sdslen(key));

            /* Make sure the key is loaded if VM is active */
            o = dictGetVal(de);

            aux = htonl(o->type);
            mixDigest(digest,&aux,sizeof(aux));
            expiretime = getExpire(db,keyobj);

            /* Save the key and associated value */
            if (o->type == REDIS_STRING) {
                mixObjectDigest(digest,o);
            } else if (o->type == REDIS_LIST) {
                listTypeIterator *li = listTypeInitIterator(o,0,REDIS_TAIL);
                listTypeEntry entry;
                while(listTypeNext(li,&entry)) {
                    robj *eleobj = listTypeGet(&entry);
                    mixObjectDigest(digest,eleobj);
                    decrRefCount(eleobj);
                }
                listTypeReleaseIterator(li);
            } else if (o->type == REDIS_SET) {
                setTypeIterator *si = setTypeInitIterator(o);
                robj *ele;
                while((ele = setTypeNextObject(si)) != NULL) {
                    xorObjectDigest(digest,ele);
                    decrRefCount(ele);
                }
                setTypeReleaseIterator(si);
            } else if (o->type == REDIS_ZSET) {
                unsigned char eledigest[20];

                if (o->encoding == REDIS_ENCODING_ZIPLIST) {
                    unsigned char *zl = o->ptr;
                    unsigned char *eptr, *sptr;
                    unsigned char *vstr;
                    unsigned int vlen;
                    long long vll;
                    double score;

                    eptr = ziplistIndex(zl,0);
                    redisAssert(eptr != NULL);
                    sptr = ziplistNext(zl,eptr);
                    redisAssert(sptr != NULL);

                    while (eptr != NULL) {
                        redisAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
                        score = zzlGetScore(sptr);

                        memset(eledigest,0,20);
                        if (vstr != NULL) {
                            mixDigest(eledigest,vstr,vlen);
                        } else {
                            ll2string(buf,sizeof(buf),vll);
                            mixDigest(eledigest,buf,strlen(buf));
                        }

                        snprintf(buf,sizeof(buf),"%.17g",score);
                        mixDigest(eledigest,buf,strlen(buf));
                        xorDigest(digest,eledigest,20);
                        zzlNext(zl,&eptr,&sptr);
                    }
                } else if (o->encoding == REDIS_ENCODING_SKIPLIST) {
                    zset *zs = o->ptr;
                    dictIterator *di = dictGetIterator(zs->dict);
                    dictEntry *de;

                    while((de = dictNext(di)) != NULL) {
                        robj *eleobj = dictGetKey(de);
                        double *score = dictGetVal(de);

                        snprintf(buf,sizeof(buf),"%.17g",*score);
                        memset(eledigest,0,20);
                        mixObjectDigest(eledigest,eleobj);
                        mixDigest(eledigest,buf,strlen(buf));
                        xorDigest(digest,eledigest,20);
                    }
                    dictReleaseIterator(di);
                } else {
                    redisPanic("Unknown sorted set encoding");
                }
            } else if (o->type == REDIS_HASH) {
                hashTypeIterator *hi;
                robj *obj;

                hi = hashTypeInitIterator(o);
                while (hashTypeNext(hi) != REDIS_ERR) {
                    unsigned char eledigest[20];

                    memset(eledigest,0,20);
                    obj = hashTypeCurrentObject(hi,REDIS_HASH_KEY);
                    mixObjectDigest(eledigest,obj);
                    decrRefCount(obj);
                    obj = hashTypeCurrentObject(hi,REDIS_HASH_VALUE);
                    mixObjectDigest(eledigest,obj);
                    decrRefCount(obj);
                    xorDigest(digest,eledigest,20);
                }
                hashTypeReleaseIterator(hi);
            } else {
                redisPanic("Unknown object type");
            }
            /* If the key has an expire, add it to the mix */
            if (expiretime != -1) xorDigest(digest,"!!expire!!",10);
            /* We can finally xor the key-val digest to the final digest */
            xorDigest(final,digest,20);
            decrRefCount(keyobj);
        }
        dictReleaseIterator(di);
    }
}

void debugCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"segfault")) {
        *((char*)-1) = 'x';
    } else if (!strcasecmp(c->argv[1]->ptr,"assert")) {
        if (c->argc >= 3) c->argv[2] = tryObjectEncoding(c->argv[2]);
        redisAssertWithInfo(c,c->argv[0],1 == 2);
    } else if (!strcasecmp(c->argv[1]->ptr,"reload")) {
        if (rdbSave(server.rdb_filename) != REDIS_OK) {
            addReply(c,shared.err);
            return;
        }
        emptyDb();
        if (rdbLoad(server.rdb_filename) != REDIS_OK) {
            addReplyError(c,"Error trying to load the RDB dump");
            return;
        }
        redisLog(REDIS_WARNING,"DB reloaded by DEBUG RELOAD");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"loadaof")) {
        emptyDb();
        if (loadAppendOnlyFile(server.aof_filename) != REDIS_OK) {
            addReply(c,shared.err);
            return;
        }
        server.dirty = 0; /* Prevent AOF / replication */
        redisLog(REDIS_WARNING,"Append Only File loaded by DEBUG LOADAOF");
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"object") && c->argc == 3) {
        dictEntry *de;
        robj *val;
        char *strenc;

        if ((de = dictFind(c->db->dict,c->argv[2]->ptr)) == NULL) {
            addReply(c,shared.nokeyerr);
            return;
        }
        val = dictGetVal(de);
        strenc = strEncoding(val->encoding);

        addReplyStatusFormat(c,
            "Value at:%p refcount:%d "
            "encoding:%s serializedlength:%lld "
            "lru:%d lru_seconds_idle:%lu",
            (void*)val, val->refcount,
            strenc, (long long) rdbSavedObjectLen(val),
            val->lru, estimateObjectIdleTime(val));
    } else if (!strcasecmp(c->argv[1]->ptr,"populate") && c->argc == 3) {
        long keys, j;
        robj *key, *val;
        char buf[128];

        if (getLongFromObjectOrReply(c, c->argv[2], &keys, NULL) != REDIS_OK)
            return;
        for (j = 0; j < keys; j++) {
            snprintf(buf,sizeof(buf),"key:%lu",j);
            key = createStringObject(buf,strlen(buf));
            if (lookupKeyRead(c->db,key) != NULL) {
                decrRefCount(key);
                continue;
            }
            snprintf(buf,sizeof(buf),"value:%lu",j);
            val = createStringObject(buf,strlen(buf));
            dbAdd(c->db,key,val);
            decrRefCount(key);
        }
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"digest") && c->argc == 2) {
        unsigned char digest[20];
        sds d = sdsempty();
        int j;

        computeDatasetDigest(digest);
        for (j = 0; j < 20; j++)
            d = sdscatprintf(d, "%02x",digest[j]);
        addReplyStatus(c,d);
        sdsfree(d);
    } else if (!strcasecmp(c->argv[1]->ptr,"sleep") && c->argc == 3) {
        double dtime = strtod(c->argv[2]->ptr,NULL);
        long long utime = dtime*1000000;

        usleep(utime);
        addReply(c,shared.ok);
    } else {
        addReplyError(c,
            "Syntax error, try DEBUG [SEGFAULT|OBJECT <key>|SWAPIN <key>|SWAPOUT <key>|RELOAD]");
    }
}

/* =========================== Crash handling  ============================== */

void _redisAssert(char *estr, char *file, int line) {
    bugReportStart();
    redisLog(REDIS_WARNING,"=== ASSERTION FAILED ===");
    redisLog(REDIS_WARNING,"==> %s:%d '%s' is not true",file,line,estr);
#ifdef HAVE_BACKTRACE
    server.assert_failed = estr;
    server.assert_file = file;
    server.assert_line = line;
    redisLog(REDIS_WARNING,"(forcing SIGSEGV to print the bug report.)");
#endif
    *((char*)-1) = 'x';
}

void _redisAssertPrintClientInfo(redisClient *c) {
    int j;

    bugReportStart();
    redisLog(REDIS_WARNING,"=== ASSERTION FAILED CLIENT CONTEXT ===");
    redisLog(REDIS_WARNING,"client->flags = %d", c->flags);
    redisLog(REDIS_WARNING,"client->fd = %d", c->fd);
    redisLog(REDIS_WARNING,"client->argc = %d", c->argc);
    for (j=0; j < c->argc; j++) {
        char buf[128];
        char *arg;

        if (c->argv[j]->type == REDIS_STRING &&
            c->argv[j]->encoding == REDIS_ENCODING_RAW)
        {
            arg = (char*) c->argv[j]->ptr;
        } else {
            snprintf(buf,sizeof(buf),"Object type: %d, encoding: %d",
                c->argv[j]->type, c->argv[j]->encoding);
            arg = buf;
        }
        redisLog(REDIS_WARNING,"client->argv[%d] = \"%s\" (refcount: %d)",
            j, arg, c->argv[j]->refcount);
    }
}

void redisLogObjectDebugInfo(robj *o) {
    redisLog(REDIS_WARNING,"Object type: %d", o->type);
    redisLog(REDIS_WARNING,"Object encoding: %d", o->encoding);
    redisLog(REDIS_WARNING,"Object refcount: %d", o->refcount);
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_RAW) {
        redisLog(REDIS_WARNING,"Object raw string len: %d", sdslen(o->ptr));
        if (sdslen(o->ptr) < 4096)
            redisLog(REDIS_WARNING,"Object raw string content: \"%s\"", (char*)o->ptr);
    } else if (o->type == REDIS_LIST) {
        redisLog(REDIS_WARNING,"List length: %d", (int) listTypeLength(o));
    } else if (o->type == REDIS_SET) {
        redisLog(REDIS_WARNING,"Set size: %d", (int) setTypeSize(o));
    } else if (o->type == REDIS_HASH) {
        redisLog(REDIS_WARNING,"Hash size: %d", (int) hashTypeLength(o));
    } else if (o->type == REDIS_ZSET) {
        redisLog(REDIS_WARNING,"Sorted set size: %d", (int) zsetLength(o));
        if (o->encoding == REDIS_ENCODING_SKIPLIST)
            redisLog(REDIS_WARNING,"Skiplist level: %d", (int) ((zset*)o->ptr)->zsl->level);
    }
}

void _redisAssertPrintObject(robj *o) {
    bugReportStart();
    redisLog(REDIS_WARNING,"=== ASSERTION FAILED OBJECT CONTEXT ===");
    redisLogObjectDebugInfo(o);
}

void _redisAssertWithInfo(redisClient *c, robj *o, char *estr, char *file, int line) {
    if (c) _redisAssertPrintClientInfo(c);
    if (o) _redisAssertPrintObject(o);
    _redisAssert(estr,file,line);
}

void _redisPanic(char *msg, char *file, int line) {
    bugReportStart();
    redisLog(REDIS_WARNING,"------------------------------------------------");
    redisLog(REDIS_WARNING,"!!! Software Failure. Press left mouse button to continue");
    redisLog(REDIS_WARNING,"Guru Meditation: %s #%s:%d",msg,file,line);
#ifdef HAVE_BACKTRACE
    redisLog(REDIS_WARNING,"(forcing SIGSEGV in order to print the stack trace)");
#endif
    redisLog(REDIS_WARNING,"------------------------------------------------");
    *((char*)-1) = 'x';
}

void bugReportStart(void) {
    if (server.bug_report_start == 0) {
        redisLog(REDIS_WARNING,
            "\n\n=== REDIS BUG REPORT START: Cut & paste starting from here ===");
        server.bug_report_start = 1;
    }
}

#ifdef HAVE_BACKTRACE
static void *getMcontextEip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
  #if __x86_64__
    return (void*) uc->uc_mcontext->__ss.__rip;
  #elif __i386__
    return (void*) uc->uc_mcontext->__ss.__eip;
  #else
    return (void*) uc->uc_mcontext->__ss.__srr0;
  #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
  #else
    return (void*) uc->uc_mcontext->__ss.__eip;
  #endif
#elif defined(__i386__)
    return (void*) uc->uc_mcontext.gregs[14]; /* Linux 32 */
#elif defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[16]; /* Linux 64 */
#elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
#else
    return NULL;
#endif
}

void logStackContent(void **sp) {
    int i;
    for (i = 15; i >= 0; i--) {
        if (sizeof(long) == 4)
            redisLog(REDIS_WARNING, "(%08lx) -> %08lx", sp+i, sp[i]);
        else
            redisLog(REDIS_WARNING, "(%016lx) -> %016lx", sp+i, sp[i]);
    }
}

void logRegisters(ucontext_t *uc) {
    redisLog(REDIS_WARNING, "--- REGISTERS");
#if defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    redisLog(REDIS_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCS :%016lx FS:%016lx  GS:%016lx",
        uc->uc_mcontext->__ss.__rax,
        uc->uc_mcontext->__ss.__rbx,
        uc->uc_mcontext->__ss.__rcx,
        uc->uc_mcontext->__ss.__rdx,
        uc->uc_mcontext->__ss.__rdi,
        uc->uc_mcontext->__ss.__rsi,
        uc->uc_mcontext->__ss.__rbp,
        uc->uc_mcontext->__ss.__rsp,
        uc->uc_mcontext->__ss.__r8,
        uc->uc_mcontext->__ss.__r9,
        uc->uc_mcontext->__ss.__r10,
        uc->uc_mcontext->__ss.__r11,
        uc->uc_mcontext->__ss.__r12,
        uc->uc_mcontext->__ss.__r13,
        uc->uc_mcontext->__ss.__r14,
        uc->uc_mcontext->__ss.__r15,
        uc->uc_mcontext->__ss.__rip,
        uc->uc_mcontext->__ss.__rflags,
        uc->uc_mcontext->__ss.__cs,
        uc->uc_mcontext->__ss.__fs,
        uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__rsp);
  #else
    redisLog(REDIS_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS:%08lx  EFL:%08lx EIP:%08lx CS :%08lx\n"
    "DS:%08lx  ES:%08lx  FS :%08lx GS :%08lx",
        uc->uc_mcontext->__ss.__eax,
        uc->uc_mcontext->__ss.__ebx,
        uc->uc_mcontext->__ss.__ecx,
        uc->uc_mcontext->__ss.__edx,
        uc->uc_mcontext->__ss.__edi,
        uc->uc_mcontext->__ss.__esi,
        uc->uc_mcontext->__ss.__ebp,
        uc->uc_mcontext->__ss.__esp,
        uc->uc_mcontext->__ss.__ss,
        uc->uc_mcontext->__ss.__eflags,
        uc->uc_mcontext->__ss.__eip,
        uc->uc_mcontext->__ss.__cs,
        uc->uc_mcontext->__ss.__ds,
        uc->uc_mcontext->__ss.__es,
        uc->uc_mcontext->__ss.__fs,
        uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__esp);
  #endif
#elif defined(__i386__)
    redisLog(REDIS_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        uc->uc_mcontext.gregs[11],
        uc->uc_mcontext.gregs[8],
        uc->uc_mcontext.gregs[10],
        uc->uc_mcontext.gregs[9],
        uc->uc_mcontext.gregs[4],
        uc->uc_mcontext.gregs[5],
        uc->uc_mcontext.gregs[6],
        uc->uc_mcontext.gregs[7],
        uc->uc_mcontext.gregs[18],
        uc->uc_mcontext.gregs[17],
        uc->uc_mcontext.gregs[14],
        uc->uc_mcontext.gregs[15],
        uc->uc_mcontext.gregs[3],
        uc->uc_mcontext.gregs[2],
        uc->uc_mcontext.gregs[1],
        uc->uc_mcontext.gregs[0]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[7]);
#elif defined(__X86_64__) || defined(__x86_64__)
    redisLog(REDIS_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        uc->uc_mcontext.gregs[13],
        uc->uc_mcontext.gregs[11],
        uc->uc_mcontext.gregs[14],
        uc->uc_mcontext.gregs[12],
        uc->uc_mcontext.gregs[8],
        uc->uc_mcontext.gregs[9],
        uc->uc_mcontext.gregs[10],
        uc->uc_mcontext.gregs[15],
        uc->uc_mcontext.gregs[0],
        uc->uc_mcontext.gregs[1],
        uc->uc_mcontext.gregs[2],
        uc->uc_mcontext.gregs[3],
        uc->uc_mcontext.gregs[4],
        uc->uc_mcontext.gregs[5],
        uc->uc_mcontext.gregs[6],
        uc->uc_mcontext.gregs[7],
        uc->uc_mcontext.gregs[16],
        uc->uc_mcontext.gregs[17],
        uc->uc_mcontext.gregs[18]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[15]);
#else
    redisLog(REDIS_WARNING,
        "  Dumping of registers not supported for this OS/arch");
#endif
}

/* Logs the stack trace using the backtrace() call. */
sds getStackTrace(ucontext_t *uc) {
    void *trace[100];
    int i, trace_size = 0;
    char **messages = NULL;
    sds st = sdsempty();

    /* Generate the stack trace */
    trace_size = backtrace(trace, 100);

    /* overwrite sigaction with caller's address */
    if (getMcontextEip(uc) != NULL) {
        trace[1] = getMcontextEip(uc);
    }
    messages = backtrace_symbols(trace, trace_size);
    for (i=1; i<trace_size; ++i) {
        st = sdscat(st,messages[i]);
        st = sdscatlen(st,"\n",1);
    }
    zlibc_free(messages);
    return st;
}

/* Log information about the "current" client, that is, the client that is
 * currently being served by Redis. May be NULL if Redis is not serving a
 * client right now. */
void logCurrentClient(void) {
    if (server.current_client == NULL) return;

    redisClient *cc = server.current_client;
    sds client;
    int j;

    redisLog(REDIS_WARNING, "--- CURRENT CLIENT INFO");
    client = getClientInfoString(cc);
    redisLog(REDIS_WARNING,"client: %s", client);
    sdsfree(client);
    for (j = 0; j < cc->argc; j++) {
        robj *decoded;

        decoded = getDecodedObject(cc->argv[j]);
        redisLog(REDIS_WARNING,"argv[%d]: '%s'", j, (char*)decoded->ptr);
        decrRefCount(decoded);
    }
    /* Check if the first argument, usually a key, is found inside the
     * selected DB, and if so print info about the associated object. */
    if (cc->argc >= 1) {
        robj *val, *key;
        dictEntry *de;

        key = getDecodedObject(cc->argv[1]);
        de = dictFind(cc->db->dict, key->ptr);
        if (de) {
            val = dictGetVal(de);
            redisLog(REDIS_WARNING,"key '%s' found in DB containing the following object:", key->ptr);
            redisLogObjectDebugInfo(val);
        }
        decrRefCount(key);
    }
}

void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
    ucontext_t *uc = (ucontext_t*) secret;
    sds infostring, clients, st;
    struct sigaction act;
    REDIS_NOTUSED(info);

    bugReportStart();
    redisLog(REDIS_WARNING,
        "    Redis %s crashed by signal: %d", REDIS_VERSION, sig);
    redisLog(REDIS_WARNING,
        "    Failed assertion: %s (%s:%d)", server.assert_failed,
                        server.assert_file, server.assert_line);

    /* Log the stack trace */
    st = getStackTrace(uc);
    redisLog(REDIS_WARNING, "--- STACK TRACE\n%s", st);
    sdsfree(st);

    /* Log INFO and CLIENT LIST */
    redisLog(REDIS_WARNING, "--- INFO OUTPUT");
    infostring = genRedisInfoString("all");
    infostring = sdscatprintf(infostring, "hash_init_value: %u\n",
        dictGetHashFunctionSeed());
    redisLogRaw(REDIS_WARNING, infostring);
    redisLog(REDIS_WARNING, "--- CLIENT LIST OUTPUT");
    clients = getAllClientsInfoString();
    redisLogRaw(REDIS_WARNING, clients);
    sdsfree(infostring);
    sdsfree(clients);

    /* Log the current client */
    logCurrentClient();

    /* Log dump of processor registers */
    logRegisters(uc);

    redisLog(REDIS_WARNING,
"\n=== REDIS BUG REPORT END. Make sure to include from START to END. ===\n\n"
"       Please report the crash opening an issue on github:\n\n"
"           http://github.com/antirez/redis/issues\n\n"
"  Suspect RAM error? Use redis-server --test-memory to veryfy it.\n\n"
);
    /* free(messages); Don't call free() with possibly corrupted memory. */
    if (server.daemonize) unlink(server.pidfile);

    /* Make sure we exit with the right signal at the end. So for instance
     * the core will be dumped if enabled. */
    sigemptyset (&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction (sig, &act, NULL);
    kill(getpid(),sig);
}
#endif /* HAVE_BACKTRACE */

/* =========================== Software Watchdog ============================ */
#include <sys/time.h>

void watchdogSignalHandler(int sig, siginfo_t *info, void *secret) {
    ucontext_t *uc = (ucontext_t*) secret;
    REDIS_NOTUSED(info);
    REDIS_NOTUSED(sig);
    sds st, log;

    log = sdsnew("\n--- WATCHDOG TIMER EXPIRED ---\n");
#ifdef HAVE_BACKTRACE
    st = getStackTrace(uc);
#else
    st = sdsnew("Sorry: no support for backtrace().\n");
#endif
    log = sdscatsds(log,st);
    log = sdscat(log,"------\n");
    redisLogFromHandler(REDIS_WARNING,log);
    sdsfree(st);
    sdsfree(log);
}

/* Schedule a SIGALRM delivery after the specified period in milliseconds.
 * If a timer is already scheduled, this function will re-schedule it to the
 * specified time. If period is 0 the current timer is disabled. */
void watchdogScheduleSignal(int period) {
    struct itimerval it;

    /* Will stop the timer if period is 0. */
    it.it_value.tv_sec = period/1000;
    it.it_value.tv_usec = (period%1000)*1000;
    /* Don't automatically restart. */
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_usec = 0;
    setitimer(ITIMER_REAL, &it, NULL);
}

/* Enable the software watchdong with the specified period in milliseconds. */
void enableWatchdog(int period) {
    if (server.watchdog_period == 0) {
        struct sigaction act;

        /* Watchdog was actually disabled, so we have to setup the signal
         * handler. */
        sigemptyset(&act.sa_mask);
        act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_SIGINFO;
        act.sa_sigaction = watchdogSignalHandler;
        sigaction(SIGALRM, &act, NULL);
    }
    if (period < 200) period = 200; /* We don't accept periods < 200 ms. */
    watchdogScheduleSignal(period); /* Adjust the current timer. */
    server.watchdog_period = period;
}

/* Disable the software watchdog. */
void disableWatchdog(void) {
    struct sigaction act;
    if (server.watchdog_period == 0) return; /* Already disabled. */
    watchdogScheduleSignal(0); /* Stop the current timer. */

    /* Set the signal handler to SIG_IGN, this will also remove pending
     * signals from the queue. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = SIG_IGN;
    sigaction(SIGALRM, &act, NULL);
    server.watchdog_period = 0;
}

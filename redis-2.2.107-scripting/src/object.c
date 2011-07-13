#include "redis.h"
#include <pthread.h>
#include <math.h>

robj *createObject(int type, void *ptr) {
    robj *o = zmalloc(sizeof(*o));
    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;
    o->ptr = ptr;
    o->refcount = 1;

    /* Set the LRU to the current lruclock (minutes resolution).
     * We do this regardless of the fact VM is active as LRU is also
     * used for the maxmemory directive when Redis is used as cache.
     *
     * Note that this code may run in the context of an I/O thread
     * and accessing server.lruclock in theory is an error
     * (no locks). But in practice this is safe, and even if we read
     * garbage Redis will not fail. */
    o->lru = server.lruclock;
    /* The following is only needed if VM is active, but since the conditional
     * is probably more costly than initializing the field it's better to
     * have every field properly initialized anyway. */
    o->storage = REDIS_VM_MEMORY;
    return o;
}

robj *createStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

robj *createStringObjectFromLongLong(long long value) {
    robj *o;
    if (value >= 0 && value < REDIS_SHARED_INTEGERS &&
        pthread_equal(pthread_self(),server.mainthread)) {
        incrRefCount(shared.integers[value]);
        o = shared.integers[value];
    } else {
        if (value >= LONG_MIN && value <= LONG_MAX) {
            o = createObject(REDIS_STRING, NULL);
            o->encoding = REDIS_ENCODING_INT;
            o->ptr = (void*)((long)value);
        } else {
            o = createObject(REDIS_STRING,sdsfromlonglong(value));
        }
    }
    return o;
}

robj *dupStringObject(robj *o) {
    redisAssert(o->encoding == REDIS_ENCODING_RAW);
    return createStringObject(o->ptr,sdslen(o->ptr));
}

robj *createListObject(void) {
    list *l = listCreate();
    robj *o = createObject(REDIS_LIST,l);
    listSetFreeMethod(l,decrRefCount);
    o->encoding = REDIS_ENCODING_LINKEDLIST;
    return o;
}

robj *createZiplistObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_LIST,zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType,NULL);
    robj *o = createObject(REDIS_SET,d);
    o->encoding = REDIS_ENCODING_HT;
    return o;
}

robj *createIntsetObject(void) {
    intset *is = intsetNew();
    robj *o = createObject(REDIS_SET,is);
    o->encoding = REDIS_ENCODING_INTSET;
    return o;
}

robj *createHashObject(void) {
    /* All the Hashes start as zipmaps. Will be automatically converted
     * into hash tables if there are enough elements or big elements
     * inside. */
    unsigned char *zm = zipmapNew();
    robj *o = createObject(REDIS_HASH,zm);
    o->encoding = REDIS_ENCODING_ZIPMAP;
    return o;
}

robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();
    o = createObject(REDIS_ZSET,zs);
    o->encoding = REDIS_ENCODING_SKIPLIST;
    return o;
}

void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

void freeListObject(robj *o) {
    switch (o->encoding) {
    case REDIS_ENCODING_LINKEDLIST:
        listRelease((list*) o->ptr);
        break;
    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown list encoding type");
    }
}

void freeSetObject(robj *o) {
    switch (o->encoding) {
    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    case REDIS_ENCODING_INTSET:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown set encoding type");
    }
}

void freeZsetObject(robj *o) {
    zset *zs = o->ptr;

    dictRelease(zs->dict);
    zslFree(zs->zsl);
    zfree(zs);
}

void freeHashObject(robj *o) {
    switch (o->encoding) {
    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    case REDIS_ENCODING_ZIPMAP:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown hash encoding type");
        break;
    }
}

void incrRefCount(robj *o) {
    o->refcount++;
}

void decrRefCount(void *obj) {
    robj *o = obj;

    /* Object is a swapped out value, or in the process of being loaded. */
    if (server.vm_enabled &&
        (o->storage == REDIS_VM_SWAPPED || o->storage == REDIS_VM_LOADING))
    {
        vmpointer *vp = obj;
        if (o->storage == REDIS_VM_LOADING) vmCancelThreadedIOJob(o);
        vmMarkPagesFree(vp->page,vp->usedpages);
        server.vm_stats_swapped_objects--;
        zfree(vp);
        return;
    }

    if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");
    /* Object is in memory, or in the process of being swapped out.
     *
     * If the object is being swapped out, abort the operation on
     * decrRefCount even if the refcount does not drop to 0: the object
     * is referenced at least two times, as value of the key AND as
     * job->val in the iojob. So if we don't invalidate the iojob, when it is
     * done but the relevant key was removed in the meantime, the
     * complete jobs handler will not find the key about the job and the
     * assert will fail. */
    if (server.vm_enabled && o->storage == REDIS_VM_SWAPPING)
        vmCancelThreadedIOJob(o);
    if (--(o->refcount) == 0) {
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o); break;
        case REDIS_LIST: freeListObject(o); break;
        case REDIS_SET: freeSetObject(o); break;
        case REDIS_ZSET: freeZsetObject(o); break;
        case REDIS_HASH: freeHashObject(o); break;
        default: redisPanic("Unknown object type"); break;
        }
        o->ptr = NULL; /* defensive programming. We'll see NULL in traces. */
        zfree(o);
    }
}

int checkType(redisClient *c, robj *o, int type) {
    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

/* Try to encode a string object in order to save space */
robj *tryObjectEncoding(robj *o) {
    long value;
    sds s = o->ptr;

    if (o->encoding != REDIS_ENCODING_RAW)
        return o; /* Already encoded */

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis. Encoded objects can only
     * appear as "values" (and not, for instance, as keys) */
     if (o->refcount > 1) return o;

    /* Currently we try to encode only strings */
    redisAssert(o->type == REDIS_STRING);

    /* Check if we can represent this string as a long integer */
    if (isStringRepresentableAsLong(s,&value) == REDIS_ERR) return o;

    /* Ok, this object can be encoded...
     *
     * Can I use a shared object? Only if the object is inside a given
     * range and if this is the main thread, since when VM is enabled we
     * have the constraint that I/O thread should only handle non-shared
     * objects, in order to avoid race conditions (we don't have per-object
     * locking).
     *
     * Note that we also avoid using shared integers when maxmemory is used
     * because very object needs to have a private LRU field for the LRU
     * algorithm to work well. */
    if (server.maxmemory == 0 && value >= 0 && value < REDIS_SHARED_INTEGERS &&
        pthread_equal(pthread_self(),server.mainthread)) {
        decrRefCount(o);
        incrRefCount(shared.integers[value]);
        return shared.integers[value];
    } else {
        o->encoding = REDIS_ENCODING_INT;
        sdsfree(o->ptr);
        o->ptr = (void*) value;
        return o;
    }
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
robj *getDecodedObject(robj *o) {
    robj *dec;

    if (o->encoding == REDIS_ENCODING_RAW) {
        incrRefCount(o);
        return o;
    }
    if (o->type == REDIS_STRING && o->encoding == REDIS_ENCODING_INT) {
        char buf[32];

        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        redisPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or alike.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: if objects are not integer encoded, but binary-safe strings,
 * sdscmp() from sds.c will apply memcmp() so this function ca be considered
 * binary safe. */
int compareStringObjects(robj *a, robj *b) {
    redisAssert(a->type == REDIS_STRING && b->type == REDIS_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    int bothsds = 1;

    if (a == b) return 0;
    if (a->encoding != REDIS_ENCODING_RAW) {
        ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
        bothsds = 0;
    } else {
        astr = a->ptr;
    }
    if (b->encoding != REDIS_ENCODING_RAW) {
        ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
        bothsds = 0;
    } else {
        bstr = b->ptr;
    }
    return bothsds ? sdscmp(astr,bstr) : strcmp(astr,bstr);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding != REDIS_ENCODING_RAW && b->encoding != REDIS_ENCODING_RAW){
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a,b) == 0;
    }
}

size_t stringObjectLen(robj *o) {
    redisAssert(o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_RAW) {
        return sdslen(o->ptr);
    } else {
        char buf[32];

        return ll2string(buf,32,(long)o->ptr);
    }
}

int getDoubleFromObject(robj *o, double *target) {
    double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssert(o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            value = strtod(o->ptr, &eptr);
            if (eptr[0] != '\0' || isnan(value)) return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    *target = value;
    return REDIS_OK;
}

int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a double");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

int getLongLongFromObject(robj *o, long long *target) {
    long long value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssert(o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            value = strtoll(o->ptr, &eptr, 10);
            if (eptr[0] != '\0') return REDIS_ERR;
            if (errno == ERANGE && (value == LLONG_MIN || value == LLONG_MAX))
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }

    if (target) *target = value;
    return REDIS_OK;
}

int getLongLongFromObjectOrReply(redisClient *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }

    *target = value;
    return REDIS_OK;
}

char *strEncoding(int encoding) {
    switch(encoding) {
    case REDIS_ENCODING_RAW: return "raw";
    case REDIS_ENCODING_INT: return "int";
    case REDIS_ENCODING_HT: return "hashtable";
    case REDIS_ENCODING_ZIPMAP: return "zipmap";
    case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
    case REDIS_ENCODING_ZIPLIST: return "ziplist";
    case REDIS_ENCODING_INTSET: return "intset";
    case REDIS_ENCODING_SKIPLIST: return "skiplist";
    default: return "unknown";
    }
}

/* Given an object returns the min number of seconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long estimateObjectIdleTime(robj *o) {
    if (server.lruclock >= o->lru) {
        return (server.lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
    } else {
        return ((REDIS_LRU_CLOCK_MAX - o->lru) + server.lruclock) *
                    REDIS_LRU_CLOCK_RESOLUTION;
    }
}

/* This is an helper function for the DEBUG command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(redisClient *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetEntryVal(de);
}

robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <verb> ... arguments ... */
void objectCommand(redisClient *c) {
    robj *o;

    if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o));
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}


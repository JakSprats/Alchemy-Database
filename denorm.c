/*
 *
 * This file implements "CREATE TABLE x AS redis_datastructure"
 *

MIT License

Copyright (c) 2010 Russell Sullivan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include "redis.h"
#include "index.h"
#include "store.h"
#include "denorm.h"

// FROM redis.c
#define RL4 redisLog(4,
extern struct sharedObjectsStruct shared;

static robj *_createStringObject(char *s) {
    return createStringObject(s, strlen(s));
}

static bool addSingle(redisClient *c,
                      redisClient *fc,
                      robj        *key,
                      long        *inserted) {
    robj *vals  = createObject(REDIS_STRING, NULL);
    vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
        sdscatprintf(sdsempty(), "%ld,%s",  *inserted, (char *)key->ptr):
        sdscatprintf(sdsempty(), "%ld,%ld", *inserted, (long)  key->ptr);
    fc->argv[2] = vals;
    //RL4 "SGL: INSERTING [1]: %s [2]: %s", fc->argv[1]->ptr, fc->argv[2]->ptr);
    legacyInsertCommand(fc);
    decrRefCount(vals);
    if (!respOk(fc)) { /* insert error */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    *inserted = *inserted + 1;
    return 1;
}

static bool addDouble(redisClient *c,
                     redisClient *fc,
                     robj        *key,
                     robj        *val,
                     long        *inserted,
                     bool         val_is_dbl) {
    robj *vals  = createObject(REDIS_STRING, NULL);
    if (val_is_dbl) {
        double d = *((double *)val);
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%f", 
                          *inserted, (char *)key->ptr, d) :
            sdscatprintf(sdsempty(), "%ld,%ld,%f",
                          *inserted, (long)  key->ptr, d);
    } else if (val->encoding == REDIS_ENCODING_RAW) {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%s", 
                          *inserted, (char *)key->ptr, (char *)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%s",
                          *inserted, (long)  key->ptr, (char *)val->ptr);
    } else {
        vals->ptr   = (key->encoding == REDIS_ENCODING_RAW) ?
            sdscatprintf(sdsempty(), "%ld,%s,%ld", 
                          *inserted, (char *)key->ptr, (long)val->ptr) :
            sdscatprintf(sdsempty(), "%ld,%ld,%ld",
                          *inserted, (long)  key->ptr, (long)val->ptr);
    }
    fc->argv[2] = vals;
    //RL4 "DBL: INSERTING [1]: %s [2]: %s", fc->argv[1]->ptr, fc->argv[2]->ptr);
    legacyInsertCommand(fc);
    decrRefCount(vals);
    if (!respOk(fc)) { /* insert error */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return 0;
    }
    *inserted = *inserted + 1;
    return 1;
}

void createTableAsObject(redisClient *c) {
    robj *key = c->argv[4];
    robj *o   = lookupKeyReadOrReply(c, key, shared.nullbulk);
    robj *cdef;
    bool single;
    if (o->type == REDIS_LIST) {
        cdef = _createStringObject("pk=INT,lvalue=TEXT");
        single = 1;
    } else if (o->type == REDIS_SET) {
        cdef = _createStringObject("pk=INT,svalue=TEXT");
        single = 1;
    } else if (o->type == REDIS_ZSET) {
        cdef = _createStringObject("pk=INT,zkey=TEXT,zvalue=TEXT");
        single = 0;
    } else if (o->type == REDIS_HASH) {
        cdef = _createStringObject("pk=INT,hkey=TEXT,hvalue=TEXT");
        single = 0;
    } else {
        addReply(c, shared.createtable_as_on_wrong_type);
        return;
    }

    robj               *argv[3];
    struct redisClient *fc = createFakeClient();
    fc->argv               = argv;

    fc->argv[1]    = c->argv[2];
    fc->argv[2]    = cdef;
    fc->argc   = 3;

    legacyTableCommand(fc);
    if (!respOk(fc)) { /* most likely table already exists */
        listNode *ln = listFirst(fc->reply);
        addReply(c, ln->value);
        return;
    }

    long inserted = 1; /* ZER0 as pk is sometimes bad */
    if (o->type == REDIS_LIST) {
        list     *list = o->ptr;
        listNode *ln   = list->head;
        while (ln) {
            robj *key = listNodeValue(ln);
            if (!addSingle(c, fc, key, &inserted)) return;
            ln = ln->next;
        }
    } else if (o->type == REDIS_SET) {
        dictEntry    *de;
        dict         *set = o->ptr;
        dictIterator *di  = dictGetIterator(set);
        while ((de = dictNext(di)) != NULL) {   
            robj *key  = dictGetEntryKey(de);
            if (!addSingle(c, fc, key, &inserted)) return;
        }
        dictReleaseIterator(di);
    } else if (o->type == REDIS_ZSET) {
        dictEntry    *de;
        zset         *zs  = o->ptr;
        dictIterator *di  = dictGetIterator(zs->dict);
        while ((de = dictNext(di)) != NULL) {   
            robj *key = dictGetEntryKey(de);
            robj *val = dictGetEntryVal(de);
            if (!addDouble(c, fc, key, val, &inserted, 1)) return;
        }
        dictReleaseIterator(di);
    } else {
        hashIterator *hi = hashInitIterator(o);
        while (hashNext(hi) != REDIS_ERR) {
            robj *key = hashCurrent(hi, REDIS_HASH_KEY);
            robj *val = hashCurrent(hi, REDIS_HASH_VALUE);
            if (!addDouble(c, fc, key, val, &inserted, 0)) return;
        }
        hashReleaseIterator(hi);
    }

    addReply(c, shared.ok);
    return;
}

#ifdef ALCHEMY_DATABASE
  #include "xdb_hooks.h"
#endif
#include "redis.h"
#include <sys/uio.h>

void *dupClientReplyValue(void *o) {
    incrRefCount((robj*)o);
    return o;
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

redisClient *createClient(int fd) {
    redisClient *c = zmalloc(sizeof(redisClient));
    c->bufpos = 0;

    if (fd != -1) {
    anetNonBlock(NULL,fd);
    anetTcpNoDelay(NULL,fd);
    if (aeCreateFileEvent(server.el,fd,AE_READABLE,
        readQueryFromClient, c) == AE_ERR)
    {
        close(fd);
        zfree(c);
        return NULL;
    }
    }

    selectDb(c,0);
    c->fd = fd;
    c->querybuf = sdsempty();
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->lastinteraction = time(NULL);
    c->authenticated = 0;
    c->replstate = REDIS_REPL_NONE;
    c->reply = listCreate();
    listSetFreeMethod(c->reply,decrRefCount);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->bpop.keys = NULL;
    c->bpop.count = 0;
    c->bpop.timeout = 0;
    c->bpop.target = NULL;
    c->io_keys = listCreate();
    c->watched_keys = listCreate();
    listSetFreeMethod(c->io_keys,decrRefCount);
    c->pubsub_channels = dictCreate(&setDictType,NULL);
    c->pubsub_patterns = listCreate();
    listSetFreeMethod(c->pubsub_patterns,decrRefCount);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    listAddNodeTail(server.clients,c);
    initClientMultiState(c);
    return c;
}

/* Set the event loop to listen for write events on the client's socket.
 * Typically gets called every time a reply is built. */
int _installWriteEvent(redisClient *c) {
    if (c->flags & REDIS_LUA_CLIENT) return REDIS_OK;
    if (c->fd <= 0) return REDIS_ERR;
    if (c->bufpos == 0 && listLength(c->reply) == 0 &&
        (c->replstate == REDIS_REPL_NONE ||
         c->replstate == REDIS_REPL_ONLINE) &&
        aeCreateFileEvent(server.el, c->fd, AE_WRITABLE,
        sendReplyToClient, c) == AE_ERR) return REDIS_ERR;
    return REDIS_OK;
}

/* Create a duplicate of the last object in the reply list when
 * it is not exclusively owned by the reply list. */
robj *dupLastObjectIfNeeded(list *reply) {
    robj *new, *cur;
    listNode *ln;
    redisAssert(listLength(reply) > 0);
    ln = listLast(reply);
    cur = listNodeValue(ln);
    if (cur->refcount > 1) {
        new = dupStringObject(cur);
        decrRefCount(cur);
        listNodeValue(ln) = new;
    }
    return listNodeValue(ln);
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

int _addReplyToBuffer(redisClient *c, char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return REDIS_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return REDIS_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return REDIS_ERR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return REDIS_OK;
}

void _addReplyObjectToList(redisClient *c, robj *o) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        incrRefCount(o);
        listAddNodeTail(c->reply,o);
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(o->ptr) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,o->ptr,sdslen(o->ptr));
        } else {
            incrRefCount(o);
            listAddNodeTail(c->reply,o);
        }
    }
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _addReplySdsToList(redisClient *c, sds s) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+sdslen(s) <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,sdslen(s));
            sdsfree(s);
        } else {
            listAddNodeTail(c->reply,createObject(REDIS_STRING,s));
        }
    }
}

void _addReplyStringToList(redisClient *c, char *s, size_t len) {
    robj *tail;

    if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

    if (listLength(c->reply) == 0) {
        listAddNodeTail(c->reply,createStringObject(s,len));
    } else {
        tail = listNodeValue(listLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            sdslen(tail->ptr)+len <= REDIS_REPLY_CHUNK_BYTES)
        {
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,len);
        } else {
            listAddNodeTail(c->reply,createStringObject(s,len));
        }
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(redisClient *c, robj *obj) {
    if (_installWriteEvent(c) != REDIS_OK) return;

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (obj->encoding == REDIS_ENCODING_RAW) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
    } else {
        /* FIXME: convert the long into string and use _addReplyToBuffer()
         * instead of calling getDecodedObject. As this place in the
         * code is too performance critical. */
        obj = getDecodedObject(obj);
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != REDIS_OK)
            _addReplyObjectToList(c,obj);
        decrRefCount(obj);
    }
}

void addReplySds(redisClient *c, sds s) {
    if (_installWriteEvent(c) != REDIS_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == REDIS_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

void addReplyString(redisClient *c, char *s, size_t len) {
    if (_installWriteEvent(c) != REDIS_OK) return;
    if (_addReplyToBuffer(c,s,len) != REDIS_OK)
        _addReplyStringToList(c,s,len);
}

void _addReplyError(redisClient *c, char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(redisClient *c, char *err) {
    _addReplyError(c,err,strlen(err));
}

void addReplyErrorFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyError(c,s,sdslen(s));
    sdsfree(s);
}

void _addReplyStatus(redisClient *c, char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(redisClient *c, char *status) {
    _addReplyStatus(c,status,strlen(status));
}

void addReplyStatusFormat(redisClient *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    _addReplyStatus(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addDeferredMultiBulkLength(redisClient *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (_installWriteEvent(c) != REDIS_OK) return NULL;
    listAddNodeTail(c->reply,createObject(REDIS_STRING,NULL));
    return listLast(c->reply);
}

/* Populate the length object and try glueing it to the next chunk. */
void setDeferredMultiBulkLength(redisClient *c, void *node, long length) {
    listNode *ln = (listNode*)node;
    robj *len, *next;

    /* Abort when *node is NULL (see addDeferredMultiBulkLength). */
    if (node == NULL) return;

    len = listNodeValue(ln);
    len->ptr = sdscatprintf(sdsempty(),"*%ld\r\n",length);
    if (ln->next != NULL) {
        next = listNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr != NULL) {
            len->ptr = sdscatlen(len->ptr,next->ptr,sdslen(next->ptr));
            listDelNode(c->reply,ln->next);
        }
    }
}

/* Add a duble as a bulk reply */
void addReplyDouble(redisClient *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
    addReplyString(c,sbuf,slen);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void _addReplyLongLong(redisClient *c, long long ll, char prefix) {
    char buf[128];
    int len;
    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong(redisClient *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        _addReplyLongLong(c,ll,':');
}

void addReplyMultiBulkLen(redisClient *c, long length) {
    _addReplyLongLong(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(redisClient *c, robj *obj) {
    size_t len;

    if (obj->encoding == REDIS_ENCODING_RAW) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }
    _addReplyLongLong(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(redisClient *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(redisClient *c, void *p, size_t len) {
    _addReplyLongLong(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(redisClient *c, char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(redisClient *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

static void acceptCommonHandler(int fd) {
    redisClient *c;
    if ((c = createClient(fd)) == NULL) {
        redisLog(REDIS_WARNING,"Error allocating resoures for the client");
        close(fd); /* May be already closed, just ingore errors */
        return;
    }
#ifdef ALCHEMY_DATABASE
    DXDB_setClientSA(c);
#endif
    /* If maxclient directive is set and this is one client more... close the
     * connection. Note that we create the client instead to check before
     * for this condition, since now the socket is already set in nonblocking
     * mode and we can send an error for free using the Kernel I/O */
    if (server.maxclients && listLength(server.clients) > server.maxclients) {
        char *err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors */
        if (write(c->fd,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        freeClient(c);
        return;
    }
    server.stat_numconnections++;
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    char cip[128];
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetTcpAccept(server.neterr, fd, cip, &cport);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted %s:%d", cip, cport);
    acceptCommonHandler(cfd);
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    REDIS_NOTUSED(privdata);

    cfd = anetUnixAccept(server.neterr, fd);
    if (cfd == AE_ERR) {
        redisLog(REDIS_WARNING,"Accepting client connection: %s", server.neterr);
        return;
    }
    redisLog(REDIS_VERBOSE,"Accepted connection to %s", server.unixsocket);
    acceptCommonHandler(cfd);
}


static void freeClientArgv(redisClient *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
}

void freeClient(redisClient *c) {
    listNode *ln;

    /* Note that if the client we are freeing is blocked into a blocking
     * call, we have to set querybuf to NULL *before* to call
     * unblockClientWaitingData() to avoid processInputBuffer() will get
     * called. Also it is important to remove the file events after
     * this, because this call adds the READABLE event. */
    sdsfree(c->querybuf);
    c->querybuf = NULL;
    if (c->flags & REDIS_BLOCKED)
        unblockClientWaitingData(c);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);
    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);
    /* Obvious cleanup */
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    listRelease(c->reply);
    freeClientArgv(c);
    close(c->fd);
    /* Remove from the list of clients */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);
    /* When client was just unblocked because of a blocking operation,
     * remove it from the list with unblocked clients. */
    if (c->flags & REDIS_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients,c);
        redisAssert(ln != NULL);
        listDelNode(server.unblocked_clients,ln);
    }
    /* Remove from the list of clients waiting for swapped keys, or ready
     * to be restarted, but not yet woken up again. */
    if (c->flags & REDIS_IO_WAIT) {
        redisAssert(server.ds_enabled);
        if (listLength(c->io_keys) == 0) {
            ln = listSearchKey(server.io_ready_clients,c);

            /* When this client is waiting to be woken up (REDIS_IO_WAIT),
             * it should be present in the list io_ready_clients */
            redisAssert(ln != NULL);
            listDelNode(server.io_ready_clients,ln);
        } else {
            while (listLength(c->io_keys)) {
                ln = listFirst(c->io_keys);
                dontWaitForSwappedKey(c,ln->value);
            }
        }
        server.cache_blocked_clients--;
    }
    listRelease(c->io_keys);
    /* Master/slave cleanup.
     * Case 1: we lost the connection with a slave. */
    if (c->flags & REDIS_SLAVE) {
        if (c->replstate == REDIS_REPL_SEND_BULK && c->repldbfd != -1)
            close(c->repldbfd);
        list *l = (c->flags & REDIS_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l,c);
        redisAssert(ln != NULL);
        listDelNode(l,ln);
    }

    /* Case 2: we lost the connection with the master. */
    if (c->flags & REDIS_MASTER) {
        server.master = NULL;
        server.replstate = REDIS_REPL_CONNECT;
        server.repl_down_since = time(NULL);
        /* Since we lost the connection with the master, we should also
         * close the connection with all our slaves if we have any, so
         * when we'll resync with the master the other slaves will sync again
         * with us as well. Note that also when the slave is not connected
         * to the master it will keep refusing connections by other slaves.
         *
         * We do this only if server.masterhost != NULL. If it is NULL this
         * means the user called SLAVEOF NO ONE and we are freeing our
         * link with the master, so no need to close link with slaves. */
        if (server.masterhost != NULL) {
            while (listLength(server.slaves)) {
                ln = listFirst(server.slaves);
                freeClient((redisClient*)ln->value);
            }
        }
    }
    /* Release memory */
    zfree(c->argv);
    freeClientMultiState(c);
    zfree(c);
}

void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = privdata;
    int nwritten = 0, totwritten = 0, objlen;
    robj *o;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    while(c->bufpos > 0 || listLength(c->reply)) {
        if (c->bufpos > 0) {
            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = c->bufpos - c->sentlen;
            } else {
                nwritten = write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if (c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = listNodeValue(listFirst(c->reply));
            objlen = sdslen(o->ptr);

            if (objlen == 0) {
                listDelNode(c->reply,listFirst(c->reply));
                continue;
            }

            if (c->flags & REDIS_MASTER) {
                /* Don't reply to a master */
                nwritten = objlen - c->sentlen;
            } else {
                nwritten = write(fd, ((char*)o->ptr)+c->sentlen,objlen-c->sentlen);
                if (nwritten <= 0) break;
            }
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                listDelNode(c->reply,listFirst(c->reply));
                c->sentlen = 0;
            }
        }
        /* Note that we avoid to send more thank REDIS_MAX_WRITE_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interfae) */
        if (totwritten > REDIS_MAX_WRITE_PER_EVENT) break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            redisLog(REDIS_VERBOSE,
                "Error writing to client: %s", strerror(errno));
            freeClient(c);
            return;
        }
    }
    if (totwritten > 0) c->lastinteraction = time(NULL);
    if (listLength(c->reply) == 0) {
        c->sentlen = 0;
        aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) freeClient(c);
    }
}

/* resetClient prepare the client to process the next command */
void resetClient(redisClient *c) {
    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;
}

void closeTimedoutClients(void) {
    redisClient *c;
    listNode *ln;
    time_t now = time(NULL);
    listIter li;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        if (server.maxidletime &&
            !(c->flags & REDIS_SLAVE) &&    /* no timeout for slaves */
            !(c->flags & REDIS_MASTER) &&   /* no timeout for masters */
            !(c->flags & REDIS_BLOCKED) &&  /* no timeout for BLPOP */
            dictSize(c->pubsub_channels) == 0 && /* no timeout for pubsub */
            listLength(c->pubsub_patterns) == 0 &&
            (now - c->lastinteraction > server.maxidletime))
        {
            redisLog(REDIS_VERBOSE,"Closing idle client");
            freeClient(c);
        } else if (c->flags & REDIS_BLOCKED) {
            if (c->bpop.timeout != 0 && c->bpop.timeout < now) {
                addReply(c,shared.nullmultibulk);
                unblockClientWaitingData(c);
            }
        }
    }
}

int processInlineBuffer(redisClient *c) {
    char *newline = strstr(c->querybuf,"\r\n");
    int argc, j;
    sds *argv;
    size_t querylen;

    /* Nothing to do without a \r\n */
    if (newline == NULL)
        return REDIS_ERR;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    argv = sdssplitlen(c->querybuf,querylen," ",1,&argc);

    /* Leave data after the first line of the query in the buffer */
    c->querybuf = sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (c->argv) zfree(c->argv);
    c->argv = zmalloc(sizeof(robj*)*argc);

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->argv[c->argc] = createObject(REDIS_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return REDIS_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
static void setProtocolError(redisClient *c, int pos) {
    c->flags |= REDIS_CLOSE_AFTER_REPLY;
    c->querybuf = sdsrange(c->querybuf,pos,-1);
}

int processMultibulkBuffer(redisClient *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        redisAssert(c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL)
            return REDIS_ERR;

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return REDIS_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        redisAssert(c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError(c,pos);
            return REDIS_ERR;
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            c->querybuf = sdsrange(c->querybuf,pos,-1);
            return REDIS_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
    }

    redisAssert(c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL)
                break;

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError(c,pos);
                return REDIS_ERR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError(c,pos);
                return REDIS_ERR;
            }

            pos += newline-(c->querybuf+pos)+2;
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            c->argv[c->argc++] = createStringObject(c->querybuf+pos,c->bulklen);
            pos += c->bulklen+2;
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    c->querybuf = sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) {
        return REDIS_OK;
    }
    return REDIS_ERR;
}

void processInputBuffer(redisClient *c) {
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & REDIS_BLOCKED || c->flags & REDIS_IO_WAIT) return;

        /* REDIS_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->flags & REDIS_CLOSE_AFTER_REPLY) return;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = REDIS_REQ_MULTIBULK;
            } else {
                c->reqtype = REDIS_REQ_INLINE;
            }
        }

        if (c->reqtype == REDIS_REQ_INLINE) {
            if (processInlineBuffer(c) != REDIS_OK) break;
        } else if (c->reqtype == REDIS_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != REDIS_OK) break;
        } else {
            redisPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
#ifdef ALCHEMY_DATABASE //NOTE: used as HTTP Request End-Delimiter
            DXDB_processInputBuffer_ZeroArgs(c);
#endif
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == REDIS_OK)
                resetClient(c);
        }
    }
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *c = (redisClient*) privdata;
    char buf[REDIS_IOBUF_LEN];
    int nread;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);

    nread = read(fd, buf, REDIS_IOBUF_LEN);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            redisLog(REDIS_VERBOSE, "Reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        redisLog(REDIS_VERBOSE, "Client closed connection");
        freeClient(c);
        return;
    }
    if (nread) {
        c->querybuf = sdscatlen(c->querybuf,buf,nread);
        c->lastinteraction = time(NULL);
    } else {
        return;
    }
    processInputBuffer(c);
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    redisClient *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

void clientCommand(redisClient *c) {
    listNode *ln;
    listIter li;
    redisClient *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        sds o = sdsempty();
        time_t now = time(NULL);

        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            char ip[32], flags[16], *p;
            int port;

            client = listNodeValue(ln);
            if (anetPeerToString(client->fd,ip,&port) == -1) continue;
            p = flags;
            if (client->flags & REDIS_SLAVE) {
                if (client->flags & REDIS_MONITOR)
                    *p++ = 'O';
                else
                    *p++ = 'S';
            }
            if (client->flags & REDIS_MASTER) *p++ = 'M';
            if (p == flags) *p++ = 'N';
            if (client->flags & REDIS_MULTI) *p++ = 'x';
            if (client->flags & REDIS_BLOCKED) *p++ = 'b';
            if (client->flags & REDIS_IO_WAIT) *p++ = 'i';
            if (client->flags & REDIS_DIRTY_CAS) *p++ = 'd';
            if (client->flags & REDIS_CLOSE_AFTER_REPLY) *p++ = 'c';
            if (client->flags & REDIS_UNBLOCKED) *p++ = 'u';
            *p++ = '\0';
            o = sdscatprintf(o,
                "addr=%s:%d fd=%d idle=%ld flags=%s db=%d sub=%d psub=%d\n",
                ip,port,client->fd,
                (long)(now - client->lastinteraction),
                flags,
                client->db->id,
                (int) dictSize(client->pubsub_channels),
                (int) listLength(client->pubsub_patterns));
        }
        addReplyBulkCBuffer(c,o,sdslen(o));
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr,"kill") && c->argc == 3) {
        listRewind(server.clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            char ip[32], addr[64];
            int port;

            client = listNodeValue(ln);
            if (anetPeerToString(client->fd,ip,&port) == -1) continue;
            snprintf(addr,sizeof(addr),"%s:%d",ip,port);
            if (strcmp(addr,c->argv[2]->ptr) == 0) {
                addReply(c,shared.ok);
                if (c == client) {
                    client->flags |= REDIS_CLOSE_AFTER_REPLY;
                } else {
                    freeClient(client);
                }
                return;
            }
        }
        addReplyError(c,"No such client");
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL ip:port)");
    }
}

void rewriteClientCommandVector(redisClient *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;
        
        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    /* We free the objects in the original vector at the end, so we are
     * sure that if the same objects are reused in the new vector the
     * refcount gets incremented before it gets decremented. */
    for (j = 0; j < c->argc; j++) decrRefCount(c->argv[j]);
    zfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    va_end(ap);
}

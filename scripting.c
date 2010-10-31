
#ifdef NOT_BEING_USED

/* single variable substitution in STORED PROCEDURE */
typedef struct vsub {
    int ncmd;  /* which command - PROCEDURE's have many */
    int varn;  /* which variable is to be substituted */
    int argc;  /* which arg of this command needs substituting */
    int ofst;  /* where in this argc needs substituting */
               /* NOTE: ofst is relative to the post "compiled" string */
} vsub_t;

typedef struct procStateControl {
    multiState  mstate;   /* holds commands */
    int         nvar;     /* procedure called w/ explicit numvars */
    sds        *vname;    /* variable names */
    list       *vsub;     /* list[varnum,argc,offset] */
} procStateControl;

typedef struct procState {
    procStateControl  p;
    robj             *name;
    sds              *vpatt;    /* variable patterns [e.g. $V ] */
    sds              *vpattext; /* variable patterns extended [e.g. $(V) ] */
} procState;

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
    procState  procstate;   /* BEGIN/END PROCDURE state */
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

static int rewriteNestedCmd(redisClient *c);
static int doCommand(redisClient *c);
/* If this function gets called we already read a whole
 * command, argments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * and other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroied (i.e. after QUIT). */
static int processCommand(redisClient *c) {
    /* Free some memory if needed (maxmemory setting) */
    if (server.maxmemory) freeMemoryIfNeeded();

    /* Handle the multi bulk command type. This is an alternative protocol
     * supported by Redis in order to receive commands that are composed of
     * multiple binary-safe "bulk" arguments. The latency of processing is
     * a bit higher but this allows things like multi-sets, so if this
     * protocol is used only for MSET and similar commands this is a big win. */
    if (c->multibulk == 0 && c->argc == 1 &&
        ((char*)(c->argv[0]->ptr))[0] == '*') {
        c->multibulk = atoi(((char*)c->argv[0]->ptr)+1);
        if (c->multibulk <= 0) {
            resetClient(c);
            return 1;
        } else {
            decrRefCount(c->argv[c->argc-1]);
            c->argc--;
            return 1;
        }
    } else if (c->multibulk) {
        if (c->bulklen == -1) {
            if (((char*)c->argv[0]->ptr)[0] != '$') {
                addReplySds(c,sdsnew("-ERR multi bulk protocol error\r\n"));
                resetClient(c);
                return 1;
            } else {
                int bulklen = atoi(((char*)c->argv[0]->ptr)+1);
                decrRefCount(c->argv[0]);
                if (bulklen < 0 || bulklen > 1024*1024*1024) {
                    c->argc--;
                    addReplySds(c,sdsnew("-ERR invalid bulk write count\r\n"));
                    resetClient(c);
                    return 1;
                }
                c->argc--;
                c->bulklen = bulklen+2; /* add two bytes for CR+LF */
                return 1;
            }
        } else {
            c->mbargv = zrealloc(c->mbargv,(sizeof(robj*))*(c->mbargc+1));
            c->mbargv[c->mbargc] = c->argv[0];
            c->mbargc++;
            c->argc--;
            c->multibulk--;
            if (c->multibulk == 0) {
                robj **auxargv;
                int auxargc;

                /* Here we need to swap the multi-bulk argc/argv with the
                 * normal argc/argv of the client structure. */
                auxargv = c->argv;
                c->argv = c->mbargv;
                c->mbargv = auxargv;

                auxargc = c->argc;
                c->argc = c->mbargc;
                c->mbargc = auxargc;

                /* We need to set bulklen to something different than -1
                 * in order for the code below to process the command without
                 * to try to read the last argument of a bulk command as
                 * a special argument. */
                c->bulklen = 0;
                /* continue below and process the command */
            } else {
                c->bulklen = -1;
                return 1;
            }
        }
    }
    /* -- end of multi bulk commands processing -- */

    /* The QUIT command is handled as a special case. Normal command
     * procs are unable to close the client connection safely */
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        freeClient(c);
        return 0;
    }

    if (rewriteNestedCmd(c)) return 1;
    return doCommand(c);
}

sds nest_bfor = NULL;
sds nest_rest = NULL;

static void subNestCommand(int j, robj *key, robj **nargv) {
    if (nest_bfor) {
        sds new  = sdsdup(nest_bfor);
        new      = sdscatlen(new, key->ptr, sdslen(key->ptr));
        nargv[j] = createStringObject(new, sdslen(new));
        sdsfree(new);
    } else {
        nargv[j] = cloneRobj(key);
    }
}

static void appendNestTrailing(int j, robj **nargv) {
    if (nest_rest) {
        int k = j - 1;
        nargv[k]->ptr = sdscatlen(nargv[k]->ptr, nest_rest, sdslen(nest_rest));
    }
}

static bool emptyCmdEval(redisClient *c,
                         void        *x,
                         robj        *key,
                         long        *nlines,
                         int          b) {
    key = NULL; nlines = NULL; /* compiler warnings */
    int argn  = (int)(long)x;
    int arity = b;
    int nargc = c->argc - arity + 1;

    sds nesta = NULL;
    if (nest_bfor) {
        nesta = sdsdup(nest_bfor);
    }
    if (nest_rest) {
        if (nesta) nesta = sdscatlen(nesta, nest_rest, sdslen(nest_rest));
        else       nesta = sdsdup(nest_rest);
    }
    robj *r  = nesta ? createStringObject(nesta, sdslen(nesta)) : NULL;
    //RL4 "emptyCmdEval: argn: %d nesta: %s nargc: %d", argn, nesta, nargc);

    robj **nargv    = zmalloc(sizeof(robj *) * nargc);
    bool   inserted = 0;
    int    j        = 0;
    for (int i = 0; i < c->argc; i++) {
        if (i >= argn && i < argn + arity) {
            if (!inserted) {
                inserted = 1;
                if (nesta) {
                    nargv[j] = r;
                    j++;
                }
            }
        } else {
            nargv[j] = cloneRobj(c->argv[i]);
            j++;
        }
    }

    if (nesta) sdsfree(nesta);

    //for (int i = 0; i < nargc; i++) RL4 "MT nargv[%d]: %s", i, nargv[i]->ptr);

    for (int k = 0; k < c->argc; k++) decrRefCount(c->argv[k]);
    zfree(c->argv);
    c->argc = nargc;
    c->argv = nargv;


    return 1;
}

static bool nestedCmdEval(redisClient *c,
                          void        *x,
                          robj        *key,
                          long        *nlines,
                          int          b) {
    int argn  = (int)(long)x;
    int arity = b;
    int nargc = c->argc - arity + 1;

    bool append = 0;
    if (*nlines > 1) { /* subsequent lines returned from command -> append */
        append = 1;
        nargc  = c->argc + 1;
    }

    robj **nargv    = zmalloc(sizeof(robj *) * nargc);
    bool   inserted = 0;
    bool   appended = 0;
    int    j        = 0;
    for (int i = 0; i < c->argc; i++) {
        bool replace = append ? 0 : (i >= argn && i < argn + arity);
        if (append && i == (argn + *nlines - 1)) { /* middle append */
            subNestCommand(j, key, nargv); /* first add new entry */
            j++;
            appendNestTrailing(j, nargv);
            nargv[j] = cloneRobj(c->argv[i]); /* then add post cmd entry */
            j++;
            appended = 1;
        } else if (replace) {
            if (!inserted) {
                subNestCommand(j, key, nargv);
                inserted = 1;
                j++;
            } else if (i == (argn + arity - 1)) { /* embedded nest */
                appendNestTrailing(j, nargv);
            }
        } else {
            nargv[j] = cloneRobj(c->argv[i]);
            j++;
        }
    }
    if (append && !appended) { /* append to end */
        subNestCommand(j, key, nargv);
        j++;
        appendNestTrailing(j, nargv);
    }
    *nlines = *nlines + 1; /* count num lines returned from cmd */

    /* cleanup old argv */
    for (int k = 0; k < c->argc; k++) decrRefCount(c->argv[k]);
    zfree(c->argv);

    //for (int i = 0; i < nargc; i++) RL4 "nargv[%d]: %s", i, nargv[i]->ptr);

    /* set new argv */
    c->argc = nargc;
    c->argv = nargv;
    return 1;
}

static int rewriteNestedCmd(redisClient *c) {
    sds ocmd = c->argv[0]->ptr;
    if (!strcasecmp(ocmd, "MSET") ||
        !strcasecmp(ocmd, "MSETNX") ||
        !strcasecmp(ocmd, "INSERT") || /* this may change */
        !strcasecmp(ocmd, "UPDATE") || /* this may change */
        !strcasecmp(ocmd, "HMSET")) return 0; /* nesting not supported */

    int ret   = 0;
    while (1) {
        nest_bfor = NULL;
        nest_rest = NULL;
        bool nest = 0; /* check for nested commands, delimited by "$(" */
        for (int i = 1; i < (c->argc - 1); i++) { /* last argv cant be nested */
            char *x = strchr(c->argv[i]->ptr, '$');
            if (x && *(x + 1) == '(') {
                nest = 1;
                break;
            }
        }
        if (nest) {
            struct redisClient *rfc = rsql_createFakeClient();
            for (int i = c->argc - 2; i != 0; i--) { /* start from end of cmd */
                sds   orig = c->argv[i]->ptr;
                char *x    = strchr(orig, '$');
                if (x && *(x + 1) == '(') {
                    if (x != orig) nest_bfor  = sdsnewlen(orig, x - orig);
                    //RL4 "nest_bfor: %s", nest_bfor);
                    x += 2; /* skip "$(" */
                    struct redisCommand *cmd = lookupCommand(x);
                    if (!cmd) {
                        addReplySds(c, sdscatprintf(sdsempty(),
                                           "-ERR nested cmd: '%s'\r\n", x));
                        resetClient(c);
                        rsql_freeFakeClient(rfc);
                        ret = 1;
                        goto rewrite_nest_err;
                    }
                    int arity = cmd->arity;
                    if (arity < 0) {
                        arity = 1;
                        for (int k = i + 1; k < c->argc; k++) {
                            arity++;
                            sds s = c->argv[k]->ptr;
                            if (strchr(s, ')')) break;
                            //if (s[sdslen(s) - 1] == ')') break;
                        }
                    }
                    //RL4 "cmd: %s arity: %d", cmd->name, arity);
                    robj **cargv = zmalloc(sizeof(robj *) * arity);
                    rfc->argv    = cargv;
                    rfc->argv[0] = createStringObject(x, strlen(x));
                    for (int j = 1; j < arity; j++) {
                        if (j == (arity - 1)) {
                            sds   s      = c->argv[j + i]->ptr;
                            char *t      = strchr(s, ')');
                            int   len    = t ? t - s : (int)sdslen(s);
                            if (t && (t -s) != ((int)sdslen(s) - 1)) {
                                t++;
                                while (*t == ')') t++;
                                if (*t) nest_rest = sdsnewlen(t, strlen(t));
                                //RL4 "nest_rest: %s", nest_rest);
                            }
                            rfc->argv[j] = createStringObject(s, len);
                        } else {
                            rfc->argv[j] = cloneRobj(c->argv[j + i]);
                        }
                    }
                    //for (int j = 0; j < arity; j++)
                         //RL4 "rfc->argv[%d]: %s", j, rfc->argv[j]->ptr);
                    rfc->argc = arity;
                    rfc->db   = c->db;
                    void *v   = (void *)(long)i;
                    // TODO if this EVALs to empty set do something
                    fakeClientPipe(c, rfc, v, arity, 
                                   nestedCmdEval, emptyCmdEval);
                    rsql_resetFakeClient(rfc);
                    for (int k = 0; k < arity; k++) decrRefCount(cargv[k]);
                    zfree(cargv);
                    if (nest_bfor) sdsfree(nest_bfor);
                    if (nest_rest) sdsfree(nest_rest);
                    break;
                }
            }
            rsql_freeFakeClient(rfc);
        } else {
            break;
        }
    }

rewrite_nest_err:
    if (nest_bfor) sdsfree(nest_bfor);
    if (nest_rest) sdsfree(nest_rest);
    return ret;
}

static int doCommand(redisClient *c) {
    /* Now lookup the command and check ASAP about trivial error conditions
     * such wrong arity, bad command name and so forth. */
    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    if (!cmd) {
        addReplySds(c,
            sdscatprintf(sdsempty(), "-ERR unknown command '%s'\r\n",
                (char*)c->argv[0]->ptr));
        resetClient(c);
        return 1;
    } else if ((cmd->arity > 0 && cmd->arity != c->argc) ||
               (c->argc < -cmd->arity)) {
        addReplySds(c,
            sdscatprintf(sdsempty(),
                "-ERR wrong number of arguments for '%s' command\r\n",
                cmd->name));
        resetClient(c);
        return 1;
    } else if (cmd->flags & REDIS_CMD_BULK && c->bulklen == -1) {
        /* This is a bulk command, we have to read the last argument yet. */
        int bulklen = atoi(c->argv[c->argc-1]->ptr);

        decrRefCount(c->argv[c->argc-1]);
        if (bulklen < 0 || bulklen > 1024*1024*1024) {
            c->argc--;
            addReplySds(c,sdsnew("-ERR invalid bulk write count\r\n"));
            resetClient(c);
            return 1;
        }
        c->argc--;
        c->bulklen = bulklen+2; /* add two bytes for CR+LF */
        /* It is possible that the bulk read is already in the
         * buffer. Check this condition and handle it accordingly.
         * This is just a fast path, alternative to call processInputBuffer().
         * It's a good idea since the code is small and this condition
         * happens most of the times. */
        if ((signed)sdslen(c->querybuf) >= c->bulklen) {
            c->argv[c->argc] = createStringObject(c->querybuf,c->bulklen-2);
            c->argc++;
            c->querybuf = sdsrange(c->querybuf,c->bulklen,-1);
        } else {
            /* Otherwise return... there is to read the last argument
             * from the socket. */
            return 1;
        }
    }
    /* Let's try to encode the bulk object to save space. */
    if (cmd->flags & REDIS_CMD_BULK)
        c->argv[c->argc-1] = tryObjectEncoding(c->argv[c->argc-1]);

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && cmd->proc != authCommand) {
        addReplySds(c,sdsnew("-ERR operation not permitted\r\n"));
        resetClient(c);
        return 1;
    }

    /* Handle the maxmemory directive */
    if (server.maxmemory && (cmd->flags & REDIS_CMD_DENYOOM) &&
        zmalloc_used_memory() > server.maxmemory) {
        addReplySds(c,sdsnew("-ERR command not allowed when used memory > 'maxmemory'\r\n"));
        resetClient(c);
        return 1;
    }

    /* Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub */
    if ((dictSize(c->pubsub_channels) > 0 || listLength(c->pubsub_patterns) > 0)
        &&
        cmd->proc != subscribeCommand && cmd->proc != unsubscribeCommand &&
        cmd->proc != psubscribeCommand && cmd->proc != punsubscribeCommand) {
        addReplySds(c,sdsnew("-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context\r\n"));
        resetClient(c);
        return 1;
    }

    /* Exec the command */
    if (c->flags   & REDIS_MULTI &&
        cmd->proc != execCommand &&
        cmd->proc != discardCommand) {
        queueMultiCommand(&c->mstate, c->argc, c->argv, cmd);
        addReply(c,shared.queued);



    } else {
        if (server.vm_enabled && server.vm_max_threads > 0 &&
            blockClientOnSwappedKeys(c,cmd)) return 1;
        call(c,cmd);
    }

    /* Prepare the client for the next command */
    resetClient(c);
    return 1;
}






/* ============================ BEGIN/END PROCEDURE ========================= */
        case REDIS_PROCEDURE: type = "+procedure"; break;

static void runCommand(redisClient *c);
static void callCommand(redisClient *c);
static void beginCommand(redisClient *c);
static void endCommand(redisClient *c);
static void showCommand(redisClient *c);
static void queueProcedureCommand(redisClient *c, struct redisCommand *cmd);

    {"run",          runCommand,            2,REDIS_CMD_INLINE,NULL,1,1,1,0},
    {"begin",        beginCommand,         -3,REDIS_CMD_INLINE,NULL,1,1,1,0},
    {"end",          endCommand,            1,REDIS_CMD_INLINE,NULL,1,1,1,0},
    {"call",         callCommand,          -2,REDIS_CMD_INLINE,NULL,1,1,1,0},
    {"show",         showCommand,          -2,REDIS_CMD_INLINE,NULL,1,1,1,0},

#define REDIS_BEGIN   64    /* This client is defining a PROCEDURE */
    shared.begin = createObject(REDIS_STRING,sdsnew("+BEGIN\r\n"));
    shared.end   = createObject(REDIS_STRING,sdsnew("+END\r\n"));
    shared.procedure_define = createObject(REDIS_STRING,sdsnew("+ADD_TO_PROCEDURE\r\n"));
    } else if (c->flags   & REDIS_BEGIN  &&
               cmd->proc != beginCommand && /* no nesting */
               cmd->proc != endCommand   &&
               cmd->proc != discardCommand) {
        queueProcedureCommand(c, cmd);
        addReply(c, shared.procedure_define);



static void runString(redisClient *c, sds s, redisClient *rfc) {
    int     argc;
    sds    *argv = sdssplitlen(s, sdslen(s), " ", 1, &argc);
    robj **rargv = zmalloc(sizeof(robj *) * argc);
    for (int j = 0; j < argc; j++) {
        rargv[j] = createObject(REDIS_STRING, argv[j]);
    }
    zfree(argv);
    rfc->argv        = rargv;
    rfc->argc        = argc;

    if (!rewriteNestedCmd(rfc)) {
        sds                  x   = rfc->argv[0]->ptr;
        struct redisCommand *cmd = lookupCommand(x);
        if (!cmd) {
            addReplySds(c, sdscatprintf(sdsempty(),
                                    "-ERR nested cmd: '%s'\r\n", x));
        } else {
            cmd->proc(rfc);
            listNode  *ln;
            listIter  *li = listGetIterator(rfc->reply, AL_START_HEAD);
            while((ln = listNext(li)) != NULL) {
                robj *nkey = ln->value;
                addReply(c, nkey);
            }
            listReleaseIterator(li);
        }
    }

    for (int j = 0; j < rfc->argc; j++) decrRefCount(rfc->argv[j]);
    zfree(rfc->argv);
}

static void runCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL)
        return;

    if (o->type == REDIS_STRING) {
        sds          s   = o->ptr;
        redisClient *rfc = rsql_createFakeClient(); /* client to read */
        runString(c, s, rfc);
        rsql_freeFakeClient(rfc);
    } else {
        addReply(c,shared.wrongtypeerr);
    }
}

static sds genCmdFromArgv(int argc, sds *argv) { /* sdsfree() ME */
    sds cmd = sdsempty();
    for (int k = 0; k < argc; k++) {
        if (k > 0) cmd = sdscatlen(cmd, " ", 1);
        cmd = sdscatlen(cmd, argv[k], sdslen(argv[k]));
    }
    return cmd;
}

static sds genCmdFromRArgv(int argc, robj **rargv) { /* sdsfree() ME */
    sds cmd = sdsempty();
    for (int k = 0; k < argc; k++) {
        if (k > 0) cmd = sdscatlen(cmd, " ", 1);
        cmd       = sdscatlen(cmd, rargv[k]->ptr, sdslen(rargv[k]->ptr));
    }
    return cmd;
}

static void laddNoModCmds(int *i, int end, procStateControl *psc, list *oput) {
    for (; *i < end; *i = *i + 1) {
        multiCmd *mc = &psc->mstate.commands[*i];
        listAddNodeTail(oput, genCmdFromRArgv(mc->argc, mc->argv));
    }
}

static void laddMargv(int               *i,
                      sds              **margv,
                      int               *added,
                      procStateControl  *psc,
                      list              *oput) {
    int  argc = psc->mstate.commands[*i].argc;
    sds *argv = *margv;
    listAddNodeTail(oput, genCmdFromArgv(argc, *margv));
    for (int k = 0; k < argc; k++) sdsfree(argv[k]); /* argv pointer arith */
    zfree(*margv);
    *margv = NULL;
    *added = 0;
    *i     = *i + 1;
}

static list *genProcOutput(sds *vname, procStateControl *psc) {
    list *oput = listCreate();

    if (!psc->vsub) {
        for (int j = 0; j < psc->mstate.count; j++) {
            multiCmd *mc = &psc->mstate.commands[j];
            listAddNodeTail(oput, genCmdFromRArgv(mc->argc, mc->argv));
        }
    } else {
        listNode *ln;
        sds      *margv = NULL;
        int       added = 0;
        int       i     = 0;
        listIter *li    = listGetIterator(psc->vsub, AL_START_HEAD);
        while((ln = listNext(li)) != NULL) {
            vsub_t *vs = ln->value;
            if (i < vs->ncmd) { /* vsub list different pos than command array */
                if (margv) {    /* modified argv from previous loop */
                    laddMargv(&i, &margv, &added, psc, oput);
                }
                laddNoModCmds(&i, vs->ncmd, psc, oput);
            }

            if (!margv) { /* start modification */
                int argc = psc->mstate.commands[i].argc;
                margv    = zmalloc(sizeof(sds) * argc);
                for (int k = 0; k < argc; k++) {
                    margv[k] = sdsdup(psc->mstate.commands[i].argv[k]->ptr);
                }
            }

            sds mcmd = NULL;
            sds sub  = vname[vs->varn];
            if (vs->ofst == -1) { /* entire argv gets replaced */
                mcmd = sdsdup(sub);
            } else {
                int  ofst  = vs->ofst + added;
                sds  cmd   = margv[vs->argc];
                int  clen  = (int)sdslen(cmd);
                bool after = (clen > ofst);

                if (ofst)  mcmd = sdsnewlen(cmd, ofst); /* before var sub */
                if (mcmd)  mcmd = sdscatlen(mcmd, sub, sdslen(sub)); /* sub */
                else       mcmd = sdsnewlen(sub, sdslen(sub));       /* sub */
                if (after) mcmd = sdscatlen(mcmd, cmd + ofst, clen - ofst);

                added += sdslen(sub);
            }

            sdsfree(margv[vs->argc]); /* free old */
            margv[vs->argc] = mcmd;   /* replace w/ new */
        }
        listReleaseIterator(li);

        if (margv) { /* final modified cmd not output */
            laddMargv(&i, &margv, &added, psc, oput);
        }
        laddNoModCmds(&i, psc->mstate.count, psc, oput);
    }
    return oput;
}

static bool prepProcedure(redisClient       *c,
                          procStateControl **psc,
                          sds              **vname) {
    robj *o = lookupKeyRead(c->db, c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return 0;
    }
    if (o->type != REDIS_PROCEDURE) {
        addReply(c,shared.wrongtypeerr);
        return 0;
    }

    *psc    = o->ptr;
    sds *vn = zmalloc(sizeof(sds) * (*psc)->nvar);
    *vname  = vn;
    bool mt = ((c->argc - 2) != (*psc)->nvar); /* no args - vname from decl */
    for (int i = 0; i < (*psc)->nvar; i++) {
        if (mt) vn[i] = sdscatprintf(sdsempty(), "${%s}", (*psc)->vname[i]);
        else      vn[i] = sdsdup(c->argv[2 + i]->ptr);
    }
    return 1;
}

static void showCommand(redisClient *c) {
    procStateControl *psc;
    sds              *vname;
    if (!prepProcedure(c, &psc, &vname)) return;

    list *oput = genProcOutput(vname, psc);

    addReplySds(c, sdscatprintf(sdsempty(), "*%d\r\n", psc->mstate.count));

    listNode *ln;
    listIter *li = listGetIterator(oput, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sds s = ln->value;
        robj *o = createObject(REDIS_STRING, s);
        addReplyBulk(c, o);
        decrRefCount(o); /* runs sdsfree */
    }
    listReleaseIterator(li);
    listRelease(oput);
    for (int i = 0; i < psc->nvar; i++) sdsfree(vname[i]);
    zfree(vname);
}

static void callCommand(redisClient *c) {
    procStateControl *psc;
    sds              *vname;
    if (!prepProcedure(c, &psc, &vname)) return;

    list *oput = genProcOutput(vname, psc);

    redisClient *rfc = rsql_createFakeClient(); /* client to read */

    listNode *ln;
    listIter *li = listGetIterator(oput, AL_START_HEAD);
    while((ln = listNext(li)) != NULL) {
        sds s = ln->value;
        runString(c, s, rfc);
        rsql_resetFakeClient(rfc);
    }
    listReleaseIterator(li);
    rsql_freeFakeClient(rfc);
    listRelease(oput);
    for (int i = 0; i < psc->nvar; i++) sdsfree(vname[i]);
    zfree(vname);
}

static void initClientProcedureState(procState *procstate) {
    procstate->name     = NULL;
    procstate->p.nvar   = 0;
    procstate->p.vname  = NULL;
    procstate->vpatt    = NULL;
    procstate->vpattext = NULL;
    procstate->p.vsub   = NULL;
    initClientMultiState(&procstate->p.mstate);
}

static void freeClientProcedureState(procState *procstate, bool free_pcs) {
    decrRefCount(procstate->name);
    for (int i = 0; i < procstate->p.nvar; i++) {
        sdsfree(procstate->vpatt[i]);
        sdsfree(procstate->vpattext[i]);
    }
    if (procstate->p.nvar) {
        zfree(procstate->vpatt);
        zfree(procstate->vpattext);
    }
    
    if (free_pcs) { /* free_pcs -> on successful "END", keep data */
        for (int i = 0; i < procstate->p.nvar; i++) {
            sdsfree(procstate->p.vname[i]);
        }
        if (procstate->p.nvar) zfree(procstate->p.vname);
        if (procstate->p.vsub) listRelease(procstate->p.vsub);
        freeClientMultiState(&procstate->p.mstate);
    }
}

static void saveClientProcedureState(redisClient *c) {
    procStateControl *psc = zmalloc(sizeof(procStateControl));
    memcpy(psc, &c->procstate.p, sizeof(procStateControl));
    robj *key             = cloneRobj(c->procstate.name);
    robj *val             = createObject(REDIS_PROCEDURE, psc);
    freeClientProcedureState(&c->procstate, 0); /* dont free procStateControl */
    dictAdd(c->db->dict, key, val);
}

static void beginCommand(redisClient *c) {
    if (c->flags & REDIS_BEGIN) {
        addReplySds(c, sdsnew("-ERR: BEGIN nesting prohibited\r\n"));
        return;
    }
    if (c->argc < 3 || strcasecmp(c->argv[1]->ptr, "PROCEDURE")) {
        addReplySds(c, sdsnew( "-ERR: SYNTAX BEGIN PROCEDURE name [var,]\r\n"));
        return;
    }
    robj *o = lookupKeyRead(c->db, c->argv[2]);
    if (o) {
        addReplySds(c, sdsnew( "-ERR: BEGIN name already taken\r\n"));
        return;
    }

    initClientProcedureState(&c->procstate);
    c->flags |= REDIS_BEGIN;

    c->procstate.name = cloneRobj(c->argv[2]);
    c->procstate.p.nvar = c->argc - 3;

    if (c->procstate.p.nvar) {
        c->procstate.p.vname  = zmalloc(sizeof(sds) * c->procstate.p.nvar);
        c->procstate.vpatt    = zmalloc(sizeof(sds) * c->procstate.p.nvar);
        c->procstate.vpattext = zmalloc(sizeof(sds) * c->procstate.p.nvar);
        int j = 0;
        for (int i = 3; i < c->argc; i++) {
            procState *ps   = &c->procstate;
            sds         s   = c->argv[i]->ptr;
            ps->p.vname[j]  = sdsdup(s);
            ps->vpatt[j]    = sdscatprintf(sdsempty(), "$%s", s);
            ps->vpattext[j] = sdscatprintf(sdsempty(), "${%s}", s);
            j++;
        }
    }
    addReply(c,shared.begin);
}

static void endCommand(redisClient *c) {
    if (!(c->flags & REDIS_BEGIN)) {
        addReplySds(c, sdsnew( "-ERR: END w/o BEGIN\r\n"));
        return;
    }
    c->flags &= (~REDIS_BEGIN);
    if (c->procstate.p.mstate.count == 0) {
        addReplySds(c, sdsnew( "-ERR: 0 length BEGIN END block not saved\r\n"));
        freeClientProcedureState(&c->procstate, 1);
        return;
    }
    saveClientProcedureState(c);
    addReply(c, shared.end);
    server.dirty++;
}

#define ADD_VS_TO_VSUB_LIST(ofst)                                 \
    if (!c->procstate.p.vsub) c->procstate.p.vsub = listCreate(); \
    vsub_t *vs = zmalloc(sizeof(vsub_t));                         \
    vs->ncmd   = c->procstate.p.mstate.count;                     \
    vs->varn   = k;                                               \
    vs->argc   = j;                                               \
    vs->ofst   = ofst;                                            \
    listAddNodeTail(c->procstate.p.vsub, vs); 
    //RL4 "hit: cmd: %d varn: %d argc: %d ofst: %d", vs->ncmd, vs->varn, vs->argc, vs->ofst);

static void queueProcedureCommand(redisClient *c, struct redisCommand *cmd) {
    for (int j = 0; j < c->argc; j++) {
        bool  matched = 0;
        char *x       = c->argv[j]->ptr;
        for (int k = 0; k < c->procstate.p.nvar; k++) { /* 1st search 4: $X */
            int hit = !strcmp(x, c->procstate.vpatt[k]);
            if (hit) {
                int ofst = -1; /* entire argument will be replaced */
                ADD_VS_TO_VSUB_LIST(ofst);
                matched = 1;
                break;
            }
        }
        if (!matched) {                                 /* 2nd search 4: $(X) */
            for (int k = 0; k < c->procstate.p.nvar; k++) {
                sds pt = c->procstate.vpattext[k]; 
                while (1) { /* need to match "${X}${X}" same var 2X in argv */
                    char *z  = c->argv[j]->ptr; /* can change below */
                    char *y  = strstr(z, pt);
                    if (y) { /* record position and remove from agv */
                        int lofst  = y - z;         /* local offset */
                        int ofst   = lofst;         /* expanded offset */
                        ADD_VS_TO_VSUB_LIST(ofst);
                        int plen   = sdslen(pt);
                        int zlen   = sdslen(z);
                        sds marg   = NULL;
                        if (ofst) marg = sdsnewlen(z, lofst); /* before var */
                        if (zlen != lofst + plen) {           /* after  var */
                            char *a        = y + plen;
                            int   len      = zlen - lofst - plen;
                            if (marg) marg = sdscatlen(marg, a, len);
                            else      marg = sdsnewlen(a, len);
                        }
                        sdsfree(c->argv[j]->ptr);
                        c->argv[j]->ptr = marg;
                    } else {
                        break;
                    }
                }
            }
        }
    }
    queueMultiCommand(&c->procstate.p.mstate, c->argc, c->argv, cmd);
}

#endif

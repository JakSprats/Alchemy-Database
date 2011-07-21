/*
 * This file implements ALCHEMY_DATABASE's advanced messaging hooks
 *

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of ALCHEMY_DATABASE

   This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */

#include <strings.h>

#include "hiredis.h"

#include "redis.h"
#include "dict.h"
#include "adlist.h"

#include "rpipe.h"

extern redisContext *context;
//PROTOTYPES
//from redis-cli.c
void setClientParams(sds hostip, int hostport);
int cliConnect(int force);
int cliSendCommand(int argc, char **argv, int repeat);

static void ignoreFCP(void *v, lolo val, char *x, lolo xlen, long *card) {
    v = NULL; val = 0; x = NULL; xlen = 0; card = NULL;
}
void messageCommand(redisClient *c) { //NOTE: this command does not reply
    sds    cmd  = c->argv[2]->ptr;
    int    argc;
    sds   *argv  = sdssplitlen(cmd, sdslen(cmd), "/", 1, &argc);
    robj **rargv = zmalloc(sizeof(robj *) * argc);
    for (int i = 0; i < argc; i++) {
        rargv[i] = createStringObject(argv[i], sdslen(argv[i]));
    }
    redisClient *rfc = getFakeClient();
    rfc->argv        = rargv;
    rfc->argc        = argc;
    fakeClientPipe(rfc, NULL, ignoreFCP);
}

void rsubscribeCommand(redisClient *c) {
    sds  ip     = c->argv[1]->ptr;
    int  port   = atoi(c->argv[2]->ptr);
    { // PING the remote machine to create a fd for it
        setClientParams(ip, port);
        cliConnect(1);
        int  argc = 1;
        sds *argv = zmalloc(sizeof(sds));
        argv[0]   = sdsnew("PING");
        cliSendCommand(argc, argv, 1);
        while(argc--) sdsfree(argv[argc]); /* Free the argument vector */
        zfree(argv);
        if (!context) { addReply(c, shared.err); return; }
    }
    cli *rc = createClient(context->fd); // use this fd for remote-subscription
    for (int j = 3; j < c->argc; j++) {
        struct dictEntry *de;
        list *clients = NULL;
        robj *channel = c->argv[j];
        /* Add the channel to the client -> channels hash table */
        if (dictAdd(rc->pubsub_channels, channel, NULL) == DICT_OK) {
            incrRefCount(channel);
            /* Add the client to the channel -> list of clients hash table */
            de = dictFind(server.pubsub_channels, channel);
            if (de == NULL) {
                clients = listCreate();
                dictAdd(server.pubsub_channels, channel, clients);
                incrRefCount(channel);
            } else {
                clients = dictGetEntryVal(de);
            }
            listAddNodeTail(clients, rc);
        }
    }
    addReply(c, shared.ok);
}

#ifndef __INTERNAL_COMMANDS_H
#define __INTERNAL_COMMANDS_H

#include "redis.h"

typedef robj **pc2a(char *as_cmd, int *argc);
typedef struct storage_command {
    void (*func)(redisClient *c);
    char *name;
    int   argc;
    pc2a *parse;
} stor_cmd;

void initAccessCommands();

#endif /* __INTERNAL_COMMANDS_H */

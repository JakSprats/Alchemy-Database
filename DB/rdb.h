#ifndef __REDIS_RDB__H
#define __REDIS_RDB__H

#include "redis.h"

int rdbSaveLen(FILE *fp, uint32_t len);
int rdbSaveStringObject(FILE *fp, robj *obj);
int rdbSaveType(FILE *fp, unsigned char type);

uint32_t rdbLoadLen(FILE *fp, int *isencoded);
int rdbLoadType(FILE *fp);
robj *rdbLoadStringObject(FILE *fp);

#endif /* __REDIS_RDB__H */

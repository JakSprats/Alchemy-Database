/*
 * This file implements parsing functions
 *

GPL License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of AlchemyDatabase

    AlchemyDatabase is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    AlchemyDatabase is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with AlchemyDatabase.  If not, see <http://www.gnu.org/licenses/>.

 */

#ifndef __ALC_PARSER__H
#define __ALC_PARSER__H

#include "common.h"

char *_strdup(char *s);
char *_strnchr(char *s, int c, int len);
bool is_int(char *s);
bool is_float(char *s);
robj *_createStringObject(char *s);
robj *cloneRobj(robj *r);
robj **copyArgv(robj **argv, int argc);
robj *convertRobj(robj *r, int type);

void StaticRobjInit(robj *r, int type);
#define INIT_ROBJ(X, T) \
    { \
        static bool X ## _inited = 0; \
        if (! X ## _inited ) { \
            StaticRobjInit( & X , T); \
            X ## _inited  = 1; \
        } \
    }

char *rem_backticks(char *token, int *len);
char *str_next_unescaped_chr(char *beg, char *s, int x);
char *strn_next_unescaped_chr(char *beg, char *s, int x, int len);
char *str_matching_end_paren(char *beg);

#define SKIP_SPACES(tok) while (isblank(*tok)) tok++;

char *min2(char *p, char *q);

char *next_token(char *nextp);
int get_token_len(char *tok);
int get_tlen_delim2(char *nextp, char x);
char *next_token_delim2(char *p, char x);
int get_tlen_delim3(char *nextp, char x, char z);
char *next_token_delim3(char *p, char x, char z);
char *get_next_token_nonparaned_comma(char *token);

robj **parseCmdToArgvReply(redisClient *c, char *as_cmd, int *rargc);
robj **parseSelectCmdToArgv(char *as_cmd, int *argc);
robj **parseScanSelectCmdToArgv(char *as_cmd, int *argc);

#endif /* __ALC_PARSER__H */

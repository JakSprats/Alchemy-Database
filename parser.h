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

char *_strnchr(char *s, int c, int len); /* HELPER */
robj *_createStringObject(char *s);
robj *cloneRobj(robj *r);                /* HELPER */
robj **cloneArgv(robj **argv, int argc);
robj *convertRobj(robj *r, int type);    /* HELPER */

char *rem_backticks(char *token, int *len);
char *str_next_unescaped_chr(char *beg, char *s, int x);
char *strn_next_unescaped_chr(char *beg, char *s, int x, int len);

char *next_token(char *nextp);
int get_token_len(char *tok);
int get_token_len_delim(char *nextp, char x, char z);
char *next_token_delim(char *p, char x, char z);
char *get_next_token_nonparaned_comma(char *token);

robj **parseCmdToArgv(char *as_cmd, int *rargc);
robj **parseSelectCmdToArgv(char *as_cmd);

#endif /* __ALC_PARSER__H */

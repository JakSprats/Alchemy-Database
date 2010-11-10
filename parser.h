/*
 * This file implements parsing functions
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ALC_PARSER__H
#define __ALC_PARSER__H

#include "common.h"

char *_strnchr(char *s, int c, int len); /* HELPER */
robj *cloneRobj(robj *r);                /* HELPER */
robj *convertRobj(robj *r, int type);    /* HELPER */

char *rem_backticks(char *token, int *len);
char *str_next_unescaped_chr(char *beg, char *s, int x);

char *next_token(char *nextp);
int get_token_len(char *tok);
int get_token_len_delim(char *nextp, char x);
char *next_token_delim(char *p, char x);
char *get_next_token_nonparaned_comma(char *token);

robj **parseCmdToArgv(char *as_cmd, int *rargc);
robj **parseSelectCmdToArgv(char *as_cmd);

#endif /* __ALC_PARSER__H */

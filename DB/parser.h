/*
 * This file implements parsing functions
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

#ifndef __ALC_PARSER__H
#define __ALC_PARSER__H

#include "redis.h"

#include "common.h"

// HELPERS
robj  *_createStringObject(char *s);
robj  *cloneRobj(robj *r);
void   destroyCloneRobj(robj *r);
robj **copyArgv(robj **argv, int argc);
robj  *convertRobj(robj *r, int type);
char  *rem_backticks(char *token, int *len);

char *_strdup(char *s);
char *_strnchr(char *s, int c, int len);

// TYPE_CHECK
bool is_int  (char *s);
bool is_u128 (char *s);
bool is_float(char *s);
bool is_text (char *beg, int len);

// PARSER
char *extract_string_col(char *start, int *len);
char *strcasestr_blockchar(char *haystack, char *needle, char blockchar);
char *next_token_wc_key(char *tkn, uchar ctype);

char *str_next_unescaped_chr(char *beg, char *s, int x);
char *strn_next_unescaped_chr(char *beg, char *s, int x, int len);
char *str_matching_end_paren(char *beg);

char *next_token(char *nextp);
int   get_token_len(char *tok);
char *get_next_insert_value_token(char *tkn);

char *strstr_not_quoted(char *h, char *n);
char *get_after_parens(char *p);
char *get_next_nonparaned_comma(char *token);

// UTILS
#define ISDIGIT(c) (c >= 48 && c <= 57)
#define ISALPHA(c) ((c >= 65 && c <= 90) || (c >= 97 && c <= 122))
#define ISALNUM(c) (ISDIGIT(c) || ISALPHA(c))
#define ISBLANK(c) (c == 32 || c == 9)
#define SKIP_SPACES(tok)     while (ISBLANK(*tok)) tok++;
#define REV_SKIP_SPACES(tok) while (ISBLANK(*tok) || !*tok) tok--;
#define ISLPAREN(c) (c == '(')
#define SKIP_LPAREN(tok)     while (ISLPAREN(*tok) || ISBLANK(*tok)) tok++;
#define ISRPAREN(c) (c == ')')
#define REV_SKIP_RPAREN(tok) \
  while (ISRPAREN(*tok) || !*tok || ISBLANK(*tok)) tok--;

//PIPE_PARSING
robj **parseScanCmdToArgv(char *as_cmd, int *argc);

#endif /* __ALC_PARSER__H */

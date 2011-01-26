/*
 * This file implements the sixbit compression of strings algorithms
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>

#include "common.h"

char *sixbitchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ _+-.,'\"#/\\";

uchar to_six_bit_strings[256];
uchar from_six_bit_strings[256];

void init_six_bit_strings() {
    bzero(to_six_bit_strings, 256);
    uint32 len = strlen(sixbitchars);
    for (uint32 i = 0; i < len; i++) {
        to_six_bit_strings[(int)sixbitchars[i]]  = i + 1;
    }

    bzero(from_six_bit_strings, 256);
    for (uint32 i = 0; i < len; i++) {
        from_six_bit_strings[i + 1]  = sixbitchars[i];
    }
}

static bool six_bit_pack(char s_c, uint32 s_i, uchar **dest) {
    //uchar *o_dest = *dest;
    char           s_p    = to_six_bit_strings[(int)s_c];
    if (!s_p) return 0;
    char           state  = s_i % 4;
    uchar  b0     = s_p;
    uchar  b1     = s_p;
    if      (state == 0) {
        b0 <<= 2;
        **dest += b0;
    } else if (state == 1) {
        b0 >>= 4;
        **dest += b0;
        b1 <<= 4;
        *dest = *dest + 1;
        **dest += b1;
    } else if (state == 2) {
        b0 >>= 2;
        **dest += b0;
        b1 <<= 6;
        *dest = *dest + 1;
        **dest += b1;
    } else {
        **dest += b0;
        *dest = *dest + 1;
    }
    //printf("pack %d: c: %c -> %d (%u,%u) res(%u,%u)\n",
            //state, s_c, s_p, b0, b1, *o_dest, **dest);
    return 1;
}

static uchar six_bit_unpack(uint32 s_i, uchar **dest) {
    //uchar *o_dest = *dest;
    char           state  = s_i % 4;
    uchar  b1     = **dest;
    uchar  b2     = 0;;
    if      (state == 0) {
        b1 >>= 2;
    } else if (state == 1) {
        b1 <<= 6;
        b1 >>= 2;
        *dest = *dest + 1;
        b2 = **dest;
        b2 >>= 4;
    } else if (state == 2) {
        b1 <<= 4;
        b1 >>= 2;
        *dest = *dest + 1;
        b2 = **dest;
        b2 >>= 6;
    } else {
        b1 <<= 2;
        b1 >>= 2;
        *dest = *dest + 1;
    }
    uchar b3 = b1 + b2;
    //printf("unpack %d: (%u,%u) -> res(%u,%u) =>%u\n",
            //state, *o_dest, **dest, b1, b2, b3);
    return from_six_bit_strings[b3];
}

uchar *_createSixBit(char *src, uint32  src_len, uint32 *new_len) {
    uchar *dest    = malloc(src_len + 1);
    //printf("_createSixBit malloc\n");
    bzero(dest, src_len + 1);
    uchar *o_dest  = dest;
    for (uint32 i = 0; i < src_len; i++) {
        if(!six_bit_pack(src[i], i, &dest)) {
            free(o_dest);
            *new_len = 0;
            return NULL;
        }
    }
    *new_len = (dest - o_dest) + 1;
    //printf("_createSixBit: o_dest: %p len: %u\n", o_dest, *new_len);
    return o_dest;
}

uchar *createSixBit(char *src, uint32 *new_len) {
    return _createSixBit(src, strlen(src), new_len);
}

uchar *unpackSixBit(uchar *src, uint32 *s_len) {
    uint32    was_len = ((*s_len * 4) / 3);
    //printf("unpackSixBit: was_len: %u s_len: %d\n", was_len, *s_len);
    //printf("unpackSixBit malloc\n");
    uchar *fdest   = malloc(was_len + 1);
    uchar *o_fdest = fdest;
    uint32   len     = 0;
    for (uint32 i = 0; i < was_len; i++) {
        uchar u = six_bit_unpack(i, &src);
        if (u) { /* final six-bit-char may be empty */
            fdest[i] = u;
            len++;
        }// else printf("%d not upackable: %u\n", i, *src);
    }
    *s_len       = len;
    o_fdest[len] = '\0';
    return o_fdest;
}

#if 0
int main() {
    char   *test  = sixbitchars;
    init_six_bit_strings();

    for (int i = 0; i < 2; i++) {
        int            s_len = 0;
        uchar *dest  = createSixBit(test, &s_len);
        printf("packed size: %d\n", s_len);
        uchar *fdest = unpackSixBit(dest, &s_len);
        printf("original: (%s) len: %ld\n", test, strlen(test));
        printf("final:    (%s) len: %d strlen: %ld\n",
                fdest, s_len, strlen(fdest));
        free(dest);
        free(fdest);
        printf("\n\n");
    }

}
#endif

//TODO if there are spare bits in the RFLAG, then try multiple encodings
#if 0
//JSON
char *sixbitchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ[]()\\/,:'\"+";
//Alphanumeric
char *sixbitchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\ ";
//Address (no "qQ")
char *sixbitchars = "abcdefghijklmnoprstuvwxyzABCDEFGHIJKLMNOPRSTUVWXYZ0123456789\ .#";
#endif

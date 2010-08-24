
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>

//from redis.c
#define RL4 redisLog(4,

#define bool unsigned char 

char *sixbitchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ _+-.,'\"#/\\";

unsigned char to_six_bit_strings[256];
unsigned char from_six_bit_strings[256];

void init_six_bit_strings() {
    bzero(to_six_bit_strings, 256);
    unsigned int len = strlen(sixbitchars);
    for (unsigned int i = 0; i < len; i++) {
        to_six_bit_strings[(int)sixbitchars[i]]  = i + 1;
    }

    bzero(from_six_bit_strings, 256);
    for (unsigned int i = 0; i < len; i++) {
        from_six_bit_strings[i + 1]  = sixbitchars[i];
    }
}

bool six_bit_pack(char s_c, unsigned int s_i, unsigned char **dest) {
    //unsigned char *o_dest = *dest;
    char           s_p    = to_six_bit_strings[(int)s_c];
    if (!s_p) return 0;
    char           state  = s_i % 4;
    unsigned char  b0     = s_p;
    unsigned char  b1     = s_p;
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

unsigned char six_bit_unpack(unsigned int s_i, unsigned char **dest) {
    //unsigned char *o_dest = *dest;
    char           state  = s_i % 4;
    unsigned char  b1     = **dest;
    unsigned char  b2     = 0;;
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
    unsigned char b3 = b1 + b2;
    //printf("unpack %d: (%u,%u) -> res(%u,%u) =>%u\n",
            //state, *o_dest, **dest, b1, b2, b3);
    return from_six_bit_strings[b3];
}

unsigned char *_createSixBitString(char         *src,
                                   unsigned int  src_len,
                                   unsigned int *new_len) {
    unsigned char *dest    = malloc(src_len + 1);
    //RL4 "_createSixBitString malloc");
    bzero(dest, src_len + 1);
    unsigned char *o_dest  = dest;
    for (unsigned int i = 0; i < src_len; i++) {
        if(!six_bit_pack(src[i], i, &dest)) {
            free(o_dest);
            *new_len = 0;
            return NULL;
        }
    }
    *new_len = (dest - o_dest) + 1;
    //RL4 "_createSixBitString: o_dest: %p len: %u", o_dest, *new_len);
    return o_dest;
}

unsigned char *createSixBitString(char *src, unsigned int *new_len) {
    return _createSixBitString(src, strlen(src), new_len);
}

unsigned char *unpackSixBitString(unsigned char *src, unsigned int *s_len) {
    unsigned int    was_len = ((*s_len * 4) / 3);
    //RL4 "unpackSixBitString: was_len: %u s_len: %d", was_len, *s_len);
    //RL4 "unpackSixBitString malloc");
    unsigned char *fdest   = malloc(was_len + 1);
    unsigned char *o_fdest = fdest;
    unsigned int   len     = 0;
    for (unsigned int i = 0; i < was_len; i++) {
        unsigned char u = six_bit_unpack(i, &src);
        if (u) { /* final six-bit-char may be empty */
            fdest[i] = u;
            len++;
        }// else printf("%d not upackable: %u\n", i, *src);
    }
    *s_len = len;
    o_fdest[len] = '\0';
    return o_fdest;
}

#if 0
int main() {
    char   *test  = sixbitchars;
    init_six_bit_strings();

    for (int i = 0; i < 2; i++) {
        int            s_len = 0;
        unsigned char *dest  = createSixBitString(test, &s_len);
        printf("packed size: %d\n", s_len);
        unsigned char *fdest = unpackSixBitString(dest, &s_len);
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

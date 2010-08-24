/*
COPYRIGHT: RUSS
 */

#ifndef __SIXBIT__H
#define __SIXBIT__H

void init_six_bit_strings();

unsigned char *createSixBitString( char *src, unsigned int *new_len);
unsigned char *_createSixBitString(char         *src,
                                   unsigned int  src_len,
                                   unsigned int *new_len);

unsigned char *unpackSixBitString( unsigned char *src, unsigned int *s_len);

#endif /* __SIXBIT__H */ 

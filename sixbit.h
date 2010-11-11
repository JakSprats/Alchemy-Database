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

#ifndef __SIXBIT__H
#define __SIXBIT__H

void init_six_bit_strings();

unsigned char *createSixBitString( char *src, unsigned int *new_len);
unsigned char *_createSixBitString(char         *src,
                                   unsigned int  src_len,
                                   unsigned int *new_len);

unsigned char *unpackSixBitString( unsigned char *src, unsigned int *s_len);

#endif /* __SIXBIT__H */ 

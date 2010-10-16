/*
 * This file implements "SELECT ... ORDER BY col LIMIT X" helper funcs
 *

MIT License

Copyright (c) 2010 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#ifndef __ORDER_BY__H
#define __ORDER_BY__H

typedef struct order_by_sort_element {
    void *val;
    void *row;
} obsl_t;

int intOrderBySort(      const void *s1, const void *s2);
int intOrderByRevSort(   const void *s1, const void *s2);
int stringOrderBySort(   const void *s1, const void *s2);
int stringOrderByRevSort(const void *s1, const void *s2);

void addORowToRQList(list *ll,
                     robj *r,
                     robj *row,
                     int   obc,
                     robj *pko,
                     int   tmatch,
                     bool  icol);

obsl_t **sortOrderByToVector(list *ll, bool icol, bool asc);

void sortedOrderByCleanup(obsl_t **vector,
                          int      vlen,
                          bool     icol,
                          bool     decr_row);

#endif /* __ORDER_BY__H */ 

/*
 * This file implements ALCHEMY_DATABASE's redis-cli hooks
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

#include <stdio.h>
#include <string.h>
#include <strings.h>

#include "sds.h"
#include "zmalloc.h"

#include "xdb_client_hooks.h"

static void merge_vals(int *argc,   char **argv, int first,
                       int  second, unsigned char space) {
    //printf("merge_vals: first: %d second: %d\n", first, second);
    if (first == second) return;
    int len = 0;
    for (int i = first; i <= second; i++) {
        if (space && i != first) len++;
        len += sdslen(argv[i]);
    }
    char *x = zmalloc(len);
    int slot = 0;
    for (int i = first; i <= second; i++) {
        if (space && i != first) {
            memcpy(x + slot, " ", 1); slot++;
        }
        memcpy(x + slot, argv[i], sdslen(argv[i]));
        slot += sdslen(argv[i]);
    }
    sds s          = sdsnewlen(x, len);
    zfree(x);
    argv[first]    = s;
    int first_copy = first;
    for (int i = second + 1; i < *argc; i++) {
        argv[++first_copy] = argv[i];
        argv[i]            = NULL; /* no double frees */
    }
    *argc = *argc - (second - first);
}

static void insert_vals_mod(int *argc, char **argv) {
    int i;
    for (i = 0; i < *argc; i++) {
        if (!strcasecmp(argv[i], "VALUES")) break;
    }
    if (i == *argc) return; // SYNTAX is BAD, server will return ERROR
    int first = i + 1;
    for (; i < *argc; i++) {
        int len = sdslen(argv[i]);
        if (*(argv[i]) == ')' || argv[i][len - 1] == ')') break;
    }
    if (i == *argc) return; // SYNTAX is BAD, server will return ERROR
    int second = i;
    merge_vals(argc, argv, first, second, 0);
}

static void update_vals_mod(int *argc, char **argv) {
    int i;
    for (i = 0; i < *argc; i++) {
        if (!strcasecmp(argv[i], "SET")) break;
    }
    if (i == *argc) return; // SYNTAX is BAD, server will return ERROR
    int first = i + 1;
    for (; i < *argc; i++) {
        if (!strcasecmp(argv[i], "WHERE")) break;
    }
    if (i == *argc) return; // SYNTAX is BAD, server will return ERROR
    int second = i - 1;
    merge_vals(argc, argv, first, second, 0);
}

static void create_table_mod(int *argc, char **argv) {
    if (*argc < 3) return;
    merge_vals(argc, argv, 3, (*argc - 1), 1);
}
static void update_where_mod(int *argc, char **argv) {
    if (*argc < 5) return;
    merge_vals(argc, argv, 5, (*argc - 1), 1);
}
static void delete_where_mod(int *argc, char **argv) {
    if (*argc < 4) return;
    merge_vals(argc, argv, 4, (*argc - 1), 1);
}
static void select_mod_from(int *argc, char **argv) {
    if (*argc < 6) return;
    int j;
    for (j = 1; j < *argc; j++) {
        if (!strcasecmp(argv[j], "FROM")) break;
    }
     if ((j >= (*argc -1)) || (j == 1)) return; // SYNTAX BAD, server ERROR
    if (j != 2) merge_vals(argc, argv, 1, (j - 1), 1);     /* ColumnList */
}
static void select_mod_where(int *argc, char **argv) {
    int k;
    for (k = 4; k < *argc; k++) {
        if (!strcasecmp(argv[k], "WHERE")) break;
    }
    if ((k >= (*argc - 1))) return; // SYNTAX BAD server ERROR
    if (k != 4) merge_vals(argc, argv, 3, (k - 1), 1);  /* TableList */
    merge_vals(argc, argv, 5, (*argc - 1), 1); /* WHERE */
}
static void select_mod(int *argc, char **argv) {
    select_mod_from(argc, argv);
    select_mod_where(argc, argv);
}
static void scan_mod_orderby(int *argc, char **argv) {
    if (!strcasecmp(argv[4], "ORDER") && (*argc > 5)) {
        merge_vals(argc, argv, 4, (*argc - 1), 1); /* WHERE */
    }
}
static void scan_mod(int *argc, char **argv) {
    select_mod_from(argc, argv);
    if (*argc > 4) {
        select_mod_where(argc, argv);
        scan_mod_orderby(argc, argv);
    }
}
void DXDB_cliSendCommand(int *argc, char **argv) {
    //printf("DXDB_cliSendCommand\n");
    if      (!strcasecmp(argv[0], "INSERT")) insert_vals_mod (argc, argv);
    else if (!strcasecmp(argv[0], "UPDATE")) update_vals_mod (argc, argv);
    else if (!strcasecmp(argv[0], "CREATE") &&
             !strcasecmp(argv[1], "TABLE"))  create_table_mod(argc, argv);

    if      (!strcasecmp(argv[0], "UPDATE")) update_where_mod(argc, argv);
    else if (!strcasecmp(argv[0], "DELETE")) delete_where_mod(argc, argv);
    else if (!strcasecmp(argv[0], "SELECT")) select_mod      (argc, argv);
    else if (!strcasecmp(argv[0], "SCAN"))   scan_mod        (argc, argv);

}


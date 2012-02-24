/*
 * Implements ALCHEMY_DATABASE EXPLAIN & debug object dump functions
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

#ifndef __ALCHEMYDB_DEBUG__H
#define __ALCHEMYDB_DEBUG__H

#include "adlist.h"
#include "redis.h"

#include "query.h"
#include "common.h"

long MAX(long a, long b);
long MIN(long a, long b);

void explainRQ(cli *c, cswc_t *w, wob_t *wb, bool cstr, int qcols, icol_t *ics);
void explainJoin   (cli *c, jb_t *jb);
void explainCommand(cli *c);

// QUEUE_PRINTF_TO_CLIENT QUEUE_PRINTF_TO_CLIENT QUEUE_PRINTF_TO_CLIENT
void initQueueOutput();
int  queueOutput(const char *format, ...);
void dumpQueueOutput(cli *c);

// OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT
void setDeferredMultiBulkSDS       (cli *c, void *node, sds s);
void resetDeferredMultiBulk_ToError(cli *c, void *node, sds error);
void setDeferredMultiBulkLong      (cli *c, void *node, long card);
void prependDeferredMultiBulkError (cli *c, void *node, sds error);

void replaceDMB                    (cli *c, void *node, robj *err);

// DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
void dumpIJ(cli *c, printer *prn, int i, ijp_t *ij, ijp_t *nij);
void dumpJB(cli *c, printer *prn, jb_t *jb);

void dumpQcols(printer *prn, int tmatch, bool cstar, int qcols, icol_t *ics);
void dumpWB   (printer *prn, wob_t *wb);
void dumpW    (printer *prn, cswc_t *w);
void dumpSds  (printer *prn, sds s, char *smsg);
void dumpRobj (printer *prn, robj *r, char *smsg, char *dmsg);
void dumpFL   (printer *prn, char *prfx, char *title, list *flist);
void dumpSL   (sl_t sl);

void dumpIC   (printer *prn, icol_t *ic);
#endif /* __ALCHEMYDB_DEBUG__H */ 

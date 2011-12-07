/*
 * Implements ALCHEMY_DATABASE EXPLAIN & debugs
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

void explainRQ     (cli *c, cswc_t *w, wob_t *wb);
void explainJoin   (cli *c, jb_t *jb);
void explainCommand(cli *c);

// OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT OUTPUT
void resetDeferredMultiBulk_ToError(cli *c, void *node, sds error);
void setDeferredMultiBulkLong      (cli *c, void *node, long card);
void prependDeferredMultiBulkError (cli *c, void *node, sds error);

void replaceDMB_WithDirtyMissErr(cli *c, void *node);
void replaceDMB_With_QO_Err     (cli *c, void *node);

/* DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG */
void dumpIJ(cli *c, printer *prn, int i, ijp_t *ij, ijp_t *nij);
void dumpJB(cli *c, printer *prn, jb_t *jb);

void initQueueOutput();
int  queueOutput(const char *format, ...);
void dumpQueueOutput(cli *c);

void dumpWB(printer *prn,   wob_t *wb);
void dumpW(printer *prn,    cswc_t *w);
void dumpSds(printer *prn,  sds s, char *smsg);
void dumpRobj(printer *prn, robj *r, char *smsg, char *dmsg);
void dumpFL(printer *prn,   char *prfx, char *title, list *flist);
void dumpSL(sl_t sl);

#endif /* __ALCHEMYDB_DEBUG__H */ 

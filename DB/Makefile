# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

# =========================== USER SETTINGS ===================================
#LUAJIT= yes
#USE_TCMALLOC=yes

# == END OF USER SETTINGS. NO NEED TO CHANGE ANYTHING BELOW THIS LINE =========

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2

ifndef PROF
  PROF?=-DREDIS3
endif
ifndef REDISDIR
  REDISDIR?=../redis/
endif

ifeq ($(uname_S),Linux)
  ifneq ($(FORCE_LIBC_MALLOC),yes)
    USE_JEMALLOC=yes
  endif
endif


ifeq ($(USE_JEMALLOC),yes)
  ALLOC_DEP=$(REDISDIR)/deps/jemalloc/lib/libjemalloc.a
  ALLOC_LINK=$(ALLOC_DEP) -ldl
  ALLOC_FLAGS=-DUSE_JEMALLOC -I$(REDISDIR)/deps/jemalloc/include
endif

CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -I./ -I$(REDISDIR)/src/ $(ALLOC_FLAGS) -DALCHEMY_DATABASE -I../redis/deps/hiredis/
CCLINK?= -lm -pthread
DEBUG?= -g -rdynamic -ggdb 

ifeq ($(uname_S),SunOS)
  @echo "Sun not supported (no BigEndian support) - sorry"
  @exit
endif
ifeq ($(LUAJIT),yes)
  CFLAGS+= -I$(REDISDIR)/deps/luajit/src/
  CCLINK+= -L$(REDISDIR)/deps/luajit/src/ -lluajit
else
  CFLAGS+= -I$(REDISDIR)/deps/lua/src/
  CCLINK+= -L$(REDISDIR)/deps/lua/src/ -llua
endif
ifeq ($(USE_TCMALLOC),yes)
  ifeq ($(uname_S),Linux)
    CCLINK+= /usr/lib/libtcmalloc.so.0
  else
    CCLINK+= -ltcmalloc
  endif
  CFLAGS+= -DUSE_TCMALLOC
endif

CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)

OBJ = bt.o bt_code.o bt_output.o ddl.o alsosql.o sixbit.o row.o index.o rdb_alsosql.o aof_alsosql.o join.o bt_iterator.o wc.o scan.o orderby.o luatrigger.o parser.o cr8tblas.o rpipe.o range.o desc.o aobj.o stream.o colparse.o filter.o qo.o lru.o internal_commands.o xdb_hooks.o xdb_client_hooks.o ctable.o shared_obj.o webserver.o messaging.o embed.o

LIBNAME = libx_db.a

lib : $(OBJ)
	ar rcs $(LIBNAME) $(OBJ)

redis2:
	make lib

redis3:
	$(MAKE) PROF="-DREDIS3" REDISDIR="../redis_unstable/" USE_JEMALLOC="yes"

all: redis3

# Deps (use make dep to generate this)
alsosql.o: alsosql.h bt.h filter.h query.h index.h range.h rpipe.h desc.h cr8tblas.h wc.h parser.h colparse.h aobj.h common.h
aobj.o: aobj.h row.h parser.h query.h common.h
bt.o: bt.h btree.h btreepriv.h query.h stream.h common.h
bt_code.o: btree.h btreepriv.h bt.h bt_iterator.h common.h
bt_output.o: btree.h stream.h colparse.h
bt_iterator.o: bt_iterator.h bt.h stream.h aobj.h query.h common.h
cr8tblas.o: cr8tblas.h wc.h alsosql.h row.h rpipe.h parser.h common.h
colparse.o: colparse.h parser.h query.h common.h
desc.o: desc.h bt_iterator.h colparse.h bt_iterator.h bt.h aobj.h common.h
index.o: index.h luatrigger.h colparse.h bt_iterator.h alsosql.h orderby.h  stream.h aobj.h common.h
join.o: join.h wc.h colparse.h range.h bt_iterator.h alsosql.h orderby.h aobj.h common.h
luatrigger.o: luatrigger.h rpipe.h
orderby.o: orderby.h join.h aobj.h common.h
parser.o: parser.h common.h
range.o: range.h filter.h colparse.h orderby.h bt_iterator.h bt.h aobj.h common.h
rdb_alsosql.o: rdb_alsosql.h lru.h bt_iterator.h alsosql.h index.h stream.h common.h rdb.h
aof_alsosql.o: aof_alsosql.h lru.h bt_iterator.h alsosql.h index.h stream.h common.h
row.o: row.h parser.h stream.h lru.h alsosql.h aobj.h common.h
rpipe.o: rpipe.h common.h
scan.o: alsosql.h colparse.h range.h bt_iterator.h wc.h orderby.h aobj.h
sixbit.o: sixbit.h
stream.o: aobj.h common.h
wc.o: wc.h colparse.h qo.h filter.h range.h parser.h bt_iterator.h cr8tblas.h rpipe.h common.h
filter.o: filter.h colparse.h aobj.h common.h
qo.o: qo.h join.h bt.h filter.h index.h alsosql.h common.h
lru.o: lru.h row.h stream.h alsosql.h aobj.h query.h common.h
internal_commands.o: internal_commands.h
xdb_hooks.o: xdb_hooks.h
xdb_client_hooks.o: xdb_client_hooks.h
ctable.o: ctable.h wc.h colparse.h common.h
shared_obj.o: xdb_hooks.h
webserver.o: webserver.h
ddl.o: ddl.h alsosql.h common.h
messaging.o: messaging.h rpipe.h
embed.o: embed.h

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(LIBNAME) *.o *.gcda *.gcno *.gcov

dep:
	$(CC) -MM *.c

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	make ARCH="-m32"

gprof:
	make PROF="-pg"

gcov:
	make PROF="-fprofile-arcs -ftest-coverage"

noopt:
	make OPTIMIZATION=""

32bitgprof:
	make PROF="-pg" ARCH="-arch i386"

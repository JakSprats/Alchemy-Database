# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

# Your platform. See PLATS for possible values.
PLAT= none
#PLAT= linux

# == END OF USER SETTINGS. NO NEED TO CHANGE ANYTHING BELOW THIS LINE =========

# Convenience platforms targets.
PLATS= aix ansi bsd freebsd generic linux macosx mingw posix solaris

# STEPS for LUAJIT compilation
# 1.) git clone https://github.com/tycho/luajit.git
# 2.) cd luajit*
# 3.) make install
# 4.) export LD_LIBRARY_PATH=/usr/local/lib/
# 5.) set LUAJIT= yes
LUAJIT= no
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  @echo "Sun not supported - sorry"
  @exit
else
  ifeq ($(LUAJIT),yes)
    CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -I./luajit-2.0/src/
    CCLINK?= -lm -pthread -L./luajit-2.0/src/ -lluajit -ldl
  else
    CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -I./lua-5.1.4/src/
    CCLINK?= -lm -pthread -L./lua-5.1.4/src/ -llua -ldl
  endif
endif
ifeq ($(LUAJIT),yes)
  EXTRA_LD= -lluajit
else
  EXTRA_LD= -llua
endif
CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)
DEBUG?= -g -rdynamic -ggdb 
ifeq ($(LUAJIT),yes)
  LUADIR=./luajit-2.0/src/
  MTYPE=
else
  LUADIR=./lua-5.1.4/src/
  MTYPE=$@
endif

OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o bt.o bt_code.o bt_output.o alsosql.o sixbit.o row.o index.o rdb_alsosql.o join.o norm.o bt_iterator.o sql.o denorm.o store.o scan.o orderby.o lua_integration.o parser.o
BENCHOBJ = ae.o anet.o redis-benchmark.o sds.o adlist.o zmalloc.o
GENBENCHOBJ = ae.o anet.o gen-benchmark.o sds.o adlist.o zmalloc.o
CLIOBJ = anet.o sds.o adlist.o redis-cli.o zmalloc.o linenoise.o
CHECKDUMPOBJ = redis-check-dump.o lzf_c.o lzf_d.o
CHECKAOFOBJ = redis-check-aof.o

PRGNAME = redisql-server
BENCHPRGNAME = redisql-benchmark
GENBENCHPRGNAME = gen-benchmark
CLIPRGNAME = redisql-cli
CHECKDUMPPRGNAME = redisql-check-dump
CHECKAOFPRGNAME = redisql-check-aof

ALL = $(PRGNAME) $(GENBENCHPRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) $(CHECKDUMPPRGNAME) $(CHECKAOFPRGNAME)

all:    $(PLAT)

bins : $(ALL)

aix:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

ansi:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

bsd:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

freebsd:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

generic:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

linux:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

macosx:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

# use this on Mac OS X 10.3-
#       $(MAKE) all MYCFLAGS=-DLUA_USE_MACOSX

mingw:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

posix:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

solaris:
	cd $(LUADIR) && $(MAKE) $(MTYPE)
	make bins

none:
	@echo "Please do"
	@echo "   make PLATFORM"
	@echo "where PLATFORM is one of these:"
	@echo "   $(PLATS)"
	@echo "See INSTALL for complete instructions."


# Deps (use make dep to generate this)
adlist.o: adlist.c adlist.h zmalloc.h
ae.o: ae.c ae.h zmalloc.h config.h ae_kqueue.c
ae_epoll.o: ae_epoll.c
ae_kqueue.o: ae_kqueue.c
ae_select.o: ae_select.c
anet.o: anet.c fmacros.h anet.h
dict.o: dict.c fmacros.h dict.h zmalloc.h
dict2.o: dict2.c fmacros.h dict2.h zmalloc.h
linenoise.o: linenoise.c fmacros.h
lzf_c.o: lzf_c.c lzfP.h
lzf_d.o: lzf_d.c lzfP.h
pqsort.o: pqsort.c
printraw.o: printraw.c
redis-benchmark.o: redis-benchmark.c fmacros.h ae.h anet.h sds.h adlist.h \
  zmalloc.h
redis-check-aof.o: redis-check-aof.c fmacros.h config.h
redis-check-dump.o: redis-check-dump.c lzf.h
redis-cli.o: redis-cli.c fmacros.h anet.h sds.h adlist.h zmalloc.h \
  linenoise.h
redis.o: redis.c fmacros.h config.h redis.h ae.h sds.h anet.h dict.h \
  adlist.h zmalloc.h lzf.h pqsort.h zipmap.h staticsymbols.h sha1.h \
  alsosql.h bt_iterator.h index.h bt.h sixbit.h row.h common.h denorm.h \
  btree.h btreepriv.h row.h join.h sql.h store.h
  
sds.o: sds.c sds.h zmalloc.h
zipmap.o: zipmap.c zmalloc.h
zmalloc.o: zmalloc.c config.h

bt.o: bt.c btree.h btreepriv.h redis.h row.h bt.h common.h
bt_code.o: bt_code.c btree.h btreepriv.h redis.h
bt_output.o: bt_output.c btree.h btreepriv.h redis.h
alsosql.o: redis.h alsosql.h bt_iterator.h index.h bt.h sixbit.h row.h common.h denorm.h
index.o: redis.h index.h common.h bt_iterator.h alsosql.h orderby.h
bt_iterator.o: redis.h bt_iterator.h btree.h btreepriv.h
norm.o: redis.h sql.h bt_iterator.h
join.o: redis.h join.h bt_iterator.h alsosql.h orderby.h store.h
store.o: redis.h store.h bt_iterator.h alsosql.h orderby.h
rdb_alsosql.o: redis.h rdb_alsosql.h common.h bt_iterator.h alsosql.h
sixbit.o: sixbit.h
row.o: row.h common.h alsosql.h
sql.o: redis.h sql.h bt_iterator.h
denorm.o: redis.h denorm.h bt_iterator.h alsosql.h parser.h
scan.o: alsosql.h bt_iterator.h sql.h
orderby.o: orderby.h
lua_integration.o: lua_integration.h redis.h zmalloc.h denorm.h
parser.o: parser.h redis.h zmalloc.h

redisql-server: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ) $(EXTRA_LD)

redisql-benchmark: $(BENCHOBJ)
	$(CC) -o $(BENCHPRGNAME) $(CCOPT) $(DEBUG) $(BENCHOBJ)

gen-benchmark: $(GENBENCHOBJ)
	$(CC) -o $(GENBENCHPRGNAME) $(CCOPT) $(DEBUG) $(GENBENCHOBJ)

redisql-cli: $(CLIOBJ)
	$(CC) -o $(CLIPRGNAME) $(CCOPT) $(DEBUG) $(CLIOBJ)

redisql-check-dump: $(CHECKDUMPOBJ)
	$(CC) -o $(CHECKDUMPPRGNAME) $(CCOPT) $(DEBUG) $(CHECKDUMPOBJ)

redisql-check-aof: $(CHECKAOFOBJ)
	$(CC) -o $(CHECKAOFPRGNAME) $(CCOPT) $(DEBUG) $(CHECKAOFOBJ)

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(PRGNAME) $(GENBENCHPRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) $(CHECKDUMPPRGNAME) $(CHECKAOFPRGNAME) *.o *.gcda *.gcno *.gcov

dep:
	$(CC) -MM *.c

staticsymbols:
	tclsh utils/build-static-symbols.tcl > staticsymbols.h

test:
	tclsh8.5 tests/test_helper.tcl

bench:
	./redisql-benchmark

log:
	git log '--pretty=format:%ad %s (%cn)' --date=short > Changelog

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

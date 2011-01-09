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
#     NOTE: for 64-bit compilation, change line 28 in luajit/src/Makefile to
#            "CC= gcc -m64 -march=native"
# 4.) export LD_LIBRARY_PATH=/usr/local/lib/
# 5.) set LUAJIT= yes
LUAJIT= no
#LUAJIT= yes
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  @echo "Sun not supported (no BigEndian support) - sorry"
  @exit
else
  ifeq ($(LUAJIT),yes)
    CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF) -I./luajit/src/
    CCLINK?= -lm -pthread -L./luajit/src/ -lluajit -ldl
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
  LUADIR=./luajit/src/
  MTYPE=
else
  LUADIR=./lua-5.1.4/src/
  MTYPE=$@
endif

OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o bt.o bt_code.o bt_output.o alsosql.o sixbit.o row.o index.o rdb_alsosql.o join.o bt_iterator.o wc.o denorm.o scan.o orderby.o lua_integration.o parser.o nri.o legacy.o cr8tblas.o rpipe.o range.o desc.o aobj.o stream.o colparse.o
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
  alsosql.h bt_iterator.h index.h bt.h sixbit.h row.h common.h \
  btree.h btreepriv.h row.h join.h wc.h rdb_alsosql.h \
  rpipe.h nri.h lua_integration.h aobj.h
  
sds.o: sds.c sds.h zmalloc.h
zipmap.o: zipmap.c zmalloc.h
zmalloc.o: zmalloc.c config.h

# ALSOSQL
alsosql.o: alsosql.h bt_iterator.h index.h range.h desc.h bt.h sixbit.h row.h cr8tblas.h aobj.h redis.h common.h
aobj.o: aobj.h redis.h common.h
bt.o: bt.h btree.h btreepriv.h row.h stream.h aobj.h redis.h common.h
bt_code.o: btree.h btreepriv.h redis.h common.h
bt_output.o: btree.h btreepriv.h redis.h
bt_iterator.o: bt_iterator.h btree.h btreepriv.h stream.h aobj.h redis.h common.h
cr8tblas.o: cr8tblas.h colparse.h wc.h rpipe.h aobj.h redis.h common.h
denorm.o: bt_iterator.h alsosql.h parser.h legacy.h aobj.h redis.h common.h
desc.o: desc.h colparse.h bt_iterator.h bt.h aobj.h redis.h common.h
index.o: index.h colparse.h bt_iterator.h alsosql.h orderby.h nri.h legacy.h stream.h aobj.h redis.h common.h
join.o: join.h wc.h colparse.h bt_iterator.h alsosql.h orderby.h aobj.h redis.h common.h
lua_integration.o: lua_integration.h rpipe.h redis.h zmalloc.h
legacy.o: legacy.h colparse.h alsosql.h redis.h common.h
#norm.o: sql.h bt_iterator.h legacy.h redis.h common.h -> DEPRECATED
nri.o: nri.h stream.h colparse.h alsosql.h aobj.h redis.h common.h
orderby.o: orderby.h join.h aobj.h redis.h common.h
parser.o: parser.h redis.h zmalloc.h common.h
range.o: range.h colparse.h orderby.h bt_iterator.h bt.h aobj.h redis.h common.h
rdb_alsosql.o: rdb_alsosql.h bt_iterator.h alsosql.h nri.h index.h stream.h redis.h common.h
row.o: row.h alsosql.h aobj.h redis.h common.h
rpipe.o: rpipe.h redis.h common.h
scan.o: alsosql.h colparse.h bt_iterator.h wc.h orderby.h aobj.h redis.h
sixbit.o: sixbit.h
stream.o: aobj.h common.h
wc.o: wc.h colparse.h bt_iterator.h cr8tblas.h rpipe.h redis.h common.h
colparse.o: colparse.h alsosql.h redis.h common.h

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

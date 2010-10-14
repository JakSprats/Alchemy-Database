# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
OPTIMIZATION?=-O2
ifeq ($(uname_S),SunOS)
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W -D__EXTENSIONS__ -D_XPG6
  CCLINK?= -ldl -lnsl -lsocket -lm -lpthread
else
  CFLAGS?= -std=c99 -pedantic $(OPTIMIZATION) -Wall -W $(ARCH) $(PROF)
  CCLINK?= -lm -pthread
endif
CCOPT= $(CFLAGS) $(CCLINK) $(ARCH) $(PROF)
DEBUG?= -g -rdynamic -ggdb 

OBJ = adlist.o ae.o anet.o dict.o redis.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o bt.o bt_code.o bt_output.o alsosql.c sixbit.c row.c index.c rdb_alsosql.c join.c norm.c bt_iterator.c sql.c denorm.c store.c scan.c
BENCHOBJ = ae.o anet.o redis-benchmark.o sds.o adlist.o zmalloc.o
CLIOBJ = anet.o sds.o adlist.o redis-cli.o zmalloc.o linenoise.o
CHECKDUMPOBJ = redis-check-dump.o lzf_c.o lzf_d.o
CHECKAOFOBJ = redis-check-aof.o

PRGNAME = redisql-server
BENCHPRGNAME = redisql-benchmark
CLIPRGNAME = redisql-cli
CHECKDUMPPRGNAME = redisql-check-dump
CHECKAOFPRGNAME = redisql-check-aof

all: redisql-server redisql-benchmark redisql-cli redisql-check-dump redisql-check-aof

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
index.o: redis.h index.h common.h bt_iterator.h alsosql.h
bt_iterator.o: redis.h bt_iterator.h btree.h btreepriv.h
norm.o: redis.h sql.h bt_iterator.h
join.o: redis.h join.h bt_iterator.h alsosql.h
store.o: redis.h store.h bt_iterator.h alsosql.h
rdb_alsosql.o: redis.h rdb_alsosql.h common.h bt_iterator.h alsosql.h
sixbit.o: sixbit.h
row.o: row.h common.h alsosql.h
sql.o: redis.h sql.h bt_iterator.h
denorm.o: redis.h denorm.h bt_iterator.h alsosql.h
scan.o: alsosql.h bt_iterator.h

redisql-server: $(OBJ)
	$(CC) -o $(PRGNAME) $(CCOPT) $(DEBUG) $(OBJ)

redisql-benchmark: $(BENCHOBJ)
	$(CC) -o $(BENCHPRGNAME) $(CCOPT) $(DEBUG) $(BENCHOBJ)

redisql-cli: $(CLIOBJ)
	$(CC) -o $(CLIPRGNAME) $(CCOPT) $(DEBUG) $(CLIOBJ)

redisql-check-dump: $(CHECKDUMPOBJ)
	$(CC) -o $(CHECKDUMPPRGNAME) $(CCOPT) $(DEBUG) $(CHECKDUMPOBJ)

redisql-check-aof: $(CHECKAOFOBJ)
	$(CC) -o $(CHECKAOFPRGNAME) $(CCOPT) $(DEBUG) $(CHECKAOFOBJ)

.c.o:
	$(CC) -c $(CFLAGS) $(DEBUG) $(COMPILE_TIME) $<

clean:
	rm -rf $(PRGNAME) $(BENCHPRGNAME) $(CLIPRGNAME) $(CHECKDUMPPRGNAME) $(CHECKAOFPRGNAME) *.o *.gcda *.gcno *.gcov

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

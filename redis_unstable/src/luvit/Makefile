CFLAGS=$(shell luvit --cflags)
LIBS=$(shell luvit --libs)

E_CFLAGS=${CFLAGS} -I../
E_BASEDIR=../../..
E_LIBS= -lm -pthread -ldl ${E_BASEDIR}/redis/src/libalchemy.a ${E_BASEDIR}/DB/libx_db.a ${E_BASEDIR}/redis/deps/jemalloc/lib/libjemalloc.a ${E_BASEDIR}/redis/deps/hiredis/libhiredis.a ${E_BASEDIR}/redis/deps/lua/src/liblua.a ${LIBS}

all: alchemy.luvit

alchemy.o: alchemy.c
	$(CC) -c $< -o $@ ${E_CFLAGS}

alchemy.luvit: alchemy.o
	$(CC) -o alchemy.luvit alchemy.o ${E_LIBS}

clean:
	rm -f alchemy.luvit alchemy.o

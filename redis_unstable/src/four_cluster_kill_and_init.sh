#!/bin/bash

DIRS="./ ./SERVER2 ./SERVER2/3/ ./SERVER2/4/"
PORTS="8080 8081 8082 8083"

echo killall ./alchemy-cli
killall ./alchemy-cli
echo sleep 1
sleep 1

if [ "$1" == "clean" ]; then
  echo killall -9 alchemy-server
  killall -9 alchemy-server
  for d in $DIRS; do
    (cd $d; rm dump.rdb);
  done
  echo "delete from posts; delete from follows; delete from users;" | \
         mysql -uroot twitter
else
  for p in $PORTS; do 
    echo ./alchemy-cli -p $p SAVE
    ./alchemy-cli -p $p SAVE
    echo ./alchemy-cli -p $p SHUTDOWN
    ./alchemy-cli -p $p SHUTDOWN
  done
  if [ "$1" == "shutdown" ]; then
    exit 0;
  fi
fi

# TODO check that all are dead
echo sleep 2
sleep 2

DIRS="./ ./SERVER2 ./SERVER2/3/ ./SERVER2/4/"
for d in $DIRS; do
  (cd $d
    echo "(cd $d; ./alchemy-server docroot/DIST/redis.conf.dist_alc_twitter >> OUTPUT & </dev/null)"
    ./alchemy-server docroot/DIST/redis.conf.dist_alc_twitter >> OUTPUT & </dev/null
  )
done

if [ "$1" != "nomysql" ]; then
  echo sleep 2
  sleep 2
  echo killall -9 alchemy-cli
  killall -9 alchemy-cli
  echo sleep 2
  sleep 2
  for p in $PORTS; do 
    echo "./alchemy-cli -p $p -po SUBPIPE sql | mysql -uroot twitter"
    (./alchemy-cli -p $p -po SUBPIPE sql | mysql -uroot twitter) & < /dev/null
  done
fi

echo sleep 2
sleep 2
for p in $PORTS; do 
  echo ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE
  ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE "echo" & < /dev/null
done

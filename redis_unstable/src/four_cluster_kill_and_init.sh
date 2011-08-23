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
else
  for p in $PORTS; do 
    echo ./alchemy-cli -p $p SAVE
    ./alchemy-cli -p $p SAVE
    echo ./alchemy-cli -p $p SHUTDOWN
    ./alchemy-cli -p $p SHUTDOWN
  done
  echo ./alchemy-cli -p 9999 SAVE
  ./alchemy-cli -p 9999 SAVE
  echo ./alchemy-cli -p 9999 SHUTDOWN
  ./alchemy-cli -p 9999 SHUTDOWN
  if [ "$1" == "shutdown" ]; then
    exit 0;
  fi
fi

# TODO check that all are dead
echo sleep 2
sleep 2

for d in $DIRS; do
  (cd $d
    echo "(cd $d; ./alchemy-server docroot/DIST/redis.conf >> OUTPUT & </dev/null)"
    ./alchemy-server docroot/DIST/redis.conf >> OUTPUT & </dev/null
  )
done
echo "(cd ./docroot/BRIDGE/; ./alchemy-server redis.conf >> OUTPUT & </dev/null)"
(cd ./docroot/BRIDGE/;
  ./alchemy-server redis.conf >> OUTPUT & </dev/null
)

echo sleep 2
sleep 2
for p in $PORTS; do 
  echo ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE
  ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE "echo" & < /dev/null
done

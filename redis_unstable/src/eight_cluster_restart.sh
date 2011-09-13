#!/bin/bash

DIRS="1 2 3 4 5 6 7 8"
PORTS="8001 8002 8003 8004 9005 9006 9007 9008"
B_PORTS="20000 20001"
B_DIRS="1 2"
S_PORTS="18001 18002 18003 18004 19005 19006 19007 19008 30000 30001"

echo sudo killall haproxy
sudo killall haproxy
echo sudo killall stunnel
sudo killall stunnel
echo killall ./alchemy-cli
killall ./alchemy-cli

if [ "$1" == "clean" ]; then
  echo killall -9 alchemy-server
  killall -9 alchemy-server
  for d in $DIRS; do   (cd SERVERS/$d;        rm dump.rdb); done
  for d in $B_DIRS; do (cd SERVERS/BRIDGE/$d; rm dump.rdb); done
  exit 0;
else
  for p in $PORTS $B_PORTS $S_PORTS; do 
    echo ./alchemy-cli -p $p SAVE
    ./alchemy-cli -p $p SAVE
    echo ./alchemy-cli -p $p SHUTDOWN
    ./alchemy-cli -p $p SHUTDOWN
  done
  if [ "$1" == "shutdown" ]; then
    exit 0;
  fi
fi

# TODO check that ALL are dead
echo sleep 2
sleep 2

echo sudo stunnel SERVERS/BRIDGE/stunnel_client.conf;
sudo stunnel SERVERS/BRIDGE/stunnel_client.conf;
echo sudo stunnel SERVERS/BRIDGE/stunnel_server.conf 
sudo stunnel SERVERS/BRIDGE/stunnel_server.conf 

for d in $DIRS; do
  (cd SERVERS/$d
    ../../alchemy-server redis.conf >> OUTPUT & </dev/null
    PID=$!
    echo "(cd SERVERS/$d; ../../alchemy-server redis.conf >> OUTPUT & </dev/null) PID: $PID"
    (cd SLAVE;
      ../../../alchemy-server redis.conf >> OUTPUT & </dev/null
      PID=$!
      echo "(cd SERVERS/$d/SLAVE; ../../../alchemy-server redis.conf >> OUTPUT & </dev/null) PID: $PID"
    )
  )
done
for d in $B_DIRS; do
(cd SERVERS/BRIDGE/$d;
  ../../../alchemy-server redis.conf >> OUTPUT & </dev/null
  PID=$!
  echo "(cd SERVERS/BRIDGE/$d; ../../../alchemy-server redis.conf >> OUTPUT & </dev/null) PID: $PID"
  (cd SLAVE;
    ../../../../alchemy-server redis.conf >> OUTPUT & </dev/null
    PID=$!
    echo "(cd SERVERS/BRIDGE/$d/SLAVE; ../../../alchemy-server redis.conf >> OUTPUT & </dev/null) PID: $PID"
  )
)
done

#TODO check that ALL are UP
echo sleep 2
sleep 2
echo HAPROXY
for d in $DIRS; do
  echo "haproxy -f SERVERS/$d/haproxy.cfg >> HAPROXY_OUTPUT & </dev/null"
  haproxy -f SERVERS/$d/haproxy.cfg >> HAPROXY_OUTPUT & </dev/null
done

echo LOOPBACK PIPES
for p in $PORTS; do 
  echo ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE
  ./alchemy-cli -p $p -ph 127.0.0.1 -pp $p SUBPIPE "echo" & < /dev/null
done

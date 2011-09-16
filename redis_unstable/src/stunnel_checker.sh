#!/bin/bash

BS_PORTS="30000 30001"

#   -- 1.) determine state [B1, B2, B1_B2]
#   -- 2.) remove old stunnel_server.conf symlink
#   -- 3.) use state to create new stunnel_server.conf symlink
#   -- 4.) SIGHUP stunner(server) to reload config file

LAST_DOWNED_BRIDGES=""
while true; do
  DOWNED_BRIDGES=""
  for p in $BS_PORTS; do 
    DOWN=$(./alchemy-cli -p $p GET MASTER_DOWN)
    if [ -n "$DOWN" ]; then
      DOWNED_BRIDGES="$DOWNED_BRIDGES $p"
    fi
  done
  if [ -n "$DOWNED_BRIDGES" -a "$DOWNED_BRIDGES" != "$LAST_DOWNED_BRIDGES" ]; then
    (cd ./SERVERS/BRIDGE/
      echo kill  $(cat /usr/local/var/lib/stunnel/stunnel_server.pid);
      kill  $(cat /usr/local/var/lib/stunnel/stunnel_server.pid);
      echo rm stunnel_server.conf ; 
      rm stunnel_server.conf ; 
      if [ "$DOWNED_BRIDGES" == " 30000" ]; then
        echo ln -s stunnel_server_SLAVEFAILOVER_B1.conf stunnel_server.conf;
        ln -s stunnel_server_SLAVEFAILOVER_B1.conf stunnel_server.conf;
      elif [ "$DOWNED_BRIDGES" == " 30001" ]; then
        echo ln -s stunnel_server_SLAVEFAILOVER_B2.conf stunnel_server.conf;
        ln -s stunnel_server_SLAVEFAILOVER_B2.conf stunnel_server.conf;
      elif [ "$DOWNED_BRIDGES" == " 30000 30001" ]; then
        echo ln -s stunnel_server_SLAVEFAILOVER_B1_B2.conf stunnel_server.conf;
        ln -s stunnel_server_SLAVEFAILOVER_B1_B2.conf stunnel_server.conf;
      fi
      sudo stunnel stunnel_server.conf
    )
  fi
  LAST_DOWNED_BRIDGES="$DOWNED_BRIDGES"
  sleep 1;
done

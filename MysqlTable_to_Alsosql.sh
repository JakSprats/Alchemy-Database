#!/bin/bash

CLI=./redis-cli

if [ -z "$2" ]; then
  echo "Usage: $0 database-name table-name"
  exit 2;
fi

DB="$1"
TBL="$2"
(
  I=0
  mysqldump -d --compact -uroot "$DB" "$TBL" | \
      grep -v \; | tr -d \` | grep -v "PRIMARY KEY" | \
      cut -f 1 -d \( | tr -d \, | sed "s/varchar/text/g" | \
  while read a; do
    if [ $I -eq 0 ]; then
      echo -ne "${a} (";
    elif [ $I -eq 1 ]; then
      echo -ne "${a}";
    else
      echo -ne ",${a}";
    fi
    I=$[${I}+1]
  done
  echo ")"
) | $CLI

(
  mysqldump -t --compact -uroot "$DB" "$TBL" | \
     tr \) "\n" | cut -f 2- -d \( | tr -d \' | while read a; do
    if [ "$a" != ";" ]; then
      echo "INSERT INTO $TBL VALUES ("${a}")"
    fi
  done
) | $CLI

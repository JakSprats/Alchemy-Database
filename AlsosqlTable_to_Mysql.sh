#!/bin/bash

CLI=./redis-cli

if [ -z "$2" ]; then
  echo "Usage: $0 database-name table-name"
  exit 2;
fi

DB="$1"
TBL="$2"

$CLI dump "$TBL" TO MYSQL| tr \; "\n" | cut -b 4- | \
while read a; do
  echo "$a;"
done | mysql -uroot ${DB}

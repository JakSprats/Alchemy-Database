#!/bin/bash

. ./bash_functions.sh

C=200
REQ=10000000

for MOD in 100 200 400 1000 2000 5000 10000 100000; do
  $CLI DROP TABLE message_list
  init_message_list_table
  echo "INSERT $REQ rows"
  time taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO message_list VALUES "(00000000000001,00000000000001,4)"
  echo "MOD: $MOD FK-RANGE: $[${REQ}/${MOD}]"
  taskset -c 1 ./gen-benchmark -c 1 -n $MOD -s -A INT -Q UPDATE message_list SET "count=$MOD" WHERE "fk=00000000000001"
done

#!/bin/bash

. ./bash_functions.sh

C=200
REQ=10000000
MOD=100

function init_ten_million() {
  $CLI DROP TABLE message_list
  init_message_list_table
}

function insert_ten_million() {
  time taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO message_list VALUES "(000000000001,000000000001,4)"
}

function select_by_fk() {
  I=0;
  while [ $I -le 100 ]; do
    echo -ne "$I:  ";
    $CLI SELECT "COUNT(*)" FROM message_list WHERE fk = $I; I=$[${I}+1];
  done
}

function update_pileup() {
  I=$START;
  while [ $I -le $END ]; do
    J=$[${I}+1];
    $CLI UPDATE message_list SET fk = $J WHERE fk = $I;
    I=$J;
  done
}
function update_pileup_10_20() {
  START=10
  END=20
}
function update_pileup_40_60() {
  START=40
  END=60
}

function validate_pks() {
  TBL=$1
  T=$(tempfile);
  echo DUMP $TBL TO FILE $T
  time $CLI DUMP $TBL TO FILE $T
  MOD=1
  if [ -n "$2" ]; then
    MOD=$2
  fi
  time echo "
    io.input('${T}');
    count = 1;
    if ${MOD} ~= 0 then
      count = 2;
    end
    while true do
      local line = io.read();
      if line == nil then
        break;
      end
      j = string.find(line, ',');
      x = tonumber(string.sub(line, 1, j - 1));
      if (x ~= count) then
        print ('x: ' .. x .. ' c: ' .. count);
      end
      count = count + 1;
      if ${MOD} ~= 0 then
        if (count - 1) % ${MOD} == 0 then
          count = count + 1;
        end
      end
    end
    " | luajit
}

# need test that delete (or updates) on key%mod ... this will stress the BT
function init_fifty_million() {
  $CLI DROP TABLE fifty_mill
  $CLI CREATE TABLE fifty_mill "(id INT, val INT)"
}

function insert_fifty_million() {
  REQ=50000000
  time taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO fifty_mill VALUES "(000000000001,000000000001)"
}

function makeeven_fifty_million() {
  ./gen-benchmark -c 200 -n 25000000 -i 2 -A OK -Q DELETE FROM fifty_mill WHERE "id=000000000001"
}

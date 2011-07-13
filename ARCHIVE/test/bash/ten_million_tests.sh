#!/bin/bash

CLI="./redisql-cli"

C=200
REQ=10000000
MOD=100

# 10_million_MOD_100 10_million_MOD_100 10_million_MOD_100 10_million_MOD_100
# 10_million_MOD_100 10_million_MOD_100 10_million_MOD_100 10_million_MOD_100
function init_ten_mill_mod100_fk() {
  $CLI DROP TABLE ten_mill_mod100_fk
  $CLI CREATE TABLE ten_mill_mod100_fk "(id int primary key, fk int, count INT)"
  $CLI CREATE INDEX ten_mill_mod100_fk:fk:index ON ten_mill_mod100_fk \(fk\)
}

function insert_ten_mill_mod100_fk() {
  MOD=100
  time T=$(taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO ten_mill_mod100_fk VALUES "(00000000000001,00000000000001,4)")
  GHZ_2_8_SPEED=50000
  luajit validate.lua SPEED "$GHZ_2_8_SPEED" "$T"
  validate_size ten_mill_mod100_fk 958456694
}

# 10_million_MOD_10 10_million_MOD_10 10_million_MOD_10 10_million_MOD_10
# 10_million_MOD_10 10_million_MOD_10 10_million_MOD_10 10_million_MOD_10
function init_ten_mill_modTEN_fk() {
  $CLI DROP TABLE ten_mill_modTEN_fk
  $CLI CREATE TABLE ten_mill_modTEN_fk "(id int primary key, fk int, count INT)"
  $CLI CREATE INDEX ten_mill_modTEN_fk:fk:index ON ten_mill_modTEN_fk \(fk\)
}

function insert_ten_mill_modTEN_fk() {
  MOD=10
  time T=$(taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO ten_mill_modTEN_fk VALUES "(00000000000001,00000000000001,4)")
  GHZ_2_8_SPEED=50000
  luajit validate.lua SPEED "$GHZ_2_8_SPEED" "$T"
  validate_size ten_mill_modTEN_fk 934273046
}


function test_large_update() {
  if [ -z "$1" ]; then
    echo "Usage: test_large_update table"
    return;
  fi
  TBL="$1"
  time $CLI UPDATE "${TBL}" SET count=99 WHERE fk=1
}
# test_large_update ten_mill_modTEN_fk

function test_large_update_cursor() {
  if [ -z "$1" ]; then
    echo "Usage: test_large_update_cursor table [count] [incr]"
    return;
  fi
  TBL="$1"
  CNT=100000
  if [ -n "$2" ]; then
    CNT=$2
  fi
  INCR=1000
  if [ -n "$3" ]; then
    INCR=$3
  fi
  time (
    I=0;
    while [ $I -le $CNT ]; do
      $CLI UPDATE "${TBL}" SET count=55 WHERE fk=1 ORDER BY fk LIMIT $INCR OFFSET $I >/dev/null
      sleep 0.01;
      I=$[${I}+${INCR}];
    done;
  )
}
# test_large_update_cursor ten_mill_modTEN_fk 1000000


function make_tables_pks_even() {
  if [ -z "$1" ]; then
    echo "Usage: make_tables_pks_even table"
    return;
  fi
  TBL="$1"
  time taskset -c 1 ./gen-benchmark -q -c 200 -n 5000000 -i 2 -A OK -Q DELETE FROM "${TBL}" WHERE "id=00000000000001"
  validate_pks "${TBL}" 2
  CNT=$($CLI SCANSELECT "COUNT(*)" FROM "${TBL}" | cut -f 2 -d \ )
  luajit validate.lua SIZE 5000000 "$CNT"
}

function select_by_fk() {
  if [ -z "$1" ]; then
    echo "Usage: make_tables_pks_even table"
    return;
  fi
  TBL="$1"
  NORM_SIZE=100000
  if [ -n "$2" ]; then
    NORM_SIZE=$2
  fi
  I=0;
  while [ $I -lt 100 ]; do
    echo -ne "$I:  ";
    CNT=$($CLI SELECT "COUNT(*)" FROM "${TBL}" WHERE fk = $I | cut -f 2 -d \ )
    I=$[${I}+1]
    luajit validate.lua SIZE "$NORM_SIZE" "$CNT"
  done
}

function update_pileup() {
  if [ -z "$1" ]; then
    echo "Usage: make_tables_pks_even table"
    return;
  fi
  TBL="$1"
  I=$START;
  while [ $I -le $END ]; do
    J=$[${I}+1];
    $CLI UPDATE "${TBL}" SET fk = $J WHERE fk = $I;
    I=$J;
  done
}
function update_pileup_10_20() {
  START=10
  END=20
  update_pileup ten_mill_mod100_fk
}
function update_pileup_40_60() {
  START=40
  END=60
  update_pileup ten_mill_mod100_fk
}

# VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE
# VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE VALIDATE
function validate_size() {
  if [ -z "$2" ]; then
    echo "Usage: validate_size table norm_size"
    return;
  fi
  TBL="$1"
  INFO=$($CLI DESC "${TBL}" | tr "\." "\n" |grep INFO)
  TOT_SIZE=$(echo $INFO |cut -f 11 -d : |cut -f 1 -d \])
  luajit validate.lua SIZE "$2" "$TOT_SIZE"
}

function validate_pks() {
   if [ -z "$1" ]; then
     echo "Usage: validate_pks table"
     return;
   fi
   TBL=$1
   TEM=$(tempfile);
   echo DUMP $TBL TO FILE $TEM
   time $CLI DUMP $TBL TO FILE $TEM
   MOD=0
   if [ -n "$2" ]; then
     MOD=$2
   fi
   echo luajit validate.lua PKS "$TEM" "$MOD"
   luajit validate.lua PKS "$TEM" "$MOD"
}


# 50_million 50_million 50_million 50_million 50_million 50_million
# 50_million 50_million 50_million 50_million 50_million 50_million
function init_fifty_mill_pk() {
  $CLI DROP TABLE fifty_mill_pk
  $CLI CREATE TABLE fifty_mill_pk "(id INT, val INT)"
}

function insert_fifty_mill_pk() {
  REQ=50000000
  time taskset -c 1 ./gen-benchmark -q -c $C -n $REQ -s -m $MOD -A OK -Q INSERT INTO fifty_mill_pk VALUES "(00000000000001,00000000000001)"
}

# need test that delete (or updates) on key%mod ... this will stress the BT
function makeeven_fifty_mill_pk() {
  ./gen-benchmark -c 200 -n 25000000 -i 2 -A OK -Q DELETE FROM fifty_mill_pk WHERE "id=00000000000001"
}


# ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD
# ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD ISUD
function validate_isud_speed() {
  INSERT_2_8_SPEED=63000
  SELECT_2_8_SPEED=67000
  UPDATE_2_8_SPEED=56000
  DELETE_2_8_SPEED=70000
  $CLI DROP TABLE test
  $CLI CREATE TABLE test "(id int primary key, field TEXT, name TEXT)"
  SPEEDS=$(taskset -c 1 ./redisql-benchmark -n 1000000 -r 1000000 -c 200 -T -q | grep "TEST:" | cut -f 2- -d :)
  I_SPEED=$(echo "${SPEEDS}" | head -n 1)
  luajit validate.lua SPEED "$INSERT_2_8_SPEED" "$I_SPEED"
  S_SPEED=$(echo "${SPEEDS}" | head -n 2 | tail -n 1)
  luajit validate.lua SPEED "$SELECT_2_8_SPEED" "$S_SPEED"
  U_SPEED=$(echo "${SPEEDS}" | head -n 3 | tail -n 1)
  luajit validate.lua SPEED "$UPDATE_2_8_SPEED" "$U_SPEED"
  D_SPEED=$(echo "${SPEEDS}" | head -n 4 | tail -n 1)
  luajit validate.lua SPEED "$DELETE_2_8_SPEED" "$D_SPEED"
}

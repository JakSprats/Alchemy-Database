#!/bin/bash

CLI=../redisql-cli

$CLI CREATE TABLE test "(obj_id int primary key, score int)"
$CLI CREATE INDEX test_score_idx ON test \(score\)

I=0;
while [ $I -lt 10 ]; do
    N=$[${I}*10000];
    J=$[${I}+1]
    M=$[${J}*10000];
    while [ $N -le $M ]; do
      $CLI INSERT INTO test VALUES "($N,$I)";
      N=$[${N}+1];
   done;
   echo sleep 30 - to free up sockets
   sleep 30
   I=$[${I}+1];
done

I=0;
while [ $I -lt 10 ]; do
  time $CLI SELECT "COUNT(*)" FROM test where "score BETWEEN 0 AND $I";
  I=$[${I}+1];
done








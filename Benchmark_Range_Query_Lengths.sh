#!/bin/bash

POP_NUM=1000000
RQ_NUM=20000
C=200

DO_MINI=0
#DO_MINI=1
if [ $DO_MINI -eq 1 ]; then
   POP_NUM=50000
   RQ_NUM=3000
   C=100
fi

JOIN=0
if [ "$1" == "JOIN" ]; then
  JOIN=2
elif [ "$1" == "3WAY" ]; then
  JOIN=3
elif [ "$1" == "10WAY" ]; then
  JOIN=10
fi
CLI="./redisql-cli"

ACT_OPT="-R"

#CREATE TABLE
if [ $JOIN -ne 10 ]; then
  echo create table test \(id int primary key, field TEXT, name TEXT\)
  $CLI create table test \(id int primary key, field TEXT, name TEXT\)
  if [ $JOIN -eq 2 -o $JOIN -eq 3 ]; then
    echo CREATE TABLE join \(id int primary key, field TEXT, name TEXT\)
    $CLI CREATE TABLE join \(id int primary key, field TEXT, name TEXT\)
    ACT_OPT="-J"
  fi
  if [ $JOIN -eq 3 ]; then
    echo CREATE TABLE third_join \(id int primary key, field TEXT, name TEXT\)
    $CLI CREATE TABLE third_join \(id int primary key, field TEXT, name TEXT\)
    ACT_OPT="-J3"
  fi
else
  I=0
  while [ $I -lt 10 ]; do
    echo CREATE TABLE join_$I \(id int primary key, field TEXT, name TEXT\)
    $CLI CREATE TABLE join_$I \(id int primary key, field TEXT, name TEXT\)
    I=$[${I}+1];
  done
  ACT_OPT="-J10"
fi

#POPULATE DATA
if [ $JOIN -eq 10 ]; then
  echo taskset -c 1 ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ10
  taskset -c 1 ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ10 >/dev/null
else
  echo ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PT 
  taskset -c 1 ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PT >/dev/null
  if [ $JOIN -gt 0 ]; then
    echo ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ
    taskset -c 1 ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ >/dev/null
  fi
  if [ $JOIN -eq 3 ]; then
    echo ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ3
    taskset -c 1 ./redisql-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ3 >/dev/null
  fi
fi

MAX=20;
I=2;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
   I=$[${I}+1];
done

MAX=50;
I=20;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+5];
done

C=100        # these can take a while
RQ_NUM=5000  # these can take a while
MAX=200;
I=50;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+10];
done

C=50         # these can take a while
RQ_NUM=2000  # these can take a while
MAX=500;
I=200;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+100];
done

C=20        # these can take a while
RQ_NUM=500  # these can take a while
MAX=5000;
I=500;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+500];
done

C=10        # these can take a while
RQ_NUM=200  # these can take a while
MAX=50000;
I=5000;
while [ $I -le $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redisql-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+5000];
done

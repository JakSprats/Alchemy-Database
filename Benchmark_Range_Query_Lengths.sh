#!/bin/bash

POP_NUM=1000000
RQ_NUM=10000
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
  JOIN=1
elif [ "$1" == "3WAY" ]; then
  JOIN=2
fi
CLI="./redis-cli"

ACT_OPT="-R"

#CREATE TABLE
echo create table test \(id int primary key, field TEXT, name TEXT\)
$CLI create table test \(id int primary key, field TEXT, name TEXT\)
if [ $JOIN -gt 0 ]; then
  echo CREATE TABLE join \(id int primary key, field TEXT, name TEXT\)
  $CLI CREATE TABLE join \(id int primary key, field TEXT, name TEXT\)
  ACT_OPT="-J"
fi
if [ $JOIN -eq 2 ]; then
  echo CREATE TABLE third_join \(id int primary key, field TEXT, name TEXT\)
  $CLI CREATE TABLE third_join \(id int primary key, field TEXT, name TEXT\)
  ACT_OPT="-J3"
fi

#POPULATE DATA
echo ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PT 
taskset -c 1 ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PT >/dev/null
if [ $JOIN -gt 0 ]; then
  echo ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ
  taskset -c 1 ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ >/dev/null
fi
if [ $JOIN -eq 2 ]; then
  echo ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ3
  taskset -c 1 ./redis-benchmark -n $POP_NUM -r $POP_NUM -c $C  -PJ3 >/dev/null
fi

MAX=20;
I=2;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
   I=$[${I}+1];
done

MAX=50;
I=20;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+5];
done

C=100        # these can take a while
RQ_NUM=5000  # these can take a while
MAX=200;
I=50;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+10];
done

C=50         # these can take a while
RQ_NUM=2000  # these can take a while
MAX=500;
I=200;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+100];
done

C=20        # these can take a while
RQ_NUM=500  # these can take a while
MAX=5000;
I=500;
while [ $I -lt $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+500];
done

C=10        # these can take a while
RQ_NUM=200  # these can take a while
MAX=50000;
I=5000;
while [ $I -le $MAX ]; do
  echo -ne "$I: ";
  taskset -c 1 ./redis-benchmark -n $RQ_NUM -r $POP_NUM -c $C  "${ACT_OPT}" -Q $I | grep "requests per second"| tail -n1;
  I=$[${I}+5000];
done

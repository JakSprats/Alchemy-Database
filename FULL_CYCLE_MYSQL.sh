#!/bin/bash

MYSQL_AUTH=" -uroot "
CLI="./redis-cli"
SLEEP=10
SLEEP_SHORT=5

if [ -z "$2" ]; then
  echo "Usage: $0 database-name NUM"
  exit 2;
fi

DB="$1"
NUM=$2

echo "DROP TABLE FC_HASH; | mysql ${MYSQL_AUTH} ${DB}"
echo "DROP TABLE FC_HASH;" | mysql ${MYSQL_AUTH} ${DB}
echo;echo
sleep $SLEEP_SHORT

echo DEL FC
$CLI DEL FC
echo "POPULATE HashTable FC"
I=1
while [ $I -lt $NUM ]; do
  echo HSET FC key"${I}" val"${I}"
  $CLI HSET FC key"${I}" val"${I}"
  I=$[${I}+1]
done
echo $CLI HKEYS FC
$CLI HKEYS FC
echo $CLI HVALS FC
$CLI HVALS FC
echo;echo
sleep $SLEEP

echo DROP TABLE FC_HASH
$CLI DROP TABLE FC_HASH
echo $CLI CREATE TABLE FC_HASH AS DUMP FC
$CLI CREATE TABLE FC_HASH AS DUMP FC
echo $CLI DESC FC_HASH
$CLI DESC FC_HASH
echo
echo $CLI DUMP FC_HASH
$CLI DUMP FC_HASH
echo;echo
sleep $SLEEP

echo $CLI KEYS "*"
$CLI KEYS "*"
echo;echo
sleep $SLEEP_SHORT

echo ./AlsosqlTable_to_Mysql.sh ${DB} FC_HASH
./AlsosqlTable_to_Mysql.sh ${DB} FC_HASH
echo;
echo "show tables; | mysql ${MYSQL_AUTH} ${DB} "
echo "show tables;" | mysql ${MYSQL_AUTH} ${DB}
echo;
echo "select * from FC_HASH; | mysql ${MYSQL_AUTH} ${DB} "
echo "select * from FC_HASH;" | mysql ${MYSQL_AUTH} ${DB}
echo;echo
sleep $SLEEP

echo $CLI DROP TABLE FC_HASH
$CLI DROP TABLE FC_HASH
echo $CLI DEL FC
$CLI DEL FC
echo;
echo $CLI KEYS "*"
$CLI KEYS "*"
echo;echo
sleep $SLEEP

echo ./MysqlTable_to_Alsosql.sh ${DB} FC_HASH
./MysqlTable_to_Alsosql.sh ${DB} FC_HASH
echo;
echo $CLI KEYS "*"
$CLI KEYS "*"
echo;echo
sleep $SLEEP_SHORT

echo $CLI DESC FC_HASH
$CLI DESC FC_HASH
echo
echo $CLI DUMP FC_HASH
$CLI DUMP FC_HASH
echo;echo
sleep $SLEEP

echo $CLI SELECT hkey,hvalue FROM FC_HASH WHERE pk BETWEEN 1 AND ${NUM} STORE HSET FC
$CLI SELECT hkey,hvalue FROM FC_HASH WHERE pk BETWEEN 1 AND ${NUM} STORE HSET FC
echo $CLI KEYS "*"
$CLI KEYS "*"
echo;echo
sleep $SLEEP_SHORT

echo $CLI HKEYS FC
$CLI HKEYS FC
echo $CLI HVALS FC
$CLI HVALS FC

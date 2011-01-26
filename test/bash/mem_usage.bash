#!/bin/bash

CLI=./redisql-cli

echo UU
$CLI CREATE TABLE uu "(pk INT, i INT)"
NUM=100000000
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -A OK -Q INSERT INTO uu VALUES "(00000000000001,00000000000001)";

echo NO FK
$CLI CREATE TABLE pk "(id INT, t TEXT)";
NUM=50000000
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -A OK -Q INSERT INTO pk VALUES "(00000000000001,abcdefghijk)" 
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -A INT -Q DELETE FROM pk WHERE id=00000000000001
echo done, hit enter
read

echo ONE FK
NUM=10000000
$CLI CREATE TABLE fk "(pk INT, fk INT, t TEXT)";
$CLI CREATE INDEX ind_fk_fk ON fk "(fk)";
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -m 1000 -A OK -Q INSERT INTO fk VALUES "(00000000000001,00000000000001,abcdefghijk)" 
echo done, hit enter
read

echo TWO FKs
$CLI CREATE TABLE fk2 "(pk INT, fk INT, fk2 INT, t TEXT)" ;
$CLI CREATE INDEX ind_fk2_fk ON fk2 "(fk)" ;
$CLI CREATE INDEX ind_fk2_fk2 ON fk2 "(fk2)";
NUM=10000000
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -m 1000,100 -A OK -Q INSERT INTO fk2 VALUES "(00000000000001,00000000000001,00000000000001,abcdefghi)" 
echo done, hit enter
read

echo TEN FKs
$CLI CREATE TABLE fk10 "(pk INT, fk INT, fk1 INT, fk2 INT, fk3 INT, fk4 INT, fk5 INT, fk6 INT, fk7 INT, fk8 INT, fk9 INT, t TEXT)" ;
$CLI CREATE INDEX ind_fk10_fk ON fk10 "(fk)" ;
$CLI CREATE INDEX ind_fk10_fk1 ON fk10 "(fk1)" ;
$CLI CREATE INDEX ind_fk10_fk2 ON fk10 "(fk2)" ;
$CLI CREATE INDEX ind_fk10_fk3 ON fk10 "(fk3)" ;
$CLI CREATE INDEX ind_fk10_fk4 ON fk10 "(fk4)" ;
$CLI CREATE INDEX ind_fk10_fk5 ON fk10 "(fk5)" ;
$CLI CREATE INDEX ind_fk10_fk6 ON fk10 "(fk6)" ;
$CLI CREATE INDEX ind_fk10_fk7 ON fk10 "(fk7)" ;
$CLI CREATE INDEX ind_fk10_fk8 ON fk10 "(fk8)" ;
$CLI CREATE INDEX ind_fk10_fk9 ON fk10 "(fk9)" ;
NUM=10000000
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -m "40000,20000,10000,5000,2500,1250,512,256,128,64,32" -A OK -Q INSERT INTO fk10 VALUES "(00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,abcdefghi)"
echo done, hit enter
read

echo "pk text 100 chars"
$CLI CREATE TABLE pk "(id INT, t TEXT)";
time taskset -c 1 ./gen-benchmark -q -c 200 -n $NUM -s -A OK -Q INSERT INTO pk VALUES "(00000000000001,abcdefghijkaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaassssss)
echo done, hit enter

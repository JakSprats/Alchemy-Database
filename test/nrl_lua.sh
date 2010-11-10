#!/bin/bash

CLI=./redisql-cli

$CLI DROP TABLE nrl
$CLI CREATE TABLE nrl \(id int primary key, state int, message TEXT\)
$CLI CREATE INDEX nrl:pub:index ON nrl "LUA len=string.len('\$message'); client('PUBLISH','NRL:\$state','LUA:message=\$message '..'len='..len);"
$CLI INSERT INTO nrl VALUES "(1,1,hi state 1)"
#$CLI INSERT INTO nrl VALUES "(2,1,state 1 is great)"


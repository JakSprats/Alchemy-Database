#!/bin/bash

function count_alchemy-server() {
  echo -ne "$1: NUM ./alchemy-server: "
  ps ax|grep ./alchemy-server |grep -v grep |wc -l
}

count_alchemy-server "INIT"
echo killall alchemy-server
killall alchemy-server
echo sleep 2; sleep 2;

echo "STARTING 3 alchemy-server"
#V=valgrind
$V ./alchemy-server ../redis.conf.1 & </dev/null
$V ./alchemy-server ../redis.conf.2 & </dev/null
$V ./alchemy-server ../redis.conf.3 & </dev/null

echo sleep 2; sleep 2;
count_alchemy-server "START"

echo 'cluster meet 127.0.0.1 6382' | ./alchemy-cli -p 6381
echo 'cluster meet 127.0.0.1 6383' | ./alchemy-cli -p 6381
echo 'cluster nodes' | ./alchemy-cli -p 6381

echo '(0..1000).each{|x| puts "CLUSTER ADDSLOTS "+x.to_s}' | ruby | ./alchemy-cli -p 6381 >/dev/null
echo '(1001..2500).each{|x| puts "CLUSTER ADDSLOTS "+x.to_s}' | ruby | ./alchemy-cli -p 6382 >/dev/null
echo '(2501..4095).each{|x| puts "CLUSTER ADDSLOTS "+x.to_s}' | ruby | ./alchemy-cli -p 6383 >/dev/null

echo
echo

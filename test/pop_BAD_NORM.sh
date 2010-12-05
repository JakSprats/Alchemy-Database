#!/bin/bash

CLI="./redisql-cli"


$CLI SET user:1:name bill >/dev/null
$CLI SET user:1:status member >/dev/null
$CLI SET user:1:age 33 >/dev/null
$CLI SET user:1:address:city "capitol city" >/dev/null
$CLI SET user:1:address:zipcode 55566 >/dev/null

$CLI SET user:2:age 22 >/dev/null
$CLI SET user:2:status premium >/dev/null
$CLI SET user:2:name jane >/dev/null
$CLI SET user:2:address:street "345 side st" >/dev/null
$CLI SET user:2:address:city "capitol city" >/dev/null
$CLI SET user:2:address:zipcode 55566 >/dev/null

$CLI SET user:3:age 44 >/dev/null
$CLI SET user:3:name ken >/dev/null
$CLI SET user:3:status guest >/dev/null
$CLI SET user:3:address:street "876 big st" >/dev/null
$CLI SET user:3:address:city "houston" >/dev/null
$CLI SET user:3:address:zipcode 87654 >/dev/null

$CLI SET user:4:status premium >/dev/null
$CLI SET user:4:name mac >/dev/null
$CLI SET user:4:age 77 >/dev/null
$CLI SET user:4:address:street "1 side st" >/dev/null
$CLI SET user:4:address:city "capitol city" >/dev/null
$CLI SET user:4:address:zipcode 55566 >/dev/null

$CLI DROP TABLE user >/dev/null
$CLI DROP TABLE user_address >/dev/null
echo OK
$CLI NORM user address

$CLI DROP TABLE user_address >/dev/null
echo "FAIL SECOND"
$CLI NORM user address

$CLI DROP TABLE user >/dev/null
echo "FAIL FIRST"
$CLI NORM user address

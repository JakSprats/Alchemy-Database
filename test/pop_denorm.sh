#!/bin/bash

CLI="./redisql-cli"

$CLI DROP TABLE user >/dev/null
$CLI DROP TABLE user_address >/dev/null
$CLI DROP TABLE user_payment >/dev/null

$CLI SET user:1:name bill >/dev/null
$CLI SET user:1:status member >/dev/null
$CLI SET user:1:age 33 >/dev/null
$CLI SET user:1:address:city "capitol city" >/dev/null
$CLI SET user:1:address:zipcode 55566 >/dev/null
$CLI SET user:1:payment:type "credit card" >/dev/null
$CLI SET user:1:payment:account "1234567890" >/dev/null

$CLI SET user:2:age 22 >/dev/null
$CLI SET user:2:status premium >/dev/null
$CLI SET user:2:name jane >/dev/null
$CLI SET user:2:address:street "345 side st" >/dev/null
$CLI SET user:2:address:city "capitol city" >/dev/null
$CLI SET user:2:address:zipcode 55566 >/dev/null
$CLI SET user:2:payment:type "checking" >/dev/null
$CLI SET user:2:payment:account "44441111" >/dev/null

$CLI SET user:3:age 44 >/dev/null
$CLI SET user:3:name ken >/dev/null
$CLI SET user:3:status guest >/dev/null
$CLI SET user:3:address:street "876 big st" >/dev/null
$CLI SET user:3:address:city "houston" >/dev/null
$CLI SET user:3:address:zipcode 87654 >/dev/null
$CLI SET user:3:payment:type "cash" >/dev/null

$CLI SET user:4:status premium >/dev/null
$CLI SET user:4:name mac >/dev/null
$CLI SET user:4:age 77 >/dev/null
$CLI SET user:4:address:street "1 side st" >/dev/null
$CLI SET user:4:address:city "capitol city" >/dev/null
$CLI SET user:4:address:zipcode 55566 >/dev/null
$CLI SET user:4:payment:type "checking" >/dev/null
$CLI SET user:4:payment:account "333333" >/dev/null

START_SCHEMA=0
if [ -n "$1" ]; then
  START_SCHEMA=$1
fi
if [ $START_SCHEMA -eq 1 ]; then
  echo $CLI NORM user address,payment
  $CLI NORM user address,payment
  echo SELECT user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode FROM user,user_address WHERE user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5
  $CLI SELECT user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode FROM user,user_address WHERE user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5
  echo DENORM THEM NOW INTO HASHES
  echo denorm user "user:*"
  $CLI denorm user "user:*"
  echo denorm user_payment "user:*:payment"
  $CLI denorm user_payment "user:*:payment"
  echo denorm user_address "user:*:address"
  $CLI denorm user_address "user:*:address"
  echo HGETALL user:1
  $CLI HGETALL user:1
  echo HGETALL user:1:address
  $CLI HGETALL user:1:address
else
  echo $CLI NORM user
  $CLI NORM user
  echo DESC user
  $CLI DESC user
  echo DUMP user
  $CLI DUMP user
fi


#!/bin/bash

CLI="./redisql-cli"

$CLI DROP TABLE user >/dev/null
$CLI DROP TABLE user_address >/dev/null
$CLI DROP TABLE user_payment >/dev/null

$CLI HSET user:1 name bill >/dev/null
$CLI HSET user:1 age 33 >/dev/null
$CLI HSET user:1 status member >/dev/null
$CLI HSET user:1:address street "12345 main st" >/dev/null
$CLI HSET user:1:address city "capitol city" >/dev/null
$CLI HSET user:1:address zipcode 55566 >/dev/null
$CLI HSET user:1:payment type "credit card" >/dev/null
$CLI HSET user:1:payment account "1234567890" >/dev/null

$CLI HSET user:2 name jane >/dev/null
$CLI HSET user:2 age 22 >/dev/null
$CLI HSET user:2 status premium >/dev/null
$CLI HSET user:2:address street "345 side st" >/dev/null
$CLI HSET user:2:address city "capitol city" >/dev/null
$CLI HSET user:2:address zipcode 55566 >/dev/null
$CLI HSET user:2:payment type "checking" >/dev/null
$CLI HSET user:2:payment account "441111" >/dev/null

$CLI HSET user:3 name ken >/dev/null
$CLI HSET user:3 age 44 >/dev/null
$CLI HSET user:3 status guest >/dev/null
$CLI HSET user:3:address street "876 big st" >/dev/null
$CLI HSET user:3:address city "houston" >/dev/null
$CLI HSET user:3:address zipcode 87654 >/dev/null
$CLI HSET user:3:payment type "cash" >/dev/null

START_SCHEMA=0
if [ -n "$1" ]; then
  START_SCHEMA=$1
fi
if [ $START_SCHEMA -eq 1 ]; then
  echo $CLI NORM user address,payment
  $CLI NORM user address,payment
  echo SELECT user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode FROM user,user_address WHERE user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5
  $CLI SELECT user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode FROM user,user_address WHERE user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5
else
  echo $CLI NORM user
  $CLI NORM user
  echo DESC user
  $CLI DESC user
  echo DUMP user
  $CLI DUMP user
fi

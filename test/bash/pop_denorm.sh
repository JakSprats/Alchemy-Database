#!/bin/bash

. ./test/bash/pop_helper.sh

CLI="./redisql-cli"

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


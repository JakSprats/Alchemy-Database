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
else
  echo $CLI NORM user
  $CLI NORM user
  echo DESC user
  $CLI DESC user
  echo DUMP user
  $CLI DUMP user
fi

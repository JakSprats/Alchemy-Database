#!/bin/bash

CLI="./redisql-cli"

# LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS
# LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS LISTS
$CLI SET LNAME numlist

$CLI DEL numlist
$CLI RPUSH numlist zero
$CLI RPUSH numlist one
$CLI RPUSH numlist two
$CLI RPUSH numlist three
$CLI RPUSH numlist four
$CLI RPUSH numlist five
$CLI RPUSH numlist six
$CLI RPUSH numlist seven
$CLI RPUSH numlist eight
$CLI RPUSH numlist nine
$CLI RPUSH numlist ten

$CLI SET zero  0
$CLI SET one   1
$CLI SET two   22
$CLI SET three 333
$CLI SET four  4444
$CLI SET five  55555
$CLI SET six   666666
$CLI SET seven 7777777
$CLI SET eight 88888888
$CLI SET nine  999999999
$CLI SET ten   AAAAAAAAAA

#get via numlist
I=0
while [ $I -le 10 ]; do
  $CLI SET I $I
  #$CLI GET $($CLI LINDEX $($CLI GET LNAME) $($CLI GET I))
  $CLI GET \$\(LINDEX \$\(GET LNAME\) \$\(GET I\)\)
  I=$[${I}+1]
done


# HASHES HASHES HASHES HASHES HASHES HASHES HASHES HASHES
# HASHES HASHES HASHES HASHES HASHES HASHES HASHES HASHES
$CLI SET HLISTNAME hashlist

$CLI DEL hashlist
$CLI RPUSH hashlist hash0
$CLI RPUSH hashlist hash1
$CLI RPUSH hashlist hash2
$CLI RPUSH hashlist hash3
$CLI RPUSH hashlist hash4
$CLI RPUSH hashlist hash5

$CLI HSET hash0 name 0
$CLI HSET hash1 name 1
$CLI HSET hash2 name 22
$CLI HSET hash3 name 333
$CLI HSET hash4 name 4444
$CLI HSET hash5 name 55555

#get via name
I=0
while [ $I -le 5 ]; do
  $CLI SET I $I
  #$CLI HGET hash$($CLI GET I) name
  $CLI HGET hash\$\(GET I\) name
  I=$[${I}+1]
done

# get via hashlist
I=0
while [ $I -le 5 ]; do
  $CLI SET I $I
  #$CLI HGET $($CLI LINDEX $($CLI GET HLISTNAME) $($CLI GET I)) name
  $CLI HGET \$\(LINDEX \$\(GET HLISTNAME\) \$\(GET I\)\) name
  I=$[${I}+1]
done

# COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST
# COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST COMPLEX_NEST
$CLI SET HNAME \$\(LINDEX hashlist 3\)
$CLI SET HFIELD name
$CLI HSET hash3 text THREE
$CLI SET HFIELD2 text
$CLI HSET hash3 desc Number3
$CLI SET HFIELD3 desc
$CLI HMGET \$\(GET HNAME\) name text desc
$CLI HMGET \$\(MGET HNAME HFIELD\) text desc
$CLI HMGET \$\(MGET HNAME HFIELD HFIELD2\) desc
$CLI HMGET \$\(MGET HNAME HFIELD HFIELD2 HFIELD3\)

$CLI SET user:1:name TED
$CLI SET I 1
$CLI GET user:\$\(GET I\):name
$CLI SET 1:name FIRST_NAME
$CLI MGET \$\(MGET I user:1\):name

$CLI SET NAME name
$CLI GET user:\$\(GET I\):\$\(GET NAME\)

$CLI SET user:10:name KENNY
$CLI DEL set1 set2
$CLI SADD set1 1
$CLI SADD set1 10
$CLI SADD set2 1
$CLI SADD set2 20
$CLI GET user:\$\(SINTER set1 set2\):name
$CLI MGET user:\$\(SINTER set1 set2\):name HFIELD3
$CLI SADD set2 10
$CLI MGET user:\$\(SINTER set1 set2\):name
$CLI MGET user:\$\(SINTER set1 set2\):name HFIELD3

# RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD
# RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD RUN_CMD
$CLI SET CMD "MGET user:\$(SINTER set1 set2):name"
$CLI RUN CMD

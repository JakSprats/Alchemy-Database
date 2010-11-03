#!/bin/bash

CLI="./redisql-cli"

# SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE
# SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE SIMPLE
$CLI LUA 'return client("SET", "lua_R", 5*5);'
$CLI LUA 'return client("GET", "lua_R");'


# LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES
# LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES LIST_OF_HASHES
for I in 1 2 3 4 5; do
  DS=$(date +%s)
  $CLI HSET "user:1:message:$I" date "$DS"
  $CLI HSET "user:1:message:$I" text "You got wrote to at $DS"
  $CLI RPUSH "user:1:list" $I
done

INDEX=2
LUA_CMD="
list = client('LINDEX', 'user:1:list', $INDEX);
msg  = 'user:1:message:' .. list;
return client('HGET', msg, 'text');"
echo $CLI LUA "${LUA_CMD}"
$CLI LUA "${LUA_CMD}"


# SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET
# SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET SINTERGET

$CLI SET user:1:name TED
$CLI SET user:10:name KENNY
$CLI DEL set1 set2
$CLI SADD set1 1
$CLI SADD set1 10
$CLI SADD set2 1
$CLI SADD set2 10

LUA_SINTERGET_CMD='
a = client("SINTER", "set1", "set2");
t = {};
i = 1;
for k,v in pairs(a) do
  u = "user:" .. v .. ":name";
  b = client("GET", u);
  t[i] = b;
  i = i + 1;
end
return t;
';

echo $CLI LUA "${LUA_SINTERGET_CMD}"
$CLI LUA "${LUA_SINTERGET_CMD}"


# HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA
# HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA HELPER.LUA

$CLI CONFIG set luafilename helper.lua
$CLI DEL user:1:list
# LIST_OF_HASHES
$CLI LUA "return rpush_hset('user:1:list', 'user:1:message:', 'text', 'Message 00000000');"
$CLI LUA "return rpush_hset('user:1:list', 'user:1:message:', 'text', 'Message 1111');"
$CLI LUA "return rpush_hset('user:1:list', 'user:1:message:', 'text', 'Message 22');"
$CLI LUA "return l_hget('user:1:list', 0, 'user:1:message:', 'text')"
$CLI LUA "return l_hget('user:1:list', 1, 'user:1:message:', 'text')"
$CLI LUA "return l_hget('user:1:list', 2, 'user:1:message:', 'text')"
$CLI LUA "return lset_hset('user:1:list', 1, 'user:1:message:', 'text', 'ONE HAS BEEN MODIFIED');"
$CLI LUA "return l_hget('user:1:list', 0, 'user:1:message:', 'text')"
$CLI LUA "return l_hget('user:1:list', 1, 'user:1:message:', 'text')"
$CLI LUA "return l_hget('user:1:list', 2, 'user:1:message:', 'text')"
$CLI LUA "return lpush_hset('user:1:list', 'user:1:message:', 'text', 'Message -1-1-1-1-1');"
$CLI LUA "return l_hget('user:1:list', 0, 'user:1:message:', 'text')"

# HASH_OF_HASHES
$CLI LUA "return hset_hset('accounting', 'bill', 'phone', '1234567');"
$CLI LUA "return hset_hset('accounting', 'bill', 'address', '987 place st');"
$CLI LUA "return hget_hget('accounting', 'bill', 'address');"

 #SQL
$CLI SELECT health from worker where id = 2
$CLI LUA "return increment_worker_health(2);"
$CLI SELECT health from worker where id = 2

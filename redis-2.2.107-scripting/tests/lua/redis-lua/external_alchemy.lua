package.path = package.path .. ";redis-lua/src/?.lua;src/?.lua"
require "redis"
redis = Redis.connect('127.0.0.1', 6379);

-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
function create_table(tname, col_defs)
  return redis:create_table(tname, col_defs);
end
function create_table_from_redis_object(tname, redis_obj)
  return redis:create_table_from_redis_object(tname, redis_obj);
end
function create_table_as(tname, redis_command)
  return redis:create_table_as(tname, redis_command)
end
function drop_table(tname)
  return redis:drop_table(tname);
end
function desc(tname)
  return redis:desc(tname);
end
function dump(tname)
  return redis:dump(tname);
end
function dump_to_mysql(tname, msname)
  return redis:dump_to_mysql(tname, msname);
end
function dump_to_file(tname, fname)
  return redis:dump_to_file(tname, fname);
end

function create_index(iname, tname, column)
  return redis:create_index(iname, tname, column);
end
function create_unique_index(iname, tname, column)
  return redis:create_unique_index(iname, tname, column);
end
function create_trigger(trname, tname, cmd)
  return redis:create_trigger(trname, tname, cmd);
end
function drop_index(iname)
  return redis:drop_index(iname);
end

function insert(tname, values_list)
  return redis:insert(tname, values_list);
end

function insert_return_size(tname, values_list)
  return redis:insert_return_size(tname, values_list);
end

function select(...)
  return redis:select(...);
end
function select_count(...)
  return redis:select_count(...);
end

function select_store(col_list, tname, where_clause, redis_command)
  return redis:select_store(col_list, tname, where_clause, redis_command);
end

function scanselect(...)
  return redis:scanselect(...);
end
function scanselect_count(...)
  return redis:scanselect_count(...);
end

function delete(tname, where_clause)
  return redis:delete(tname, where_clause);
end

function update(tname, update_list, where_clause)
  return redis:update(tname, update_list, where_clause);
end

function normalize(main_wildcard, secondary_wildcard_list)
  return redis:normalize(main_wildcard, secondary_wildcard_list);
end

function denormalize(tname, main_wildcard)
  return redis:denormalize(tname, main_wildcard);
end

function lua(x)
  return redis:lua(x);
end

-- REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION
-- REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION
function rewriteaof()
    return redis:rewriteaof()
end
function set(key, val)
    return redis:set(key, val)
end
function auth(password)
    return redis:auth(password)
end
function info()
    return redis:info()
end
function config(args)
    return redis:config(args)
end
function flushdb()
    return redis:flushdb()
end
function flushall()
    return redis:flushall()
end
function save()
    return redis:save()
end
function bgsave()
    return redis:bgsave()
end
function bgrewriteaof()
    return redis:bgrewriteaof()
end
function get(key)
    return redis:get(key)
end
function getset(key, value)
    return redis:getset(key, value)
end
function mget(...)
    return redis:mget(...)
end
function append(key, value)
    return redis:append(key, value)
end
function substr(key, start, stop)
    return redis:substr(key, start, stop)
end
function hgetall(key)
    return redis:hgetall(key)
end
function hget(key, field)
    return redis:hget(key, field)
end
function hdel(key, field)
    return redis:hdel(key, field)
end
function hkeys(key)
    return redis:hkeys(key)
end
function keys(pattern)
    return redis:keys(pattern)
end
function randomkey()
    return redis:randomkey()
end
function echo(value)
    return redis:echo(value)
end
function ping()
    return redis:ping()
end
function lastsave()
    return redis:lastsave()
end
function dbsize()
    return redis:dbsize()
end
function exists(key)
    return redis:exists(key)
end
function llen(key)
    return redis:llen(key)
end
function lrange(key, start, stop)
    return redis:lrange(key, start, stop)
end
function ltrim(key, start, stop)
    return redis:ltrim(key, start, stop)
end
function lindex(key, index)
    return redis:lindex(key, index)
end
function linsert(key, where, pivot, value)
    return redis:linsert(key, where, pivot, value)
end
function lset(key, index, value)
    return redis:lset(key, index, value)
end
function lrem(key, count, value)
    return redis:lrem(key, count, value)
end
function rpush(key, value)
    return redis:rpush(key, value)
end
function rpushx(key, value)
    return redis:rpushx(key, value)
end
function lpush(key, value)
    return redis:lpush(key, value)
end
function lpushx(key, value)
    return redis:lpushx(key, value)
end
function rpop(key)
    return redis:rpop(key)
end
function blpop(...)
    return redis:blpop(...)
end
function brpop(...)
    return redis:brpop(...)
end
function rpoplpush(source, destination)
    return redis:rpoplpush(source, destination)
end
function lpop(key)
    return redis:lpop(key)
end
function smembers(key)
    return redis:smembers(key)
end
function sismember(key, member)
    return redis:sismember(key, member)
end
function sadd(key, value)
    return redis:sadd(key, value)
end
function srem(key, value)
    return redis:srem(key, value)
end
function smove(source, destination, member)
    return redis:smove(source, destination, member)
end
function spop(key)
    return redis:spop(key)
end
function scard(key)
    return redis:scard(key)
end
function sinter(...)
    return redis:sinter(...)
end
function sinterstore(destination, ...)
    return redis:sinterstore(destination, ...)
end
function sunion(...)
    return redis:sunion(...)
end
function sunionstore(destination, ...)
    return redis:sunionstore(destination, ...)
end
function sdiff(...)
    return redis:sdiff(...)
end
function sdiffstore(destination, ...)
    return redis:sdiffstore(destination, ...)
end
function srandmember(key)
    return redis:srandmember(key)
end
function zadd(key, score, member)
    return redis:zadd(key, score, member)
end
function zrank(key, member)
    return redis:zrank(key, member)
end
function zrevrank(key, member)
    return redis:zrevrank(key, member)
end
function zincrby(key, increment, member)
    return redis:zincrby(key, increment, member)
end
function zcard(key)
    return redis:zcard(key)
end
function zrange(key, start, stop, ...)
    return redis:zrange(key, start, stop, ...)
end
function zrangebyscore(key, min, max, ...)
    return redis:zrangebyscore(key, min, max, ...)
end
function zcount(key, start, stop)
    return redis:zcount(key, start, stop)
end
function zrevrange(key, start, stop, ...)
    return redis:zrevrange(key, start, stop, ...)
end
function zremrangebyscore(key, min, max)
    return redis:zremrangebyscore(key, min, max)
end
function zremrangebyrank(key, start, stop)
    return redis:zremrangebyrank(key, start, stop)
end
function zscore(key, member)
    return redis:zscore(key, member)
end
function zrem(key, member)
    return redis:zrem(key, member)
end
function zinterstore(destination, keys, ...)
    return redis:zinterstore(destination, keys, ...)
end
function zunionstore(destination, keys, ...)
    return redis:zunionstore(destination, keys, ...)
end
function move(key, db)
    return redis:move(key, db)
end
function setnx(key, value)
    return redis:setnx(key, value)
end
function del(...)
    return redis:del(...)
end
function rename(old_name, new_name)
    return redis:rename(old_name, new_name)
end
function renamenx(old_name, new_name)
    return redis:renamenx(old_name, new_name)
end
function expire(key, seconds)
    return redis:expire(key, seconds)
end
function persist(key)
    return redis:persist(key)
end
function ttl(key)
    return redis:ttl(key)
end
function expireat(key, unix_time)
    return redis:expireat(key, unix_time)
end
function hset(key, field, value)
    return redis:hset(key, field, value)
end
function hsetnx(key, field, value)
    return redis:hsetnx(key, field, value)
end
function hmset(key, ...)
    return redis:hmset(key, ...)
end
function hmget(key, ...)
    return redis:hmget(key, ...)
end
function hlen(key)
    return redis:hlen(key)
end
function hvals(key)
    return redis:hvals(key)
end
function hincrby(key, field, increment)
    return redis:hincrby(key, field, increment)
end
function discard()
    return redis:discard()
end
function hexists(key, field)
    return redis:hexists(key, field)
end
function monitor()
    return redis:monitor()
end
function debug(...)
    return redis:debug(...)
end
function sync()
    return redis:sync()
end
function setex(key, ttl, value)
    return redis:setex(key, ttl, value)
end
function mset(...)
    return redis:mset(...)
end
function msetnx(...)
    return redis:msetnx(...)
end
function sort(key, ...)
    return redis:sort(key, ...)
end
function incr(key)
    return redis:incr(key)
end
function incrby(key, increment)
    return redis:incrby(key, increment)
end
function decr(key)
    return redis:decr(key)
end
function decrby(key, decrement)
    return redis:decrby(key, decrement)
end
function atype(key) -- type is a keyword in LUA
    return redis:type(key)
end
function quit()
    return redis:quit()
end
function shutdown()
    return redis:shutdown()
end
function slaveof(host, port)
    return redis:slaveof(host, port)
end
function watch(...)
    return redis:watch(...)
end
function unwatch()
    return redis:unwatch()
end
function exec()
    return redis:exec()
end
function publish(channel, message)
    return redis:publish(channel, message)
end
function unsubscribe(...)
    return redis:unsubscribe(...)
end
function punsubscribe(...)
    return redis:punsubscribe(...)
end
function subscribe(...)
    return redis:subscribe(...)
end
function psubscribe(...)
    return redis:psubscribe(...)
end

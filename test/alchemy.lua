
-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
function create_table(tname, col_defs)
  return client('create', 'table', tname, "(" .. col_defs .. ")");
end
function create_table_from_redis_object(tname, redis_obj)
  return client('create', 'table', tname, "AS DUMP " .. redis_obj);
end
function create_table_as(tname, redis_command)
  return client('create', 'table', tname, "AS " .. redis_command);
end
function drop_table(tname)
  return client('drop', 'table', tname);
end
function desc(tname)
  return client('desc', tname);
end
function dump(tname)
  return client('dump', tname);
end
function dump_to_mysql(tname, msname)
    if (msname) then
        return client('dump', tname, "TO", "MYSQL", msname);
    else
        return client('dump', tname, "TO", "MYSQL");
    end
end
function dump_to_file(tname, fname)
  return client('dump', tname, "TO", "FILE", fname);
end


function create_index(iname, tname, column)
   col_w_paren = "(" .. column .. ")";
  return client('create', "INDEX", iname, "ON", tname, col_w_paren);
end

function drop_index(iname)
  return client('drop', 'index', iname);
end

function insert(tname, values_list)
  return client('insert', "INTO", tname, "VALUES", "(" .. values_list .. ")");
end

function insert_return_size(tname, values_list)
  return client('insert', "INTO", tname, "VALUES", "(" .. values_list .. ")",
                "RETURN", "SIZE");
end

-- "select" is used in both redis and Redisql, so it must be overridden here
-- for redis: select(db)
-- for SQL: select(col_list, tname, where_clause)
function select(...)
  local args = {...};
  argCount = #args;
  if (argCount == 1) then
    return client('select', args[1]);
  else
    return client('select', args[1], "FROM", args[2], "WHERE", args[3]);
  end
end

function select_store(col_list, tname, where_clause, redis_command)
  wc = where_clause .. " STORE " .. redis_command;
  return client('select', col_list, "FROM", tname, "WHERE", wc);
end

function scanselect(...)
  local args = {...};
  argCount = #args;
  if (argCount == 2) then
    return client('scanselect', args[1], "FROM", args[2]);
  else
    return client('scanselect', args[1], "FROM", args[2], "WHERE", args[3]);
  end
end

function delete(tname, where_clause)
  return client('delete', "FROM", tname, "WHERE", where_clause);
end

function update(tname, update_list, where_clause)
  return client('update', tname, "SET", update_list, "WHERE", where_clause);
end

function normalize(main_wildcard, secondary_wildcard_list)
  return client('norm', main_wildcard, secondary_wildcard_list);
end

function denormalize(tname, main_wildcard)
  return client('denorm', main_wildcard);
end

-- REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION
-- REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION REDIS_INTEGRATION
function rewriteaof()
  return client('rewriteaof', fname);
end

function set(key, val)
  return client('set', key, val);
end

function auth(password)
  return client('auth', password)
end

function info()
  return client('info');
end

function config(args)
  return client('config', args);
end

function flushdb()
  return client('flushdb')
end

function flushall()
  return client('flushall')
end

function save()
  return client('save')
end

function bgsave()
  return client('bgsave')
end

function bgrewriteaof()
  return client('bgrewriteaof')
end

function get(key)
  return client('get', key)
end

function getset(key, value)
  return client('getset', key, value)
end

function mget(...)
  return client('mget', ...)
end

function append(key, value)
  return client('append', key, value)
end

function substr(key, start, stop)
  return client('substr', key, start, stop)
end

function hgetall(key)
  return client('hgetall', key)
end

function hget(key, field)
  return client('hget', key, field)
end

function hdel(key, field)
  return client('hdel', key, field)
end

function hkeys(key)
  return client('hkeys', key)
end

function keys(pattern)
  return client('keys', pattern)
end

function randomkey()
  return client('randomkey')
end

function echo(value)
  return client('echo', value)
end

function ping()
  return client('ping')
end

function lastsave()
  return client('lastsave')
end

function dbsize()
  return client('dbsize')
end

function exists(key)
  return client('exists', key)
end

function llen(key)
  return client('llen', key)
end

function lrange(key, start, stop)
  return client('lrange', key, start, stop)
end

function ltrim(key, start, stop)
  return client('ltrim', key, start, stop)
end

function lindex(key, index)
  return client('lindex', key, index)
end

function linsert(key, where, pivot, value)
  return client('linsert', key, where, pivot, value)
end

function lset(key, index, value)
  return client('lset', key, index, value)
end

function lrem(key, count, value)
  return client('lrem', key, count, value)
end

function rpush(key, value)
  return client('rpush', key, value)
end

function rpushx(key, value)
  return client('rpushx', key, value)
end

function lpush(key, value)
  return client('lpush', key, value)
end

function lpushx(key, value)
  return client('lpushx', key, value)
end

function rpop(key)
  return client('rpop', key)
end

function blpop(...)
  return client('blpop', ...)
end

function brpop(...)
  return client('brpop', ...)
end

function rpoplpush(source, destination)
  return client('rpoplpush', source, destination)
end

function lpop(key)
  return client('lpop', key)
end

function smembers(key)
  return client('smembers', key)
end

function sismember(key, member)
  return client('sismember', key, member)
end

function sadd(key, value)
  return client('sadd', key, value)
end

function srem(key, value)
  return client('srem', key, value)
end

function smove(source, destination, member)
  return client('smove', source, destination, member)
end

function spop(key)
  return client('spop', key)
end

function scard(key)
  return client('scard', key)
end

function sinter(...)
  return client('sinter', ...)
end

function sinterstore(destination, ...)
  return client('sinterstore', destination, ...)
end

function sunion(...)
  return client('sunion', ...)
end

function sunionstore(destination, ...)
  return client('sunionstore', destination, ...)
end

function sdiff(...)
  return client('sdiff', ...)
end

function sdiffstore(destination, ...)
  return client('sdiffstore', destination, ...)
end

function srandmember(key)
  return client('srandmember', key)
end

function zadd(key, score, member)
  return client('zadd', key, score, member)
end

function zrank(key, member)
  return client('zrank', key, member)
end

function zrevrank(key, member)
  return client('zrevrank', key, member)
end

function zincrby(key, increment, member)
  return client('zincrby', key, increment, member)
end

function zcard(key)
  return client('zcard', key)
end

function zrange(key, start, stop, ...)
  return client('zrange', key, start, stop, ...)
end

function zrangebyscore(key, min, max, ...)
  return client('zrangebyscore', key, min, max, ...)
end

function zcount(key, start, stop)
  return client('zcount', key, start, stop)
end

function zrevrange(key, start, stop, ...)
  return client('zrevrange', key, start, stop, ...)
end

function zremrangebyscore(key, min, max)
  return client('zremrangebyscore', key, min, max)
end

function zremrangebyrank(key, start, stop)
  return client('zremrangebyrank', key, start, stop)
end

function zscore(key, member)
  return client('zscore', key, member)
end

function zrem(key, member)
  return client('zrem', key, member)
end

function zinterstore(destination, keys, ...)
  return client('zinterstore', destination, keys.size, ...)
end

function zunionstore(destination, keys, ...)
  return client('zunionstore', destination, keys.size, ...)
end

function move(key, db)
  return client('move', key, db)
end

function setnx(key, value)
  return client('setnx', key, value)
end

function del(...)
  return client('del', ...)
end

function rename(old_name, new_name)
  return client('rename', old_name, new_name)
end

function renamenx(old_name, new_name)
  return client('renamenx', old_name, new_name)
end

function expire(key, seconds)
  return client('expire', key, seconds)
end

function persist(key)
  return client('persist', key)
end

function ttl(key)
  return client('ttl', key)
end

function expireat(key, unix_time)
  return client('expireat', key, unix_time)
end

function hset(key, field, value)
  return client('hset', key, field, value)
end

function hsetnx(key, field, value)
  return client('hsetnx', key, field, value)
end

function hmset(key, ...)
  return client('hmset', key, ...)
end

function hmget(key, ...)
  return client('hmget', key, ...)
end

function hlen(key)
  return client('hlen', key)
end

function hvals(key)
  return client('hvals', key)
end

function hincrby(key, field, increment)
  return client('hincrby', key, field, increment)
end

function discard()
  return client('discard')
end

function hexists(key, field)
  return client('hexists', key, field)
end

function monitor()
  return client('monitor')
end

function debug(...)
  return client('debug', ...)
end

function sync()
  return client('sync')
end

function setex(key, ttl, value)
  return client('setex', key, ttl, value)
end

function mset(...)
  return client('mset', ...)
end

function msetnx(...)
  return client('msetnx', ...)
end

function sort(key, ...)
  return client('sort', key, ...)
end

function incr(key)
  return client('incr', key)
end

function incrby(key, increment)
  return client('incrby', key, increment)
end

function decr(key)
  return client('decr', key)
end

function decrby(key, decrement)
  return client('decrby', key, decrement)
end

function atype(key)
  return client('type', key)
end

function quit()
  return client('quit')
end

function shutdown()
  return client('shutdown')
end

function slaveof(host, port)
  return client('slaveof', host, port)
end

function watch(...)
  return client('watch', ...)
end

function unwatch()
  return client('unwatch')
end

function exec()
  return client('exec')
end

--function multi(...)
--  need to queue and exec
--end

function publish(channel, message)
  return client('publish', channel, message)
end

function unsubscribe(...)
  return client('unsubscribe', ...)
end

function punsubscribe(...)
  return client('punsubscribe', ...)
end

function subscribe(...)
  return client('subscribe', ...)
end

function psubscribe(...)
  return client('psubscribe', ...)
end


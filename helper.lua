
-- usage lpush_hash('user:1:list', 'user:1:message:', 'text')
function lpush_hset(list,  hprefix, key, val)
  llen = redis('LLEN', list);
  hash  = hprefix .. llen;
  redis("HSET", hash, key, val);
  return redis("LPUSH", list, llen);
end

-- usage rpush_hash('user:1:list', 'user:1:message:', 'text')
function rpush_hset(list,  hprefix, key, val)
  llen = redis('LLEN', list);
  hash  = hprefix .. llen;
  redis("HSET", hash, key, val);
  return redis("RPUSH", list, llen);
end

-- usage lset_hset('user:1:list', 1, 'user:1:message:', 'text')
function lset_hset(list, index, hprefix, key, val)
  hash  = hprefix .. index;
  redis("HSET", hash, key, val);
  if redis('LLEN', list) == "0" then
    return
       error("function l_hset: list: " .. list .. " is empty, use l_hinit()");
  else
    return redis("LSET", list, index, index);
  end
end

-- usage l_hget('user:1:list', 1, 'user:1:message:', 'text');
function l_hget(list, index, hprefix, key)
  l    = redis('LINDEX', list, index);
  print (l)
  hash = hprefix .. l;
  print (hash)
  print (key)
  return redis('HGET', hash, key);
end

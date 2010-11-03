
-- usage lpush_hash('user:1:list', 'user:1:message:', 'text')
function lpush_hset(list, hprefix, key, val)
  hash = hprefix .. "0";
  client("HSET", hash, key, val);
  ret  = client("LPUSH", list, 0);
  -- must reorder indices
  llen = client('LLEN', list);
  for i=1,llen do
    hash  = hprefix .. i;
    client("INCR", hash);
  end
  return ret;
end

-- usage rpush_hash('user:1:list', 'user:1:message:', 'text')
function rpush_hset(list, hprefix, key, val)
  llen = client('LLEN', list);
  hash  = hprefix .. llen;
  client("HSET", hash, key, val);
  return client("RPUSH", list, llen);
end

-- usage lset_hset('user:1:list', 1, 'user:1:message:', 'text')
function lset_hset(list, index, hprefix, key, val)
  hash  = hprefix .. index;
  client("HSET", hash, key, val);
  if client('LLEN', list) == "0" then
    return
       error("function l_hset: list: " .. list .. " is empty, use l_hinit()");
  else
    return client("LSET", list, index, index);
  end
end

-- usage l_hget('user:1:list', 1, 'user:1:message:', 'text');
function l_hget(list, index, hprefix, key)
  l    = client('LINDEX', list, index);
  print (l)
  hash = hprefix .. l;
  return client('HGET', hash, key);
end

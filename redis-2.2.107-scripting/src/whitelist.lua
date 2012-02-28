module("whitelist", package.seeall);

function set(key, val)
  --print ('set');
  return alchemy('set', key, val);
end
function get(key)
  --print ('get');
  return alchemy('get', key);
end

function subscribe(channel)
  --print ('subscribe');
  return alchemy('subscribe', channel);
end
function publish(channel, msg)
  print ('publish');
  return alchemy('publish', channel, msg);
end

function set(key, val)
  --print ('set');
  return redis('set', key, val);
end
function get(key)
  --print ('get');
  return redis('get', key);
end
function info()
  return redis('info');
end

function subscribe(channel)
  print ('subscribe');
  return redis('subscribe', channel);
end
function publish(channel, msg)
  print ('publish');
  return redis('publish', channel, msg);
end

function print_packages()
  table.foreach(package, print)
end

function dump(o)
    if type(o) == 'table' then
        local s = '{ '
        for k,v in pairs(o) do
                if type(k) ~= 'number' then k = '"'..k..'"' end
                s = s .. '['..k..'] = ' .. dump(v) .. ','
        end
        return s .. '} '
    else
        return tostring(o)
    end
end


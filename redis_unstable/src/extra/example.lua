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

function ltrig_cnt(tbl, ...)
  redis("incr", 'ltrig_cnt');
  print('ltrig_cnt: tbl: ' .. tbl);
  print('ltrig_cnt: ' .. redis("get", 'ltrig_cnt'));
  local args = {...};
  print('ltrig_cnt: #args: ' .. #args);
end

function hiya() print ('hiya'); end

function fib(n) return n<2 and n or fib(n-1)+fib(n-2) end

function lcap_add(tname, fk1, pk)
  print('lcap_add: tname: ' .. tname .. ' fk1: ' .. fk1 .. ' pk: ' .. pk);
end
function lcap_del(tname, fk1, pk)
  print('lcap_del: tname: ' .. tname .. ' fk1: ' .. fk1 .. ' pk: ' .. pk);
end

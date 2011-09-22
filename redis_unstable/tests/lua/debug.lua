
function debug_print(msg, every, i, x)
    if ((i % every) == 0) then 
      print(msg, ' iteration: ' .. i .. ' elapsed time: ' .. 
                                            (socket.gettime()*1000 - x) / 1000);
      return socket.gettime()*1000;
    end
    return x;
end

function os.capture(cmd, raw)
  local f = assert(io.popen(cmd, 'r'))
  local s = assert(f:read('*a'))
  f:close()
  if raw then return s end
  s = string.gsub(s, '^%s+', '')
  s = string.gsub(s, '%s+$', '')
  s = string.gsub(s, '[\n\r]+', ' ')
  return s
end


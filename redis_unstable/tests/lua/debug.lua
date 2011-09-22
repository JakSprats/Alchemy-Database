
function debug_print(msg, every, i, x)
    if ((i % every) == 0) then 
      print(msg, ' iteration: ' .. i .. ' elapsed time: ' .. 
                                            (socket.gettime()*1000 - x) / 1000);
      return socket.gettime()*1000;
    end
    return x;
end


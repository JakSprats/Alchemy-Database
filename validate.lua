
function ltrim(s)
  return (s:gsub("^%s*", ""))
end

function validate_speed(norm_speed, speed_string) 
    speed_string = ltrim(speed_string);
    j            = string.find(speed_string, ' ');
    x            = tonumber(string.sub(speed_string, 1, j - 1));
    if x < norm_speed * 0.9 or x > norm_speed * 1.1 then
        print ('FAILURE: norm_speed: ' .. norm_speed .. ' test_speed: ' .. x);
    else
        print ('SUCCESS: speed: ' .. x);
    end
end

function validate_size(norm_size, size_string) 
    size_string = ltrim(size_string);
    x           = tonumber(size_string);
    if x < norm_size * 0.95 or x > norm_size * 1.05 then
        print ('FAILURE: norm_size: ' .. norm_size .. ' test_size: ' .. x);
    else
        print ('SUCCESS: size: ' .. x);
    end
end

function validate_pks(file, mod)
    io.input(file);
    count = 1;
    mod   = tonumber(mod);
    if mod ~= 0 then
      count = 2;
    end
    while true do
      local line = io.read();
      if line == nil then
        break;
      end
      j = string.find(line, ',');
      x = tonumber(string.sub(line, 1, j - 1));
      if (x ~= count) then
        print ('x: ' .. x .. ' c: ' .. count);
      end
      count = count + 1;
      if mod ~= 0 then
        if (count - 1) % mod == 0 then
          count = count + 1;
        end
      end
    end
end

if arg[1] == "SPEED" then
    validate_speed(arg[2], arg[3]);
elseif arg[1] == "SIZE" then
    validate_size(arg[2], arg[3]);
elseif arg[1] == "PKS" then
    validate_pks(arg[2], arg[3]);
else
    print ('first arg can be [SPEED], did not recognize: ' .. arg[1]);
end


-- TODO: test findXth exactness (i.e. OFFSET 1234, finds 1234)

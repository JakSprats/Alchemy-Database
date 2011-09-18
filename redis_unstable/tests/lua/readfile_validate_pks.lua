
local cnt       = tonumber(arg[1]);
local direction = arg[2];
local inc       = tonumber(arg[3]);
local adapt     = (arg[4] ~= nil);
if (inc == nil) then inc = 1; end
print ('ARGS: cnt: ' .. cnt .. ' direction: ' .. direction .. ' inc: ' .. inc);
local ok        = 0;
while true do
  local line = tonumber(io.read());
  if line == nil then break end
  if (cnt ~= line) then
    print('cnt: ' .. cnt .. ' line: ' .. line);
    if (adapt) then cnt = line; end
  else ok = ok + 1;
  end
  if (direction == "rev") then cnt = cnt - inc; 
  else                         cnt = cnt + inc; end
end
print ('ok: ' .. ok);

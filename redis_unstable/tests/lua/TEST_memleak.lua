package.path = package.path .. ";;test/?.lua"
dofile 'external_alchemy.lua';
dofile 'debug.lua';

local c        = 200;
local req      = 1000000;
local tbl      = "memleak";
local rand_del = false;
if (#arg > 0) then rand_del = true; end

function init_memleak_tbl()
  drop_table(tbl);
  create_table(tbl, "id INT, fk INT, msg TEXT");
  create_index("ind_ml_p", tbl, "fk");
  local icmd = 'taskset -c  1 ../../src/alchemy-gen-benchmark -q -c ' .. c ..
               ' -n ' .. req ..
               ' -s 1 -A OK ' .. 
               ' -Q INSERT INTO memleak VALUES ' .. 
               '"(00000000000001,1,\'pagename_00000000000001\')"';
  print ('executing: (' .. icmd .. ')');
  local out = os.capture(icmd, 0);
  print ('out: ' .. out);
end

function delete_memleak_tbl()
  local icmd = 'taskset -c  1 ../../src/alchemy-gen-benchmark -q -c ' .. c ..
               ' -n ' .. req;
  if (rand_del) then icmd = icmd .. ' -r ' .. req;
  else               icmd = icmd .. ' -s 1 ';      end
  icmd = icmd .. ' -A INT ' .. 
                 ' -Q DELETE FROM memleak WHERE id=00000000000001';
  local x   = socket.gettime()*1000;
  print ('executing: (' .. icmd .. ')');
  local out = os.capture(icmd, 0);
end

init_memleak_tbl();
local t = desc(tbl);
for k,v in pairs(t) do print (v); end

delete_memleak_tbl();
t = desc(tbl);
for k,v in pairs(t) do print (v); end

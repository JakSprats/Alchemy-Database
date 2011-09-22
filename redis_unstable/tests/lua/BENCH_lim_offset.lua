package.path = package.path .. ";;./?.lua"
dofile 'external_alchemy.lua';
dofile 'debug.lua';

local populater = false;
local saver     = false;
-- populater = true; saver = true;

local mod       = 4;
local tbl       = "ten_mill_mod_" .. mod;

local c         = 200;
local req       = 10000000;

function init_ten_mill_modX()
  local indx = "ind_" .. tbl .. "_fk";
  drop_table(tbl);
  create_table(tbl, "id INT, fk INT, i INT");
  create_index(indx, tbl, 'fk');
  local icmd = 'taskset -c  1  ../../src/alchemy-gen-benchmark -q -c ' .. c ..
         ' -n ' .. req ..  ' -s 1 -m ' .. mod .. ' -A OK ' ..
         ' -Q INSERT INTO ' .. tbl .. ' VALUES ' ..
         '"(00000000000001,00000000000001,1)" > /dev/null';
  local x   = socket.gettime()*1000;
  print ('executing: (' .. icmd .. ')');
  os.execute(icmd);
  x = debug_print('time: (' .. icmd .. ')', 1, 1, x);
  local x   = socket.gettime()*1000;
  if (saver) then
    print ('save()'); redis:save(); x = debug_print('time: save()', 1, 1, x);
  end
  return true;
end

function update_ten_mill_modX(loops)
  for i = 1, loops do
    print('UPDATE[' .. i .. '] ' .. tbl .. ' SET i=333 WHERE fk = 2');
    local x   = socket.gettime()*1000;
    update(tbl, 'i=333', 'fk = 2');
    x = debug_print('UPDATE ', 1, 1, x);
  end
end

function batch_update_ten_mill_modX(loops)
  for i = 1, loops do
    print('BATCH_UPDATE[' .. i .. '] ' .. tbl .. ' SET i=999 WHERE fk = 2');
    local x   = socket.gettime()*1000;
    local curs_num       = redis:incr("CURSOR_NUMBER");
    local lim            = 1000;
    local uniq_curs_name = "CURS_" .. curs_num;
    local x   = socket.gettime()*1000;
    local updated_rows       = lim; -- initialised to lim
    while updated_rows == lim do
      updated_rows = update(tbl, "i = 999", 'fk = 2 ORDER BY id LIMIT ' .. 
                                            lim .. ' OFFSET '.. uniq_curs_name);
    end
    x = debug_print('BATCH UPDATE ', 1, 1, x);
  end
end

if (populater) then init_ten_mill_modX(); end
batch_update_ten_mill_modX(3);
update_ten_mill_modX(3);

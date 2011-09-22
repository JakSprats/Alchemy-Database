package.path = package.path .. ";;./?.lua"
dofile 'external_alchemy.lua';
dofile 'debug.lua';

local populater = false;
local saver     = false; --populater = true; saver = true;

local mod       = 3;
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
    print ('save()'); save(); x = debug_print('time: save()', 1, 1, x);
  end
  return true;
end

function test_PK_lim_offset_ten_mill_modX()
  local lim         = 10;
  local debug_every = 50000;
  local nrows       = 10000000;
  local start       = 1; --start = 5000000;
  print ('PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST');
  local pks = scan("id", tbl, "ORDER BY id DESC LIMIT 1");
  local mpk = tonumber(pks[1]);
  local x   = socket.gettime()*1000;
  local y   = x;
  for k = 0, 1 do
    local asc, desc;
    if (k == 0) then asc = false; desc = ' DESC '; print ('DESC TEST');
    else             asc = true;  desc = '';       print ('ASC TEST');  end
    for i = start, nrows - lim do
      local res = scan('id', tbl, 'ORDER BY id ' .. desc ..
                                  ' LIMIT ' .. lim ..  ' OFFSET '.. i);
      local first = tonumber(res[1]);
      if (asc          and first ~= (i + 1)    or
          asc == false and first ~= (mpk - i)) then
        print('ORDER BY id LIMIT ' .. lim ..  ' OFFSET '.. i);
        if (asc) then
          print('PK: first: ' .. first .. ' should be: ' .. (i + 1));
        else
          print('PK: first: ' .. first .. ' should be: ' .. (mpk - i));
        end
        return false;
      end
      for j = 1, (lim - 1) do
        local one = tonumber(res[j]); local two = tonumber(res[j + 1]);
        if (asc          and (two - one ~= 1) or
            asc == false and (one - two ~= 1)) then
          print('ORDER BY id LIMIT ' .. lim ..  ' OFFSET '.. i);
          print ('PK: ERROR: one: ' .. one .. ' two: ' .. two);
          return false;
        end
      end
      x = debug_print('DEBUG: PK: ' .. desc, debug_every, i, x);
    end
  end
  debug_print('FINISHED: (test_PK_lim_offset) nrows: ' .. nrows, 1, 1, y);
  return true;
end

function test_FK_lim_offset_ten_mill_modX()
  local lim         = 10;
  local debug_every = 50000;
  local nrows       = 3000000;
  local start       = 1;
  -- FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST
  local pks = select("id", tbl, "fk=2 ORDER BY id DESC LIMIT 1");
  local mpk = tonumber(pks[1]);
  local x   = socket.gettime()*1000;
  local y   = x;
  for k = 0, 1 do
    local asc, desc;
    if (k == 0) then asc = false; desc = ' DESC '; print ('DESC TEST');
    else             asc = true;  desc = '';       print ('ASC TEST');  end
    for i = start, nrows - lim do
      local res = select('id', tbl, 'fk=2 ORDER BY id ' .. desc ..
                                    ' LIMIT ' .. lim .. ' OFFSET '.. i);
      local first = tonumber(res[1]);
      if ((asc          and first ~= (i * mod + 2))or
           asc == false and (first ~= (mpk - mod * i))) then
        print('fk=2 ORDER BY fk LIMIT ' .. lim .. ' OFFSET '..i);
        if (asc) then
          print('FK: 1st: ' .. first .. ' correct: ' .. (i * mod + 2));
        else
          print('FK: 1st: ' .. first .. ' correct: ' .. (mpk - mod * i));
        end
         return false;
      end
      for j = 1, (lim - 1) do
        local one = tonumber(res[j]); local two = tonumber(res[j + 1]);
        if (asc          and (two - one ~= mod) or
            asc == false and (one - two ~= mod)) then
          print('fk=2 ORDER BY fk LIMIT ' .. lim .. ' OFFSET ' .. i);
          print ('FK: ERROR: one: ' .. one .. ' two: ' .. two);
          return false;
        end
      end
      x = debug_print('DEBUG: FK: ' .. desc, debug_every, i, x);
    end
  end
  debug_print('FINISHED: (test_FK_lim_offset) nrows: ' .. nrows, 1, 1, y);
  return true;
end

if (populater) then init_ten_mill_modX(); end
if (test_PK_lim_offset_ten_mill_modX() == false) then return; end
if (test_FK_lim_offset_ten_mill_modX() == false) then return; end

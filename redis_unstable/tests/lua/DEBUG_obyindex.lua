package.path = package.path .. ";;./?.lua"
dofile 'external_alchemy.lua';
dofile 'debug.lua';

local populater  = false;
local saver      = false;
-- populater = true; saver = true;

local Mod        = "4,-110000000,9999999999";
local FKMod      = 4;
local Tbl        = "ob_tenmill";
local Col_decl   = "pk INT, fk INT, ts INT, col TEXT";
local InsertVals = "(00000000000001,00000000000001,00000000000001,'msg_00000000000001')";

local c          = 200;
local req        = 10000000;

function init_ten_mill_modX(tbl, mod, col_decl, insert_vals)
  local indx = "ind_" .. tbl .. "_fk";
  drop_table(tbl);
  create_table(tbl, col_decl);
  create_index(indx, tbl, 'fk', "ORDER", "BY", "ts");
  local icmd = 'taskset -c  1  ../../src/alchemy-gen-benchmark -c ' .. c ..
         ' -n ' .. req ..  ' -s 1 -m "' .. mod .. '" -A OK ' ..
         ' -Q INSERT INTO ' .. tbl .. ' VALUES "' .. insert_vals  .. '" ';
  local x   = socket.gettime()*1000;
  print ('executing: (' .. icmd .. ')');
  local out = os.capture(icmd, 0);
  print ('out: ' .. out);
  x = debug_print('time: (' .. icmd .. ')', 1, 1, x);
  local x   = socket.gettime()*1000;
  if (saver) then
    print ('save()'); redis:save(); x = debug_print('time: save()', 1, 1, x);
  end
  return true;
end

function test_FK_OB_ten_mill(tbl, fkmod)
  local lim         = 10;
  local debug_every = 50000;
  local nrows       = 3000000;
  local start       = 1;
  local pks = select("pk", tbl, "fk=2 ORDER BY ts LIMIT 1");
  local mpk = tonumber(pks[1]);
  local x   = socket.gettime()*1000;
  local y   = x;
  for k = 0, 1 do
    local asc, desc;
    if (k == 0) then asc = false; desc = ' DESC '; print ('DESC TEST');
    else             asc = true;  desc = '';       print ('ASC TEST');  end
    for i = start, nrows - lim do
      local res = select('pk', tbl, 'fk=2 ORDER BY ts ' .. desc ..
                                    ' LIMIT ' .. lim .. ' OFFSET '.. i);
      local first = tonumber(res[1]);
      if ((asc == false and first ~= (i * fkmod + 2))or
           asc          and (first ~= (mpk - fkmod * i))) then
        print('fk=2 ORDER BY ts ' .. desc .. ' LIMIT ' .. lim .. ' OFFSET '..i);
        if (asc) then
          print('FK: 1st: ' .. first .. ' correct: ' .. (i * fkmod + 2));
        else
          print('FK: 1st: ' .. first .. ' correct: ' .. (mpk - fkmod * i));
        end
         return false;
      end
      for j = 1, (lim - 1) do
        local one = tonumber(res[j]); local two = tonumber(res[j + 1]);
        if (asc == false and (two - one ~= fkmod) or
            asc          and (one - two ~= fkmod)) then
          print('fk=2 ORDER BY ts LIMIT ' .. lim .. ' OFFSET ' .. i);
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

if (populater) then init_ten_mill_modX(Tbl, Mod, Col_decl, InsertVals); end
if (test_FK_OB_ten_mill(Tbl, FKMod) == false) then return; end

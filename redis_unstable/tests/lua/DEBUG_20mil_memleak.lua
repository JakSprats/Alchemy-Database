package.path = package.path .. ";;./?.lua"
dofile 'external_alchemy.lua';
dofile 'debug.lua';

local populate  = false;
local fullcheck = false;

local req         = 20000000;
local tbl         = "memleak";
tbl = "ten_mill_mod_4"; -- DEBUG
local pkname      = "id";
local debug_every = 50000;

function init_memleak_tbl()
  drop_table(tbl);
  create_table(tbl, "id INT, fk INT, msg TEXT");
  create_index("ind_ml_p", tbl, "fk");
  for i= 1, req do
      local in_s = i .. ',1,\'pagename_' .. i .. '\'';
      insert(tbl, in_s);
  end
end
function check_tbl(req, tbl, pkname)
  local x = socket.gettime()*1000;
  for i= 1, req do
    local res = select(pkname, tbl, pkname .. ' = ' .. i);
    if (res == nil) then
      print ('ERROR: ' .. tbl .. ' missing: ' .. i); return;
    else
      local id  = res[1];
      if (tonumber(id) ~= i) then
        print ('ERROR: ' .. tbl .. ' missing: ' .. i .. ' got: ' .. id);
        return;
      end
    end
    x = debug_print('check_tbl: ' .. tbl, debug_every, i, x);
  end
  return true;
end
function check_pks(tbl)
  local cnt = scan("COUNT(*)", tbl);
  print ('cnt: ' .. cnt);
  local x   = socket.gettime()*1000;
  local res = scan("id", tbl);
  local key = 1;
  for k, pk in pairs(res) do
    if (key ~= tonumber(pk)) then
      print ('ERROR: check_pks: ' .. tbl .. ' expected: ' .. key ..
                                            ' got: ' .. pk);
      return;
    end
    x = debug_print('check_pks: ' .. tbl, debug_every, key, x);
    key = key + 1;
  end
  local tot = key - 1;
  print ('total: ' .. tot .. ' -> OK');
end
function check_pks_rev(tbl)
  local cnt = tonumber(scan("COUNT(*)", tbl));
  print ('cnt: ' .. cnt);
  local x   = socket.gettime()*1000;
  local res = scan("id", tbl, "ORDER BY id DESC");
  local tot = 1;
  for k, pk in pairs(res) do
    if (cnt ~= tonumber(pk)) then
      print ('ERROR: check_pks_rev: ' .. tbl .. ' expected: ' .. cnt ..
                                            ' got: '      .. pk);
      return;
    end
    cnt = cnt - 1; tot = tot + 1;
    x = debug_print('check_pks_rev: ' .. tbl, debug_every, tot, x);
  end
  tot = tot - 1;
  print ('total: ' .. tot .. ' -> OK');
end

if (populate) then init_memleak_tbl(); end
check_pks_rev(tbl);
check_pks(tbl);
if (fullcheck) then
  if(check_tbl(req, tbl, pkname)) then
      print("Table: " .. tbl .. " OK: 1-" .. req .. " rows");
  end
end


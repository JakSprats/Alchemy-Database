package.path = package.path .. ";;test/?.lua"
require "is_external"

local populate  = false;
local fullcheck = false;

local req       = 20000000;
local tbl       = "memleak";
local pkname    = "id";

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
  end
  return true;
end
function check_pks(tbl)
  local cnt = scan("COUNT(*)", tbl);
  print ('cnt: ' .. cnt);
  local res = scan("id", tbl);
  local key = 1;
  for k, pk in pairs(res) do
    if (key ~= tonumber(pk)) then
      print ('ERROR: check_pks: ' .. tbl .. ' expected: ' .. key ..
                                            ' got: ' .. pk);
      return;
    end
    key = key + 1;
  end
  local tot = key - 1;
  print ('total: ' .. tot);
end

if is_external.yes == 1 then
    if (populate) then init_memleak_tbl(); end
    check_pks(tbl);
    if (fullcheck) then
      if(check_tbl(req, tbl, pkname)) then
          print("Table: " .. tbl .. " OK: 1-" .. req .. " rows");
      end
    end
end


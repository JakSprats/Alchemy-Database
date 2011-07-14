package.path = package.path .. ";;test/?.lua"
require "is_external"

local req    = 1000000;
local tbl    = "memleak";
local pkname = "id";

function lua_init_memleak_tbl()
    drop_table(tbl);
    create_table(tbl, "id INT, fk INT, msg TEXT");
    create_index("ind_ml_p", tbl, "fk");
    for i= 1, req do
        local in_s = i .. ',1,\'pagename_' .. i .. '\'';
        insert(tbl, in_s);
    end
    return "+OK";
end
function lua_check_tbl(req, tbl, pkname)
    for i= 1, req do
        local res = select(pkname, tbl, pkname .. ' = ' .. i);
        if (res == nil) then
            print (tbl .. ' missing: ' .. i);
        else
            local id  = res[1];
            if (tonumber(id) ~= i) then
                print (tbl .. ' missing: ' .. i .. ' got: ' .. id);
            end
        end
    end
    return "Table: " .. tbl .. " OK: 1-" .. req .. " rows";
end

if is_external.yes == 1 then
    print (lua_init_memleak_tbl());
    print (lua_check_tbl(req, tbl, pkname));
end


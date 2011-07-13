package.path = package.path .. ";;test/?.lua"
require "is_external"

local fname = "WNP_text.txt";
local tbl    = "wnp";

function lua_init_wnp_tbl()
    drop_table(tbl);
    create_table(tbl, "id INT, line TEXT");

    io.input(fname)
    local lines = {}
    for line in io.lines() do
      table.insert(lines, line)
    end
    for i, l in ipairs(lines) do
        local in_s = i .. ',' .. l;
        insert(tbl, in_s);
    end

    return "+OK";
end

if is_external.yes == 1 then
    print (lua_init_wnp_tbl());
end


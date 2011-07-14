package.path = package.path .. ";;test/?.lua"
require "is_external"

local c    = 200;
local req  = 1000000;
local tbl  = "memleak";

function init_memleak_tbl()
    drop_table(tbl);
    create_table(tbl, "id INT, fk INT, msg TEXT");
    create_index("ind_ml_p", tbl, "fk");
    local icmd = 'taskset -c  1 ../../src/xdb-gen-benchmark -q -c ' .. c ..
                 ' -n ' .. req ..
                 ' -s 1 -A OK ' .. 
                 ' -Q INSERT INTO memleak VALUES ' .. 
                 '"(00000000000001,1,\'pagename_00000000000001\')"';
    local x   = socket.gettime()*1000;
    print ('executing: (' .. icmd .. ')');
    os.execute(icmd);
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    return "+OK";
end

function delete_memleak_tbl()
    local icmd = 'taskset -c  1 ../gen-benchmark -q -c ' .. c ..' -n ' .. req ..
                 ' -s 1 -A INT ' .. 
                 ' -Q DELETE FROM memleak WHERE id=00000000000001';
    local x   = socket.gettime()*1000;
    print ('executing: (' .. icmd .. ')');
    os.execute(icmd);
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    return "+OK";
end
if is_external.yes == 1 then
    print (init_memleak_tbl());
    local t = desc(tbl);
    for k,v in pairs(t) do print (v); end
    print (delete_memleak_tbl());
    t = desc(tbl);
    for k,v in pairs(t) do print (v); end
end

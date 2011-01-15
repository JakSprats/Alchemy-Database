package.path = package.path .. ";;test/?.lua"
require "is_external"

local c    = 200;
local req  = 1000000;
local tbl  = "thread";

function init_thread_tbl()
    drop_table(tbl);
    create_table(tbl, "id INT, page_no INT, msg TEXT");
    create_index("ind_t_p", tbl, "page_no");
    create_nri_index("int_t_nri", tbl, "DELETE FROM thread WHERE page_no = $page_no ORDER BY id LIMIT 1");

    local icmd = '../gen-benchmark -q -c ' .. c ..' -n ' .. req ..
                 ' -s -A OK ' .. 
                 ' -Q INSERT INTO thread VALUES ' .. 
                 '"(000000000001,1,\'pagename_000000000001\')"';
    local x   = socket.gettime()*1000;
    print ('executing: (' .. icmd .. ')');
    os.execute(icmd);
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    return "+OK";
end

function test_del_lim_offset()
    local cnt   = select("COUNT(*)", tbl, 'page_no = 1');
    local o_cnt = cnt;
    while (cnt > 100) do
        delete(tbl, 'page_no = 1 ORDER BY id LIMIT 5 OFFSET 100');
        cnt   = select("COUNT(*)", tbl, 'page_no = 1');
        if (o_cnt < 105) then break; end
        if (cnt ~= (o_cnt - 5)) then
            print('ERROR: test_del_lim_offset expected: ' .. (o_cnt - 5) .. ' got: ' .. cnt);
        end
        o_cnt = cnt;
    end
    cnt   = select("COUNT(*)", tbl, 'page_no = 1');
    if (cnt ~= 100) then
        print ('ERROR: finish: test_del_lim_offset: cnt: ' .. cnt);
    end
    return "+OK";
end

if is_external.yes == 1 then
    print (init_thread_tbl());
    print (test_del_lim_offset());
end

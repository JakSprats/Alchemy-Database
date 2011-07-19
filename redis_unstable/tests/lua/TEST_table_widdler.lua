package.path = package.path .. ";;test/?.lua"
require "is_external"

require "socket"

local c    = 200;
local req  = 10000000;
local mod  = 100;
local tbl  = "ten_mill_mod100";

--req = 1000000; -- for quick testing

function init_ten_mill_mod100()
    local indx = "ind_ten_mill_mod100_fk";
    drop_table(tbl);
    drop_index(indx);
    create_table(tbl, "id INT, fk INT, i INT");
    create_index(indx, tbl, 'fk');
    local icmd = 'taskset -c  1  ../../src/alchemy-gen-benchmark -q -c ' .. c ..
                 ' -n ' .. req ..  ' -s 1 -m ' .. mod .. ' -A OK ' .. 
                 ' -Q INSERT INTO ten_mill_mod100 VALUES ' .. 
                 '"(00000000000001,00000000000001,1)" > /dev/null';
    local x   = socket.gettime()*1000;
    print ('executing: (' .. icmd .. ')');
    os.execute(icmd);
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    local x   = socket.gettime()*1000;
    --print ('save()');
    --is_external.print_diff_time('time: save()', x);
    --save();
    return "+OK";
end

function widdle_delete_pk()
    print ('RUNNING TEST: widdle_delete_pk');
    local cnt = scan_count(tbl);
    print ('table cnt: ' .. cnt);
    while (cnt > 0) do
        math.randomseed(socket.gettime()*10000)
        local res = scan("id", tbl, "ORDER BY id LIMIT 1");
        local pks = res[1];
        local r   = math.floor(math.random() * cnt);
        local pke = pks + r;
        local wc  = 'id BETWEEN ' .. pks .. ' AND ' .. pke;
        --print ('cnt: ' .. cnt .. ' r: ' .. r .. ' pks: ' .. pks ..
               --' pke: ' .. pke .. ' wc: ' .. wc);
        x          = socket.gettime()*1000;
        delete(tbl, wc);
        is_external.print_diff_time('delete: (' .. wc .. ')', x);
        local new_cnt = cnt - (pke - pks) - 1;
        cnt = scan_count(tbl);
        if (cnt ~= new_cnt) then
            print ('expected: ' .. new_cnt .. ' got: ' .. cnt);
        end
    end
end

function widdle_update_FK()
    print ('RUNNING TEST: widdle_update_FK');
    local cnt        = scan_count(tbl);
    print ('table cnt: ' .. cnt);
    local val_list   = "i = 1";
    local wc         = "id BETWEEN 1 AND " .. cnt; -- ENTIRE TABLE
    local x          = socket.gettime()*1000;
    update(tbl, val_list, wc);                     -- set ENTIRE TABLE to "i=1"
    is_external.print_diff_time('update: ENTIRE TABLE', x);
    val_list         = "i = 99";
    local cnt_per_fk = math.floor(cnt / mod);
    -- cnt never == req, Btree not 100% balanced, some FKs have more PKs
    local variance   = (cnt - req) / 10;
    local fks        = 0;
    while (fks < mod) do
        local r    = math.floor(math.random() * 10);
        local fke  = fks + r;
        if (fke > mod) then fke = mod; end
        wc         = 'fk BETWEEN ' .. fks .. ' AND ' .. fke;
        print ('cnt: ' .. cnt .. ' r: ' .. r .. ' fks: ' .. fks ..
               ' fke: ' .. fke .. ' val_list: ' .. val_list .. ' wc: ' .. wc);
        x          = socket.gettime()*1000;
        update(tbl, val_list, wc);
        is_external.print_diff_time('update: (' .. wc .. ')', x);

        local ncnt = cnt - (((fke - fks) + 1) * cnt_per_fk);
        x          = socket.gettime()*1000;
        cnt        = scan_count(tbl, "i = 1");  -- not-UPDATEd rows
        is_external.print_diff_time('SCAN: (i = 99)', x);
        if ((ncnt - cnt) > variance) then
            print ('expected: ' .. ncnt .. ' got: ' .. cnt);
        end

        fks = fke + 1; -- increment FK start
    end
end

function widdle_delete_FK()
    print ('RUNNING TEST: widdle_delete_FK');
    local cnt        = scan_count(tbl);
    print ('table cnt: ' .. cnt);
    local cnt_per_fk = math.floor(cnt / mod);
    -- cnt never == req, Btree not 100% balanced, some FKs have more PKs
    local variance   = (cnt - req) / 10;
    local fks        = 0;
    --print ('cnt_per_fk: ' .. cnt_per_fk .. ' variance: ' .. variance);
    while (cnt > 0) do
        local r   = math.floor(math.random() * 10);
        local fke = fks + r;
        if (fke > mod) then
            fke = mod;
        end
        local wc  = 'fk BETWEEN ' .. fks .. ' AND ' .. fke;
        --print ('cnt: ' .. cnt .. ' r: ' .. r .. ' fks: ' .. fks ..
               --' fke: ' .. fke .. ' wc: ' .. wc);
        local x   = socket.gettime()*1000;
        delete(tbl, wc);
        is_external.print_diff_time('delete: (' .. wc .. ')', x);
        local new_cnt = cnt - (((fke - fks) + 1) * cnt_per_fk);
        cnt = scan_count(tbl);
        if ((new_cnt - cnt) > variance) then
            print ('expected: ' .. new_cnt .. ' got: ' .. cnt);
        end
        fks = fke + 1; -- increment FK start
    end
end

function run_widdler_test()
    init_ten_mill_mod100();
    widdle_delete_pk();
    init_ten_mill_mod100();
    widdle_update_FK();
    widdle_delete_FK();
    return "+OK";
end

if is_external.yes == 1 then
    --print (init_ten_mill_mod100());
    print (run_widdler_test());
end

package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end

local wc     = "fk = 1";
local c      = 200;
local req    = 10000000;
local mod    = 100;
local tbl    = "ten_mill_mod100";

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

function test_pk_lim_offset_ten_mill_modTEN_fk()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    debug_every = 50000
    lim         = 10
    nrows       = 10000000
    -- PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST
    _print ('PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST');
    local x = socket.gettime()*1000;
    local y = x;
    for i= 1, nrows do
        res = select('id', tbl, 'id BETWEEN 1 AND 10000000 ' .. 
                                ' ORDER BY id LIMIT ' .. lim ..
                                           ' OFFSET '.. i);
        for j = 1, (lim - 1) do
            one = res[j];
            two = res[j + 1];
            if (two - one ~= 1) then
                _print ('ERROR: one: ' .. one .. ' two: ' .. two);
            end
        end
        if ((i % debug_every) == 0) then
            is_external.print_diff_time('DEBUG: iteration: ' .. i, x);
            x = socket.gettime()*1000;
        end
    end
    is_external.print_diff_time(
      'FINISHED: (test_pk_lim_offset_ten_mill_modTEN_fk) nrows: ' ..  nrows, y);
    return "FINISHED";
end


function test_FK_lim_offset_ten_mill_modTEN_fk()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    lim         = 10
    debug_every = 10000
    nrows       = 100000
    -- FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST
    local x = socket.gettime()*1000;
    local y = x;
    for i= 1, nrows - lim do
        res = select('id', tbl, 'fk=1 ORDER BY fk LIMIT ' .. lim ..
                                                ' OFFSET '..i);
        for j = 1, (lim - 1) do
            one = res[j];
            two = res[j + 1];
            if (two - one ~= 100) then
                _print ('ERROR: one: ' .. one .. ' two: ' .. two);
            end
        end
        if ((i % debug_every) == 0) then
            is_external.print_diff_time('DEBUG: iteration: ' .. i, x);
            x = socket.gettime()*1000;
        end
    end
    is_external.print_diff_time(
       'FINISHED: (test_FK_lim_offset_ten_mill_modTEN_fk) nrows: ' .. nrows ,y);
    return "FINISHED";
end

if is_external.yes == 1 then
    init_ten_mill_mod100();
    test_FK_lim_offset_ten_mill_modTEN_fk();
    test_pk_lim_offset_ten_mill_modTEN_fk();
end


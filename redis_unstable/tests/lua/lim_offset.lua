package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then _print = print;
else                          _print = is_external._print; end

local mod       = 3;
local tbl       = "ten_mill_mod_" .. mod;

local c         = 200;
local req       = 10000000;

local populater = false;
local saver     = false;
--populater = true; saver = true;

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
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    local x   = socket.gettime()*1000;
    if (saver) then
      print ('save()'); is_external.print_diff_time('time: save()', x); save();
    end
    return true;
end

function test_PK_lim_offset_ten_mill_modTEN_fk()
    if is_external.yes == 0 then is_external.output = ''; end
    local lim         = 10;
    local debug_every = 50000;
    local nrows       = 10000000;
    local start       = 1;
    _print ('PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST');
    local x = socket.gettime()*1000;
    local y = x;
    for i= start, nrows do
        local res = select('id', tbl,
                       'id BETWEEN 1 AND 10000000 ORDER BY id LIMIT ' .. lim ..
                                                            ' OFFSET '.. i);
        local first = tonumber(res[1]);
        if (first ~= (i + 1)) then
          _print('PK: first: ' .. first .. ' should be: ' .. (i + 1));
           return false;
        end
        for j = 1, (lim - 1) do
            local one = tonumber(res[j]); local two = tonumber(res[j + 1]);
            if (two - one ~= 1) then
                _print('id BETWEEN 1 AND 10000000 ORDER BY id LIMIT ' .. lim ..
                                                            ' OFFSET '.. i);
                _print ('PK: ERROR: one: ' .. one .. ' two: ' .. two);
                return false;
            end
        end
        if ((i % debug_every) == 0) then
            is_external.print_diff_time('DEBUG: iteration: ' .. i, x);
            x = socket.gettime()*1000;
        end
    end
    is_external.print_diff_time(
      'FINISHED: (test_PK_lim_offset_ten_mill_modTEN_fk) nrows: ' ..  nrows, y);
    return true;
end

function test_FK_lim_offset_ten_mill_modTEN_fk()
    if is_external.yes == 0 then is_external.output = ''; end
    local lim         = 10;
    local debug_every = 50000;
    local nrows       = 3000000;
    local start       = 1;
    -- FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST
    local x = socket.gettime()*1000;
    local y = x;
    for i= start, nrows - lim do
        local res = select('id', tbl, 'fk=2 ORDER BY fk LIMIT ' .. lim ..
                                                      ' OFFSET '..i);
        local first = tonumber(res[1]);
        if (first ~= (i * mod + 2)) then
          _print('fk=2 ORDER BY fk LIMIT ' .. lim .. ' OFFSET '..i);
          _print('FK: first: ' .. first .. ' should be: ' .. (i * mod + 2));
           return false;
        end
        for j = 1, (lim - 1) do
            local one = tonumber(res[j]); local two = tonumber(res[j + 1]);
            if (two - one ~= mod) then
                _print('fk=2 ORDER BY fk LIMIT ' .. lim .. ' OFFSET '..i);
                _print ('FK: ERROR: one: ' .. one .. ' two: ' .. two);
                return false;
            end
        end
        if ((i % debug_every) == 0) then
            is_external.print_diff_time('DEBUG: iteration: ' .. i, x);
            x = socket.gettime()*1000;
        end
    end
    is_external.print_diff_time(
       'FINISHED: (test_FK_lim_offset_ten_mill_modTEN_fk) nrows: ' .. nrows ,y);
    return true;
end

if is_external.yes == 1 then
    if (populater) then init_ten_mill_modX(); end
    if (test_FK_lim_offset_ten_mill_modTEN_fk() == false) then return; end
    if (test_PK_lim_offset_ten_mill_modTEN_fk() == false) then return; end
end

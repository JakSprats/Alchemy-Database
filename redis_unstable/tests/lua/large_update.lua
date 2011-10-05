package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end

local wc     = "fk = 1";
local wc_oby = wc .. " ORDER BY fk";
local lim    = 1000;
local c      = 200;
local req    = 10000000;
local mod    = 100;
local tbl    = "ten_mill_mod100";
local populater = false;
populater = true;

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

function validate_updated_row_count()
    local nrows = scan_count(tbl, "i = 1"); 
    _print ('nrows: (WHERE fk=1) i = 4: ' .. nrows);
    nrows       = scan_count(tbl, "i = 99"); 
    _print ('nrows: (WHERE fk=1) i = 99: ' .. nrows);
end

function large_update_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    local nrows        = select_count(tbl, wc);
    _print ('nrows: TOTAL: (WHERE fk=1): ' .. nrows .. "\n");
    
    print ("set i back to 1");
    updated_rows       = update(tbl, "i = 1", wc);
    validate_updated_row_count()

    local x            = socket.gettime()*1000;
    _print ('SINGLE UPDATE: set i=99 WHERE fk = 1');
    local updated_rows = update(tbl, "i = 99", wc);
    is_external.print_diff_time('SINGLE UPDATE: set i=99 WHERE fk = 1', x);
    
    validate_updated_row_count()
    
    print ("set i back to 1");
    updated_rows       = update(tbl, "i = 1", wc_oby);
    validate_updated_row_count()

    updated_rows       = lim;
    unique_cursor_name = "update_cursor_for_ten_mill_modTEN_fk";
    
    _print ('\nBATCH UPDATE: set i=99 WHERE fk = 1');
    local x = socket.gettime()*1000;
    while updated_rows == lim do
        updated_rows = update(tbl, "i = 99", 
                                      wc_oby .. 
                                      ' LIMIT ' .. lim ..
                                      ' OFFSET '.. unique_cursor_name);
        --socket.sleep(1);
        --_print ('updated_rows: ' .. updated_rows);
    end
    is_external.print_diff_time('BATCH UPDATE: set i=99 WHERE fk = 1', x);
    
    validate_updated_row_count()

    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end

function large_delete_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    local deleted_rows = lim;
    
    _print ('\nBATCH DELETE: WHERE fk = 1');
    local x = socket.gettime()*1000;
    while deleted_rows == lim do
        deleted_rows = delete(tbl, wc_oby .. 
                                   ' LIMIT ' .. lim); 
        --socket.sleep(1);
        --_print ('deleted_rows: ' .. deleted_rows);
    end
    is_external.print_diff_time('BATCH DELETE: WHERE fk = 1', x);
end

if is_external.yes == 1 then
    if (populater) then init_ten_mill_mod100(); end
    large_update_test();
    --large_delete_test();
end

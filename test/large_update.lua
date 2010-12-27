package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end

local tbl    = "ten_mill_mod100";
local wc     = "fk = 1";
local wc_oby = wc .. " ORDER BY fk";
local lim    = 1000;

function validate_updated_row_count()
    local nrows = scanselect("COUNT(*)", tbl, "i = 1"); 
    _print ('nrows: (WHERE fk=1) i = 4: ' .. nrows);
    nrows       = scanselect("COUNT(*)", tbl, "i = 99"); 
    _print ('nrows: (WHERE fk=1) i = 99: ' .. nrows);
end

function large_update_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    local nrows        = select("COUNT(*)", tbl, wc);
    _print ('nrows: TOTAL: (WHERE fk=1): ' .. nrows .. "\n");
    
    print ("set i back to 1");
    updated_rows       = update(tbl, "i = 1", wc_oby);

    local x            = socket.gettime()*1000;
    _print ('SINGLE UPDATE: set i=99 WHERE fk = 1');
    local updated_rows = update(tbl, "i = 99", wc_oby);
    is_external.print_diff_time('SINGLE UPDATE: set i=99 WHERE fk = 1', x);
    
    validate_updated_row_count()
    
    print ("set i back to 1");
    updated_rows       = update(tbl, "i = 1", wc_oby);

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
    --large_update_test();
    large_delete_test();
end

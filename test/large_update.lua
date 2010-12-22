package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end

function large_upate_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    tbl="ten_mill_modTEN_fk";
    update_where_clause = "fk = 1 ORDER BY fk";
    
    nrows = select("COUNT(*)", tbl, update_where_clause);
    _print ('nrows: TOTAL: (WHERE fk=1): ' .. nrows .. "\n");
    
    local x = socket.gettime()*1000;
    _print ('SINGLE UPDATE: set count=4 WHERE fk = 1');
    updated_rows = update(tbl, "count = 4", update_where_clause);
    is_external.print_diff_time('SINGLE UPDATE: set count=4 WHERE fk = 1', x);
    
    nrows = scanselect("COUNT(*)", tbl, "count = 4"); 
    _print ('nrows: (WHERE fk=1) count = 4: ' .. nrows);
    nrows = scanselect("COUNT(*)", tbl, "count = 99"); 
    _print ('nrows: (WHERE fk=1) count = 99: ' .. nrows);
    
    lim = 1000;
    updated_rows = lim;
    unique_cursor_name = "update_cursor_for_ten_mill_modTEN_fk";
    
    _print ('\nBATCH UPDATE: set count=99 WHERE fk = 1');
    local x = socket.gettime()*1000;
    while updated_rows == lim do
        updated_rows = update(tbl, "count = 99", 
                                      update_where_clause .. 
                                      ' LIMIT ' .. lim ..
                                      ' OFFSET '.. unique_cursor_name);
        -- _print ('updated_rows: ' .. updated_rows);
    end
    is_external.print_diff_time('BATCH UPDATE: set count=99 WHERE fk = 1', x);
    
    nrows = scanselect("COUNT(*)", tbl, "count = 4"); 
    _print ('nrows: (WHERE fk=1) count = 4: ' .. nrows);
    nrows = scanselect("COUNT(*)", tbl, "count = 99"); 
    _print ('nrows: (WHERE fk=1) count = 99: ' .. nrows);

    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end

if is_external.yes == 1 then
    large_upate_test();
end


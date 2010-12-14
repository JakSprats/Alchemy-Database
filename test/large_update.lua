package.path = package.path .. ";redis-lua/src/?.lua;src/?.lua"
require "socket"

require "redis"

function print_diff_time(x) 
    print(string.format("elapsed time: %.2f(ms)\n", socket.gettime()*1000 - x))
end

local redis = Redis.connect('127.0.0.1', 6379);

tbl="ten_mill_modTEN_fk";
update_where_clause = "fk = 1 ORDER BY fk";

nrows = redis:select("COUNT(*)", tbl, update_where_clause);
print ('nrows: TOTAL: (WHERE fk=1): ' .. nrows .. "\n");

local x = socket.gettime()*1000;
print ('set count=4 WHERE fk = 1');
updated_rows = redis:update(tbl, "count = 4", update_where_clause);
print_diff_time(x);

nrows = redis:scanselect("COUNT(*)", tbl, "count = 4"); 
print ('nrows: (WHERE fk=1) count = 4: ' .. nrows);
nrows = redis:scanselect("COUNT(*)", tbl, "count = 99"); 
print ('nrows: (WHERE fk=1) count = 99: ' .. nrows);

lim = 1000;
updated_rows = lim;
unique_cursor_name = "update_cursor_for_ten_mill_modTEN_fk";

print ('\nBATCH UPDATE: set count=99 WHERE fk = 1');
local x = socket.gettime()*1000;
while updated_rows == lim do
    updated_rows = redis:update(tbl, "count = 99", 
                                  update_where_clause .. 
                                  ' LIMIT ' .. lim ..
                                  ' OFFSET '.. unique_cursor_name);
    -- print ('updated_rows: ' .. updated_rows);
end
print_diff_time(x);

nrows = redis:scanselect("COUNT(*)", tbl, "count = 4"); 
print ('nrows: (WHERE fk=1) count = 4: ' .. nrows);
nrows = redis:scanselect("COUNT(*)", tbl, "count = 99"); 
print ('nrows: (WHERE fk=1) count = 99: ' .. nrows);

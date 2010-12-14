package.path = package.path .. ";redis-lua/src/?.lua;src/?.lua"

require "redis"

local redis = Redis.connect('127.0.0.1', 6379);

-- CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE
-- CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE
local t     = redis:drop_table('logical_test_fk');
print (t);
local t     = redis:create_table('logical_test_fk','id INT, fk INT, count INT');
print (t);

local t     = redis:create_index('ind_ltestfk', 'logical_test_fk','fk');
print (t);

-- DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
-- DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
local t     = redis:desc('logical_test_fk');
for i,v in ipairs(t) do print(i,v); end


-- INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
-- INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
print ('NINE INSERTS');
local t     = redis:insert('logical_test_fk', '1,1,11');
print (t);
local t     = redis:insert('logical_test_fk', '2,1,11');
print (t);
local t     = redis:insert('logical_test_fk', '3,2,22');
print (t);
local t     = redis:insert('logical_test_fk', '4,2,22');
print (t);
local t     = redis:insert('logical_test_fk', '5,3,33');
print (t);
local t     = redis:insert('logical_test_fk', '6,3,33');
print (t);
local t     = redis:insert('logical_test_fk', '7,4,44');
print (t);
local t     = redis:insert('logical_test_fk', '8,4,44');
print (t);
local t     = redis:insert_return_size('logical_test_fk', '9,5,55');
print (t);

-- SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
-- SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
local t     = redis:select(0);
print (t);

print ('SELECT * FROM logical_test_fk WHERE id = 8');
local t     = redis:select('*', 'logical_test_fk', 'id = 8');
for i,v in ipairs(t) do print(i,v); end
print ('SELECT * FROM logical_test_fk WHERE fk = 4');
local t     = redis:select('*', 'logical_test_fk', 'fk = 4');
for i,v in ipairs(t) do print(i,v); end

print ('SCANSELECT * FROM logical_test_fk');
local t     = redis:scanselect('*', 'logical_test_fk');
for i,v in ipairs(t) do print(i,v); end
print ('SCANSELECT * FROM logical_test_fk WHERE count = 33');
local t     = redis:scanselect('*', 'logical_test_fk', 'count = 33');
for i,v in ipairs(t) do print(i,v); end

-- UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
-- UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
print ('UPDATE logical_test_fk SET count = 99 WHERE fk=4')
local t     = redis:update('logical_test_fk', 'count = 99', 'fk=4');
print (t);
print ('SCANSELECT * FROM logical_test_fk');
local t     = redis:scanselect('*', 'logical_test_fk');
for i,v in ipairs(t) do print(i,v); end

-- DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
-- DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
print ('DELETE FROM logical_test_fk WHERE fk=5')
local t     = redis:delete('logical_test_fk', 'fk=5');
print (t);
print ('SCANSELECT * FROM logical_test_fk');
local t     = redis:scanselect('*', 'logical_test_fk');
for i,v in ipairs(t) do print(i,v); end

-- DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP
-- DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP
print ('DUMP logical_test_fk');
local t     = redis:dump('logical_test_fk');
for i,v in ipairs(t) do print(i,v); end

print ('DUMP logical_test_fk TO MYSQL');
local t     = redis:dump_to_mysql('logical_test_fk');
for i,v in ipairs(t) do print(i,v); end

print ('DUMP logical_test_fk TO FILE ./test/DUMP_logical_test_fk.txt');
local t     = redis:dump_to_file('logical_test_fk', './test/DUMP_logical_test_fk.txt');
for i,v in ipairs(t) do print(i,v); end


-- DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX
-- DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX
local t     = redis:drop_index('ind_ltestfk');
print (t);

-- CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS
-- CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS
redis:zadd('test_zset', 1.11, 'ONE');
redis:zadd('test_zset', 2.22, 'two');
redis:zadd('test_zset', 3.33, 'thr33');
redis:drop_table('copy_test_zset');
redis:create_table_as('copy_test_zset', 'DUMP test_zset');
local t     = redis:desc('copy_test_zset');
for i,v in ipairs(t) do print(i,v); end
local t     = redis:dump('copy_test_zset');
for i,v in ipairs(t) do print(i,v); end
redis:drop_table('copy_PART_test_zset');
redis:create_table_as('copy_PART_test_zset', 'ZRANGE test_zset 0 1');
local t     = redis:desc('copy_PART_test_zset');
for i,v in ipairs(t) do print(i,v); end
local t     = redis:dump('copy_PART_test_zset');
for i,v in ipairs(t) do print(i,v); end

-- LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA
-- LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA
redis:config('SET', 'luafilename', 'helper.lua');
print ('LUA SCANSELECT * FROM logical_test_fk');
local t     = redis:lua("return scanselect('*', 'logical_test_fk');");
for i,v in ipairs(t) do print(i,v); end

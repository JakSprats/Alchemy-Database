#!/bin/bash

CLI="../redisql-cli"

# STRING
echo "external TEST_string_update_expr.lua"
time luajit TEST_string_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL TEST_string_update_expr.lua"
$CLI CONFIG SET luafilename test/TEST_string_update_expr.lua
$CLI LUA "return init_string_appendone_test();"
$CLI LUA "return string_appendone_test();"
echo sleep 5
sleep 5

# FLOAT
echo "external TEST_float_update_expr.lua"
time luajit TEST_float_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL TEST_float_update_expr.lua"
$CLI CONFIG SET luafilename test/TEST_float_update_expr.lua 
echo -ne "$($CLI LUA "return all_float_tests();")\n"

# INT
echo "external TEST_int_update_expr.lua"
time luajit TEST_int_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL TEST_int_update_expr.lua"
$CLI CONFIG SET luafilename test/TEST_int_update_expr.lua
echo -ne "$($CLI LUA "return all_int_tests();")\n"
echo sleep 5
sleep 5

# ERROR
echo "external error_update_expr.lua"
luajit error_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL error_update_expr.lua"
$CLI CONFIG SET luafilename test/error_update_expr.lua
$CLI LUA "return err_updates_test();"
echo sleep 5
sleep 5

# BASIC
echo "external basic.lua"
luajit basic.lua
echo sleep 5
sleep 5

echo "INTERNAL basic.lua"
$CLI CONFIG SET luafilename test/basic.lua
echo -ne "$($CLI LUA 'return basic_test();')\n"
echo sleep 5
sleep 5

# LARGE_UPDATE
echo "external large_update.lua"
time luajit large_update.lua
echo sleep 5
sleep 5

echo "INTERNAL large_update.lua"
$CLI CONFIG SET luafilename test/large_update.lua
echo -ne "$($CLI LUA 'return large_upate_test();')\n"
echo sleep 5
sleep 5

# MYSQL
echo "drop database backupdb; create database backupdb;" | mysql -uroot backupdb
echo "external mysql.lua"
luajit mysql.lua 
echo "show tables" | mysql -uroot backupdb
echo "drop database backupdb; create database backupdb;" | mysql -uroot backupdb
echo sleep 5
sleep 5

echo "INTERNAL mysql.lua"
$CLI CONFIG SET luafilename test/mysql.lua
$CLI LUA "return backup_database();"
echo "show tables" | mysql -uroot backupdb
echo "drop database backupdb; create database backupdb;" | mysql -uroot backupdb

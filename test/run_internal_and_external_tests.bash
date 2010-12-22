#!/bin/bash

CLI="../redisql-cli"

# BASIC
echo "external basic_test.lua"
luajit basic_test.lua
echo sleep 5
sleep 5

echo "INTERNAL basic_test.lua"
$CLI CONFIG SET luafilename test/basic_test.lua
echo -ne "$($CLI LUA 'return basic_test();')\n"
echo sleep 5
sleep 5

# STRING
echo "external string_update_expr.lua"
time luajit string_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL string_update_expr.lua"
$CLI CONFIG SET luafilename test/string_update_expr.lua
$CLI LUA "return init_string_appendone_test();"
$CLI LUA "return string_appendone_test();"
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

# FLOAT
echo "external float_update_expr.lua"
time luajit float_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL float_update_expr.lua"
$CLI CONFIG SET luafilename test/float_update_expr.lua 
echo -ne "$($CLI LUA "return all_float_tests();")\n"

# INT
echo "external int_update_expr.lua"
time luajit int_update_expr.lua
echo sleep 5
sleep 5

echo "INTERNAL int_update_expr.lua"
$CLI CONFIG SET luafilename test/int_update_expr.lua
echo -ne "$($CLI LUA "return all_int_tests();")\n"
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

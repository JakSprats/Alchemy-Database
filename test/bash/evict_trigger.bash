#!/bin/bash

CLI=./redisql-cli

$CLI CONFIG ADD LUA test/alchemy.lua
$CLI LUA "function cap_per_fk(max, tbl, fkname, fkval, pkname)
              wc    = fkname .. '=' .. fkval;
              cnt_s = select('COUNT(*)', tbl, wc);
              cnt   = tonumber(string.sub(cnt_s, 2));
              if (cnt > max) then
                  dnum = cnt - max;
                  wcob = wc .. ' ORDER BY ' .. pkname .. ' LIMIT ' .. dnum;
                  delete(tbl, wcob);
              end
              return '+OK';
          end
          return 'cap_per_fk() added';"
$CLI CREATE TABLE thread "(id INT, page_no INT, msg TEXT)"
$CLI CREATE INDEX ind_t_p ON thread "(page_no)"
$CLI CREATE INDEX int_t_nri ON thread "LUA return cap_per_fk(100,'thread','page_no',\$page_no,'id');"

time taskset -c 1 ./gen-benchmark -c 200 -n 1000000 -s -A OK -Q INSERT INTO thread VALUES "(000000000001,1,'pagename_000000000001')"

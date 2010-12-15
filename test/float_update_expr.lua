package.path = package.path .. ";redis-lua/src/?.lua;src/?.lua"
require "socket"

require "redis"

function print_diff_time(msg, x) 
    print(string.format("%s elapsed time: %.2f(s)\n",
                         msg, (socket.gettime()*1000 - x) / 1000))
end

local redis = Redis.connect('127.0.0.1', 6379);

tbl="updateme";
redis:drop_table(tbl);
redis:create_table(tbl, "id INT, i INT, f FLOAT, t TEXT");
redis:insert(tbl, "1,10,1.111,ONE");
redis:insert(tbl, "2,20,2.222,xTWOx");
redis:insert(tbl, "3,30,3.333,three");
redis:insert(tbl, "4,40,4.444,...FOUR...");

function float_add_oneone_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 2000 do
      f   = f + 1.1;
      redis:update(tbl, "f = f + 1.1", "id = 2");
      res = redis:select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_add_one: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: float_add_one:', x);
end

function float_times_twotwo_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 90 do
      f   = f * 1.1;
      redis:update(tbl, "f = f * 1.1", "id = 2");
      res = redis:select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_times_two: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: float_times_two:', x);
end

function float_div_twotwo_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "f = 10000", "id = 2");
    f       = 10000;
    loops   = 0;
    while loops < 1000 do
      f   = f / 1.1;
      redis:update(tbl, "f = f / 1.1", "id = 2");
      res = redis:select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_div_two: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: float_div_two:', x);
end

function float_powerto_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 40 do
      f     = math.pow(f, 1.1);
      redis:update(tbl, "f = f ^ 1.1", "id = 2");
      res   = redis:select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_powerto: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: float_powerto:', x);
end

float_add_oneone_test();
float_times_twotwo_test();
float_div_twotwo_test();
float_powerto_test();

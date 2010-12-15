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

function int_add_one_test() 
    local x = socket.gettime()*1000;
    res   = redis:select("i", tbl, "id = 1");
    i     = tonumber(res[1]);
    loops = 0;
    while loops < 10000 do
      i   = i + 1;
      redis:update(tbl, "i = i + 1", "id = 1");
      res = redis:select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_add_one: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: int_add_one:', x);
end

function int_times_two_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "i = 1", "id = 1");
    i       = 1;
    loops   = 0;
    while loops < 30 do
      i   = i * 2;
      redis:update(tbl, "i = i * 2", "id = 1");
      res = redis:select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_times_two: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: int_times_two:', x);
end

function int_divide_two_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "i = 2147483648", "id = 1");
    i       = 2147483648;
    loops   = 0;
    while loops < 30 do
      i   = i / 2;
      redis:update(tbl, "i = i / 2", "id = 1");
      res = redis:select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_divide_two: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: int_divide_two:', x);
end

function int_powerto_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "i = 6", "id = 1");
    i       = 6;
    loops   = 0;
    while loops < 27 do
      i     = math.floor(math.pow(i, 1.1));
      redis:update(tbl, "i = i ^ 1.1", "id = 1");
      res   = redis:select("i", tbl, "id = 1");
      close = (i / tonumber(res[1]));
      if (close < 0.9 and close > 1.1) then
          error('TEST: int_powerto: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: int_powerto:', x);
end

function int_mod_two_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "i = 2147483648", "id = 1");
    i       = 2147483648;
    mod     = math.floor(i / 2) + 1;
    loops   = 0;
    while loops < 30 do
      i   = math.floor(i % mod);
      redis:update(tbl, "i = i % " .. mod, "id = 1");
      res = redis:select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_mod_two: loops: ' .. loops .. ' mod: ' .. mod .. 
                ' expected: ' .. i .. ' got: ' .. res[1]);
      end
      mod     = math.floor(i / 2) + 1;
      loops = loops + 1;
    end
    print_diff_time('TEST: int_divide_two:', x);
end

int_add_one_test();
int_times_two_test();
int_divide_two_test();
int_powerto_test();
int_mod_two_test();

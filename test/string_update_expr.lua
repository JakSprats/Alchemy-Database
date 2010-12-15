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

function string_appendone_test() 
    local x = socket.gettime()*1000;
    redis:update(tbl, "t = three", "id = 3");
    s       = "three"
    loops   = 0;
    while loops < 100000 do
      s   = s .. ".";
      redis:update(tbl, "t = t || '.'", "id = 3");
      res = redis:select("t", tbl, "id = 3");
      if (s ~= res[1]) then
          error('TEST: stringappendone: expected: ' .. s .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    print_diff_time('TEST: stringappendone:', x);
end

string_appendone_test();

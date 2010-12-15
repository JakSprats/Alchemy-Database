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

-- STRINGS
print ('STRINGS');
res = redis:update(tbl, "t = t ||", "id = 3");
res = redis:update(tbl, "t = t || ", "id = 3");
res = redis:update(tbl, "t = t || '", "id = 3");
res = redis:update(tbl, "t = t || ''", "id = 3");
res = redis:update(tbl, "t = t || x'", "id = 3");
res = redis:update(tbl, "t = t || 'x", "id = 3");
res = redis:update(tbl, "t = t || xxxxxxx'", "id = 3");
res = redis:update(tbl, "t = t || 'xxxxxxx", "id = 3");
res = redis:update(tbl, "t = t || xxxxxxx", "id = 3");
res = redis:update(tbl, "t = t || 4 ", "id = 3");
res = redis:update(tbl, "t = t || 4.44 ", "id = 3");
res = redis:update(tbl, "t = t + 4", "id = 3");
res = redis:update(tbl, "t = t - 4", "id = 3");
res = redis:update(tbl, "t = t * 4", "id = 3");
res = redis:update(tbl, "t = t / 4", "id = 3");
res = redis:update(tbl, "t = t ^ 4", "id = 3");
res = redis:update(tbl, "t = t % 4", "id = 3");

-- INTS
print ('INTS');
res = redis:update(tbl, "i = i ||", "id = 1");
res = redis:update(tbl, "i = i || hey", "id = 1");
res = redis:update(tbl, "i = i || 'hey'", "id = 1");
res = redis:update(tbl, "i = i || 'hey' ", "id = 1");
res = redis:update(tbl, "i = i +    ", "id = 1");
res = redis:update(tbl, "i = i + 'hey'   ", "id = 1");
res = redis:update(tbl, "i = i +++", "id = 1");
res = redis:update(tbl, "i = i +++ ===", "id = 1");
res = redis:update(tbl, "i = i / 0", "id = 1");
res = redis:update(tbl, "i = i % 0", "id = 1");
res = redis:update(tbl, "i = i + 4rr", "id = 1");

-- FLOATS
print ('FLOATS');
res = redis:update(tbl, "f = f ||", "id = 1");
res = redis:update(tbl, "f = i + 4.44 ", "id = 1");
res = redis:update(tbl, "f = f / 0.00 ", "id = 1");
res = redis:update(tbl, "f = f % 4 ", "id = 1");
res = redis:update(tbl, "f = f ^ 9E99 ", "id = 4");
res = redis:update(tbl, "f = f * 4rr ", "id = 1");

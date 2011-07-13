package.path = package.path .. ";;test/?.lua"
require "is_external"

function init_int_tests()
    tbl = "updateme";
    drop_table(tbl);
    create_table(tbl, "id INT, i INT, f FLOAT, t TEXT");
    insert(tbl, "1,10,1.111,\'ONE\'");
    insert(tbl, "2,20,2.222,\'xTWOx\'");
    insert(tbl, "3,30,3.333,\'three\'");
    insert(tbl, "4,40,4.444,\'...FOUR...\'");
end

function int_add_one_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    res   = select("i", tbl, "id = 1");
    i     = tonumber(res[1]);
    loops = 0;
    while loops < 50000 do
      i   = i + 1;
      update(tbl, "i = i + 1", "id = 1");
      res = select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_add_one: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: int_add_one:', x);
end

function int_times_two_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "i = 1", "id = 1");
    i       = 1;
    loops   = 0;
    while loops < 30 do
      i   = i * 2;
      update(tbl, "i = i * 2", "id = 1");
      res = select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_times_two: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: int_times_two:', x);
end

function int_divide_two_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "i = 2147483648", "id = 1");
    i       = 2147483648;
    loops   = 0;
    while loops < 30 do
      i   = i / 2;
      update(tbl, "i = i / 2", "id = 1");
      res = select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_divide_two: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: int_divide_two:', x);
end

function int_powerto_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "i = 6", "id = 1");
    i       = 6;
    loops   = 0;
    while loops < 27 do
      i     = math.floor(math.pow(i, 1.1));
      update(tbl, "i = i ^ 1.1", "id = 1");
      res   = select("i", tbl, "id = 1");
      close = (i / tonumber(res[1]));
      if (close < 0.9 and close > 1.1) then
          error('TEST: int_powerto: expected: ' .. i .. ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: int_powerto:', x);
end

function int_mod_two_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "i = 2147483648", "id = 1");
    i       = 2147483648;
    mod     = math.floor(i / 2) + 1;
    loops   = 0;
    while loops < 30 do
      i   = math.floor(i % mod);
      update(tbl, "i = i % " .. mod, "id = 1");
      res = select("i", tbl, "id = 1");
      if (i ~= tonumber(res[1])) then
          error('TEST: int_mod_two: loops: ' .. loops .. ' mod: ' .. mod .. 
                ' expected: ' .. i .. ' got: ' .. res[1]);
      end
      mod     = math.floor(i / 2) + 1;
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: int_divide_two:', x);
end

function all_int_tests()
    if is_external.yes == 0 then
        is_external.output = '';
    end
    init_int_tests();
    int_add_one_test();
    int_times_two_test();
    int_divide_two_test();
    int_powerto_test();
    int_mod_two_test();
    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end

if is_external.yes == 1 then
    all_int_tests();
end


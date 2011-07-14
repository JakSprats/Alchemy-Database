package.path = package.path .. ";;test/?.lua"
require "is_external"

function init_float_tests()
    tbl = "updateme";
    drop_table(tbl);
    create_table(tbl, "id INT, i INT, f FLOAT, t TEXT");
    insert(tbl, "1,10,1.111,\'ONE\'");
    insert(tbl, "2,20,2.222,\'xTWOx\'");
    insert(tbl, "3,30,3.333,\'three\'");
    insert(tbl, "4,40,4.444,\'...FOUR...\'");
end

function float_add_oneone_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 2000 do
      f   = f + 1.1;
      update(tbl, "f = f + 1.1", "id = 2");
      res = select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_add_one: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: float_add_one:', x);
end

function float_times_twotwo_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 90 do
      f   = f * 1.1;
      update(tbl, "f = f * 1.1", "id = 2");
      res = select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_times_two: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: float_times_two:', x);
end

function float_div_twotwo_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "f = 10000", "id = 2");
    f       = 10000;
    loops   = 0;
    while loops < 1000 do
      f   = f / 1.1;
      update(tbl, "f = f / 1.1", "id = 2");
      res = select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_div_two: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: float_div_two:', x);
end

function float_powerto_test() 
    tbl     = "updateme";
    local x = socket.gettime()*1000;
    update(tbl, "f = 1.1", "id = 2");
    f       = 1.1;
    loops   = 0;
    while loops < 40 do
      f     = math.pow(f, 1.1);
      update(tbl, "f = f ^ 1.1", "id = 2");
      res   = select("f", tbl, "id = 2");
      f1 = string.format("%.1f", f);
      r1 = string.format("%.1f", tonumber(res[1]));
      if (f1 ~= r1) then
          error('TEST: float_powerto: expected: ' .. f1 .. ' got: ' .. r1);
      end
      loops = loops + 1;
    end
    is_external.print_diff_time('TEST: float_powerto:', x);
end

function all_float_tests()
    if is_external.yes == 0 then
        is_external.output = '';
    end
    init_float_tests();
    float_add_oneone_test();
    float_times_twotwo_test();
    float_div_twotwo_test();
    float_powerto_test();
    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end

if is_external.yes == 1 then
    all_float_tests();
end

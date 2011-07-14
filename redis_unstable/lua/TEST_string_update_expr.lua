package.path = package.path .. ";;test/?.lua"
require "is_external"

function init_string_appendone_test()
    local tbl = "updateme";
    drop_table(tbl);
    create_table(tbl, "id INT, i INT, f FLOAT, t TEXT");
    insert(tbl, "1,10,1.111,\'ONE\'");
    insert(tbl, "2,20,2.222,\'xTWOx\'");
    insert(tbl, "3,30,3.333,\'three\'");
    insert(tbl, "4,40,4.444,\'...FOUR...\'");
    return "+OK";
end

function string_appendone_test() 
    if is_external.yes == 0 then
        is_external.output = '';
    end
    local tbl   = "updateme";
    update(tbl, "t = \'three\'", "id = 3");
    local s     = "three"
    local x     = socket.gettime()*1000;
    local loops = 0;
    while loops < 30000 do
      s   = s .. ".";
      local expect = "\'" .. s .. "\'";
      update(tbl, "t = t || \'.\'", "id = 3");
      local res = select("t", tbl, "id = 3");
      if (expect ~= res[1]) then
          error('TEST: stringappendone: expected: ' .. expect ..
                 ' got: ' .. res[1]);
      end
      loops = loops + 1;
    end
    return is_external.diff_time('TEST: stringappendone:', x);
end

if is_external.yes == 1 then
    print (init_string_appendone_test());
    print (string_appendone_test());
end

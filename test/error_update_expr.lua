package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 0 then
    print = is_external._print;
end

function err_updates_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    tbl="updateme";
    drop_table(tbl);
    create_table(tbl, "id INT, i INT, f FLOAT, t TEXT");
    insert(tbl, "1,10,1.111,ONE");
    insert(tbl, "2,20,2.222,xTWOx");
    insert(tbl, "3,30,3.333,three");
    insert(tbl, "4,40,4.444,...FOUR...");
    
    -- STRINGS
    print ('STRINGS');
    res = update(tbl, "t = t ||", "id = 3");
    res = update(tbl, "t = t || ", "id = 3");
    res = update(tbl, "t = t || '", "id = 3");
    res = update(tbl, "t = t || ''", "id = 3");
    res = update(tbl, "t = t || x'", "id = 3");
    res = update(tbl, "t = t || 'x", "id = 3");
    res = update(tbl, "t = t || xxxxxxx'", "id = 3");
    res = update(tbl, "t = t || 'xxxxxxx", "id = 3");
    res = update(tbl, "t = t || xxxxxxx", "id = 3");
    res = update(tbl, "t = t || 4 ", "id = 3");
    res = update(tbl, "t = t || 4.44 ", "id = 3");
    res = update(tbl, "t = t + 4", "id = 3");
    res = update(tbl, "t = t - 4", "id = 3");
    res = update(tbl, "t = t * 4", "id = 3");
    res = update(tbl, "t = t / 4", "id = 3");
    res = update(tbl, "t = t ^ 4", "id = 3");
    res = update(tbl, "t = t % 4", "id = 3");
    
    -- INTS
    print ('INTS');
    res = update(tbl, "i = i ||", "id = 1");
    res = update(tbl, "i = i || hey", "id = 1");
    res = update(tbl, "i = i || 'hey'", "id = 1");
    res = update(tbl, "i = i || 'hey' ", "id = 1");
    res = update(tbl, "i = i +    ", "id = 1");
    res = update(tbl, "i = i + 'hey'   ", "id = 1");
    res = update(tbl, "i = i +++", "id = 1");
    res = update(tbl, "i = i +++ ===", "id = 1");
    res = update(tbl, "i = i / 0", "id = 1");
    res = update(tbl, "i = i % 0", "id = 1");
    res = update(tbl, "i = i + 4rr", "id = 1");
    
    -- FLOATS
    print ('FLOATS');
    res = update(tbl, "f = f ||", "id = 1");
    res = update(tbl, "f = i + 4.44 ", "id = 1");
    res = update(tbl, "f = f / 0.00 ", "id = 1");
    res = update(tbl, "f = f % 4 ", "id = 1");
    res = update(tbl, "f = f ^ 9E99 ", "id = 4");
    res = update(tbl, "f = f * 4rr ", "id = 1");
    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end

if is_external.yes == 1 then
    err_updates_test();
end


package.path = package.path .. ";;test/?.lua"
require "is_external"

function init_scanselect_test()
    local tbl  = "ONEK_mod100";
    local indx = "ind_ONEK_mod100";
    drop_table(tbl);
    drop_index(indx);
    create_table(tbl, "id INT, fk INT, i INT, f FLOAT, s TEXT");
    create_index(indx, tbl, 'fk');
    local c   = 20;
    local req = 1000;
    local mod = 20;
    local icmd = '../gen-benchmark -q -c ' .. c ..' -n ' .. req ..
                 ' -s -m ' .. mod .. ' -A OK ' .. 
                 ' -Q INSERT INTO ONEK_mod100 VALUES ' .. 
                 '"(000000000001,000000000001,1,9.99,\'HI\')" > /dev/null';
    os.execute(icmd);
    return "+OK";
end

function scanselect_int()
    local tbl  = "ONEK_mod100";
    update(tbl, 'i = 2', 'id BETWEEN 100 AND 200');
    local icnt = scanselect("COUNT(*)", tbl, 'i = 2');
    if (icnt ~= 101) then
        print ('FAILURE: SCANSELECT int SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, 'i = 3', 'id BETWEEN 150 AND 250');
    local icnt = scanselect("COUNT(*)", tbl, 'i BETWEEN 2 AND 3');
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT int RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scanselect("COUNT(*)", tbl, 'i IN (2,3)');
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT int RANGE cnt should be 151: ' .. icnt);
    end
end

function scanselect_float()
    local tbl  = "ONEK_mod100";
    update(tbl, 'f = 333.33', 'id BETWEEN 400 AND 500');
    local icnt = scanselect("COUNT(*)", tbl, 'f = 333.3299866');
    if (icnt ~= 101) then
        print ('FAILURE: SCANSELECT float SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, 'f = 444.44', 'id BETWEEN 450 AND 550');
    local icnt = scanselect("COUNT(*)", tbl, 'f BETWEEN 333 AND 445');
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT float RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scanselect("COUNT(*)", tbl, 'f IN (333.3299866,444.4400024)');
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT float RANGE cnt should be 151: ' .. icnt);
    end
end

function scanselect_string()
    local tbl  = "ONEK_mod100";
    update(tbl, "s = 'TEST'", 'id BETWEEN 700 AND 800');
    local icnt = scanselect("COUNT(*)", tbl, "s = 'TEST'");
    if (icnt ~= 101) then
        print ('FAILURE: SCANSELECT string SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, "s = 'TTT'", 'id BETWEEN 750 AND 850');
    local icnt = scanselect("COUNT(*)", tbl, "s BETWEEN 'T' AND 'U'");
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT string RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scanselect("COUNT(*)", tbl, "s IN ('TEST', 'TTT')");
    if (icnt ~= 151) then
        print ('FAILURE: SCANSELECT string RANGE cnt should be 151: ' .. icnt);
    end
end

function run_scanselect_test()
    local tbl  = "ONEK_mod100";
    local cnt = scanselect("COUNT(*)", tbl);
    --print ('cnt: ' .. cnt);
    scanselect_int();
    scanselect_float();
    scanselect_string();
    return "+OK";
end

if is_external.yes == 1 then
    print (init_scanselect_test());
    print (run_scanselect_test());
end

package.path = package.path .. ";;test/?.lua"
require "is_external"
local tbl  = "ONEK_mod100";

function init_scan_test()
    local indx = "ind_ONEK_mod100";
    drop_table(tbl);
    drop_index(indx);
    create_table(tbl, "id INT, fk INT, i INT, f FLOAT, s TEXT");
    create_index(indx, tbl, 'fk');
    local c   = 20;
    local req = 1000;
    local mod = 20;
    local icmd = 'taskset -c  1 ../../src/alchemy-gen-benchmark -q -c ' .. c ..
                 ' -n ' .. req ..
                 ' -s 1 -m ' .. mod .. ' -A OK ' .. 
                 ' -Q INSERT INTO ONEK_mod100 VALUES ' .. 
                 '"(00000000000001,00000000000001,1,9.99,\'HI\')" > /dev/null';
    os.execute(icmd);
    return "+OK";
end

function scan_int()
    update(tbl, 'i = 2', 'id BETWEEN 100 AND 200');
    local icnt = scan_count(tbl, 'i = 2');
    if (icnt ~= 101) then
        print ('FAILURE: SCAN int SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, 'i = 3', 'id BETWEEN 150 AND 250');
    local icnt = scan_count(tbl, 'i BETWEEN 2 AND 3');
    if (icnt ~= 151) then
        print ('FAILURE: SCAN int RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scan_count(tbl, 'i IN (2,3)');
    if (icnt ~= 151) then
        print ('FAILURE: SCAN int IN cnt should be 151: ' .. icnt);
    end
end

function scan_float()
    update(tbl, 'f = 333.33', 'id BETWEEN 400 AND 500');
    local icnt = scan_count(tbl, 'f = 333.3299866');
    if (icnt ~= 101) then
        print ('FAILURE: SCAN float SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, 'f = 444.44', 'id BETWEEN 450 AND 550');
    local icnt = scan_count(tbl, 'f >= 333 AND f <= 445');
    if (icnt ~= 151) then
        print ('FAILURE: SCAN float RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scan_count(tbl, 'f IN (333.3299866,444.4400024)');
    if (icnt ~= 151) then
        print ('FAILURE: SCAN float IN cnt should be 151: ' .. icnt);
    end
end

function scan_string()
    update(tbl, "s = \'TEST\'", 'id BETWEEN 700 AND 800');
    local icnt = scan_count(tbl, "s = \'TEST\'");
    if (icnt ~= 101) then
        print ('FAILURE: SCAN string SINGLE cnt should be 101: ' .. icnt);
    end
    update(tbl, "s = \'TTT\'", 'id BETWEEN 750 AND 850');
    local icnt = scan_count(tbl, "s >= \'T\' AND s <= \'U\'");
    if (icnt ~= 151) then
        print ('FAILURE: SCAN string RANGE cnt should be 151: ' .. icnt);
    end
    local icnt = scan_count(tbl, "s IN (\'TEST\', \'TTT\')");
    if (icnt ~= 151) then
        print ('FAILURE: SCAN string IN cnt should be 151: ' .. icnt);
    end
end

function run_scan_test()
    local cnt = scan_count(tbl);
    --print ('cnt: ' .. cnt);
    scan_int();
    scan_float();
    scan_string();
    return "+OK";
end

if is_external.yes == 1 then
    print (init_scan_test());
    print (run_scan_test());
end

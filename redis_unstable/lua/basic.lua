package.path = package.path .. ";;test/?.lua"
require "is_external"
    
if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end
    
function basic_test()
    if is_external.yes == 0 then
        is_external.output = '';
    end
    -- CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE
    -- CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE CREATE_TABLE
    local t     = drop_table('logical_test_fk');
    _print (t);
    local t     = create_table('logical_test_fk','id INT, fk INT, count INT');
    _print (t);
    
    local t     = create_index('ind_ltestfk', 'logical_test_fk','fk');
    _print (t);
    
    -- DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
    -- DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC DESC
    local t     = desc('logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    
    
    -- INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
    -- INSERT INSERT INSERT INSERT INSERT INSERT INSERT INSERT
    _print ('NINE INSERTS');
    local t     = insert('logical_test_fk', '1,1,11');
    _print (t);
    local t     = insert('logical_test_fk', '2,1,11');
    _print (t);
    local t     = insert('logical_test_fk', '3,2,22');
    _print (t);
    local t     = insert('logical_test_fk', '4,2,22');
    _print (t);
    local t     = insert('logical_test_fk', '5,3,33');
    _print (t);
    local t     = insert('logical_test_fk', '6,3,33');
    _print (t);
    local t     = insert('logical_test_fk', '7,4,44');
    _print (t);
    local t     = insert('logical_test_fk', '8,4,44');
    _print (t);
    local t     = insert_return_size('logical_test_fk', '9,5,55');
    _print (t);
    
    -- SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
    -- SELECT SELECT SELECT SELECT SELECT SELECT SELECT SELECT
    local t     = select(0);
    _print (t);
    
    _print ('SELECT * FROM logical_test_fk WHERE id = 8');
    local t     = select('*', 'logical_test_fk', 'id = 8');
    for i,v in ipairs(t) do _print(i,v); end
    _print ('SELECT * FROM logical_test_fk WHERE fk = 4');
    local t     = select('*', 'logical_test_fk', 'fk = 4');
    for i,v in ipairs(t) do _print(i,v); end
    
    _print ('SCANSELECT * FROM logical_test_fk');
    local t     = scanselect('*', 'logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    _print ('SCANSELECT * FROM logical_test_fk WHERE count = 33');
    local t     = scanselect('*', 'logical_test_fk', 'count = 33');
    for i,v in ipairs(t) do _print(i,v); end
    
    -- UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
    -- UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE UPDATE
    _print ('UPDATE logical_test_fk SET count = 99 WHERE fk=4')
    local t     = update('logical_test_fk', 'count = 99', 'fk=4');
    _print (t);
    _print ('SCANSELECT * FROM logical_test_fk');
    local t     = scanselect('*', 'logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    
    -- DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
    -- DELETE DELETE DELETE DELETE DELETE DELETE DELETE DELETE
    _print ('DELETE FROM logical_test_fk WHERE fk=5')
    local t     = delete('logical_test_fk', 'fk=5');
    _print (t);
    _print ('SCANSELECT * FROM logical_test_fk');
    local t     = scanselect('*', 'logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    
    -- DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP
    -- DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP
    _print ('DUMP logical_test_fk');
    local t     = dump('logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    
    _print ('DUMP logical_test_fk TO MYSQL');
    local t     = dump_to_mysql('logical_test_fk', 'mysql_logical_test_fk');
    for i,v in ipairs(t) do _print(i,v); end
    
    _print ('DUMP logical_test_fk TO FILE /tmp/DUMP_logical_test_fk.txt');
    local t     = dump_to_file('logical_test_fk', '/tmp/DUMP_logical_test_fk.txt');
    for i,v in ipairs(t) do _print(i,v); end
    
    -- DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX
    -- DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX DROP_INDEX
    local t     = drop_index('ind_ltestfk');
    _print (t);
    
    if is_external.yes == 1 then
        return "FINISHED";
    else
        return is_external.output;
    end
end
    
if is_external.yes == 1 then
    basic_test();
end

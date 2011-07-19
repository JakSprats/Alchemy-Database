package.path = package.path .. ";;test/?.lua"
require "is_external"

local tbl = "obycol";

function init_obycol()
  drop_table(tbl);
  create_table(tbl, "id INT, i INT, j INT, k INT, l INT, m INT");
  insert(tbl, "1,1,1,2,4,1");
  insert(tbl, "2,1,1,2,4,3");
  insert(tbl, "3,1,2,2,3,5");
  insert(tbl, "4,1,2,2,3,7");
  insert(tbl, "5,2,3,2,2,9");
  insert(tbl, "6,2,3,1,2,2");
  insert(tbl, "7,2,4,1,1,4");
  insert(tbl, "8,2,4,1,1,6");
  insert(tbl, "9,2,4,1,1,8");
end

function check_equals(a, b)
    if (#a ~= #b) then
        printf('tables have different num elements: ' .. #a .. ' and ' .. #b);
        return 0;
    end
    local ret = 1;
    for i = 1, #a do
        if (tonumber(a[i]) ~= tonumber(b[i])) then
            print (i .. ': a: ' .. a[i] .. ' b: ' .. b[i]);
            ret = 0;
        end
    end
    return ret;
end

function test_mult_i_k()
    local i_k = {1,2,3,4,6,7,8,9,5};
    local res = scan('id', tbl, 'ORDER BY i,k');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_i_k: OK");
    end
end

function test_mult_k_l()
    local i_k = {7,8,9,6,5,3,4,1,2};
    local res = scan('id', tbl, 'ORDER BY k,l');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_K_l: OK");
    end
end

function test_mult_kdesc_l()
    local i_k = {5,3,4,1,2,7,8,9,6};
    local res = scan('id', tbl, 'ORDER BY k DESC,l');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_Kdesc_l: OK");
    end
end

function test_mult_kdesc_jdesc()
    local i_k = {5,3,4,1,2,7,8,9,6};
    local res = scan('id', tbl, 'ORDER BY k DESC,j DESC');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_Kdesc_jdesc: OK");
    end
end

function test_mult_i_j_m()
    local i_k = {1,2,3,4,6,5,7,8,9};
    local res = scan('id', tbl, 'ORDER BY i,j,m');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_i_j_m: OK");
    end
end

function test_mult_j_mdesc()
    local i_k = {2,1,4,3,5,6,9,8,7};
    local res = scan('id', tbl, 'ORDER BY j,m DESC');
    if (check_equals(i_k, res)) then
        print ("TEST: test_mult_j_mdesc: OK");
    end
end

if is_external.yes == 1 then
    init_obycol();
    test_mult_i_k();
    test_mult_k_l();
    test_mult_kdesc_l();
    test_mult_kdesc_jdesc();
    test_mult_i_j_m();
    test_mult_j_mdesc();
end


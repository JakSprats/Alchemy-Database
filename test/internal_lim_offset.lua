require "socket"

function print_diff_time(x)
    print(string.format("elapsed time: %.2f(ms)\n", socket.gettime()*1000 - x))
end

function test_pk_lim_offset_ten_mill_modTEN_fk()
    tbl="ten_mill_modTEN_fk";
    debug_every=50000
    lim=10
    nrows=10000000
    -- PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST
    print ('PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST');
    local x = socket.gettime()*1000;
    local y = x;
    for i= 1, nrows do
        res = select('id', tbl, 'id BETWEEN 1 AND 10000000 ' .. 
                                ' ORDER BY id LIMIT ' .. lim ..
                                           ' OFFSET '.. i);
        for j = 1, (lim - 1) do
            one = res[j];
            two = res[j + 1];
            if (two - one ~= 1) then
                print ('ERROR: one: ' .. one .. ' two: ' .. two);
            end
        end
        if ((i % debug_every) == 0) then
            print ('DEBUG: iteration: ' .. i);
            print_diff_time(x);
            x = socket.gettime()*1000;
        end
    end
    print ('FINISHED: (test_pk_lim_offset_ten_mill_modTEN_fk) nrows: ' .. nrows);
    print_diff_time(y);
    return "LAUNCHED";
end



function test_FK_lim_offset_ten_mill_modTEN_fk()
    tbl="ten_mill_modTEN_fk";
    lim=10
    debug_every=10000
    nrows=1000000
    -- FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST
    local x = socket.gettime()*1000;
    local y = x;
    for i= 1, nrows do
        res = select('id', tbl, 'fk=1 ORDER BY fk LIMIT ' .. lim ..
                                                ' OFFSET '..i);
        for j = 1, (lim - 1) do
            one = res[j];
            two = res[j + 1];
            if (two - one ~= 10) then
                print ('ERROR: one: ' .. one .. ' two: ' .. two);
            end
        end
        if ((i % debug_every) == 0) then
            print ('DEBUG: iteration: ' .. i);
            print_diff_time(x);
            x = socket.gettime()*1000;
        end
    end
    print ('FINISHED: (test_FK_lim_offset_ten_mill_modTEN_fk) nrows: ' .. nrows);
    print_diff_time(y);
    return "LAUNCHED";
end

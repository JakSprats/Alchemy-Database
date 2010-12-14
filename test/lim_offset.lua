package.path = package.path .. ";redis-lua/src/?.lua;src/?.lua"

require "redis"

local redis = Redis.connect('127.0.0.1', 6379);

tbl="ten_mill_modTEN_fk";
debug_every=50000

lim=10

nrows=10000000
-- PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST
print ('PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST PK TEST');
for i= 1, nrows do
    res = redis:scanselect('id', tbl, 'ORDER BY id LIMIT ' .. lim ..
                                                 ' OFFSET '..i);
    for j = 1, (lim - 1) do
        one = res[j];
        two = res[j + 1];
        if (two - one ~= 1) then
            print ('ERROR: one: ' .. one .. ' two: ' .. two);
        end
    end
    if ((i % debug_every) == 0) then
        print ('DEBUG: iteration: ' .. i);
    end
end



debug_every=10000
nrows=1000000
-- FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST FK_TEST
for i= 1, nrows do
    res = redis:select('id', tbl, 'fk=1 ORDER BY fk LIMIT ' .. lim ..
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
    end
end



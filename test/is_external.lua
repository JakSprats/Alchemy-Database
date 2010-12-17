module('is_external', package.seeall);
yes   = 0;
delim = '';
if pcall(function () x = #arg; end) then 
    dofile 'external_alchemy.lua';
    yes   = 1;
    delim = '';
else 
    dofile 'test/alchemy.lua'
    yes   = 0;
    delim = '+';
end

output = '';
function _print(t,x)
    if type(t) == "table" then
        for i,v in ipairs(t) do
            --output = output .. i .. "\t" .. v .. "\n";
            output = output .. v .. "\n";
        end
    elseif t ~= nil then
        if x ~= nil then
            output = output .. t .. "\t" .. x .. "\n";
        else
            output = output .. t .. "\n";
        end
    end
end

require "socket"
function print_diff_time(msg, x)
    x =string.format("%s elapsed time: %.2f(s)\n",
                      msg, (socket.gettime()*1000 - x) / 1000);
    if yes == 1 then
        print(x);
    else
        _print(x);
    end
end

function iTableCopy(t)
  local t2 = {}
  for k,v in ipairs(t) do t2[k] = v end
  return t2
end

function ParseCSVLine(line, sep) 
    local res = {}
    local pos = 1
    sep = sep or ','
    while true do 
        local c = string.sub(line, pos, pos)
        if (c == "") then break end
        if (c == '"') then
            -- quoted value (ignore separator within)
            local txt = ""
            repeat
                local startp, endp = string.find(line, '^%b""', pos)
                txt = txt .. string.sub(line, startp + 1, endp - 1)
                pos = endp + 1
                c = string.sub(line, pos, pos) 
                if (c == '"') then txt = txt .. '"' end 
            until (c ~= '"')
            table.insert(res, txt)
            assert(c == sep or c == "")
            pos = pos + 1
        else    
            -- no quotes used, just look for the first separator
            local startp, endp = string.find(line, sep, pos)
            if (startp) then 
                table.insert(res, string.sub(line, pos, startp - 1))
                pos = endp + 1
            else
                -- no separator found -> use rest of string and terminate
                table.insert(res, string.sub(line, pos))
                break
            end 
        end
    end
    return res
end

local StorageCommands = {LPUSH = 1, RPUSH =  1, LSET = 2, ZADD = 2, HSET = 2, SET = 1, SETNX = 1, APPEND = 1, SETEX = 1, SELECT = 4, SCANSELECT = 2};

function internal_select_store(col_list, tbls, whereclause)
    -- 1.) parse STORE "command" out of WhereClause
    local x    = string.find(whereclause, ' STORE ');
    local cmd  = string.sub(whereclause, x + 7);
    local argv = {};
    for arg in string.gmatch(cmd, "[%w_]+") do
        table.insert(argv, arg);
    end
    --for ka,va in ipairs(argv) do print('CMD ARGV: ' .. ka .. "\t" .. va) end
    if (#argv ~= 2) then
        return raw_write("-ERR STORE: SELECT STORE redis_cmd redis_obj[$]");
    end
    local sto = StorageCommands[string.upper(argv[1])];
    if (sto == nil) then
        return raw_write("-ERR STORE: StorageType must be write commands (LPUSH,RPUSH,LSET,SADD,ZADD,HSET,SET,SETNX,APPEND,SETEX,SELECT,SCANSELECT)");
    end

    local argc = StorageCommands[string.upper(argv[1])];
    local cols = ParseCSVLine(col_list, ",");

    -- 2.) if last arg of command ends w/ "$" -> ComplexStore
    local cmplx = (string.sub(cmd, -1) == '$');
    if (cmplx) then argc = argc + 1; end -- ComplexStore -> additional arg
    if ((argc > 0 and argc ~= #cols) or
        (argc < 0 and math.abs(argc) <= #cols)) then
        return raw_write('-ERR STORE: command argc mismatch');
    end

    local wc = string.sub(whereclause, 1, x - 1);
    --print ('wc: ' .. wc .. ' cmd: ' .. cmd);

    -- 3.) execute select, storing results in a table
    local t = client('select', col_list, "FROM", tbls, "WHERE", wc);
    if (t == nil) then return 0; end

    -- 4.) iterate thru results, running "command" on each row
    for key,value in ipairs(t) do
        -- 5.) arguments are comma delimited - parse
        local rcols = ParseCSVLine(value, ",");
        --for k,v in pairs(rcols) do print('COLS: ' .. k .. "\t" .. v) end
        local nargv = iTableCopy(argv); -- copy argv
        if (cmplx) then
            local larg  = argv[#argv];         -- get final arg
            local nlarg = larg .. rcols[1];    -- modify: append first column
            table.remove(nargv);               -- remove final arg
            table.insert(nargv, nlarg);        -- add modified @end
            for i = 2, #rcols do
                table.insert(nargv, rcols[i]); -- then add rest of rcols
            end
            --for k,v in ipairs(nargv) do print('CX_ARGV: ' .. v) end
            client(unpack(nargv));
        else
            for i = 1, #rcols do
                table.insert(nargv, rcols[i]); -- add rcols
            end
            --for k,v in ipairs(nargv) do print('ARGV: ' .. v) end
            client(unpack(nargv));
        end
    end
    return #t;
end

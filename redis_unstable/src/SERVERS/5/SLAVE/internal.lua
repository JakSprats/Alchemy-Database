--
-- NOTE: DO NOT MOVE THIS FILE
-- this file is loaded when the server starts and certain commands
-- (e.g. SELECT_STORE and CREATE_TABLE_AS_DUMP) use lua functions internally
--

-- SELECT_STORE SELECT_STORE SELECT_STORE SELECT_STORE SELECT_STORE
-- SELECT_STORE SELECT_STORE SELECT_STORE SELECT_STORE SELECT_STORE
function internal_iTableCopy(t)
  local t2 = {}
  for k,v in ipairs(t) do t2[k] = v end
  return t2
end

function internal_ParseCSVLine(line, sep) 
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

local internal_StorageCommands = {LPUSH = 1, RPUSH =  1, LSET = 2, ZADD = 2, HSET = 2, SET = 1, SETNX = 1, APPEND = 1, SETEX = 1, SELECT = 4, SCAN = 2};

function internal_select_store(clist, tlist, whereclause)
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
    local sto = internal_StorageCommands[string.upper(argv[1])];
    if (sto == nil) then
        return raw_write("-ERR STORE: StorageType must be write commands (LPUSH,RPUSH,LSET,SADD,ZADD,HSET,SET,SETNX,APPEND,SETEX,SELECT,SCAN)");
    end

    local argc = internal_StorageCommands[string.upper(argv[1])];
    local cols = internal_ParseCSVLine(clist, ",");

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
    local t = alchemy('select', clist, "FROM", tlist, "WHERE", wc);
    if (t == nil) then return 0; end

    -- 4.) iterate thru results, running "command" on each row
    for key,value in ipairs(t) do
        -- 5.) arguments are comma delimited - parse
        local rcols = internal_ParseCSVLine(value, ",");
        --for k,v in pairs(rcols) do print('COLS: ' .. k .. "\t" .. v) end
        local nargv = internal_iTableCopy(argv); -- copy argv
        if (cmplx) then
            local larg  = argv[#argv];         -- get final arg
            local nlarg = larg .. rcols[1];    -- modify: append first column
            table.remove(nargv);               -- remove final arg
            table.insert(nargv, nlarg);        -- add modified @end
            for i = 2, #rcols do
                table.insert(nargv, rcols[i]); -- then add rest of rcols
            end
            --for k,v in ipairs(nargv) do print('CX_ARGV: ' .. v) end
            alchemy(unpack(nargv));
        else
            for i = 1, #rcols do
                table.insert(nargv, rcols[i]); -- add rcols
            end
            --for k,v in ipairs(nargv) do print('ARGV: ' .. v) end
            alchemy(unpack(nargv));
        end
    end
    return #t;
end

-- CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS
-- CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS
function internal_run_cmd(cmd)
    local argv = {};
    for token in string.gmatch(cmd, "[^%s]+") do -- poor man's split()
       table.insert(argv, token);
    end
    return alchemy(unpack(argv));
end

function internal_copy_table_from_select(tname, clist, tlist, whereclause)
    --print ('internal_copy_table_from_select tname: ' .. tname ..
        --' clist: ' .. clist .. ' tlist: ' .. tlist .. ' wc: ' .. whereclause);
    local argv      = {"SELECT", clist, "FROM", tlist, "WHERE", whereclause};
    local res      = alchemy(unpack(argv));
    local inserter = {"INSERT", "INTO", tname, "VALUES", "()" };
    for k,v in pairs(res) do
         inserter[5] = '(' .. v .. ')';
         alchemy(unpack(inserter));
    end
    return #res;
end

function internal_cr8tbl_as_command(tname, ascmd)
    local cmd = string.sub(ascmd, 4);
    --print ('internal_cr8tbl_as_command cmd: ' .. cmd);
    local res = internal_run_cmd(cmd);
    if type(res) == "table" then
        alchemy("CREATE", "TABLE", tname, "(pk INT, value TEXT)");
        local inserter = {"INSERT", "INTO", tname, "VALUES", "()" };
        for k,v in pairs(res) do
             inserter[5] = '(' .. k .. ',\'' .. v .. '\')';
             alchemy(unpack(inserter));
        end
    else
        return res; -- single line responses should be errors
    end
    return #res;
end

function internal_cr8tbl_as_dump(tname, dtype, dumpee)
    --print ('internal_cr8tbl_as_dump: dtype: ' .. dtype .. ' ee: ' .. dumpee);
    local ttype; local cdef;
    if (dtype == 'LIST') then
        cdef  = "(pk INT, value TEXT)";
        ttype = 1;
    elseif (dtype == 'SET') then
        cdef  = "(pk INT, value TEXT)";
        ttype = 1;
    elseif (dtype == 'ZSET') then
        cdef  = "(pk INT, name TEXT, value FLOAT)";
        ttype = 2;
    elseif (dtype == 'HASH') then
        cdef  = "(pk INT, name TEXT, value TEXT)";
        ttype = 3;
    end
    --print ('cdef: ' .. cdef);
    alchemy("CREATE", "TABLE", tname, cdef);
    local inserter = {"INSERT", "INTO", tname, "VALUES", "()" };
    res = {};
    if (dtype == 'LIST') then
        res    = alchemy("LRANGE", dumpee, 0, -1);
    elseif (dtype == 'SET') then
        res    = alchemy("SMEMBERS", dumpee);
    elseif (dtype == 'ZSET') then
        res    = alchemy("ZRANGE", dumpee, 0, -1, "WITHSCORES");
    elseif (dtype == 'HASH') then
        res    = alchemy("HGETALL", dumpee);
    end
    local cnt = 1;
    if (ttype == 1) then
        for k,v in pairs(res) do
            local value_string = '(' .. k .. ',\'' .. v .. '\')';
            inserter[5] = value_string;
            alchemy(unpack(inserter));
            cnt = cnt + 1;
            --for k,v in pairs(inserter) do print (k .. ': ' .. v); end
        end
    else
        local value_string;
        local dres = {};
        local i    = 0;
        for k,v in pairs(res) do
            if (i == 0) then
                value_string = '(' .. cnt .. ',\'' .. v .. '\',';
                i = 1;
            else
                if (ttype == 2) then -- ZSET
                    value_string = value_string .. v .. ')';
                else                 -- HASH
                    value_string = value_string .. '\'' .. v .. '\')';
                end
                inserter[5] = value_string;
                alchemy(unpack(inserter));
                --for k2,v2 in pairs(inserter) do print (k2 .. ': ' .. v2); end
                cnt = cnt + 1;
                i = 0;
            end
         end
    end
    return (cnt - 1);
end

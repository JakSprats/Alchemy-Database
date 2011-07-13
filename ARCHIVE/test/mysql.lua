package.path = package.path .. ";;test/?.lua"
require "is_external"

if is_external.yes == 1 then
    _print = print;
else
    _print = is_external._print;
end

-- MYSQL INTEGRATION
require "luasql.mysql" 
mysql_db   = "backupdb";
mysql_user = "root";
mysql_pass = "";
mysql_host = "localhost";

function connect_mysql()
    env  = luasql.mysql();
    conn = env:connect(mysql_db, mysql_user, mysql_pass, mysql_host);
end

function close_mysql()
    conn:close();
    env:close();
end

function backup_database()
    if is_external.yes == 0 then
        is_external.output = '';
    end

    connect_mysql();
    tbls = {};
    date = os.date('*t');
    ds   = date["year"] .. "_" .. date["month"] .. "_" .. date["day"];
    ks   = keys('*');
    for num, rkey in pairs(ks) do
        rtype = atype(rkey);
        if rtype ~= is_external.delim .. "index" and 
           rtype ~= is_external.delim .. "string" then
            bname = ds .. "_backup_" .. rkey;
            table.insert(tbls, rkey .. ' -> ' .. bname)
            backup_object(rkey, rtype, bname);
        end
    end
    close_mysql();
    if is_external.yes == 1 then
        for i,v in ipairs(tbls) do _print(i,v); end
    end
    return tbls;
end

function backup_object(rkey, rtype, bname)
    if rtype == "table" then
        -- print ("table " .. rkey .. " bname: " .. bname);
        dump = dump_to_mysql(rkey, bname);
    else
        -- print ("object " .. rkey);
        create_table_as(bname, 'DUMP ' .. rkey);
        dump = dump_to_mysql(bname, bname);
        drop_table(bname);
    end
    for key, mline in pairs(dump) do
        conn:execute(mline);
    end
end

if is_external.yes == 1 then
    backup_database();
end

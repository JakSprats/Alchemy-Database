
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
  connect_mysql();
  tbls = {};
  date = os.date('*t');
  ds   = date["year"] .. "_" .. date["month"] .. "_" .. date["day"];
  keys = client('keys', '*');
  for num, rkey in pairs(keys) do
    rtype = client("TYPE", rkey);
    if rtype ~= "+index" and rtype ~= "+string" then
      bname = ds .. "_backup_" .. rkey;
      table.insert(tbls, rkey .. ' -> ' .. bname)
      backup_object(rkey, rtype, bname);
    end
  end
  close_mysql();
  return tbls;
end

function backup_object(rkey, rtype, bname)
  if rtype == "+table" then
    --print ("table " .. rkey);
    dump = client('dump', rkey, 'to', 'mysql', bname);
  else
    --print ("object " .. rkey);
    client('create', 'table', bname, 'AS DUMP ' .. rkey);
    dump = client('dump', bname, 'to', 'mysql', bname);
    client('drop', 'table', bname);
  end
  for key, mline in pairs(dump) do
    conn:execute(mline);
  end
end

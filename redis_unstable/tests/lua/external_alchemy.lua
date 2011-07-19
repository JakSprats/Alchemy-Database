package.path = package.path .. ";redis-lua/src/?.lua;"
require "redis"

Redis.define_command('create', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args < 2 then
      print ('Usage: create [\'table\'|\'index\'|\'luatrigger\'] name [column_definitions]');
        return false;
    end
    table.insert(arguments, args[1]);
    table.insert(arguments, args[2]);
    if #args > 2 and string.lower(tostring(args[1])) == 'table' then
      table.insert(arguments, '(' .. args[3] .. ')');
    else
      for i = 3, #args do
        table.insert(arguments, args[i]);
      end
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('drop', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args ~= 2 then
      print ('Usage: drop([\'table\'|\'index\'|\'luatrigger\'], name)');
      return false;
    end
    table.insert(arguments, args[1]);
    table.insert(arguments, args[2]);
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('desc', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args ~= 1 then
      print ('Usage: desc(tablename)');
      return false;
    end
    table.insert(arguments, args[1]);
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('dump', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
print (#args);
    if #args < 1 then
      print ('Usage: dump(tablename, [\'TO\', \,MYSQL\', mysql_tablename] OR [\'TO\', \,FILE\', filename])');
      return false;
    end
    for i = 1, #args do
      table.insert(arguments, args[i]);
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('insert', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args < 4 then
      print ('Usage: insert(\'INTO\', tablename, \'VALUE\', valuelist)');
      return false;
    end
    for i = 1, #args do
      table.insert(arguments, args[i]);
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('select', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args == 1 then
      table.insert(arguments, args[1]);
    elseif #args == 3 then
      table.insert(arguments, args[1]);
      table.insert(arguments, 'FROM');
      table.insert(arguments, args[2]);
      table.insert(arguments, 'WHERE');
      table.insert(arguments, args[3]);
    else
      print ('Usage: select(cols, tables, whereclause)');
      return false;
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('scan', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args < 2 then
      print ('Usage: scan(cols, tables, whereclause)');
      return false;
    end
    table.insert(arguments, args[1]);
    table.insert(arguments, 'FROM');
    table.insert(arguments, args[2]);
    if #args > 2 then
      local ordby = string.match(args[3], "^ORDER BY");
      if (not ordby) then
        table.insert(arguments, 'WHERE');
      end
      table.insert(arguments, args[3]);
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('update', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args < 5 then
      print ('Usage: update(tablename, \'SET\', columnlist, \'WHERE\', whereclause)');
      return false;
    end
    for i = 1, #args do
      table.insert(arguments, args[i]);
    end
    client.requests.multibulk(client, command, arguments)
  end
})
Redis.define_command('delete', {
  request = function(client, command, ...)
    local args, arguments = {...}, {}
    if #args < 5 then
      print ('Usage: delete(\'FROM\', tablename, \'WHERE\', whereclause)');
      return false;
    end
    for i = 1, #args do
      table.insert(arguments, args[i]);
    end
    client.requests.multibulk(client, command, arguments)
  end
})

redis = Redis.connect('127.0.0.1', 6379);

-- LUA
function luafunc(...)
  return redis:luafunc(...);
end

-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
function create_table(tname, col_defs)
  return redis:create('TABLE', tname, col_defs);
end
function create_table_as_select(tname, select_command)
  return redis:create('TABLE', tname, select_command)
end
function drop_table(tname)
  local s,e = pcall(redis.drop, redis, 'TABLE', tname);
  if not s then
    print ('DROP TABLE ERROR');
    print (s,e);
  end
end
function desc(tname)
  return redis:desc(tname);
end
function dump(tname)
  return redis:dump(tname);
end
function dump_to_mysql(tname, msname)
  if (msname) then
    return redis:dump(tname, 'TO', 'MYSQL', msname);
  else
    return redis:dump(tname, 'TO', 'MYSQL');
  end
end
function dump_to_file(tname, fname)
  return redis:dump(tname, 'TO', 'FILE', fname);
end

function create_index(iname, tname, column)
  return redis:create('INDEX', iname, 'ON', tname, '(' .. column .. ')');
end
function create_unique_index(iname, tname, column)
  return redis:create('UNIQUE', 'INDEX', iname, 'ON', tname, '(' .. column .. ')');
end
function drop_index(iname)
  local s,e = pcall(redis.drop, redis, 'INDEX', iname);
  if not s then
    print ('DROP INDEX ERROR');
    print (s,e);
  end
end

function create_luatrigger(trname, tname, ...)
  return redis:create("LUATRIGGER", trname, "ON", tname, ...);
end
function drop_luatrigger(trname)
  local s,e = pcall(redis.drop, redis, 'LUATRIGGER', iname);
  if not s then
    print ('DROP LUATRIGGER ERROR');
    print (s,e);
  end
end

function insert(tname, values_list)
  return redis:insert('INTO', tname, 'VALUES', '(' .. values_list .. ')');
end
function insert_return_size(tname, values_list)
  return redis:insert('INTO', tname, 'VALUES', '(' .. values_list .. ')', 'RETURN SIZE');
end

function select(...)
  return redis:select(...);
end
function select_count(...)
  return redis:select('COUNT(*)', ...);
end
function scan(...)
  return redis:scan(...);
end
function scan_count(...)
  return redis:scan('COUNT(*)', ...);
end

function delete(tname, where_clause)
  return redis:delete('FROM', tname, 'WHERE', where_clause);
end
function update(tname, update_list, where_clause)
  return redis:update(tname, 'SET', update_list, 'WHERE', where_clause);
end

-- TODO bulk_insert & insert_on_duplicate_key_update & replace
-- TODO "show tables"

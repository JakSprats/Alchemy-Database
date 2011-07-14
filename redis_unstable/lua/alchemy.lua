--package.path = package.path .. ";;test/?.lua"

-- LUA
function luafunc(...)
  return redis('LUAFUNC', ...)
end

-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
-- SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
function create_table(tname, col_defs)
  return redis('CREATE', 'TABLE', tname, "(" .. col_defs .. ")");
end
function create_table_as_select(tname, select_command)
  return redis('CREATE', 'TABLE', tname, select_command);
end
function drop_table(tname)
  return redis('DROP', 'TABLE', tname);
end
function desc(tname)
  return redis('DESC', tname);
end
function dump(tname)
  return redis('DUMP', tname);
end
function dump_to_mysql(tname, msname)
    if (msname) then
        return redis('DUMP', tname, "TO", "MYSQL", msname);
    else
        return redis('DUMP', tname, "TO", "MYSQL");
    end
end
function dump_to_file(tname, fname)
  return redis('DUMP', tname, "TO", "FILE", fname);
end

function create_index(iname, tname, column)
  col_w_paren = "(" .. column .. ")";
  return redis('CREATE', "INDEX", iname, "ON", tname, col_w_paren);
end
function create_unique_index(iname, tname, column)
  col_w_paren = "(" .. column .. ")";
  return redis('CREATE', "UNIQUE", "INDEX", iname, "ON", tname, col_w_paren);
end
function drop_index(iname)
  return redis('DROP', 'index', iname);
end

function create_luatrigger(trname, tname, ...)
  return redis('CREATE', "LUATRIGGER", trname, "ON", tname, ...);
end
function drop_luatrigger(trname)
  return redis('DROP', 'LUATRIGGER', trname);
end

function insert(tname, values_list)
  return redis('INSERT', "INTO", tname, "VALUES", "(" .. values_list .. ")");
end

function insert_return_size(tname, values_list)
  return redis('INSERT', "INTO", tname, "VALUES", "(" .. values_list .. ")",
                "RETURN SIZE");
end

-- "select" is used in both redis and Redisql, so it must be overridden here
-- for redis: select(db)
-- for SQL: select(col_list, tname, where_clause)
function select(...)
  local args = {...};
  argCount = #args;
  if (argCount == 1) then
    return redis('SELECT', args[1]);
  else
    return redis('SELECT', args[1], "FROM", args[2], "WHERE", args[3]);
  end
end
function select_count(...)
  local args = {...};
  return redis('SELECT', 'COUNT(*)', "FROM", args[1], "WHERE", args[2]);
end

function scanselect(...)
  local args = {...};
  argCount = #args;
  if (argCount == 2) then
    return redis('SCANSELECT', args[1], "FROM", args[2]);
  else
    return redis('SCANSELECT', args[1], "FROM", args[2], "WHERE", args[3]);
  end
end
function scanselect_count(...)
  local args = {...};
  argCount = #args;
  local cnt;
  if (argCount == 1) then
    return redis('SCANSELECT', "COUNT(*)", "FROM", args[1]);
  else
    return redis('SCANSELECT', "COUNT(*)", "FROM", args[1], "WHERE", args[2]);
  end
end

function delete(tname, where_clause)
  return redis('DELETE', "FROM", tname, "WHERE", where_clause);
end
function update(tname, update_list, where_clause)
  return redis('UPDATE', tname, "SET", update_list, "WHERE", where_clause);
end

-- TODO bulk_insert & insert_on_duplicate_key_update & replace
-- TODO "show tables"

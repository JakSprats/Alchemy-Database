--
-- NOTE: DO NOT MOVE THIS FILE
-- this file is loaded when the server starts and 
-- the server relies on these functions
--
require "pluto"

dofile 'extra/dumper.lua';

-- DOT_NOTATION_INDEX DOT_NOTATION_INDEX DOT_NOTATION_INDEX DOT_NOTATION_INDEX
function setIndex(tbl, col, el, pk, val)
  return alchemySetIndex(tbl, col, el, pk, val);
end
function updateIndex(tbl, col, el, pk, old, new)
  return alchemyUpdateIndex(tbl, col, el, pk, old, new);
end
function deleteIndex(tbl, col, el, pk, old)
  return alchemyDeleteIndex(tbl, col, el, pk, old);
end

STBL = {}; IEL = {};

function ASQL_setter(rname, k, v)
  local tbl, col, pk = rname._tbl, rname._col, rname._pk;
  --print('LUA: ASQL_setter: tbl: ' .. tbl .. ' col: ' .. col .. ' pk: ' .. pk);
  local ok = true;
  if (IEL[tbl][col][k] ~= nil) then
    if     (STBL[tbl][col][pk][k] == nil) then
      ok = setIndex   (tbl, col, k, pk, v);
    elseif (v == nil)          then
      ok = deleteIndex(tbl, col, k, pk, STBL[tbl][col][pk][k]);
    else 
      ok = updateIndex(tbl, col, k, pk, STBL[tbl][col][pk][k], v);
    end
  end
  if (ok) then rawset(STBL[tbl][col][pk], k, v); end
end

-- NOTE this MUST not be called from Lua (only From C)
function dropIndLuaEl(tbl, col, el)
  IEL[tbl][col][el] = nil;
  --print('LUA: dropIndLuaEl: IEL: '); dump(IEL);
end
-- NOTE this MUST not be called from Lua (only From C)
function createIndLuaEl(tbl, col, el)
  if (IEL[tbl][col] == nil) then IEL[tbl][col] = {}; end
  IEL[tbl][col][el] = true;
  --print('LUA: createIndLuaEl: IEL: '); dump(IEL);
end

-- LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT
--TODO "ASQL" defined in Lua, not in C
function create_nested_table(asql, tbl, col)
  --print ('LUA: create_nested_table: tbl: ' .. tbl .. ' col: ' .. col);
  if (_G[asql]           == nil) then
    _G[asql]           = {};
  end
  if (_G[asql][tbl]      == nil) then
    _G[asql][tbl]      = {}; STBL[tbl]      = {}; IEL[tbl]      = {};
  end
  if (_G[asql][tbl][col] == nil) then
    _G[asql][tbl][col] = {}; STBL[tbl][col] = {}; IEL[tbl][col] = {};
  end
end

function luaobj_assign(asql, tbl, col, pk, luae) -- create Lua Object Row
  --print('LUA: luaobj_assign: STBL: '); dump(STBL);
  _G[asql][tbl][col][pk] = {};
  local cmd = 'EVALED = ' ..  luae .. ';'; assert(loadstring(cmd))()
  STBL[tbl][col][pk] = EVALED;
  _G[asql][tbl][col][pk]._tbl = tbl;
  _G[asql][tbl][col][pk]._col = col;
  _G[asql][tbl][col][pk]._pk  = pk;
  setmetatable(_G[asql][tbl][col][pk],
               {__index=STBL[tbl][col][pk], __newindex=ASQL_setter});
end
function delete_luaobj(asql, tbl, col, pk)
  --print ('LUA: delete_luaobj');
  _G[asql][tbl][col][pk] = nil; STBL[tbl][col][pk] = nil;
end

function DataDumperLuaObj(tbl, col, pk)
  return DataDumper(STBL[tbl][col][pk]);
end

-- CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS CREATE_TABLE_AS
function internal_copy_table_from_select(tname, clist, tlist, whereclause)
    -- print ('internal_copy_table_from_select tname: ' .. tname ..
       -- ' clist: ' .. clist .. ' tlist: ' .. tlist .. ' wc: ' .. whereclause);
    local argv      = {"SELECT", clist, "FROM", tlist, "WHERE", whereclause};
    local res      = redis(unpack(argv));
    local inserter = {"INSERT", "INTO", tname, "VALUES", "()" };
    for k,v in pairs(res) do
         local vallist = '';
         for kk,vv in pairs(v) do
             if (string.len(vallist) > 0) then
                 vallist = vallist .. vv .. ",";
             end
             if (type(vv) == "number")  then
                 vallist = vallist .. vv;
             else
                 vallist = vallist .. "'" .. vv .. "'";
             end
         end
         inserter[5] = '(' .. vallist .. ')';
         redis(unpack(inserter));
    end
    return #res;
end

-- LUAOBJ_PERSISTENCE LUAOBJ_PERSISTENCE LUAOBJ_PERSISTENCE
function open_or_error(file)
  infile, err = io.open(file, "rb")
  if (infile == nil) then error("While opening: " .. (err or "no error")) end
  buf, err = infile:read("*a")
  if (buf == nil) then error("While reading: " .. (err or "no error")) end
  infile:close()
  return buf
end

local ASQL_dump_file="ASQL.lua.rdb"
local STBL_dump_file="STBL.lua.rdb"
local IEL_dump_file ="IEL.lua.rdb"

function save_lua_universe()
  local ptable  = { 1234 };
  local perms   = { [coroutine.yield] = 1, [ptable] = 2 };
  local buf     = pluto.persist(perms, ASQL);
  local outfile = io.open(ASQL_dump_file, "wb");
  outfile:write(buf); outfile:close();
  buf           = pluto.persist(perms, STBL);
  outfile       = io.open(STBL_dump_file, "wb");
  outfile:write(buf); outfile:close();
  buf           = pluto.persist(perms, IEL);
  outfile       = io.open(IEL_dump_file, "wb");
  outfile:write(buf); outfile:close();
end

function get_lua_universe()
  local ptable  = { 1234 };
  local perms   = { [coroutine.yield] = 1, [ptable] = 2 };
  local buf     = open_or_error(ASQL_dump_file);
  ASQL          = pluto.unpersist(perms, buf);
  buf           = open_or_error(STBL_dump_file);
  STBL          = pluto.unpersist(perms, buf)
  buf           = open_or_error(IEL_dump_file);
  IEL           = pluto.unpersist(perms, buf)
end

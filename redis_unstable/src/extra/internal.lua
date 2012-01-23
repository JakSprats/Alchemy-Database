--
-- NOTE: DO NOT MOVE THIS FILE
-- this file is loaded when the server starts and 
-- the server relies on these functions
--

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

Stbl = {}; Iel = {};

function ASQL_setter(rname, k, v)
  local tbl, col, pk = rname._tbl, rname._col, rname._pk;
  --print('LUA: ASQL_setter: tbl: ' .. tbl .. ' col: ' .. col .. ' pk: ' .. pk);
  local ok = true;
  if (Iel[tbl][col][k] ~= nil) then
    if     (Stbl[tbl][col][pk][k] == nil) then
      ok = setIndex   (tbl, col, k, pk, v);
    elseif (v == nil)          then
      ok = deleteIndex(tbl, col, k, pk, Stbl[tbl][col][pk][k]);
    else 
      ok = updateIndex(tbl, col, k, pk, Stbl[tbl][col][pk][k], v);
    end
  end
  if (ok) then rawset(Stbl[tbl][col][pk], k, v); end
end

-- NOTE this MUST not be called from Lua (only From C)
function dropIndLuaEl(tbl, col, el)
  Iel[tbl][col][el] = nil;
  --print('LUA: dropIndLuaEl: Iel: '); dump(Iel);
end
-- NOTE this MUST not be called from Lua (only From C)
function createIndLuaEl(tbl, col, el)
  if (Iel[tbl][col] == nil) then Iel[tbl][col] = {}; end
  Iel[tbl][col][el] = true;
  --print('LUA: createIndLuaEl: Iel: '); dump(Iel);
end

-- LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT
function create_nested_table(asql, tbl, col)
  --print ('LUA: create_nested_table: tbl: ' .. tbl .. ' col: ' .. col);
  if (_G[asql]           == nil) then
    _G[asql]           = {};
  end
  if (_G[asql][tbl]      == nil) then
    _G[asql][tbl]      = {}; Stbl[tbl]      = {}; Iel[tbl]      = {};
  end
  if (_G[asql][tbl][col] == nil) then
    _G[asql][tbl][col] = {}; Stbl[tbl][col] = {}; Iel[tbl][col] = {};
  end
end

function luaobj_assign(asql, tbl, col, pk, luae) -- create Lua Object Row
  --print('LUA: luaobj_assign: Stbl: '); dump(Stbl);
  _G[asql][tbl][col][pk] = {};
  local cmd = 'EVALED = ' ..  luae .. ';'; assert(loadstring(cmd))()
  Stbl[tbl][col][pk] = EVALED;
  _G[asql][tbl][col][pk]._tbl = tbl;
  _G[asql][tbl][col][pk]._col = col;
  _G[asql][tbl][col][pk]._pk  = pk;
  setmetatable(_G[asql][tbl][col][pk],
               {__index=Stbl[tbl][col][pk], __newindex=ASQL_setter});
end
function delete_luaobj(asql, tbl, col, pk)
  --print ('LUA: delete_luaobj');
  _G[asql][tbl][col][pk] = nil; Stbl[tbl][col][pk] = nil;
end

function DataDumperLuaObj(tbl, col, pk)
  return DataDumper(Stbl[tbl][col][pk]);
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

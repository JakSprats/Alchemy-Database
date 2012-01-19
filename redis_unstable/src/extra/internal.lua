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

-- TODO Stbl & Iel should mimic ASQL[] -> X[tbl][col][pk]
Stbl = {}; Iel = {};

function ASQL_setter(rname, k, v)
  local e, p = get_ASQL_data_slots(rname._tbl, rname._col, rname._pk);
  print('ASQL_setter: p: ' .. p .. ' e: ' .. e);
  local ok   = true;
  if (Iel[e][k] ~= nil) then
    if     (Stbl[p][k] == nil) then
      ok = setIndex   (rname._tbl, rname._col, k, rname._pk, v);
    elseif (v == nil)          then
      ok = deleteIndex(rname._tbl, rname._col, k, rname._pk, Stbl[p][k]);
    else 
      ok = updateIndex(rname._tbl, rname._col, k, rname._pk, Stbl[p][k], v);
    end
  end
  if (ok) then rawset(Stbl[p], k, v); end
end

function get_ASQL_data_slots(tbl, col, pk)
  local ielname = tbl .. '.' .. col;
  local pkname  = tbl .. '.' .. col .. '.pk_' .. pk;
  return ielname, pkname;
end

-- NOTE this MUST not be called from Lua (only From C)
function indexLORfield(tbl, col, pk, el)
  local e, p = get_ASQL_data_slots(tbl, col, pk);
  Iel [e][el] = true;
  --print ('indexLORfield: ' .. e .. ' el: ' .. el); dump(Iel);
end

-- LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT
function create_nested_table(asql, tbl, col)
  if (_G[asql]           == nil) then _G[asql]           = {}; end
  if (_G[asql][tbl]      == nil) then _G[asql][tbl]      = {}; end
  if (_G[asql][tbl][col] == nil) then _G[asql][tbl][col] = {}; end
end

function luaobj_assign(asql, tbl, col, pk, luae) -- create Lua Object Row
  local e, p = get_ASQL_data_slots(tbl, col, pk);
  Stbl[p] = {}; Iel[e] = {};
  _G[asql][tbl][col][pk] = {};
  local cmd  = 'Stbl[\'' .. p .. '\'] = ' .. luae .. '; '; --print(cmd);
  assert(loadstring(cmd))()
  _G[asql][tbl][col][pk]._tbl = tbl; --TODO store at "tbl" level
  _G[asql][tbl][col][pk]._col = col; --TODO store at "col" level
  _G[asql][tbl][col][pk]._pk  = pk;
  setmetatable(_G[asql][tbl][col][pk],
               {__index=Stbl[p], __newindex=ASQL_setter});
end

function DataDumperLuaObj(tbl, col, pk)
  local name = tbl .. '.' .. col .. '.pk_' .. pk;
  local e, p = get_ASQL_data_slots(tbl, col, pk);
  return DataDumper(Stbl[p]);
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

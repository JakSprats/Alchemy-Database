--
-- NOTE: DO NOT MOVE THIS FILE
-- this file is loaded when the server starts and 
-- the server relies on these functions
--
package.path = package.path .. ";./core/?.lua;" .. 
                                "./core/luaforge_json/trunk/json/?.lua"

require "pluto"
dofile  "./core/dumper.lua";
Json = require("json") -- Alchemy's IO is done via JSON (must be global)

-- KEYWORDS(reserved DataStructureNames) KEYWORDS(reserved DataStructureNames)
local ReadOnlyKeywords = {node = true;}
function checkReadOnlyKeywords(k)
  if (ReadOnlyKeywords[k]) then
    error("ERROR: '" .. k .. "' is a ReadOnlyKeywords");
  end
end

-- DOT_NOTATION_INDEX DOT_NOTATION_INDEX DOT_NOTATION_INDEX DOT_NOTATION_INDEX
local LOTBL = "ASQL";
STBL = {}; IEL = {};

function ASQL_setter(rname, k, v)
  checkReadOnlyKeywords(k)
  local tbl, col, pk = rname._tbl, rname._col, rname._pk;
  --print('LUA: ASQL_setter: tbl: ' .. tbl .. ' col: ' .. col .. ' pk: ' .. pk);
  local ok = true;
  if (IEL[tbl][col][k] ~= nil) then
    if     (STBL[tbl][col][pk][k] == nil) then
      ok = alchemySetIndex   (tbl, col, k, pk, v);
    elseif (v == nil)          then
      ok = alchemyDeleteIndex(tbl, col, k, pk, STBL[tbl][col][pk][k]);
    else 
      ok = alchemyUpdateIndex(tbl, col, k, pk, STBL[tbl][col][pk][k], v);
    end
  end
  if (ok) then rawset(STBL[tbl][col][pk], k, v); end
end

-- NOTE this MUST not be called from Lua (only From C)
function dropIndLuaEl(tbl, col, el)
  IEL[tbl][col][el] = nil;
end
-- NOTE this MUST not be called from Lua (only From C)
function createIndLuaEl(tbl, col, el)
  checkReadOnlyKeywords(el)
  if (IEL[tbl][col] == nil) then IEL[tbl][col] = {}; end
  IEL[tbl][col][el] = true;
end

-- LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT LUAOBJ_ASSIGNMENT
-- NOTE: fromrdb means a fully nested STBL[] was already created
function create_nested_table(tbl, col, fromrdb)
  --print ('LUA: create_nested_table: tbl: ' .. tbl .. ' col: ' .. col);
  if (_G[LOTBL]           == nil) then _G[LOTBL]           = {}; end
  if (_G[LOTBL][tbl]      == nil) then _G[LOTBL][tbl]      = {}; 
    if (not fromrdb) then STBL[tbl]      = {}; IEL[tbl]      = {}; end
  end
  if (_G[LOTBL][tbl][col] == nil) then _G[LOTBL][tbl][col] = {}; 
    if (not fromrdb) then STBL[tbl][col] = {}; IEL[tbl][col] = {}; end
  end
end

local function createASQLforSTBL(tbl, col, pk)
  _G[LOTBL][tbl][col][pk] = {};
  _G[LOTBL][tbl][col][pk]._tbl = tbl;
  _G[LOTBL][tbl][col][pk]._col = col;
  _G[LOTBL][tbl][col][pk]._pk  = pk;
  setmetatable(_G[LOTBL][tbl][col][pk],
               {__index=STBL[tbl][col][pk], __newindex=ASQL_setter});
end
function luaobj_assign(tbl, col, pk, luae) -- create Lua Object Row
  --print ('luaobj_assign: tbl: ' .. tbl .. ' col: ' .. col .. ' pk: ' .. pk);
  createASQLforSTBL(tbl, col, pk);
  STBL[tbl][col][pk] = Json.decode(luae);
end
function delete_luaobj(tbl, col, pk)
  _G[LOTBL][tbl][col][pk] = nil; STBL[tbl][col][pk] = nil;
end

-- LUAOBJ_TO_OUTPUT LUAOBJ_TO_OUTPUT LUAOBJ_TO_OUTPUT LUAOBJ_TO_OUTPUT
function DumpLuaObjForOutput(tbl, col, pk)
  return Json.encode(STBL[tbl][col][pk]);
end
function DumpFunctionForOutput(func, ...)
  local res = func(...);
  if (type(res) == "boolean" or
      type(res) == "number"  or
      type(res) == "string")    then return res; end
  return Json.encode(res);
end

-- LUAOBJ_PERSISTENCE LUAOBJ_PERSISTENCE LUAOBJ_PERSISTENCE
local function open_or_error(file)
  infile, err = io.open(file, "rb")
  if (infile == nil) then error("While opening: " .. (err or "no error")) end
  buf, err = infile:read("*a")
  if (buf == nil) then error("While reading: " .. (err or "no error")) end
  infile:close()
  return buf
end

local STBL_dump_file = "STBL.lua.rdb" local IEL_dump_file = "IEL.lua.rdb"
function save_lua_universe()
  local ptable = { 1234 };
  local perms  = { [coroutine.yield] = 1, [ptable] = 2 };
  local buf    = pluto.persist(perms, STBL);
  local ofile  = io.open(STBL_dump_file, "wb"); ofile:write(buf); ofile:close();
  buf          = pluto.persist(perms, IEL);
  ofile        = io.open(IEL_dump_file,  "wb"); ofile:write(buf); ofile:close();
end

function get_lua_universe()
  local ptable = { 1234 };
  local perms  = { [coroutine.yield] = 1, [ptable] = 2 };
  local buf    = open_or_error(STBL_dump_file);
  STBL         = pluto.unpersist(perms, buf)
  buf          = open_or_error(IEL_dump_file);
  IEL          = pluto.unpersist(perms, buf)
  _G[LOTBL]    = {};
  for tbl, colt in pairs(STBL) do -- Create ASQL[] from STBL[] values
    for col, pkt in pairs(colt) do
      create_nested_table(LOTBL, tbl, col, 1)
      for pk, n in pairs(pkt) do
        createASQLforSTBL(LOTBL, tbl, col, pk);
      end
    end
  end
end

-- SAMPLE_LUA_RESPONSE_ROUTINES SAMPLE_LUA_RESPONSE_ROUTINES
function output_start(card)
    --print ('LUA: output_start: card: ' .. card);
    if (card > 0) then return '*' .. (card + 1) .. '\r\n';
    else               return '*-1\r\n';                    end
end
local function output_delim(...)
   local printResult = '|';
   for i,v in ipairs(arg) do
     if (string.len(printResult) > 1) then printResult = printResult .. "|"; end
     printResult = printResult .. tostring(v);
   end
   printResult = printResult .. "|";
   return '$' .. string.len(printResult) .. '\r\n' .. printResult .. '\r\n';
end
function output_cnames(...)
   --print ('LUA: output_cnames');
   return output_delim(...)
end
function output_row(...)
   --print ('LUA: output_row');
   return output_delim(...)
end

-- HTTP_LUA_RESPONSE_ROUTINES HTTP_LUA_RESPONSE_ROUTINES
RowCounter = 0;
function output_start_http(card)
    RowCounter = 0;
    return "nrows=" .. card .. ';\r\n';
end
local function output_delim_http(...)
   local printResult = '';
   for i,v in ipairs(arg) do
     if (string.len(printResult) > 0) then printResult = printResult .. ","; end
     printResult = printResult .. tostring(v);
   end
   return printResult .. ';\r\n';
end
function output_cnames_http(...)
   return 'ColumnNames=' .. output_delim_http(...);
end
function output_row_http(...)
  RowCounter = RowCounter + 1;
  return 'row[' .. RowCounter .. ']=' .. output_delim_http(...);
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


--TODO make OUTGOING & INCOMING GlobalConstants

local Queue = require "Queue"
DIRECTION_OUT = 2;
DIRECTION_IN  = 1;

-- NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES
function createNode(lo, pk)
  if     (lo      == nil) then error("createNode(x) - x does not exist");
  elseif (lo.node ~= nil) then error("createNode - Node already exists");
  elseif (pk      == nil) then error("createNode(x, pk) - pk not defined"); end
  lo.node = { __pk=pk; }
  --TODO add in __newindex to make lo.node READONLY
  --      (need bool in mod funcs to turn off/on READONLY)
end

function deleteNode(lo)
  if     (lo      == nil) then error("deleteNode(x) - x does not exist");
  elseif (lo.node == nil) then error("deleteNode - Node does not exists"); end
  lo.node = nil;
end

-- PROPERTIES PROPERTIES PROPERTIES PROPERTIES PROPERTIES PROPERTIES
function addNodeProperty(node, key, value)
  if     (node      == nil) then
    error("addNodePropery(x) - x does not exist");
  elseif (node[key] ~= nil) then
    error("addNodePropery(x, key) - key already exists");
  end
  node[key] = value;
end
function deleteNodeProperty(node, key)
  if     (node      == nil) then
    error("deleteNodePropery(x) - x does not exist");
  elseif (node[key] == nil) then
    error("deleteNodePropery(x, key) - key does not exists");
  end
  node[key] = nil;
end

-- RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS
local function validateNodesInRel(snode, rtype, tnode)
  if     (snode      == nil)                                  then
    error("addNodeRelationShip(snode, ...) - snode does not exist");
  elseif (snode.__pk == nil)                                  then
    error("addNodeRelationShip(snode, ..., snode) - snode is not a NODE");
  elseif (tnode      == nil)                                  then
    error("addNodeRelationShip(snode, ..., tnode) - tnode does not exist");
  elseif (tnode.__pk == nil)                                  then
    error("addNodeRelationShip(snode, ..., tnode) - tnode is not a NODE");
  elseif (rtype      == nil)                                  then
    error("addNodeRelationShip(snode, rtype, ...) - rtype must be defined");
  end
end

function addNodeRelationShip(snode, rtype, tnode)
  validateNodesInRel(snode, rtype, tnode)
  local sd, td = 2, 1;
  if (snode.r            == nil) then snode.r            = {}; end
  if (snode.r[rtype]     == nil) then snode.r[rtype]     = {}; end
  if (snode.r[rtype][sd] == nil) then snode.r[rtype][sd] = {}; end
  if (tnode.r            == nil) then tnode.r            = {}; end
  if (tnode.r[rtype]     == nil) then tnode.r[rtype]     = {}; end
  if (tnode.r[rtype][td] == nil) then tnode.r[rtype][td] = {}; end
  snode.r[rtype][sd][tnode.__pk]        = {};
  snode.r[rtype][sd][tnode.__pk].target = tnode;
  tnode.r[rtype][td][snode.__pk]        = {};
  tnode.r[rtype][td][snode.__pk].target = snode;
end

local function isTableEmpty(t)
  for k,v in pairs(t) do return false; end
  return true;
end
local function reduceRel(snode, rtype, sd)
  if (isTableEmpty(snode.r[rtype][sd])) then
    snode.r[rtype][sd] = nil;
    if (isTableEmpty(snode.r[rtype])) then
      snode.r[rtype] = nil;
      if (isTableEmpty(snode.r)) then snode.r = nil; end
    end
  end
end
local function existsRel(snode, rtype, tnode, sd)
  if     (snode.r[rtype]     == nil) then
    error("snode does not have this relationship");
  elseif (snode.r[rtype][sd] == nil) then
    error("snode does not have this relationship in this direction");
  elseif (snode.r[rtype][sd][tnode.__pk] == nil) then
    error("snode does not have this relationship in this direction for tnode");
  end
end
function deleteNodeRelationShip(snode, rtype, tnode)
  validateNodesInRel(snode, rtype, tnode)
  local sd, td = 2, 1;
  existsRel(snode, rtype, tnode, sd);
  snode.r[rtype][sd][tnode.__pk] = nil; reduceRel(snode, rtype, sd);
  tnode.r[rtype][td][snode.__pk] = nil; reduceRel(tnode, rtype, td);
end

function addProperyToRelationship(snode, rtype, tnode, key, value)
  validateNodesInRel(snode, rtype, tnode)
  local sd, td = 2, 1;
  existsRel(snode, rtype, tnode, sd);
  snode.r[rtype][sd][tnode.__pk][key] = value;
  tnode.r[rtype][td][snode.__pk][key] = value;
end

-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
local function getDirections(direction)
  if     (direction == DIRECTION_IN)  then return 1;
  elseif (direction == DIRECTION_OUT) then return 2; 
  else error("direction must be:[INCOMING|OUTGOING]");
  end
end
function getAllNodesFromRelationship(snode, rtype, direction)
  local sd = getDirections(direction);
  if (snode.r == nil or snode.r[rtype] == nil or snode.r[rtype][sd] == nil) then
      return {};
  end
  return snode.r[rtype][sd];
end
function printNameFromRel(snode, rtype, direction)
  for k,v in pairs(getAllNodesFromRelationship(snode, rtype, direction)) do
    print ('PK: ' .. k .. ' NAME: ' .. v.target.name);
    if (v.weight) then print('WEIGHT: ' .. v.weight); end
  end
end

function getAllRelatedNodes(snode, direction)
  local sd = getDirections(direction);
  if (snode.r == nil) then return {}; end
  local t = {};
  for rtype, relt in pairs(snode.r) do
    if (relt[sd]) then
      pkt = relt[sd];
      for pk, targt in pairs(pkt) do
        --print('name: ' .. snode.name .. ' rtype: ' .. rtype .. ' pk: ' .. pk .. ' tname: ' .. targt.target.name);
        table.insert(t, targt.target);
      end
    end
  end
  return t;
end

-- BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS
function traverse_bfs(v)
  assert(v ~= nil, "vertex not in graph");
  local visited = {}; -- control set
  local t       = {}; -- return table
  local Q       = Queue.new();
  Q:insert(v);
  while not Q:isempty() do
    local u = Q:retrieve();
    if (not visited[u]) then
      visited[u] = true;
      table.insert(t, u.__pk, u); print ('BFS: name: ' .. u.name);
    end
    for k, w in pairs(getAllRelatedNodes(u, DIRECTION_OUT)) do
      if (not visited[w]) then Q:insert(w); end
    end
    --print ('next loop finish name: ' .. u.name);
  end
  return t;
end

-- DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS
local function search (v, visited)
  visited[v] = true -- mark v as visited
  for k, w in pairs(getAllRelatedNodes(v, DIRECTION_OUT)) do
    if not visited[w] then -- search deeper?
      print ('DFS: name: ' .. w.name);
      search(w, visited)
    end
  end
end

function traverse_dfs(s)
  assert(s ~= nil, "vertex not in graph")
  local visited = {} -- control variable (set)
  print ('DFS: name: ' .. s.name);
  return search(s, visited)
end

-- TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST
function initial_test()
  lo1 = {}; createNode(lo1, 111) addNodeProperty(lo1.node, 'name', 'Joe_1');
  lo2 = {}; createNode(lo2, 222) addNodeProperty(lo2.node, 'name', 'Bill_2');
  lo3 = {}; createNode(lo3, 333) addNodeProperty(lo3.node, 'name', 'Jane_3');
  lo4 = {}; createNode(lo4, 444) addNodeProperty(lo4.node, 'name', 'Ken_4');
  lo5 = {}; createNode(lo5, 555) addNodeProperty(lo5.node, 'name', 'Kate_5');
  lo6 = {}; createNode(lo6, 655) addNodeProperty(lo6.node, 'name', 'Mack_6');
  lo7 = {}; createNode(lo7, 777) addNodeProperty(lo7.node, 'name', 'Lyle_7');
  lo8 = {}; createNode(lo8, 888) addNodeProperty(lo8.node, 'name', 'Bud_8');
  addNodeRelationShip(lo1.node, 'LIKES', lo2.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo1.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo3.node);
  addNodeRelationShip(lo3.node, 'KNOWS', lo4.node);
  addNodeRelationShip(lo4.node, 'KNOWS', lo5.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo6.node);
  addNodeRelationShip(lo6.node, 'KNOWS', lo7.node);
  addNodeRelationShip(lo1.node, 'KNOWS', lo8.node);
  print ('2 matches'); printNameFromRel(lo2.node, 'KNOWS', DIRECTION_OUT);
  print ('1 match');   printNameFromRel(lo1.node, 'KNOWS', DIRECTION_IN);
  deleteNodeRelationShip(lo1.node, 'LIKES', lo2.node);
  addProperyToRelationship(lo2.node, 'KNOWS', lo3.node, 'weight', 10);
  print ('1 match');   printNameFromRel(lo2.node, 'KNOWS', DIRECTION_OUT);
end

function dofile_graph()
  dofile 'graph.lua';
  dofile 'dumper.lua';
  initial_test();
end


local math  = math
local Queue = require "Queue"
local Heap  = require "Heap"

Vset = {}; -- table(unique-list) of vertices

DIRECTION_OUT = 2;
DIRECTION_IN  = 1;

-- NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES
function createNode(tname, lo, pk)
  if     (lo      == nil) then error("createNode(x) - x does not exist");
  elseif (lo.node ~= nil) then error("createNode - Node already exists");
  elseif (pk      == nil) then error("createNode(x, pk) - pk not defined"); end
  lo.node       = { __pk=pk; __tname=tname; }
  Vset[lo.node] = true;
  --TODO add in __newindex to make lo.node READONLY
  --      (need bool in mod funcs to turn off/on READONLY)
  --     should also be recursively to lo.node.r[]
end

function deleteNode(lo)
  if     (lo      == nil) then error("deleteNode(x) - x does not exist");
  elseif (lo.node == nil) then error("deleteNode - Node does not exists"); end
  Vset[lo.node] = nil;
  lo.node       = nil;
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

-- NOTE: example-usage: add weight to a relationship
function addPropertyToRelationship(snode, rtype, tnode, prop, value)
  validateNodesInRel(snode, rtype, tnode)
  local sd, td = 2, 1;
  existsRel(snode, rtype, tnode, sd);
  snode.r[rtype][sd][tnode.__pk][prop] = value;
  tnode.r[rtype][td][snode.__pk][prop] = value;
end

-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
--TODO direction==0 means either direction
local function getDirections(direction)
  if     (direction == DIRECTION_IN)  then return 1;
  elseif (direction == DIRECTION_OUT) then return 2; 
  else   error("direction must be:[INCOMING|OUTGOING]"); end
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
    print ("\tPK: " .. k .. ' NAME: ' .. v.target.name);
    if (v.weight) then print("\t\tWEIGHT: " .. v.weight); end
  end
end

--TODO getAllRelatedNodesByRelation(snode, relt[], direction)
--TODO direction = 0, means EITHER direction
local function getAllRelatedNodes(snode, direction)
  local sd = getDirections(direction);
  if (snode.r == nil) then return {}; end
  local t = {};
  for rtype, relt in pairs(snode.r) do
    if (relt[sd]) then
      pkt = relt[sd];
      for pk, targt in pairs(pkt) do
        table.insert(t, targt.target);
      end
    end
  end
  return t;
end
local function getAllRelNodesWithRelProp(snode, direction, prop)
  local sd = getDirections(direction);
  if (snode.r == nil) then return {}; end
  local t = {};
  for rtype, relt in pairs(snode.r) do
    if (relt[sd]) then
      pkt = relt[sd];
      for pk, targt in pairs(pkt) do
        local pval = math.huge;
        if (targt[prop] ~= nil) then pval = targt[prop]; end
        table.insert(t, pval, targt.target);
      end
    end
  end
  return t;
end

-- TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS
function getPath(x)
  local Q      = Queue.new();
  local parent = x.parent;
  while (parent ~= nil) do
    Q:insert(parent.node);
    local gparent = parent.parent;
    parent        = gparent;
  end
  local paths = '';
  while (not Q:isempty()) do
    local u = Q:retrieveFromEnd();
    if (string.len(paths) > 0) then paths = paths .. '.'; end
    paths = paths .. u.name;
  end
  if (string.len(paths) > 0) then paths = paths .. '.'; end
  paths = paths .. x.node.name;
  return paths;
end

-- REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC
function rf_node_name    (x) return x.node.name;   end
function rf_path         (x) return getPath(x); end
function rf_node_and_path(x) return {node = x.node, path = getPath(x)}; end

-- BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS
function traverse_bfs(v, reply_func)
  assert(Vset[v]    ~= nil, "vertex not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  local visited    = {}; -- control set
  local t          = {}; -- return table
  local Q          = Queue.new();
  Q:insert({node = v; parent = nil;});
  while (not Q:isempty()) do
    local x = Q:retrieve();
    local u = x.node;
    if (not visited[u]) then
      visited[u]      = true;
      table.insert(t, reply_func(x));
      local newparent = x;
      for k, w in pairs(getAllRelatedNodes(u, DIRECTION_OUT)) do
        if (not visited[w]) then
          Q:insert({node=w; parent=newparent});
        end
      end
    end
  end
  return t;
end

-- DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS
local function search(v, visited, t, par, reply_func)
  visited[v] = true -- mark v as visited
  for k, w in pairs(getAllRelatedNodes(v, DIRECTION_OUT)) do
    if (not visited[w]) then -- search deeper?
      local child = {node = w; parent = par;};
      table.insert(t, reply_func(child)); 
      search(w, visited, t, child, reply_func)
    end
  end
end
function traverse_dfs(v, reply_func)
  assert(Vset[v]    ~= nil, "vertex not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  local visited = {}; -- control set
  local t       = {}; -- return table
  local par = {node = v; parent = nil;};
  table.insert(t, reply_func(par)); 
  search(v, visited, t, par, reply_func)
  return t;
end

-- SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH
function vertices() return next, Vset, nil end

function get_val_func(v) return v.cost; end

--TODO ProofOfConceptCode: this was a global shortestpath - for ALL nodes
--     then I quickly hacked on it, to make it for [FromStartNode->ToEndNode]
--     but it might be terribly INEFFICIENT on big-graphs
function shortestpath(snode, tnode)
  local min   = math.huge;
  local paths = {};
  local dist  = {};
  local H     = Heap.new(get_val_func) -- keep unmarked vertices ordered by dist
  for u in vertices() do dist[u] = math.huge end
  dist[snode] = 0;
  for u, d in pairs(dist) do -- build heap
    H:insert(u, {node = u; parent = nil; cost = d;})
  end
  while (not H:isempty()) do
    local u, x = H:retrieve()
    local du   = x.cost;
    if (u == tnode) then break; end
    for wt, n in pairs(getAllRelNodesWithRelProp(u, DIRECTION_OUT, 'weight')) do
      local dn = dist[n];
      local d  = du + wt;
      if (d < min) then
        if (dn > d) then
          dist[n] = d;
          local path = {node = n; parent = x; cost = d;};
          paths[n] = path;
          if (n == tnode and d < min) then min = d; end
          H:update(n, path);
        end
      end
    end
  end
  return {cost = dist[tnode]; path=getPath(paths[tnode])};
end

-- TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST
function initial_test()
  local tname = 'users';
  lo1 = {}; createNode(tbl, lo1, 11) addNodeProperty(lo1.node, 'name', 'Joe1');
  lo2 = {}; createNode(tbl, lo2, 22) addNodeProperty(lo2.node, 'name', 'Bill2');
  lo3 = {}; createNode(tbl, lo3, 33) addNodeProperty(lo3.node, 'name', 'Jane3');
  lo4 = {}; createNode(tbl, lo4, 44) addNodeProperty(lo4.node, 'name', 'Ken4');
  lo5 = {}; createNode(tbl, lo5, 55) addNodeProperty(lo5.node, 'name', 'Kate5');
  lo6 = {}; createNode(tbl, lo6, 65) addNodeProperty(lo6.node, 'name', 'Mack6');
  lo7 = {}; createNode(tbl, lo7, 77) addNodeProperty(lo7.node, 'name', 'Lyle7');
  lo8 = {}; createNode(tbl, lo8, 88) addNodeProperty(lo8.node, 'name', 'Bud8');
  lo9 = {}; createNode(tbl, lo9, 99) addNodeProperty(lo9.node, 'name', 'Rick9');
  loa = {}; createNode(tbl, loa, 12) addNodeProperty(loa.node, 'name', 'Lori9');
  addNodeRelationShip(lo1.node, 'LIKES', lo2.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo1.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo3.node);
  addNodeRelationShip(lo3.node, 'KNOWS', lo4.node);
  addNodeRelationShip(lo4.node, 'KNOWS', lo5.node);
  addNodeRelationShip(lo2.node, 'KNOWS', lo6.node);
  addNodeRelationShip(lo6.node, 'KNOWS', lo7.node);
  addNodeRelationShip(lo1.node, 'KNOWS', lo8.node);
  addNodeRelationShip(lo8.node, 'KNOWS', lo9.node);
  addNodeRelationShip(lo4.node, 'KNOWS', loa.node);

  print ('3 matches: KNOWS: 2 OUT');
  printNameFromRel(lo2.node, 'KNOWS', DIRECTION_OUT);
  print ('1 match: LIKE: 2 IN');
  printNameFromRel(lo2.node, 'LIKES', DIRECTION_IN);

  deleteNodeRelationShip(lo1.node, 'LIKES', lo2.node);
  print ('0 matches: LIKE: 2 IN - deleted');
  printNameFromRel(lo2.node, 'LIKES', DIRECTION_IN);

  addPropertyToRelationship(lo2.node, 'KNOWS', lo3.node, 'weight', 10);
  print ('3 matches: KNOWS: 2 OUT - one w/ weight');
  printNameFromRel(lo2.node, 'KNOWS', DIRECTION_OUT);

  local x = traverse_bfs(lo2.node, rf_path);
  print('BreadthFirst: rf_path');
  for k,v in pairs(x) do print("\tPATH: " .. v); end

  local y = traverse_bfs(lo2.node, rf_node_name);
  print('BreadthFirst: reply_func_node_name');
  for k,v in pairs(y) do print("\tNAME: " .. v); end

  print('BreadthFirst: rf_node_and_path');
  local z = traverse_bfs(lo2.node, rf_node_and_path);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end

  print('DepthFirst: rf_node_and_path');
  z=traverse_dfs(lo2.node, rf_node_and_path);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end
end

function best_path_test()
  local tname = 'cities';
  loA = {}; createNode(tbl, loA, 11) addNodeProperty(loA.node, 'name', 'A');
  loB = {}; createNode(tbl, loB, 12) addNodeProperty(loB.node, 'name', 'B');
  loC = {}; createNode(tbl, loC, 14) addNodeProperty(loC.node, 'name', 'C');
  loD = {}; createNode(tbl, loD, 15) addNodeProperty(loD.node, 'name', 'D');
  loE = {}; createNode(tbl, loE, 16) addNodeProperty(loE.node, 'name', 'E');
  loF = {}; createNode(tbl, loF, 17) addNodeProperty(loF.node, 'name', 'F');
  loG = {}; createNode(tbl, loG, 18) addNodeProperty(loG.node, 'name', 'G');
  loI = {}; createNode(tbl, loI, 19) addNodeProperty(loI.node, 'name', 'I');
  loJ = {}; createNode(tbl, loJ, 20) addNodeProperty(loJ.node, 'name', 'J');
  loK = {}; createNode(tbl, loK, 21) addNodeProperty(loK.node, 'name', 'K');
  loL = {}; createNode(tbl, loL, 22) addNodeProperty(loL.node, 'name', 'L');
  loM = {}; createNode(tbl, loM, 23) addNodeProperty(loM.node, 'name', 'M');

  addNodeRelationShip(loA.node, 'PATH', loB.node);                 -- cost: 100
  addPropertyToRelationship(loA.node, 'PATH', loB.node, 'weight', 100);

  addNodeRelationShip(loA.node, 'PATH', loC.node);                 -- cost: 70
  addPropertyToRelationship(loA.node, 'PATH', loC.node, 'weight', 20);
  addNodeRelationShip(loC.node, 'PATH', loB.node);
  addPropertyToRelationship(loC.node, 'PATH', loB.node, 'weight', 50);

  addNodeRelationShip(loC.node, 'PATH', loD.node);                 -- cost: 60
  addPropertyToRelationship(loC.node, 'PATH', loD.node, 'weight', 20);
  addNodeRelationShip(loD.node, 'PATH', loB.node);
  addPropertyToRelationship(loD.node, 'PATH', loB.node, 'weight', 20);

  addNodeRelationShip(loA.node, 'PATH', loE.node);                 -- cost: 50
  addPropertyToRelationship(loA.node, 'PATH', loE.node, 'weight', 10);
  addNodeRelationShip(loE.node, 'PATH', loF.node);
  addPropertyToRelationship(loE.node, 'PATH', loF.node, 'weight', 10);
  addNodeRelationShip(loF.node, 'PATH', loB.node);
  addPropertyToRelationship(loF.node, 'PATH', loB.node, 'weight', 30);

  addNodeRelationShip(loF.node, 'PATH', loG.node);                 -- cost: 40
  addPropertyToRelationship(loF.node, 'PATH', loG.node, 'weight', 10);
  addNodeRelationShip(loG.node, 'PATH', loB.node);
  addPropertyToRelationship(loG.node, 'PATH', loB.node, 'weight', 10);

  addNodeRelationShip(loA.node, 'PATH', loI.node);                 -- cost: 30
  addPropertyToRelationship(loA.node, 'PATH', loI.node, 'weight', 5);
  addNodeRelationShip(loI.node, 'PATH', loJ.node);
  addPropertyToRelationship(loI.node, 'PATH', loJ.node, 'weight', 5);
  addNodeRelationShip(loJ.node, 'PATH', loK.node);
  addPropertyToRelationship(loJ.node, 'PATH', loK.node, 'weight', 5);
  addNodeRelationShip(loK.node, 'PATH', loL.node);
  addPropertyToRelationship(loK.node, 'PATH', loL.node, 'weight', 5);
  addNodeRelationShip(loL.node, 'PATH', loM.node);
  addPropertyToRelationship(loL.node, 'PATH', loM.node, 'weight', 5);
  addNodeRelationShip(loM.node, 'PATH', loB.node);
  addPropertyToRelationship(loM.node, 'PATH', loB.node, 'weight', 5);

  local t = shortestpath(loA.node, loB.node);
  print('shortestpath[A->B]: cost: ' .. t.cost .. ' path: ' .. t.path);
end

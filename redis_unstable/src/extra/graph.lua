local math  = math
local Queue = require "Queue"
local Heap  = require "Heap"

--NOTE: Vset[] used by shortestpath()
Vset = {}; -- table(unique-list) of vertices

-- CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS
Direction      = {}; -- RELATIONSHIP Directions
Direction.OUT  = 2;
Direction.IN   = 1;
Direction.BOTH = 0;

Uniqueness             = {};
Uniqueness.NODE_GLOBAL = 1;
Uniqueness.NONE        = 2;
Uniqueness.PATH_GLOBAL = 3;

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
--TODO Direction.BOTH
local function getDirection(direction)
  if     (direction == Direction.IN or
          direction == Direction.OUT) then return direction; 
  else error("direction must be:[INCOMING|OUTGOING]"); end
end
function getAllNodesFromRelationship(snode, rtype, direction)
  local sd = getDirection(direction);
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

--TODO getNeighborhoodByRelation(snode, rel_and_dir_t[])

local function getNeighborhood(snode, direction)
  local sd = getDirection(direction);
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
--TODO combine getHood_MatchRelProp & getNeighborhood
local function getHood_MatchRelProp(snode, direction, prop)
  local sd = getDirection(direction);
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

local function getDepths(depth)
  local mind, maxd = 0, math.huge;
  if (depth ~= nil and type(depth) == 'table') then
    maxd = depth.max; mind = depth.min;
  end
  return {min = mind; max = maxd};
end
local function getUniqueness(uniq)
  if     (uniq == nil)                    then return Uniqueness.NODE_GLOBAL;
  elseif (uniq == Uniqueness.NODE_GLOBAL or
          uniq == Uniqueness.NONE        or
          uniq == Uniqueness.PATH_GLOBAL) then return uniq;
  else error("Uniquness.[NODE_GLOBAL|NONE|PATH_GLOBAL]"); end
end
local function getVirgin(u, x, vizd)
  local doit;
  local which;
  if     (u == Uniqueness.NODE_GLOBAL and not vizd[x.node]) then
    doit = true; which = x.node;
  elseif (u == Uniqueness.NONE)                             then
    doit = true; which = x.node;
  elseif (u == Uniqueness.PATH_GLOBAL) then
    if (x.parent == nil) then
      doit = true; which = 0;
    else
      which = x.parent.node.name .. '.' .. x.node.name;
      doit  = (not vizd[which]);
    end
  else
    doit = false;
  end
  return doit, which;
end

-- REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC
function rf_node_name    (x) return x.node.name;                        end
function rf_path         (x) return getPath(x);                         end
function rf_node_and_path(x) return {node = x.node, path = getPath(x)}; end

-- BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS
function traverse_bfs(v, reply_func, depth, uniq)
  assert(Vset[v]    ~= nil, "vertex not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  local d     = getDepths(depth);
  local u     = getUniqueness(uniq);
  local vizd  = {}; -- control set
  local t     = {}; -- return table
  local Q     = Queue.new();
  local x     = {node = v; parent = nil; depth = 1;};
  Q:insert(x);
  while (not Q:isempty()) do
    local x  = Q:retrieve();
    if (x.depth > d.max) then break; end
    local n      = x.node;
    local doit, which = getVirgin(u, x, vizd);
    if (doit) then
      vizd [which] = true;
      if (x.depth >= d.min) then table.insert(t, reply_func(x)); end
      for k, w in pairs(getNeighborhood(n, Direction.OUT)) do
        local y  = {node = w; parent = x; depth = (x.depth + 1)};
        local doity, whichy = getVirgin(u, y, vizd);
        if (doity) then Q:insert(y); end
      end
    end
  end
  return t;
end

-- DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS
local function dfs_search(v, vizd, t, x, reply_func, d, u)
  local doit, which = getVirgin(u, x, vizd);
  vizd[which] = true
  if (x.depth >= d.max) then return; end
  for k, n in pairs(getNeighborhood(v, Direction.OUT)) do
    local child = {node = n; parent = x; depth = (x.depth + 1)};
    local doity, whichy = getVirgin(u, child, vizd);
    if (doity) then
      if (child.depth >= d.min) then table.insert(t, reply_func(child)); end
      dfs_search(n, vizd, t, child, reply_func, d, u)
    end
  end
end
function traverse_dfs(v, reply_func, depth, uniq)
  assert(Vset[v]    ~= nil, "vertex not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  local d     = getDepths(depth);
  local u     = getUniqueness(uniq);
  local vizd  = {}; -- control set
  local t     = {}; -- return table
  local x     = {node = v; parent = nil; depth = 1;};
  if (x.depth >= d.min) then table.insert(t, reply_func(x));  end
  dfs_search(v, vizd, t, x, reply_func, d, u)
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
    for wt, n in pairs(getHood_MatchRelProp(u, Direction.OUT, 'weight')) do
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

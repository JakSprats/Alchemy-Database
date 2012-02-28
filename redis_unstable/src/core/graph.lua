local math  = math
local Queue = require "Queue"
local Heap  = require "Heap"

--TODO Vset && PKset are redundant ... PK is a better idea {all INTs}
--NOTE: Vset[] used by findShortestPath()
local Vset  = {}; -- table(unique-list) of nodes    -> {key=node}
local PKset = {}; -- table(unique-list) of LuaObj's -> {key=pk}

-- MAP_API_CONSTANTS_TO_FUNCTIONS MAP_API_CONSTANTS_TO_FUNCTIONS
ReplyFuncs                   = { __delim = 'REPLY';             }
RelationshipCostFuncs        = { __delim = 'RELATIONSHIP_COST'; }
NodeDeltaFuncs               = { __delim = 'NODE_DELTA';        }
EdgeEvalFuncs                = { __delim = 'EDGE_EVAL';         }
ExpanderFuncs                = { __delim = 'EXPANDER';          }
AllRelationshipExpanderFuncs = { __delim = 'ALL_RELATIONSHIP_EXPANDER'; }

function registerFunc(array, name, func)
  if (array == nil) then error("registerFunc() array does not exist");    end
  if (name  == nil) then error("registerFunc() arg: 'name' is required"); end
  if (func  == nil) then error("registerFunc() arg: 'func' is required"); end
  array[array.__delim .. '.' .. name] = func;
end

dofile './core/graph_custom.lua';

-- CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS CONSTANTS
Direction           = {}; -- RELATIONSHIP Directions
Direction.OUTGOING  = 2;
Direction.INCOMING  = 1;
Direction.BOTH      = 0;

Uniqueness             = {};
Uniqueness.NODE_GLOBAL = 1;
Uniqueness.NONE        = 2;
Uniqueness.PATH_GLOBAL = 3;

Evaluation                      = {};
Evaluation.INCLUDE_AND_CONTINUE = 1;
Evaluation.INCLUDE_AND_PRUNE    = 2;
Evaluation.EXCLUDE_AND_CONTINUE = 3;
Evaluation.EXCLUDE_AND_PRUNE    = 4;

-- READ_ONLY_TABLES READ_ONLY_TABLES READ_ONLY_TABLES READ_ONLY_TABLES
local ReadOnlyLock = true;
local function readOnlyLock_ON()  ReadOnlyLock = true;  end
local function readOnlyLock_OFF() ReadOnlyLock = false; end
local function readOnlySetter(rname, k, v)
  if (ReadOnlyLock) then error("ERROR: trying to set a ReadOnly table");
  else                   rawset(rname, k, v);                            end
end
local function createEmptyReadOnlyTable(t)
  t = {}; setmetatable(t, {__newindex = readOnlySetter}); return t;
end

-- NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES NODES
NodeKeywords = {};
NodeKeywords['__tname'] = true;
NodeKeywords['__cname'] = true;
NodeKeywords['__pk']    = true;
NodeKeywords['__name']  = true;
NodeKeywords['__dump']  = true;
NodeKeywords['r']       = true;

-- CREATE_NODE CREATE_NODE CREATE_NODE CREATE_NODE CREATE_NODE CREATE_NODE
--TODO MAKE_LOCAL
function internalCreateNamedNode(tname, cname, pk, lo, name)
  if     (lo.node ~= nil) then
    error("createNamedNode - Node already exists");
  elseif (pk      == nil) then
    error("createNamedNode(x, pk) - pk not defined");
  elseif (name    == nil) then
    error("createNamedNode(,,name) - name not defined");
  end
  readOnlyLock_OFF();
  lo.node = createEmptyReadOnlyTable();
  lo.node.__tname  = tname;            lo.node.__cname = cname;
  lo.node.__pk     = pk;               lo.node.__name  = name;
  lo.__dump        = loWithNodeDumper; lo.node.__dump   = nodeDumper;
  readOnlyLock_ON();
  Vset[lo.node]    = true;
  if (PKset[tname] == nil) then PKset[tname] = {}; end
  PKset[tname][pk] = lo.node;
  return "CREATED NODE";
end
function createNamedNode(tname, cname, pk, name)
  if     (cname == nil) then
    error("createNamedNode(tname, cname) - cname does not exist");
  else
    if (STBL[tname] == nil or STBL[tname][cname] == nil or
        STBL[tname][cname][pk] == nil) then
      error("createNamedNode(tname, cname, pk) - ROW does not exist");
    end
  end
  local lo = STBL[tname][cname][pk];
  return internalCreateNamedNode(tname, cname, pk, lo, name);
end
local function deleteNode(lo)
  if     (lo      == nil) then error("deleteNode(x) - x does not exist");
  elseif (lo.node == nil) then error("deleteNode - Node does not exists"); end
  PKset[lo.node.__tname][lo.node.__pk] = nil;
  readOnlyLock_OFF(); lo.node = nil; readOnlyLock_ON();
  Vset[lo.node]       = nil;
end

-- PROPERTIES PROPERTIES PROPERTIES PROPERTIES PROPERTIES PROPERTIES
-- TODO MAKE_LOCAL
function addNodeProperty(node, key, value)
  if     (node      == nil) then
    error("addNodePropery(x) - x does not exist");
  elseif (node[key] ~= nil) then
    error("addNodePropery(x, key) - key already exists");
  elseif (NodeKeywords[key]) then
    error("Keyword violation: '" .. key .."' is reserved"); 
  end
  readOnlyLock_OFF(); node[key] = value; readOnlyLock_ON();
  return "PROPERTY ADDED";
end
-- TODO MAKE_LOCAL
function deleteNodeProperty(node, key)
  if     (node      == nil) then
    error("deleteNodePropery(x) - x does not exist");
  elseif (node[key] == nil) then
    error("deleteNodePropery(x, key) - key does not exists");
  end
  readOnlyLock_OFF(); node[key] = nil; readOnlyLock_ON();
  return "PROPERTY DELETED";
end

-- RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS RELATIONSHIPS
local function validateNodesInRel(snode, rtype, tnode)
  assert(snode,      "func(snode, ...) - snode does not exist");
  assert(snode.__pk, "func(snode, ..., snode) - snode is not a NODE");
  assert(tnode,      "func(snode, ..., tnode) - tnode does not exist");
  assert(tnode.__pk, "func(snode, ..., tnode) - tnode is not a NODE");
  assert(rtype,      "funct(snode, rtype, ...) - rtype must be defined");
end

local function createRelationship(snode, rtype, sd, tnode)
  if (snode.r            == nil) then
    snode.r                      = createEmptyReadOnlyTable();
  end
  if (snode.r[rtype]     == nil) then
    snode.r[rtype]               = createEmptyReadOnlyTable();
  end
  if (snode.r[rtype][sd] == nil) then
    snode.r[rtype][sd]           = createEmptyReadOnlyTable();
  end
  snode.r[rtype][sd][tnode.__pk] = createEmptyReadOnlyTable();
  snode.r[rtype][sd][tnode.__pk].target = tnode;
end

hooks_addNodeRelationShip    = {}; -- NOTE: user_cities uses to Index Relations
hooks_deleteNodeRelationShip = {}; -- NOTE: user_cities uses to Index Relations

--TODO MAKE_LOCAL
function addNodeRelationShip(snode, rtype, tnode)
  validateNodesInRel(snode, rtype, tnode)
  local sd, td = Direction.OUTGOING, Direction.INCOMING;
  readOnlyLock_OFF();
  createRelationship(snode, rtype, sd, tnode);
  createRelationship(tnode, rtype, td, snode)
  readOnlyLock_ON();
  if (hooks_addNodeRelationShip ~= nil) then
    for k, v in pairs(hooks_addNodeRelationShip) do
      v.func(v.iname, snode, rtype, tnode);
    end
  end
  return "ADDED RELATIONSHIP";
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
--TODO MAKE_LOCAL
function deleteNodeRelationShip(snode, rtype, tnode)
  validateNodesInRel(snode, rtype, tnode);
  local sd, td = Direction.OUTGOING, Direction.INCOMING;
  existsRel(snode, rtype, tnode, sd);
  readOnlyLock_OFF();
  snode.r[rtype][sd][tnode.__pk] = nil; reduceRel(snode, rtype, sd);
  tnode.r[rtype][td][snode.__pk] = nil; reduceRel(tnode, rtype, td);
  readOnlyLock_ON();
  if (hooks_deleteNodeRelationShip ~= nil) then
    for k, v in pairs(hooks_deleteNodeRelationShip) do
      v.func(v.iname, snode, rtype, tnode);
    end
  end
  return "DELETED RELATIONSHIP";
end

-- NOTE: example-usage: add weight to a relationship
--TODO MAKE_LOCAL
function addPropertyToRelationship(snode, rtype, tnode, prop, value)
  validateNodesInRel(snode, rtype, tnode);
  local sd, td = Direction.OUTGOING, Direction.INCOMING;
  existsRel(snode, rtype, tnode, sd);
  readOnlyLock_OFF();
  snode.r[rtype][sd][tnode.__pk][prop] = value;
  tnode.r[rtype][td][snode.__pk][prop] = value;
  readOnlyLock_ON();
  return "PROPERTY ADDED TO RELATION"
end
--TODO MAKE_LOCAL
function deletePropertyToRelationship(snode, rtype, tnode, prop)
  validateNodesInRel(snode, rtype, tnode);
  local sd, td = Direction.OUTGOING, Direction.INCOMING;
  existsRel(snode, rtype, tnode, sd);
  readOnlyLock_OFF();
  snode.r[rtype][sd][tnode.__pk][prop] = nil;
  tnode.r[rtype][td][snode.__pk][prop] = nil;
  readOnlyLock_ON();
  return "PROPERTY DELETED TO RELATION"
end

-- NEIGHBORHOOD NEIGHBORHOOD NEIGHBORHOOD NEIGHBORHOOD NEIGHBORHOOD
function expanderOutgoing(x, rtype, relation)
  return (relation[Direction.OUTGOING] ~= nil), Direction.OUTGOING;
end
registerFunc(ExpanderFuncs, 'OUTGOING', expanderOutgoing);
function expanderIncoming(x, rtype, relation)
  return (relation[Direction.INCOMING] ~= nil), Direction.INCOMING;
end
registerFunc(ExpanderFuncs, 'INCOMING', expanderIncoming);
function expanderBoth(x, rtype, relation)
  return ((relation[Direction.INCOMING] ~= nil) or
          (relation[Direction.OUTGOING] ~= nil)), Direction.BOTH;
end
registerFunc(ExpanderFuncs, 'BOTH', expanderBoth);
function defaultExpanderFunc(x, rtype, relation)
  return expanderOutgoing(x, rtype, relation);
end
registerFunc(ExpanderFuncs, 'DEFAULT', defaultExpanderFunc);

local function validateRelEvalFunc(doit, dir)
  assert(doit ~= nil           and dir ~= nil and
         dir >= Direction.BOTH and dir <= Direction.OUTGOING,
         "RelationEvaluationFuncs: return [yes,direction]");
end
local function getSingleNBhoodRel(t, x, rtype, relation, dir, nopts)
  local pkt = relation[dir];
  if (pkt == nil) then return; end
  for pk, trgt in pairs(pkt) do
    if     (nopts.rel_cost_func ~= nil) then 
      local pval = nopts.rel_cost_func(trgt);
      table.insert(t, pval, 
                   {node = trgt.target; rtype = rtype; relation = relation;});
    elseif (nopts.node_delta_func ~= nil) then 
      local pval = nopts.node_delta_func(x.w.node, trgt.target);
      table.insert(t, pval,
                   {node = trgt.target; rtype = rtype; relation = relation;});
    else
      table.insert(t,
                   {node = trgt.target; rtype = rtype; relation = relation;});
    end
  end
end
local function getNBhoodRel(x, nopts, t, rtype, relation)
    local doit, dir = nopts.expander_func(x, rtype, relation, x.w.node.r);
    validateRelEvalFunc(doit, dir);
    if (doit) then
      if (dir == Direction.BOTH) then
        getSingleNBhoodRel(t, x, rtype, relation, Direction.OUTGOING, nopts);
        getSingleNBhoodRel(t, x, rtype, relation, Direction.INCOMING, nopts);
      else
        getSingleNBhoodRel(t, x, rtype, relation, dir,                nopts);
      end
    end
end
local function getNeighborhood(x, nopts) 
  if (x.w.node.r == nil) then return {}; end
  local t = {};
  if (nopts.all_rel_expander_func ~= nil) then 
    local do_us = nopts.all_rel_expander_func(x, x.w.node.r);
    for k, v in pairs(do_us) do
      getNBhoodRel(x, nopts, t, v.rtype, v.relation);
    end
  else
    for rtype, relation in pairs(x.w.node.r) do
      getNBhoodRel(x, nopts, t, rtype, relation);
    end
  end
  return t;
end

-- OPTION_PARSING OPTION_PARSING OPTION_PARSING OPTION_PARSING OPTION_PARSING
local function getDepths(o)
  local mind, maxd = 0, math.huge;
  if (o ~= nil) then
    if (o.min_depth ~= nil) then mind = o.min_depth; end
    if (o.max_depth ~= nil) then maxd = o.max_depth; end
  end
  return {min = mind; max = maxd};
end
local function getUniqueness(o)
  if     (o == nil or o.uniqueness == nil) then
    return Uniqueness.NODE_GLOBAL;
  elseif (o.uniqueness == Uniqueness.NODE_GLOBAL or
          o.uniqueness == Uniqueness.NONE        or
          o.uniqueness == Uniqueness.PATH_GLOBAL) then return o.uniqueness;
  else error("Uniquness.[NODE_GLOBAL|NONE|PATH_GLOBAL]"); end
end
local function defaultEdgeEvalFunc(x)
  return Evaluation.INCLUDE_AND_CONTINUE;
end
local function getEdgeEvalFunc(o)
  if  (o == nil or o.edge_eval_func == nil) then
    return defaultEdgeEvalFunc;
  else
    return o.edge_eval_func;
  end
end
local function getExpanderFunc(o) 
  if  (o == nil or o.expander_func == nil) then
    return defaultExpanderFunc;
  else
    return o.expander_func;
  end
end
local function getEvalAllRelFunc(o)
  if  (o == nil or o.all_rel_expander_func == nil) then return nil; end
  return o.all_rel_expander_func;
end


-- TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS TRAVERSERS
StartPK = 0; -- Used to Include/Exclude start-node

local function getRelationText(w)
  if (w.relation[Direction.OUTGOING] ~= nil) then
    return '-['  .. w.rtype .. ']->';
  else -- Direction.INCOMING
    return '<-[' .. w.rtype .. ']-';
  end
end
function getPath(x)
  local Q      = Queue.new();
  local parent = x.parent;
  while (parent ~= nil) do
    Q:insert(parent.w);
    local gparent = parent.parent;
    parent        = gparent;
  end
  local paths = '';
  while (not Q:isempty()) do
    local w = Q:retrieveFromEnd();
    if (string.len(paths) > 0) then paths = paths .. getRelationText(w); end
    paths = paths .. w.node.__name;
  end
  if (string.len(paths) > 0) then paths = paths .. getRelationText(x.w); end
  paths = paths .. x.w.node.__name;
  return paths;
end

local function isVirgin(u, x, vizd)
  local doit; local which;
  if     (u == Uniqueness.NODE_GLOBAL and not vizd[x.w.node]) then
    doit = true; which = x.w.node;
  elseif (u == Uniqueness.NONE)                             then
    doit = true; which = x.w.node;
  elseif (u == Uniqueness.PATH_GLOBAL)                      then
    if (x.parent == nil) then
      doit = true; which = 0;
    else
      which = x.parent.w.node.__name .. getRelationText(x.w) .. x.w.node.__name;
      doit  = (not vizd[which]);
    end
  else
    doit = false;
  end
  return doit, which;
end

local function validateEvaled(eed)
  assert(eed >= Evaluation.INCLUDE_AND_CONTINUE and
         eed <= Evaluation.EXCLUDE_AND_PRUNE,
          "eval_func_must return Evaluation.*");
  local cont = (eed == Evaluation.INCLUDE_AND_CONTINUE) or
               (eed == Evaluation.EXCLUDE_AND_CONTINUE);
  local inc  = (eed == Evaluation.INCLUDE_AND_CONTINUE) or
               (eed == Evaluation.INCLUDE_AND_PRUNE);
  local prun = (eed == Evaluation.INCLUDE_AND_PRUNE) or
               (eed == Evaluation.EXCLUDE_AND_PRUNE);
  return cont, inc, prun;
end

-- BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS BFS
--TODO MAKE_LOCAL
function traverse_bfs(v, reply_func, options)
  assert(Vset[v]    ~= nil, "node not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  StartPK     = v.__pk;
  local d     = getDepths      (options);
  local u     = getUniqueness  (options);
  local evalf = getEdgeEvalFunc(options);
  local nopts = {expander_func         = getExpanderFunc(options);
                 all_rel_expander_func = getEvalAllRelFunc(options);}
  local vizd  = {}; -- control set
  local t     = {}; -- return table
  local Q     = Queue.new();
  local x     = {w = {node = v;}; parent = nil; depth = 1;};
  Q:insert(x);
  while (not Q:isempty()) do
    local x  = Q:retrieve();
    if (x.depth > d.max) then break; end
    local doit, which = isVirgin(u, x, vizd);
    if (doit) then
      vizd [which]          = true;
      local eed             = evalf(x);
      local cont, inc, prun = validateEvaled(eed)
      if (cont) then
        if (inc and x.depth >= d.min) then
          table.insert(t, reply_func(x));
        end
        if (not prun) then
          for k, w in pairs(getNeighborhood(x, nopts)) do
            local child = {w = w; parent = x; depth = (x.depth + 1);};
            local doity, whichy = isVirgin(u, child, vizd);
            if (doity) then Q:insert(child); end
          end
        end
      end
    end
  end
  return t;
end

-- DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS DFS
local function dfs_search(vizd, t, x, reply_func, d, u, nopts, evalf)
  local doit, which = isVirgin(u, x, vizd);
  if (doit) then
    vizd[which] = true
    local eed             = evalf(x);
    local cont, inc, prun = validateEvaled(eed)
    if (inc and x.depth >= d.min) then
      table.insert(t, reply_func(x));
    end
    if (x.depth >= d.max) then return; end
    if (cont) then
      for k, w in pairs(getNeighborhood(x, nopts)) do
        local child = {w = w; parent = x; depth = (x.depth + 1)};
        local doity, whichy = isVirgin(u, child, vizd);
        if (doity) then
          dfs_search(vizd, t, child, reply_func, d, u, nopts, evalf)
        end
      end
    end
  end
end
--TODO MAKE_LOCAL
function traverse_dfs(v, reply_func, options)
  assert(Vset[v]    ~= nil, "node not in graph");
  assert(reply_func ~= nil, "arg: reply_func not defined");
  StartPK     = v.__pk;
  local d     = getDepths      (options);
  local u     = getUniqueness  (options);
  local evalf = getEdgeEvalFunc(options);
  local nopts = {expander_func         = getExpanderFunc(options);
                 all_rel_expander_func = getEvalAllRelFunc(options);}
  local vizd  = {}; -- control set
  local t     = {}; -- return table
  local x     = {w = {node = v;}; parent = nil; depth = 1;};
  dfs_search(vizd, t, x, reply_func, d, u, nopts, evalf)
  return t;
end

-- SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH SHORTEST_PATH
local function vertices() return next, Vset, nil end

local function get_val_func(v) return v.cost; end

--TODO MAKE_LOCAL
--TODO ProofOfConceptCode: this was a global findShortestPath - for ALL nodes
--     then I quickly hacked on it, to make it for [FromStartNode->ToEndNode]
--     but it might be terribly INEFFICIENT on big-graphs
function findShortestPath(snode, tnode, options)
  assert(Vset[snode]  ~= nil, "start-node not in graph");
  assert(Vset[tnode]  ~= nil, "end-node not in graph");
  StartPK     = snode.__pk;
  local min   = math.huge;
  local paths = {};
  local dist  = {};
  local nopts = {expander_func   = defaultExpanderFunc;
                 rel_cost_func   = options.rel_cost_func;
                 node_delta_func = options.node_delta_func};
  local H     = Heap.new(get_val_func) -- keep unmarked vertices ordered by dist
  for u in vertices() do dist[u] = math.huge end
  dist[snode] = 0;
  for u, d in pairs(dist) do -- build heap
    H:insert(u, {w = {node = u;}; parent = nil; cost = d;})
  end
  while (not H:isempty()) do
    local u, x = H:retrieve()
    local du   = x.cost;
    if (u == tnode) then break; end
    for wt, w in pairs(getNeighborhood(x, nopts)) do
      local n  = w.node;
      local dn = dist[n];
      local d  = du + wt;
      if (d < min) then
        if (dn > d) then
          dist[n] = d;
          local path = {w = w; parent = x; cost = d;};
          paths[n] = path;
          if (n == tnode and d < min) then min = d; end
          H:update(n, path);
        end
      end
    end
  end
  return {cost = dist[tnode]; path=getPath(paths[tnode])};
end

-- SQL_API SQL_API SQL_API SQL_API SQL_API SQL_API SQL_API SQL_API
local function getNodeByPK(tname, pk)
  --print ('getNodeByPK: tname: ' .. tname .. ' pk: ' .. pk);
  return PKset[tname][pk];
end
function deleteNodeByPk(tname, pk)
  return deleteNode(getNodeByPK(tname, pk));
end
function addNodeRelationShipByPK(stbl, spk, rtype, ttbl, tpk)
  return addNodeRelationShip(getNodeByPK(stbl, spk), rtype,
                             getNodeByPK(ttbl, tpk));
end
function deleteNodeRelationShipByPK(stbl, spk, rtype, ttbl, tpk)
  return deleteNodeRelationShip(getNodeByPK(stbl, spk), rtype,
                                getNodeByPK(ttbl, tpk));
end
function addNodePropertyByPK(tname, pk, key, value)
  return addNodeProperty(getNodeByPK(tname, pk), key, value);
end
function deleteNodePropertyByPK(tname, pk, key)
  return deleteNodeProperty(getNodeByPK(tname, pk), key);
end
function addPropertyToRelationshipByPK(stbl, spk, rtype, ttbl, tpk, prop, value)
  return addPropertyToRelationship(getNodeByPK(stbl, spk), rtype,
                                   getNodeByPK(ttbl, tpk), prop,  value);
end
function deletePropertyToRelationshipByPK(stbl, spk, rtype, ttbl, tpk, prop)
  return deletePropertyToRelationship(getNodeByPK(stbl, spk), rtype,
                                      getNodeByPK(ttbl, tpk), prop);
end


function getOptions(...)
  local options = {};
  for i,v in ipairs(arg) do
    if (string.sub(v, 1, 9) == "EXPANDER.") then
      options.expander_func = ExpanderFuncs[v];
      if (options.expander_func == nil) then
          error("EXPANDER.* function not found");
      end
    elseif (string.sub(v, 1, 18) == "RELATIONSHIP_COST.") then
      options.rel_cost_func = RelationshipCostFuncs[v];
      if (options.rel_cost_func == nil) then
          error("RELATIONSHIP_COST.* function not found");
      end
    elseif (string.sub(v, 1, 26) == "ALL_RELATIONSHIP_EXPANDER.") then
      options.all_rel_expander_func = AllRelationshipExpanderFuncs[v];
      if (options.all_rel_expander_func == nil) then
          error("ALL_RELATIONSHIP_EXPANDER.* function not found");
      end
    elseif (string.sub(v, 1, 11) == "NODE_DELTA.") then
      options.node_delta_func = NodeDeltaFuncs[v];
      if (options.node_delta_func == nil) then
          error("NODE_DELTA.* function not found");
      end
    elseif (string.sub(v, 1, 10) == "EDGE_EVAL.") then
      options.edge_eval_func = EdgeEvalFuncs[v];
      if (options.edge_eval_func == nil) then
          error("EDGE_EVAL.* function not found");
      end
    elseif (string.sub(v, 1, 11) == "UNIQUENESS.") then
      if     (string.sub(v, 12) == "NODE_GLOBAL") then
        options.uniqueness = Uniqueness.NODE_GLOBAL;
      elseif (string.sub(v, 12) == "NONE") then
        options.uniqueness = Uniqueness.NONE;
      elseif (string.sub(v, 12) == "PATH_GLOBAL") then
        options.uniqueness = Uniqueness.PATH_GLOBAL;
      else
        error("UNIQUENESS.* setting unknown");
      end
    elseif (string.sub(v, 1, 10) == "MIN_DEPTH.") then
      options.min_depth = tonumber(string.sub(v, 11));
    elseif (string.sub(v, 1, 10) == "MAX_DEPTH.") then
      options.max_depth = tonumber(string.sub(v, 11));
    end
  end
  return options;
end
function traverseByPK(trav_type, tname, pk, reply_fname, ...)
  local tfunc;
  if     (trav_type == 'BFS') then tfunc = traverse_bfs;
  elseif (trav_type == 'DFS') then tfunc = traverse_dfs;
  else                         error("traversal_type:[BFS,DFS]"); end
  if (tname == nil) then error("arg: 'tname' is required"); end
  local reply_func = ReplyFuncs[reply_fname];
  if (reply_func == nil) then
    error("reply_fname: [NODENAME, PATH, NODENAME_AND_PATH]");
  end
  local options = getOptions(...);
  return tfunc(getNodeByPK(tname, pk), reply_func, options)
end

function shortestPathByPK(tname, spk, epk, ...)
  local snode   = getNodeByPK(tname, spk);
  local enode   = getNodeByPK(tname, epk);
  local options = getOptions(...);
  return findShortestPath(snode, enode, options)
end

-- LUA_FUNCTION_INDEX  LUA_FUNCTION_INDEX  LUA_FUNCTION_INDEX 
IndexInited = {};
function buildIndex(add_index_func, iname) --print('buildIndex');
  for snode in vertices() do
    if (snode.r ~= nil) then
      for rtype, relation in pairs(snode.r) do
        local pkt = relation[Direction.OUTGOING];
        if (pkt ~= nil) then
          for pk, trgt in pairs(pkt) do
            local tnode = trgt.target;
            add_index_func(iname, snode, rtype, tnode);
          end
        end
      end
    end
  end
end

-- PERSISTENCE PERSISTENCE PERSISTENCE PERSISTENCE PERSISTENCE PERSISTENCE
-- TODO save/load shoud be binary (smaller and avoides eval())
function CNN(tname, cname, pk, name) -- shorter function name for DUMP
  return createNamedNode(tname, cname, pk, name);
end
function ANR(stbl, spk, rtype, ttbl, tpk) -- shorter func name for DUMP
  return addNodeRelationShipByPK(stbl, spk, rtype, ttbl, tpk);
end
function ANP(tname, pk, key, value)
  return addNodePropertyByPK(tname, pk, key, value);
end
function APR(stbl, spk, rtype, ttbl, tpk, prop, value)
  return addPropertyToRelationshipByPK(stbl, spk, rtype, ttbl, tpk,
                                       prop, value);
end
local function addNodeToSTBL(dumpt, n)
  table.insert(dumpt, 'CNN("' ..  n.__tname .. '","' .. n.__cname .. '",' ..
                                  n.__pk    .. ',"'  .. n.__name  .. '");\n');
end
local function addPropToNode(dumpt, n, prop, value)
  local dumpval;
  if (type(value) == "string") then dumpval = "'" .. value .. "'";
  else                              dumpval =        value;        end
  table.insert(dumpt, 'ANP("' ..  n.__tname .. '",' .. n.__pk .. ',"' ..
                                  prop      .. '",' .. dumpval  .. ');\n'); 
end
local function addRelToNode(dumpt, sn, rtype, tn)
  table.insert(dumpt, 'ANR("' ..  sn.__tname .. '",' .. sn.__pk .. ',"' ..
                                  rtype      .. '","' ..
                                  tn.__tname .. '",' .. tn.__pk .. ');\n');
end
local function addPropToRel(dumpt, sn, rtype, tn, prop, value)
  local dumpval;
  if (type(value) == "string") then dumpval = "'" .. value .. "'";
  else                              dumpval =        value;        end
  table.insert(dumpt, 'APR("' ..  sn.__tname .. '",' .. sn.__pk .. ',"' ..
                                  rtype      .. '","' ..
                                  tn.__tname .. '",' .. tn.__pk .. ',"' ..
                                  prop       .. '",' .. dumpval .. ');\n');
end

local GRAPH_dump_file = "GRAPH.lua.rdb";
function saveGraphNodes() --print ('saveGraphNodes');
  local dumpt = {};
  for n in vertices() do
    addNodeToSTBL(dumpt, n);
    for prop, value in pairs(n) do
      if (not NodeKeywords[prop]) then
        addPropToNode(dumpt, n, prop, value);
      end
    end
  end
  for n in vertices() do
    if (n.r ~= nil) then
      for rtype, relation in pairs(n.r) do
        local pkt = relation[Direction.OUTGOING];
        if (pkt ~= nil) then
          for pk, trgt in pairs(pkt) do
            local tn = trgt.target;
            addRelToNode(dumpt, n, rtype, tn);
            for prop, value in pairs(trgt) do
              if (prop ~= 'target') then
                addPropToRel(dumpt, n, rtype, tn, prop, value);
              end
            end
          end
        end
      end
    end
    readOnlyLock_OFF();
    STBL[n.__tname][n.__cname][n.__pk].node = nil; -- NOT dumped by PLUTO
    readOnlyLock_ON();
  end
  local ds    = table.concat(dumpt);
  local ofile = io.open(GRAPH_dump_file, "wb"); ofile:write(ds); ofile:close();
end
function loadGraphNodes() --print ('loadGraphNodes');
  hooks_saveLuaUniverse = {}; hooks_loadLuaUniverse = {};
  local buf = open_or_error(GRAPH_dump_file);
  assert(loadstring(buf))()
end

-- DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP DUMP
function nodeDumper(n)
  local r = {};
  table.insert(r, '{"name":"' .. n.__name .. '"');
  for prop, value in pairs(n) do
    if (not NodeKeywords[prop]) then
      table.insert(r, '",{"' .. prop .. '":"' .. value .. '"}');
    end
  end
  if (n.r ~= nil) then
      table.insert(r, ',"Relations":[');
      for rtype, relation in pairs(n.r) do
        local pkt = relation[Direction.OUTGOING];
        if (pkt ~= nil) then
          local nrels = 0;
          for pk, trgt in pairs(pkt) do
            if (nrels > 0) then table.insert(r, ','); end nrels = nrels + 1;
            local tn = trgt.target;
            table.insert(r, '{"type":"'      .. rtype   .. '",' ..
                             '"target_pk":"' .. tn.__pk .. '"');
            for prop, value in pairs(trgt) do
              if (prop ~= 'target') then
                table.insert(r, ',"' .. prop .. '":"' ..  value .. '"');
              end
            end
            table.insert(r, "}");
          end
        end
      end
      table.insert(r, "]");
  end
  table.insert(r, "}");
  return table.concat(r);
end
function loWithNodeDumper(lo)
  local node = lo.node;
  readOnlyLock_OFF(); lo.node = nil;  readOnlyLock_ON();
  local ret = Json.encode(lo);
  readOnlyLock_OFF(); lo.node = node; readOnlyLock_ON();
  local start;
  if (string.len(ret) > 2) then
    start = string.sub(ret, 1 , string.len(ret) - 1) .. ',';
  else
    start = '{';
  end
  return start .. '"GraphNode":' ..  nodeDumper(node) .. '}';
end




-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
function debugPrintNameFromRel(v, rtype, direction)
  local nopts = {};
  if     (direction == Direction.INCOMING) then 
    nopts.expander_func = expanderIncoming;
  elseif (direction == Direction.OUTGOING) then
    nopts.expander_func = expanderOutgoing;
  elseif (direction == Direction.BOTH)     then
    nopts.expander_func = expanderBoth;
  else 
    error("Direction must be [Direction.INCOMING|OUTGOING|BOTH]");
  end
  local x = {w = {node = v;}; parent = nil; depth = 1;};
  for k, w in pairs(getNeighborhood(x, nopts)) do
    print ("\tPK: " .. k .. ' NAME: ' .. w.node.__name);
    if (w.weight) then print("\t\tWEIGHT: " .. w.weight); end
  end
end

function dump_node_and_path(z)
  local i = 1;
  for k,v in pairs(z) do
    print("\t" .. i .. ': NAME: ' .. v.node.__name .. "\tPATH: " .. v.path);
    i = i + 1;
  end
end


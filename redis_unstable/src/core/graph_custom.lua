
-- REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC REPLY_FUNC
function rf_nodename     (x) return x.w.node.__name;                      end
function rf_path         (x) return getPath(x);                           end
function rf_node_and_path(x) return {node = x.w.node, path = getPath(x)}; end
function rf_nodename_and_path(x)
  return {x.w.node.__name, getPath(x)};
end
registerFunc(ReplyFuncs, 'NODENAME',          rf_nodename)
registerFunc(ReplyFuncs, 'PATH',              rf_path);
registerFunc(ReplyFuncs, 'NODENAME_AND_PATH', rf_nodename_and_path);

-- WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT WEIGHT
function getWeightProp(n)
  if (n['weight'] ~= nil) then return n['weight']; else return math.huge; end
end
registerFunc(RelationshipCostFuncs, 'WEIGHT', getWeightProp);

-- GEO_DIST GEO_DIST GEO_DIST GEO_DIST GEO_DIST GEO_DIST GEO_DIST GEO_DIST
function getGeoDist(a, b)
  if (a == nil or a.x == nil or a.y == nil) then return math.huge; end
  if (b == nil or b.x == nil or b.y == nil) then return math.huge; end
  local dx = a.x - b.x; local dy = a.y - b.y;
  return math.sqrt((dx * dx) + (dy * dy));
end
registerFunc(NodeDeltaFuncs, 'GEO', getGeoDist);

-- FOF_EDGE_EVAL (used in ALL FOF tests)
function fof_edge_eval(x)
print('fof_edge_eval: depth: ' .. x.depth);
  if     (x.depth <  4) then return Evaluation.EXCLUDE_AND_CONTINUE;
  elseif (x.depth == 4) then return Evaluation.INCLUDE_AND_CONTINUE;
  else                       return Evaluation.INCLUDE_AND_PRUNE; end
end
registerFunc(EdgeEvalFuncs, 'FOF', fof_edge_eval);

-- FOF_SINGLE_EXPANDER
function fof_expander(x, rtype, relation)
  local ok;
  if     (x.depth < 3) then
    ok = ((rtype == 'KNOWS')      and (relation[Direction.OUTGOING] ~= nil));
  else
    ok = false;
    if ((rtype == 'VIEWED_PIC') and (relation[Direction.OUTGOING] ~= nil)) then
      local pk;
      for k, v in pairs(relation[Direction.OUTGOING]) do pk = k; end
      if (pk == StartPK) then ok = true; end
    end
  end
  return ok, Direction.OUTGOING;
end
registerFunc(ExpanderFuncs, 'FOF', fof_expander);

-- FOF_ALL_EXPANDER
-- Examines ALL relationships a NODE has and returns relations to be expanded
function fof_all_rel_expander(x, relations)
  local knows_out  = {};
  local viewed_out = {};
  local do_us      = {};
  for rtype, relation in pairs(relations) do
    if (relation[Direction.OUTGOING] ~= nil) then
      if (rtype == 'KNOWS')      then
          if (viewed_out[x.w.node.__pk] ~= nil) then
            table.insert(do_us,
                         {rtype = 'VIEWED_PIC';
                          relation = viewed_out[x.w.node.__pk]});
          else
            table.insert(knows_out, x.w.node.__pk, relation);
          end
      end
      if (rtype == 'VIEWED_PIC') then
          if (knows_out[x.w.node.__pk] ~= nil) then
            table.insert(do_us, {rtype = rtype; relation = relation}); -- HIT
          else
            table.insert(viewed_out, x.w.node.__pk, relation);
          end
      end
    end
  end
  return do_us;
end
registerFunc(AllRelationshipExpanderFuncs, 'FOF', fof_all_rel_expander);


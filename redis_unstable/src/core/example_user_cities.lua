
local function isUserHasVisitedCity(snode, rtype, tnode)
  return (snode.__tname == 'users' and rtype == 'HAS_VISITED' and
          tnode.__tname == 'cities');
end
function addIndexUserHasVisitedCity(iname, snode, rtype, tnode)
  if (isUserHasVisitedCity(snode, rtype, tnode)) then
     alchemySetIndexByName(iname, snode.__pk, tnode.__pk);
  end
end
function deleteIndexUserHasVisitedCity(iname, snode, rtype, tnode)
  if (isUserHasVisitedCity(snode, rtype, tnode)) then
     alchemyDeleteIndexByName(iname, snode.__pk, tnode.__pk);
  end
end

function CNN(tname, cname, pk, name) -- shorter function name for DUMP
  return createNamedNode(tname, cname, pk, name);
end
function ANRBPK(stbl, spk, rtype, ttbl, tpk) -- shorter func name for DUMP
  return addNodeRelationShipByPK(stbl, spk, rtype, ttbl, tpk);
end
local function addNodeToSTBL(dumpt, n)
  table.insert(dumpt, 'CNN("' ..  n.__tname .. '","' .. n.__cname .. '",' ..
                                  n.__pk    .. ',"'  .. n.__name  .. '");\n');
end
local function addRelToNode(dumpt, sn, rtype, tn)
  table.insert(dumpt, 'ANRBPK("' ..  sn.__tname .. '",' .. sn.__pk .. ',"' ..
                                     rtype .. '","' ..
                                     tn.__tname .. '",' .. tn.__pk .. ');\n');
end
local GRAPH_dump_file = "GRAPH.lua.rdb";
function saveGraphNodes() print ('saveGraphNodes');
  local dumpt = {};
  for n in vertices() do
    --TODO dump node properties
    addNodeToSTBL(dumpt, n);
    if (n.r ~= nil) then
      for rtype, relation in pairs(n.r) do
        local pkt = relation[Direction.OUTGOING];
        if (pkt ~= nil) then
          for pk, trgt in pairs(pkt) do
            --TODO dump relationship properties
            local tn = trgt.target;
            addRelToNode(dumpt, n, rtype, tn);
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
function loadGraphNodes() print ('loadGraphNodes');
  hooks_saveLuaUniverse = {}; hooks_loadLuaUniverse = {};
  local buf = open_or_error(GRAPH_dump_file);
  assert(loadstring(buf))()
end

function constructUserGraphHooks(tname, iname)
  print ('constructUserGraphHooks: tname: ' .. tname .. ' iname: ' .. iname);
  if (not IndexInited[iname]) then
    buildIndex(addIndexUserHasVisitedCity, iname);
  end
  IndexInited[iname] = true;
  table.insert(hooks_addNodeRelationShip, 
               {func  = addIndexUserHasVisitedCity;
                iname = iname;});
  table.insert(hooks_deleteNodeRelationShip,
               {func  = deleteIndexUserHasVisitedCity;
                iname = iname;});
  table.insert(hooks_saveLuaUniverse, {func  = saveGraphNodes;});
  table.insert(hooks_loadLuaUniverse, {func  = loadGraphNodes;});
end

function destructUserGraphHooks(tname, iname)
  IndexInited[iname] = false;
  print ('destructGraphHooks: tname: ' .. tname .. ' iname: ' .. iname);
  for k, hook in pairs(hooks_addNodeRelationShip) do
    if (hook.iname == iname) then
      hooks_addNodeRelationShip[k] = nil; break;
    end
  end
  for k, hook in pairs(hooks_deleteNodeRelationShip) do
    if (hook.iname == iname) then
      hooks_deleteNodeRelationShip[k] = nil; break;
    end
  end
  for k, hook in pairs(hooks_saveLuaUniverse) do
    if (hook.func == saveGraphNodes) then
      hooks_saveLuaUniverse[k] = nil; break;
    end
  end
  for k, hook in pairs(hooks_loadLuaUniverse) do
    if (hook.func == loadGraphNodes) then
      hooks_loadLuaUniverse[k] = nil; break;
    end
  end
end

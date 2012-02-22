
-- INDEXING INDEXING INDEXING INDEXING INDEXING INDEXING INDEXING INDEXING
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

-- CONSTRUCT/DESTRUCT CONSTRUCT/DESTRUCT CONSTRUCT/DESTRUCT CONSTRUCT/DESTRUCT
function constructUserGraphHooks(tname, iname)
  print ('constructUserGraphHooks: tname: ' .. tname .. ' iname: ' .. iname);
  if (not IndexInited[iname]) then
    buildIndex(addIndexUserHasVisitedCity, iname);
  end
  IndexInited[iname] = true;
  table.insert(hooks_addNodeRelationShip, 
               {func  = addIndexUserHasVisitedCity;    iname = iname;});
  table.insert(hooks_deleteNodeRelationShip,
               {func  = deleteIndexUserHasVisitedCity; iname = iname;});
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

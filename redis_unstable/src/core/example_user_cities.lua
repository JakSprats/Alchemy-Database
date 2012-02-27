
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
  --print ('constructUserGraphHooks: tname: ' .. tname .. ' iname: ' .. iname);
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
  --print ('destructGraphHooks: tname: ' .. tname .. ' iname: ' .. iname);
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

-- LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER LUATRIGGER
UserData.CityToPK = {}; -- UserData[] gets persisted
function add_city(name, pk) UserData.CityToPK[name] = pk;  end
function del_city(name)     UserData.CityToPK[name] = nil; end

-- API_TO_OUTSIDE API_TO_OUTSIDE API_TO_OUTSIDE API_TO_OUTSIDE API_TO_OUTSIDE
function addSqlCityRowAndNode(pk, cityname, cityabbrv)
  local r1 = redis('INSERT', 'INTO', 'cities', 'VALUES',
                   "(" .. pk .. ", {}, '" .. cityname .. "')");
  local r2 = redis('SELECT',
                   "createNamedNode('cities', 'lo', pk, '" .. cityabbrv .."')",
                   'FROM', 'cities', 'WHERE', 'pk = ' .. pk);
  return '{ "INSERT": '   .. DataDumper(r1) .. ", " ..
           '"ADD_NODE": ' .. DataDumper(r2) .. "}";
end

function addCityDistance(to, from, dist)
  addNodeRelationShipByPK('cities', UserData.CityToPK[to], "PATH",
                          'cities', UserData.CityToPK[from]);
  return addPropertyToRelationshipByPK('cities', UserData.CityToPK[to], "PATH",
                                       'cities', UserData.CityToPK[from],
                                       'weight', dist);
end
function shortestPathByCityName(tname, startcity, endcity, ...)
  local r = shortestPathByPK(tname, UserData.CityToPK[startcity],
            UserData.CityToPK[endcity], ...);
  return {r.cost, r.path};
end

--  $CLI INSERT INTO users VALUES "(1, 10, {})";
--  $CLI SELECT "createNamedNode('users', 'lo', pk, 'A')" FROM users WHERE pk=1
function addSqlUserRowAndNode(pk, citypk, nodename)
  local r1 = redis('INSERT', 'INTO', 'users', 'VALUES',
                   "(" .. pk .. ", " .. citypk .. ", {})");
  local r2 = redis('SELECT',
                   "createNamedNode('users', 'lo', pk, '" .. nodename .."')",
                   'FROM', 'users', 'WHERE', 'pk = ' .. pk);
  return '{ "INSERT": '   .. DataDumper(r1) .. ", " ..
           '"ADD_NODE": ' .. DataDumper(r2) .. "}";
end


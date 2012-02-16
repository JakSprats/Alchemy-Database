
-- TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST TEST
function initial_test()
  V = {};
  local tname = 'users';
  V.lo1 = {}; createNode(tbl, V.lo1, 11);
  addNodeProperty(V.lo1.node, 'name', 'Joe1');
  V.lo2 = {}; createNode(tbl, V.lo2, 22);
  addNodeProperty(V.lo2.node, 'name', 'Bill2');
  V.lo3 = {}; createNode(tbl, V.lo3, 33);
  addNodeProperty(V.lo3.node, 'name', 'Jane3');
  V.lo4 = {}; createNode(tbl, V.lo4, 44);
  addNodeProperty(V.lo4.node, 'name', 'Ken4');
  V.lo5 = {}; createNode(tbl, V.lo5, 55);
  addNodeProperty(V.lo5.node, 'name', 'Kate5');
  V.lo6 = {}; createNode(tbl, V.lo6, 65);
  addNodeProperty(V.lo6.node, 'name', 'Mack6');
  V.lo7 = {}; createNode(tbl, V.lo7, 77);
  addNodeProperty(V.lo7.node, 'name', 'Lyle7');
  V.lo8 = {}; createNode(tbl, V.lo8, 88);
  addNodeProperty(V.lo8.node, 'name', 'Bud8');
  V.lo9 = {}; createNode(tbl, V.lo9, 99);
  addNodeProperty(V.lo9.node, 'name', 'Rick9');
  V.loa = {}; createNode(tbl, V.loa, 12);
  addNodeProperty(V.loa.node, 'name', 'Lori9');
  addNodeRelationShip(V.lo1.node, 'LIKES', V.lo2.node);
  addNodeRelationShip(V.lo2.node, 'KNOWS', V.lo1.node);
  addNodeRelationShip(V.lo2.node, 'KNOWS', V.lo3.node);
  addNodeRelationShip(V.lo3.node, 'KNOWS', V.lo4.node);
  addNodeRelationShip(V.lo4.node, 'KNOWS', V.lo5.node);
  addNodeRelationShip(V.lo2.node, 'KNOWS', V.lo6.node);
  addNodeRelationShip(V.lo6.node, 'KNOWS', V.lo7.node);
  addNodeRelationShip(V.lo1.node, 'KNOWS', V.lo8.node);
  addNodeRelationShip(V.lo8.node, 'KNOWS', V.lo9.node);
  addNodeRelationShip(V.lo4.node, 'KNOWS', V.loa.node);

  print ('3 matches: KNOWS: 2 OUT');
  printNameFromRel(V.lo2.node, 'KNOWS', Direction.OUT);
  print ('1 match: LIKE: 2 IN');
  printNameFromRel(V.lo2.node, 'LIKES', Direction.IN);

  deleteNodeRelationShip(V.lo1.node, 'LIKES', V.lo2.node);
  print ('0 matches: LIKE: 2 IN - deleted');
  printNameFromRel(V.lo2.node, 'LIKES', Direction.IN);

  addPropertyToRelationship(V.lo2.node, 'KNOWS', V.lo3.node, 'weight', 10);
  print ('3 matches: KNOWS: 2 OUT - one w/ weight');
  printNameFromRel(V.lo2.node, 'KNOWS', Direction.OUT);

  local x = traverse_bfs(V.lo2.node, rf_path);
  print('BreadthFirst: rf_path');
  for k,v in pairs(x) do print("\tPATH: " .. v); end

  local y = traverse_bfs(V.lo2.node, rf_node_name);
  print('BreadthFirst: reply_func_node_name');
  for k,v in pairs(y) do print("\tNAME: " .. v); end

  print('BreadthFirst: rf_node_and_path {min=2; max=3;}');
  local z = traverse_bfs(V.lo2.node, rf_node_and_path, {min=2; max=3;});
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end

  print('DepthFirst: rf_node_and_path');
  z=traverse_dfs(V.lo2.node, rf_node_and_path);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end

  x = traverse_dfs(V.lo2.node, rf_path, {min=2; max=3;});
  print('DepthFirst: rf_path {min=2; max=3;}');
  for k,v in pairs(x) do print("\tPATH: " .. v); end
end

function best_path_test()
  V = {};
  local tname = 'cities';
  V.loA = {}; createNode(tbl, V.loA, 11);
  addNodeProperty(V.loA.node, 'name', 'A');
  V.loB = {}; createNode(tbl, V.loB, 12);
  addNodeProperty(V.loB.node, 'name', 'B');
  V.loC = {}; createNode(tbl, V.loC, 14);
  addNodeProperty(V.loC.node, 'name', 'C');
  V.loD = {}; createNode(tbl, V.loD, 15);
  addNodeProperty(V.loD.node, 'name', 'D');
  V.loE = {}; createNode(tbl, V.loE, 16);
  addNodeProperty(V.loE.node, 'name', 'E');
  V.loF = {}; createNode(tbl, V.loF, 17);
  addNodeProperty(V.loF.node, 'name', 'F');
  V.loG = {}; createNode(tbl, V.loG, 18);
  addNodeProperty(V.loG.node, 'name', 'G');
  V.loI = {}; createNode(tbl, V.loI, 19);
  addNodeProperty(V.loI.node, 'name', 'I');
  V.loJ = {}; createNode(tbl, V.loJ, 20);
  addNodeProperty(V.loJ.node, 'name', 'J');
  V.loK = {}; createNode(tbl, V.loK, 21);
  addNodeProperty(V.loK.node, 'name', 'K');
  V.loL = {}; createNode(tbl, V.loL, 22);
  addNodeProperty(V.loL.node, 'name', 'L');
  V.loM = {}; createNode(tbl, V.loM, 23);
  addNodeProperty(V.loM.node, 'name', 'M');

  addNodeRelationShip(V.loA.node, 'PATH', V.loB.node);             -- cost: 100
  addPropertyToRelationship(V.loA.node, 'PATH', V.loB.node, 'weight', 100);

  addNodeRelationShip(V.loA.node, 'PATH', V.loC.node);             -- cost: 70
  addPropertyToRelationship(V.loA.node, 'PATH', V.loC.node, 'weight', 20);
  addNodeRelationShip(V.loC.node, 'PATH', V.loB.node);
  addPropertyToRelationship(V.loC.node, 'PATH', V.loB.node, 'weight', 50);

  addNodeRelationShip(V.loC.node, 'PATH', V.loD.node);             -- cost: 60
  addPropertyToRelationship(V.loC.node, 'PATH', V.loD.node, 'weight', 20);
  addNodeRelationShip(V.loD.node, 'PATH', V.loB.node);
  addPropertyToRelationship(V.loD.node, 'PATH', V.loB.node, 'weight', 20);

  addNodeRelationShip(V.loA.node, 'PATH', V.loE.node);             -- cost: 50
  addPropertyToRelationship(V.loA.node, 'PATH', V.loE.node, 'weight', 10);
  addNodeRelationShip(V.loE.node, 'PATH', V.loF.node);
  addPropertyToRelationship(V.loE.node, 'PATH', V.loF.node, 'weight', 10);
  addNodeRelationShip(V.loF.node, 'PATH', V.loB.node);
  addPropertyToRelationship(V.loF.node, 'PATH', V.loB.node, 'weight', 30);

  addNodeRelationShip(V.loF.node, 'PATH', V.loG.node);             -- cost: 40
  addPropertyToRelationship(V.loF.node, 'PATH', V.loG.node, 'weight', 10);
  addNodeRelationShip(V.loG.node, 'PATH', V.loB.node);
  addPropertyToRelationship(V.loG.node, 'PATH', V.loB.node, 'weight', 10);

  addNodeRelationShip(V.loA.node, 'PATH', V.loI.node);             -- cost: 30
  addPropertyToRelationship(V.loA.node, 'PATH', V.loI.node, 'weight', 5);
  addNodeRelationShip(V.loI.node, 'PATH', V.loJ.node);
  addPropertyToRelationship(V.loI.node, 'PATH', V.loJ.node, 'weight', 5);
  addNodeRelationShip(V.loJ.node, 'PATH', V.loK.node);
  addPropertyToRelationship(V.loJ.node, 'PATH', V.loK.node, 'weight', 5);
  addNodeRelationShip(V.loK.node, 'PATH', V.loL.node);
  addPropertyToRelationship(V.loK.node, 'PATH', V.loL.node, 'weight', 5);
  addNodeRelationShip(V.loL.node, 'PATH', V.loM.node);
  addPropertyToRelationship(V.loL.node, 'PATH', V.loM.node, 'weight', 5);
  addNodeRelationShip(V.loM.node, 'PATH', V.loB.node);
  addPropertyToRelationship(V.loM.node, 'PATH', V.loB.node, 'weight', 5);

  local t = shortestpath(V.loA.node, V.loB.node);
  print('shortestpath[A->B]: cost: ' .. t.cost .. ' path: ' .. t.path);
end

function unique_none_test()
  V = {};
  V.loX = {}; createNode(tbl, V.loX, 11);
  addNodeProperty(V.loX.node, 'name', 'X');
  V.loY = {}; createNode(tbl, V.loY, 12);
  addNodeProperty(V.loY.node, 'name', 'Y');
  V.loZ = {}; createNode(tbl, V.loZ, 14);
  addNodeProperty(V.loZ.node, 'name', 'Z');
  addNodeRelationShip(V.loX.node, 'KNOWS', V.loY.node);
  addNodeRelationShip(V.loY.node, 'KNOWS', V.loZ.node);
  addNodeRelationShip(V.loZ.node, 'KNOWS', V.loX.node);

  print('BreadthFirst: rf_node_and_path - UNIQ: NONE max_depth=10');
  local z = traverse_bfs(V.loX.node, rf_node_and_path,
                         {min=1; max=10;}, Uniqueness.NONE);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end
end

function unique_path_test()
  V = {};
  V.loU = {}; createNode(tbl, V.loU,  8);
  addNodeProperty(V.loU.node, 'name', 'U');
  V.loV = {}; createNode(tbl, V.loV,  9);
  addNodeProperty(V.loV.node, 'name', 'V');
  V.loW = {}; createNode(tbl, V.loW, 10);
  addNodeProperty(V.loW.node, 'name', 'W');
  V.loX = {}; createNode(tbl, V.loX, 11);
  addNodeProperty(V.loX.node, 'name', 'X');
  V.loY = {}; createNode(tbl, V.loY, 12);
  addNodeProperty(V.loY.node, 'name', 'Y');
  V.loZ = {}; createNode(tbl, V.loZ, 14);
  addNodeProperty(V.loZ.node, 'name', 'Z');
  addNodeRelationShip(V.loX.node, 'KNOWS', V.loY.node);
  addNodeRelationShip(V.loY.node, 'KNOWS', V.loZ.node);
  addNodeRelationShip(V.loZ.node, 'KNOWS', V.loX.node);

  addNodeRelationShip(V.loX.node, 'KNOWS', V.loW.node);
  addNodeRelationShip(V.loW.node, 'KNOWS', V.loZ.node);

  addNodeRelationShip(V.loW.node, 'KNOWS', V.loV.node);
  addNodeRelationShip(V.loV.node, 'KNOWS', V.loZ.node);

  addNodeRelationShip(V.loZ.node, 'KNOWS', V.loU.node);
  addNodeRelationShip(V.loU.node, 'KNOWS', V.loV.node);

  print('BreadthFirst: rf_node_and_path - UNIQ: NONE max_depth=10');
  local z = traverse_bfs(V.loX.node, rf_node_and_path,
                         {min=1; max=10;}, Uniqueness.PATH_GLOBAL);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end

  print('DepthFirst: rf_node_and_path - UNIQ: NONE max_depth=10');
  z = traverse_dfs(V.loX.node, rf_node_and_path,
                   {min=1; max=10;}, Uniqueness.PATH_GLOBAL);
  for k,v in pairs(z) do
    print("\tNAME: " .. v.node.name .. "\tPATH: " .. v.path);
  end
end

function run_tests()
  initial_test();
  best_path_test();
  unique_none_test();
  unique_path_test();
end

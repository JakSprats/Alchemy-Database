
local DefaultFDomain = "www.retwis.com";

AutoIncRange = 10000;

NodeData = {};
-- NODES
NodeData[1]  = {fip   = "127.0.0.1", fport    = 8001,  bridge  =  1,
                                     lbport   = 28001};
NodeData[2]  = {fip   = "127.0.0.1", fport    = 8002,  bridge  =  1,
                                     lbport   = 28002};
NodeData[3]  = {fip   = "127.0.0.1", fport    = 8003,  bridge  =  1,
                                     lbport   = 28003};
NodeData[4]  = {fip   = "127.0.0.1", fport    = 8004,  bridge  =  1,
                                     lbport   = 28004};
NodeData[5]  = {fip   = "127.0.0.1", fport    = 9005,  bridge  =  2,
                                     lbport   = 29005};
NodeData[6]  = {fip   = "127.0.0.1", fport    = 9006,  bridge  =  2,
                                     lbport   = 29006};
NodeData[7]  = {fip   = "127.0.0.1", fport    = 9007,  bridge  =  2,
                                     lbport   = 29007};
NodeData[8]  = {fip   = "127.0.0.1", fport    = 9008,  bridge  =  2,
                                     lbport   = 29008};
-- BRIDGES
NodeData[9]  = {fip   = "127.0.0.1", fport    = 20000, bport   = 10000, 
                isb   = true;                          bridge  =  1};
NodeData[10] = {fip   = "127.0.0.1", fport    = 20001, bport   = 10001, 
                isb   = true,                          bridge  =  2};
-- NODE SLAVES
NodeData[11] = {fip   = "127.0.0.1", fport    = 18001,
                                     lbport   = 28001,
                slave = true,        masterid = 1;     bridge  = 1};
NodeData[12] = {fip   = "127.0.0.1", fport    = 18002,
                                     lbport   = 28002,
                slave = true,        masterid = 2;     bridge  =  1};
NodeData[13] = {fip   = "127.0.0.1", fport    = 18003,  
                                     lbport   = 28003,
                slave = true,        masterid = 3;     bridge  =  1};
NodeData[14] = {fip   = "127.0.0.1", fport    = 18004,  
                                     lbport   = 28004,
                slave = true,        masterid = 4;     bridge  =  1};
NodeData[15] = {fip   = "127.0.0.1", fport    = 19005,  
                                     lbport   = 29005,
                slave = true,        masterid = 5;     bridge  =  2};
NodeData[16] = {fip   = "127.0.0.1", fport    = 19006,  
                                     lbport   = 29006,
                slave = true,        masterid = 6;     bridge  =  2};
NodeData[17] = {fip   = "127.0.0.1", fport    = 19007,  
                                     lbport   = 29007,
                slave = true,        masterid = 7;     bridge  =  2};
NodeData[18] = {fip   = "127.0.0.1", fport    = 19008,  
                                     lbport   = 29008,
                slave = true,        masterid = 8;     bridge  =  2};
-- BRIDGE SLAVE
NodeData[19] = {fip   = "127.0.0.1", fport    = 30000, bport   = 10000, 
                isb   = true;
                slave = true,        masterid = 9;     bridge  =  1};
NodeData[20] = {fip   = "127.0.0.1", fport    = 30001, bport   = 10001, 
                isb   = true,
                slave = true,        masterid = 10;    bridge  =  2};

-- COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER
for inid, data in pairs(NodeData) do -- DEFAULTS
  data['synced'] = false;
  if (data['masterid'] == nil) then data['masterid'] = -1;               end
  if (data['isb']      == nil) then data['isb']      =  false;           end
  if (data['slave']    == nil) then data['slave']    =  false;           end
  if (data['fdomain']  == nil) then data['fdomain']  =  DefaultFDomain;  end
  if (data['lbdomain'] == nil) then data['lbdomain'] =  data['fdomain']; end
  if (data['lbip']     == nil) then data['lbip']     =  data['fip'];     end
  if (data['lbport']   == nil) then data['lbport']   =  data['fport'];   end
  if (data['bip']      == nil) then data['bip']      =  data['fip'];     end
  if (data['bport']    == nil) then data['bport']    =  data['fport'];   end
end

AmBridge   = NodeData[MyNodeId]['isb'];
AmSlave    = NodeData[MyNodeId]['slave'];

IslandData = {};
for inid, data in pairs(NodeData) do
  if (data['slave'] == false and data['isb'] == false) then
    IslandData[inid] = tonumber(data['bridge']);
  end
end

BridgeData = {};
for inid, data in pairs(NodeData) do
  if (data['slave'] == false and data['isb']) then
    table.insert(BridgeData, tonumber(data['bridge']), inid);
  end
end

BridgeId   = tonumber(NodeData[MyNodeId]['bridge']);
PeerData   =  {};
for inid, data in pairs(NodeData) do
  if (data['slave'] == false and tonumber(data['bridge']) == BridgeId) then
    table.insert(PeerData, inid);
  end
end

MasterData  = {};
for inid, data in pairs(NodeData) do
  if (data['slave']) then
    MasterData[inid] = tonumber(data['masterid']);--TODO table (multiple slaves)
  end
end

GlobalReadData = {};
for inid, data in pairs(NodeData) do
  if (data['isb'] == false) then
    table.insert(GlobalReadData, inid);
  end
end

AutoIncStep = #PeerData - 1;
if (AmBridge) then
  NumPeers     = #PeerData + #BridgeData - 2; -- HBs from ALL except self(twice)
  NumHBBridges = #BridgeData - 1;             -- no self
else
  NumPeers     = #PeerData - 1;               -- no bridge
  NumHBBridges = 0;
end
NumHBPeers = #PeerData - 1; -- no self

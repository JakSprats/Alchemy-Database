
AutoIncRange = 10000;

NodeData = {};
-- NODES
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 8001,
                        isb    = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge = 1});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 8002,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 8003,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 8004,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 9005,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 9006,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 9007,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 9008,  
                        isb     = false,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  2});
-- BRIDGES
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 20000, 
                        bip     = "127.0.0.1",      bport  = 10000, 
                        isb     = true;
                        fdomain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {fip     = "127.0.0.1",      fport  = 20001, 
                        bip     = "127.0.0.1",      bport  = 10001, 
                        isb     = true,
                        fdomain = "www.retwis.com", synced = 0,   bridge =  2});

-- COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER
AmBridge   = NodeData[MyNodeId]['isb'];

for inid, data in pairs(NodeData) do -- missing [bip,bport] -> [fip,fport]
  if (data['bip']   == nil) then data['bip']   = data['fip']; end
  if (data['bport'] == nil) then data['bport'] = data['fport']; end
end

IslandData = {};
NumNodes   = -1;
for inid, data in pairs(NodeData) do
  if (data['isb'] == false) then
    IslandData[inid] = tonumber(data['bridge']);
    NumNodes = NumNodes + 1;
  end
end

BridgeData = {};
for inid, data in pairs(NodeData) do
  if (data['isb']) then
    table.insert(BridgeData, tonumber(data['bridge']), inid);
  end
end

BridgeId   = tonumber(NodeData[MyNodeId]['bridge']);
PeerData   =  {};
for inid, data in pairs(NodeData) do
  if (tonumber(data['bridge']) == BridgeId) then
    table.insert(PeerData, inid);
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

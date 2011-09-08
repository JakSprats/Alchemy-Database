
AutoIncRange = 10000;

NodeData = {};
-- NODES
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8001,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge = 1});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8002,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8003,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8004,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 9005,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 9006,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 9007,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  2});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 9008,  isb    = false,
                        domain = "www.retwis.com", synced = 0,   bridge =  2});
-- BRIDGES
table.insert(NodeData, {ip     = "127.0.0.1",      port = 20000, isb    = true;
                        domain = "www.retwis.com", synced = 0,   bridge =  1});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 20001, isb    = true,
                        domain = "www.retwis.com", synced = 0,   bridge =  2});

-- COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER COMPUTE_CLUSTER
AmBridge   = NodeData[MyNodeId]['isb'];

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
if (AmBridge) then
  NumPeers = #PeerData + #BridgeData - 2; -- HBs from ALL except self (twice)
  NumHBs   = #PeerData + #BridgeData - 2; -- HBs from ALL except self (twice)
else
  NumPeers = #PeerData - 1; -- no bridge
  NumHBs   = #PeerData - 2; -- no self, no bridge
end

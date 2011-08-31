AutoIncRange = 5;

MyNodeId = 1;
NodeData = {};
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8080,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8081,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8082,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8083,
                        domain = "www.retwis.com", synced = 0});

BridgeId   =  1;
MyBridgeId = -1;
BridgeData = {};
table.insert(BridgeData, {ip     = "127.0.0.1",      port = 10000,
                          domain = "www.retwis.com", synced = 0});
table.insert(BridgeData, {ip     = "127.0.0.1",      port = 10001,
                          domain = "www.retwis.com", synced = 0});

PeerData = {1, 2, -1};

-- CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT
IslandData    = {};
IslandData[1] = 1; IslandData[2] = 1;   
IslandData[3] = 2; IslandData[4] = 2;   

NumNodes  = #NodeData;
NumPeers  = #PeerData - 1; -- no bridge
NumToSync = #PeerData - 1; -- no self

MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

-- WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS
dofile "./docroot/DIST/whitelist.lua";


AutoIncRange = 10000;

MyNodeId = 7;
NodeData = {};
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8080,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8081,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8082,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8083,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8084,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8085,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8086,
                        domain = "www.retwis.com", synced = 0});
table.insert(NodeData, {ip     = "127.0.0.1",      port = 8087,
                        domain = "www.retwis.com", synced = 0});

BridgeId   =  2;
MyBridgeId = -1;
BridgeData = {};
table.insert(BridgeData, {ip     = "127.0.0.1",      port = 20000,
                          domain = "www.retwis.com", synced = 0});
table.insert(BridgeData, {ip     = "127.0.0.1",      port = 20001,
                          domain = "www.retwis.com", synced = 0});

PeerData = {5, 6, 7, 8, -1};

-- CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT
IslandData    = {};
IslandData[1] = 1; IslandData[2] = 1;   
IslandData[3] = 1; IslandData[4] = 1;   
IslandData[5] = 2; IslandData[6] = 2;   
IslandData[7] = 2; IslandData[8] = 2;   

NumNodes  = #NodeData;
NumPeers  = #PeerData - 1; -- no bridge
NumHBs    = #PeerData - 2; -- no self, no bridge
NumToSync = #PeerData - 1; -- no self

MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

-- WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS
dofile "../../docroot/DIST/whitelist.lua";


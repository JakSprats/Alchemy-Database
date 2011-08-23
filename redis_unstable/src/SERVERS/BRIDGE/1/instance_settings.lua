AutoIncRange = 5;

MyNodeId = -1; -- BRIDGE has no data
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

BridgeData = {ip     = "127.0.0.1",      port = 9999,
              domain = "www.retwis.com", synced = 0};

PeerData = {1, 2, 3, 4, 5, 6, 7, 8};

NetworkData           = {};
NetworkData[1]        = 1;
NetworkData[2]        = 1;
NetworkData[3]        = 1;
NetworkData[4]        = 1;
NetworkData[5]        = 2;
NetworkData[6]        = 2;
NetworkData[7]        = 2;
NetworkData[8]        = 2;

ForwardNetworkData    = {};
ForwardNetworkData[1] = 2;
ForwardNetworkData[2] = 2;
ForwardNetworkData[3] = 2;
ForwardNetworkData[4] = 2;
ForwardNetworkData[5] = 1;
ForwardNetworkData[6] = 1;
ForwardNetworkData[7] = 1;
ForwardNetworkData[8] = 1;

-- CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT CONSTANT
NumNodes  = #NodeData;
NumToSync = #PeerData; -- SYNC to ALL

MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

-- APP_STACK
dofile "app_stack.lua"


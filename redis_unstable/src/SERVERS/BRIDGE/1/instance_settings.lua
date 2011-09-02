
-- CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG
dofile "../../../docroot/DIST/config.lua"

MyNodeId   = -1; -- BRIDGE has no data
MyBridgeId =  1;
BridgeId   =  1;
PeerData   = {1, 2, 3, 4};
NumNodes   = #NodeData;
NumPeers   = #PeerData;
NumHBs     = #NodeData;
NumToSync  = #PeerData + #BridgeData - 1; -- SYNC to ALL except self

-- APP_STACK
dofile "../app_stack.lua"


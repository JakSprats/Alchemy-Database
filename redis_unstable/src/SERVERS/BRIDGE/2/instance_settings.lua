
-- CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG
dofile "../../../docroot/DIST/config.lua"

MyNodeId   = -1; -- BRIDGE has no data
MyBridgeId =  2;
BridgeId   =  2;
PeerData   = {5, 6, 7, 8};
NumNodes   = #NodeData;
NumPeers   = #PeerData;
NumHBs     = #NodeData;
NumToSync  = #PeerData + #BridgeData - 1; -- SYNC to ALL except self

-- APP_STACK
dofile "../app_stack.lua"


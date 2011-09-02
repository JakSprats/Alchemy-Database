
-- CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG
dofile "../../docroot/DIST/config.lua"

MyNodeId   =  2;
BridgeId   =  1;
MyBridgeId = -1;
PeerData   = {1, 2, 3, 4, -1};
NumNodes   = #NodeData;
NumPeers   = #PeerData - 1; -- no bridge
NumHBs     = #PeerData - 2; -- no self, no bridge
NumToSync  = #PeerData - 1; -- no self

-- WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS
dofile "../../docroot/DIST/whitelist.lua";


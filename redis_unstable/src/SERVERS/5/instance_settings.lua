
-- CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG CONFIG
dofile "../../docroot/DIST/config.lua"

MyNodeId   =  5;
BridgeId   =  2;
MyBridgeId = -1;
PeerData   = {5, 6, 7, 8, -1};
NumNodes   = #NodeData;
NumPeers   = #PeerData - 1; -- no bridge
NumHBs     = #PeerData - 2; -- no self, no bridge
NumToSync  = #PeerData - 1; -- no self

-- WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS WHITELISTED_FUNCTIONS
dofile "../../docroot/DIST/whitelist.lua";


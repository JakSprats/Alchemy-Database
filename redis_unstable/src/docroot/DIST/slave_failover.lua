
-- NOTES: there are 2 mechanisms that communicate w/ external processes
--      1.) SLAVE_LUA_FUNCTIONS: any LUA command inserted into this will
--                               be EVALED on the Node's SLAVE
--      2.) MASTER_DOWN: this is a flag for cron_scripts
--                       if a BRIDGE MASTER is down,
--                       stunnels must be re-configured for the SLAVE
--                                                          (that is now master)

function CheckSlaveToMasterConnection()
  if     (IsConnectedToMaster()) then MasterConnection = true;
  elseif (MasterConnection)      then 
    print('ERROR: SLAVE LOST CONNECTION'); 
    PromoteSlave(MyNodeId); MasterConnection = false;
  end
end
function CheckSlaveLuaFunctions()
  if (AmSlave == false) then return end;
  local funcs = redis("lrange", "SLAVE_LUA_FUNCTIONS", 0, -1);
  if (funcs ~= nil) then
    for k, func in ipairs(funcs) do
      assert(loadstring(func))() -- Lua eval - each func
    end
    redis("del", "SLAVE_LUA_FUNCTIONS");
  end
end

function ReconfigStunnels(mynid) print('ReconfigStunnels');
  --NOTE: MASTER_DOWN will be checked by cron scripts to reconfig stunnels
  if (mynid == MyNodeId) then redis("set", "MASTER_DOWN", os.time()); end
end

DeadMasters = {}; -- USED in linking.lua: UserNode()
function PromoteSlave(o_mynid) print('PROMO4SLAVE: inid: ' .. o_mynid);
  local mynid = tonumber(o_mynid);
  if (AmBridge) then ReconfigStunnels(mynid) end
  if (AmSlave == false) then -- SLAVE_LUA_FUNCTIONS will be run on SLAVEs
    redis("rpush", "SLAVE_LUA_FUNCTIONS", "PromoteSlave(" .. mynid .. ")");
  end
  if (mynid == MyNodeId) then redis("SLAVEOF", "NO", "ONE"); end
  NodeData[mynid]['slave']   = false;             -- NodeData[] promotion
  NodeData[mynid]['synced']  = false;             -- PING ME
  local master               = MasterData[mynid];
  NodeData[master]['active'] = false;             -- disable DOWNED master
  NodeData[master]['synced'] = false;
  ComputeCluster();                               -- recreate_cluster
  DeadMasters[master]        = mynid;             -- linking -> mynid
  if (mynid ~= MyNodeId) then                     -- remote run, swap SyncedHW[]
    AllSynced       = false;            NumSynced        = NumSynced - 1;
    if (AmBridge) then
      SyncedBridgeHW[mynid]  = SyncedBridgeHW[master];
      SyncedBridgeHW[master] = nil;
    end
    SyncedHW[mynid] = SyncedHW[master]; SyncedHW[master] = nil;
    local hwname    = getHWname(master);          -- sync_Q -> master's state
    local hw        = tonumber(redis("get", hwname));
    if (hw ~= nil) then
      redis("del", hwname);
      hwname = getHWname(mynid); redis("set", hwname, hw); return hw;
    end
  end
  return 0;
end

function HandleRW(rw) -- called per InitRequest()
  if (AmSlave and rw == WRITE_OP_URL) then PromoteSlave(MyNodeId); end
end


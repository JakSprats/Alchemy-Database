
-- SETTINGS SETTINGS SETTINGS SETTINGS SETTINGS SETTINGS SETTINGS
local TS_TIMEOUT = 10; -- no HB w/in TS_TIMEOUT secs -> resync

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
local MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);

local MyHB_ID = redis("get", "alchemy_heartbeat");
if (MyHB_ID == nil) then MyHB_ID = 0; end

-- FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
function getGenerationName(nodeid) return 'GENERATION_' .. nodeid; end
function getHWname        (nodeid) return 'HIGHWATER_'  .. nodeid; end

local BridgeHW = 0; -- TODO -> array
function aiweq(inid) return '[' .. inid .. ']='; end -- HELPER FUNCTION

function GenerateHBID()
  return 'HB_ID=' .. MyHB_ID .. ';HB_TS=' .. os.time() .. ';';
end
function GenerateBridgeHBCommand()-- this command will be remotely EVALed
  local b_gnr_cmd = 'B_GNR={' .. aiweq(MyNodeId) .. MyGeneration .. '};';
  local b_hw_cmd  = 'B_HW={'  .. aiweq(MyNodeId) .. BridgeHW     .. '};';
  return GenerateHBID() .. b_hw_cmd .. b_gnr_cmd;
end

local LastHB_TS  = {};
function DetectStaleNodes(o_now)
  local now = tonumber(o_now);
  for inid, ts in pairs(LastHB_TS) do
    if ((tonumber(ts) + TS_TIMEOUT) < now) then
      print ('TIMEOUT: inid: ' .. inid .. ' ts: ' .. ts .. ' now: ' .. now);
      LastHB_TS[inid] = nil;
      resync_ping(inid, 'sync');
    end
  end
end

function GenerateHBCommand() -- this command will be remotely EVALed
  local n_hw_cmd  = 'N_HW={';
  local n_gnr_cmd = 'N_GNR={';
  local i         = 1;
  for k, inid in pairs(PeerData) do
    if (inid == MyNodeId) then
      n_hw_cmd  = n_hw_cmd  .. aiweq(inid) .. AutoInc['In_Xactid'];
      n_gnr_cmd = n_gnr_cmd .. aiweq(inid) .. MyGeneration;
    else
      local hw  = redis("get", getHWname(inid));
      if (hw == nil) then hw = '0'; end
      local gnr = redis("get", getGenerationName(inid));
      if (gnr == nil) then gnr = '0'; end
      n_hw_cmd  = n_hw_cmd  .. aiweq(inid) .. hw;
      n_gnr_cmd = n_gnr_cmd .. aiweq(inid) .. gnr;
    end
    if (i ~= #PeerData) then
      n_hw_cmd  = n_hw_cmd  .. ','; n_gnr_cmd = n_gnr_cmd .. ',';
    end
    i = i + 1;
  end
  n_hw_cmd  = n_hw_cmd  .. '};'; n_gnr_cmd = n_gnr_cmd .. '};';
  return GenerateHBID() .. n_hw_cmd .. n_gnr_cmd;
end
function HeartBeat() -- lua_cron function, called every second
  if (PingSyncAllNodes() == false) then return; end -- wait until synced
  MyHB_ID = MyHB_ID + 1; redis("set", "alchemy_heartbeat", MyHB_ID);
  local cmd  = GenerateHBCommand();
  local msg  = Redisify('LUA', 'handle_heartbeat', MyNodeId, cmd);
  print ('   HEARTBEAT: myid: ' .. MyNodeId .. ' cmd: ' .. cmd);
  redis("publish", 'sync', msg);
  if (AmBridge) then
    local myid = MyNodeId;
    cmd = cmd .. GenerateBridgeHBCommand();
    msg = Redisify('LUA', 'handle_bridge_heartbeat', myid, cmd);
    print ('   BRIDGE_HEARTBEAT: myid: ' .. myid .. ' cmd: ' .. cmd);
    redis("publish", 'sync_bridge', msg);
  end
  DetectStaleNodes(os.time());
end

local SyncedHW  = {}; local LastHB_SyncedHW = {}; local GenerationSet = {};
local LastHB_ID = {};

function handle_HB_ID_TS(inid, HB_ID, HB_TS)
  if (LastHB_ID[inid] ~= nil and HB_ID <= LastHB_ID[inid]) then return; end
  LastHB_ID[inid] = tonumber(HB_ID);
  LastHB_TS[inid] = tonumber(HB_TS);
end

function handle_HB_GNR(inid, GNR)
  for nid, gnr in pairs(GNR) do
    local i = tonumber(nid);
    if (i == inid) then
      if (GenerationSet[inid]) then
        local lgnr = redis("get", getGenerationName(inid));
        if (lgnr ~= nil) then
          if (tonumber(lgnr) ~= tonumber(gnr)) then
            print('handle_HB_GNR: lgnr: ' .. lgnr .. ' gnr: ' .. gnr);
            resync_ping(inid, 'sync');
          end
        end
      end
      redis("set", getGenerationName(inid), gnr);
      GenerationSet[inid] = true;
    end
  end
end

function handle_heartbeat(nodeid, hb_eval_cmd)
  print ('handle_heartbeat: inid: ' .. nodeid .. ' hb_cmd: ' .. hb_eval_cmd)
  local inid = tonumber(nodeid);
  -- Lua eval - defines variables: [N_HW, N_GNR]
  assert(loadstring(hb_eval_cmd))()
  handle_HB_ID_TS(inid, HB_ID, HB_TS);
  for num, data in pairs(N_HW) do
    local inum = tonumber(num);
    if (inum == MyNodeId) then
      --print ('X to NODE: SyncedHW[' .. inid .. ']: ' .. data);
      SyncedHW[inid] = data;
      if (LastHB_SyncedHW[inid] == nil) then LastHB_SyncedHW[inid] = data; end
    end
  end
  handle_HB_GNR(inid, N_GNR);
  --check_heartbeat()
end

function handle_bridge_heartbeat(nodeid, hb_eval_cmd)
  print ('handle_bridge_heartbeat: inid: '  .. nodeid ..
                                ' hb_cmd: ' .. hb_eval_cmd)
  local inid = tonumber(nodeid);
  -- Lua eval - defines variables: [N_HW, N_GNR, B_HW, B_GNR]
  assert(loadstring(hb_eval_cmd))()
  handle_HB_ID_TS(inid, HB_ID, HB_TS);
  for num, data in pairs(N_HW) do
    local inum = tonumber(num);
    --print ('BRIDGE to BRIDGE: SyncedHW[' .. inum .. ']: ' .. data);
    SyncedHW[inum] = data;
    if (LastHB_SyncedHW[inum] == nil) then LastHB_SyncedHW[inum] = data; end
  end
  handle_HB_GNR(inid, B_GNR);
end

function rexmit_ops(nodeid, beg, fin)
  local inid     = tonumber(nodeid);
  print ('rexmit_ops: inid: ' .. inid .. ' beg: ' .. beg .. ' fin: ' .. fin);
  local channel;
  if (AmBridge) then channel = 'Q_sync_bridge';
  else               channel = 'Q_sync';        end
  local msgs     = redis("zrangebyscore", channel, beg, fin);
  local pipeline = '';
  for k,v in pairs(msgs) do pipeline = pipeline .. v; end
  --print ('pipeline: ' .. pipeline);
  RemoteMessage(NodeData[inid]["bip"], NodeData[inid]["bport"], pipeline);
end
function sync_ooo(inid, beg, fin)
  print ('SYNC OOO inid: ' .. inid .. ' beg: ' .. beg .. ' fin: ' .. fin);
  local cmd = Redisify('LUA', 'rexmit_ops', MyNodeId, beg, fin);
  RemoteMessage(NodeData[inid]["bip"], NodeData[inid]["bport"], cmd);
end

function mod_hw(nodeid, o_xactid, issync)
  local inid   = tonumber(nodeid);
  local xactid = tonumber(o_xactid);
  local hwname = getHWname(nodeid);
  local hw     = tonumber(redis("get", hwname));
  local fromb  = NodeData[inid]['isb'];
  if (hw == nil) then  -- CHERRY OP or SYNC
    local beg = giveInitialAutoInc(inid);
    if (beg == xactid) then redis("set", hwname, xactid);
    else                    sync_ooo(inid, beg, xactid);   end
  elseif (issync) then -- SYNC
      if (hw == xactid) then redis("set", hwname, xactid);
      else
        local beg;
        if (fromb) then beg = getNextBridgeAutoInc(hw);
        else            beg = getNextAutoInc(hw);       end
        sync_ooo(inid, beg, xactid);
      end
  else                 -- NORMAL OP
    if (xactid < hw) then return false; end -- REPEAT GLOBAL OP
    if (fromb) then
      if (hw == getPrevBridgeAutoInc(xactid)) then redis("set", hwname, xactid);
      else
        sync_ooo(inid, getNextBridgeAutoInc(hw), getPrevBridgeAutoInc(xactid));
      end
    else
      if (hw == getPrevAutoInc(xactid)) then redis("set", hwname, xactid);
      else           sync_ooo(inid, getNextAutoInc(hw), getPrevAutoInc(xactid));
      end
    end
  end
  return true;
end
function sync_hw(nodeid, xactid)
  return mod_hw(nodeid, xactid, true);
end
function update_hw(nodeid, xactid)
  return mod_hw(nodeid, xactid, false);
end
function update_bridge_hw(nodeid, bxactid)
  BridgeHW = bxactid;
  print ('BRIDGE: BridgeHW: ' .. BridgeHW);
  return mod_hw(nodeid, bxactid, false);
end


-- OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING
--TODO
local GlobalSyncedHW   = 0;
function trim_sync_Q(hw)
  if (GlobalSyncedHW == hw) then return; end
  redis("zremrangebyscore", 'Q_sync', "-inf", hw);
  GlobalSyncedHW = hw;
end
--TODO
function check_heartbeat()
  local nnodes = 0;
  local lw     = -1;
  for num, data in pairs(SyncedHW) do
    nnodes = nnodes +1;
    if     (lw == -1)  then lw = data;
    elseif (data < lw) then lw = data; end
  end
  if (nnodes ~= NumHBs) then return; end
  trim_sync_Q(lw);
  if (tonumber(SyncedHW[inid]) < tonumber(LastHB_SyncedHW[inid])) then
    rexmit_ooo(LastHB_SyncedHW[inid]); 
  end
  LastHB_SyncedHW[inid] = AutoInc['In_Xactid'];
end

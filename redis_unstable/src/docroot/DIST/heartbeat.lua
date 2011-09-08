
local MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

local MyHB_ID = redis("get", "alchemy_heartbeat");
if (MyHB_ID == nil) then MyHB_ID = 0; end

-- FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
function getGenerationName(nodeid) return 'GENERATION_' .. nodeid; end
function getHWname        (nodeid) return 'HIGHWATER_'  .. nodeid; end

local BridgeHW = 0;
function aiweq(inid) return '[' .. inid .. ']='; end -- HELPER

function GenerateHBID()
  return 'HB_ID=' .. MyHB_ID .. ';';
end
function GenerateBridgeHBCommand()-- this command will be remotely EVALed
  local b_gnr_cmd = 'B_GNR=' .. MyGeneration .. ';';
  local b_hw_cmd  = 'B_HW={' .. aiweq(MyNodeId) .. BridgeHW .. '};';
  return GenerateHBID() .. b_hw_cmd .. b_gnr_cmd;
end

function GenerateHBCommand() -- this command will be remotely EVALed
  local n_hw_cmd  = 'N_HW={';
  local n_gnr_cmd = 'N_GNR={';
  local i         = 1;
  for k, inid in pairs(PeerData) do
    if (inid == MyNodeId) then
      n_hw_cmd  = n_hw_cmd  .. aiweq(inid) .. AutoInc['Next_sync_XactId'];
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
  if (AmBridge) then -- HBs from BRIDGES append BRIDGE's HW & GNR
      n_hw_cmd  = n_hw_cmd  .. ',' .. aiweq(-1) .. BridgeHW;
      n_gnr_cmd = n_gnr_cmd .. ',' .. aiweq(-1) .. MyGeneration;
  end
  n_hw_cmd  = n_hw_cmd  .. '};'; n_gnr_cmd = n_gnr_cmd .. '};';
  return GenerateHBID() .. n_hw_cmd .. n_gnr_cmd;
end
function HeartBeat() -- lua_cron function, called every second
  if (PingSyncAllNodes() == false) then return; end -- wait until synced
  MyHB_ID = MyHB_ID + 1; redis("set", "alchemy_heartbeat", MyHB_ID);
  local cmd  = GenerateHBCommand();
  local msg  = Redisify('LUA', 'handle_heartbeat', MyNodeId, MyGeneration, cmd);
  print ('   HEARTBEAT: myid: ' .. MyNodeId .. ' cmd: ' .. cmd);
  redis("publish", 'sync', msg);
  if (AmBridge) then
    local myid = MyNodeId;
    cmd = cmd .. GenerateBridgeHBCommand();
    msg = Redisify('LUA', 'handle_bridge_heartbeat', myid, MyGeneration, cmd);
    print ('   BRIDGE_HEARTBEAT: myid: ' .. myid .. ' cmd: ' .. cmd);
    redis("publish", 'sync_bridge', msg);
  end
end

local SyncedHW = {}; local LastHB_SyncedHW = {}; local GenerationSet = {};
local LastHB   = {};

function handle_heartbeat(nodeid, generation, hb_eval_cmd)
  print ('handle_heartbeat: inid: ' .. nodeid .. ' hb_cmd: ' .. hb_eval_cmd)
  local inid = tonumber(nodeid);
  -- Lua eval - defines variables: [N_HW, N_GNR]
  assert(loadstring(hb_eval_cmd))()
  if (LastHB[inid] ~= nil and HB_ID <= LastHB[inid]) then return; end
  LastHB[inid] = tonumber(HB_ID);
  for num, data in pairs(N_HW) do
    local inum = tonumber(num);
    if (inum == MyNodeId) then
      print ('X to NODE: SyncedHW[' .. inid .. ']: ' .. data);
      SyncedHW[inid] = data;
      if (LastHB_SyncedHW[inid] == nil) then LastHB_SyncedHW[inid] = data; end
    end
  end
  for nid, gnr in pairs(N_GNR) do
    local i = tonumber(nid);
    if (i == inid) then
      if (GenerationSet[inid]) then
        local lgnr = redis("get", getGenerationName(inid));
        if (lgnr ~= nil) then
          if (tonumber(lgnr) ~= tonumber(gnr)) then
            resync_ping(inid, 'sync');
          end
        end
      end
      redis("set", getGenerationName(inid), gnr);
      GenerationSet[inid] = true;
    end
  end
  check_heartbeat()
end

function handle_bridge_heartbeat(nodeid, generation, hb_eval_cmd)
  print ('handle_bridge_heartbeat: inid: '  .. nodeid ..
                                ' hb_cmd: ' .. hb_eval_cmd)
  local inid = tonumber(nodeid);
  -- Lua eval - defines variables: [N_HW, N_GNR, B_HW, B_GNR]
  assert(loadstring(hb_eval_cmd))()
  for num, data in pairs(N_HW) do
    local inum = tonumber(num);
    print ('BRIDGE to BRIDGE: SyncedHW[' .. inum .. ']: ' .. data);
    SyncedHW[inum] = data;
    if (LastHB_SyncedHW[inum] == nil) then LastHB_SyncedHW[inum] = data; end
  end
end

function update_bridge_hw(bid, bxactid)
  BridgeHW = bxactid;
  print ('BRIDGE: BridgeHW: ' .. BridgeHW);
end

function update_remote_hw(nodeid, xactid)
  local qname  = 'sync';
  local inid   = tonumber(nodeid);
  local hwname = getHWname(nodeid);
  local hw     = tonumber(redis("get", hwname));
  local dbg    = hw; if (hw == nil) then dbg = "(nil)"; end
  print('update_remote_hw: nodeid: ' .. nodeid ..  ' xactid: ' .. xactid ..
                         ' HW: '     .. dbg);
  local ooo    = false;
  if     (hw == nil or hw == 0) then
    redis("set", hwname, xactid);
  else
    if (AmBridge) then
      if (hw == getPreviousBridgeAutoInc(xactid)) then
        redis("set", hwname, xactid);
      else
          ooo = true;
      end
    else
      if (hw == getPreviousAutoInc(xactid)) then
        redis("set", hwname, xactid);
      else
        ooo = true;
      end
    end
  end
  if (ooo) then
    print ('OOO');
    --recover_from_ooo()
  end
end

-- OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING
--TODO
local GlobalSyncedHW   = 0;
function trim_sync_Q(hw)
  if (GlobalSyncedHW == hw) then return; end
  redis("zremrangebyscore", 'Q_sync', "-inf", hw);
  GlobalSyncedHW = hw;
end
function handle_ooo(fromnode, hw, xactid)
  local ifromnode = tonumber(fromnode);
  --print ('handle_ooo: fromnode: ' .. fromnode .. ' hw: ' .. hw .. 
                    --' xactid: ' .. xactid);
  local beg      = tonumber(hw)     + 1;
  local fin      = tonumber(xactid) - 1;
  local msgs     = redis("zrangebyscore", "Q_sync", beg, fin);
  local pipeline = '';
  for k,v in pairs(msgs) do pipeline = pipeline .. v; end
  local data     = NodeData[ifromnode];
  RemoteMessage(data["ip"], data["port"], pipeline);
end
function natural_net_recovery(hw)
  for num, data in pairs(SyncedHW) do
    if (tonumber(data) ~= tonumber(hw)) then
      handle_ooo(num, data, (hw + 1));
    end
  end
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
    natural_net_recovery(LastHB_SyncedHW[inid]); 
  end
  LastHB_SyncedHW[inid] = AutoInc['Next_sync_XactId'];
end

--TODO
function recover_from_ooo()
 -- TODO handle multiple missing blocks
    local mabove = 'HW_' .. nodeid .. '_mabove';
    local mbelow = 'HW_' .. nodeid .. '_mbelow';
    local o_mav  = redis("get", mabove);
    if (o_mav ~= nil) then
      local mav    = tonumber(o_mav);
      if (tonumber(mav) == tonumber(getPreviousAutoInc(xactid))) then
        local o_mbv = redis("get", mbelow);
        if (o_mbv == nil) then print ('MBV ERROR'); return end
        local mbv = tonumber(mav);
        if (tonumber(xactid) == tonumber(getPreviousAutoInc(mbv))) then
          redis("del", mabove, mbelow); -- OOO done
        else
          redis("set", mabove, xactid); -- some more OOO left
        end
      end
    else
      local myid = MyNodeId;
      local cmd  = Redisify('LUA', 'handle_ooo', myid, hw, xactid, 0);
      local data = NodeData[inid];
      RemoteMessage(data["ip"], data["port"], cmd);
      redis("set", mabove, tostring(hw));
      redis("set", mbelow, xactid);
      redis("set", hwname, xactid); -- [mabove,mbelow] will catch OOO
    end
end

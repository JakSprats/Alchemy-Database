
-- FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS FUNCTIONS
function getHWname        (nodeid) return 'HIGHWATER_'  .. nodeid; end

local BridgeHW = 0; -- TODO -> array + persistent
function aiweq(inid) return '[' .. inid .. ']='; end -- HELPER FUNCTION

function GenerateBridgeHBCommand()-- this command will be remotely EVALed
  local b_hw_cmd  = 'B_HW={'  .. aiweq(MyNodeId) .. BridgeHW     .. '};';
  return b_hw_cmd;
end
function GenerateHBCommand() -- this command will be remotely EVALed
  local n_hw_cmd  = 'N_HW={';
  local i         = 1;
  for k, inid in pairs(PeerData) do
    if (inid == MyNodeId) then
      n_hw_cmd = n_hw_cmd  .. aiweq(inid) .. AutoInc['In_Xactid'];
    else
      local hw = redis("get", getHWname(inid));
      if (hw == nil) then hw = '0'; end
      n_hw_cmd = n_hw_cmd  .. aiweq(inid) .. hw;
    end
    if (i ~= #PeerData) then n_hw_cmd  = n_hw_cmd  .. ','; end
    i = i + 1;
  end
  n_hw_cmd  = n_hw_cmd  .. '};';
  return n_hw_cmd;
end
function HeartBeat() -- lua_cron function, called every second
  if (AmSlave) then InitServer(); return; end
  if (PingSyncAllNodes() == false) then return; end -- wait until synced
  local cmd  = GenerateHBCommand();
  local msg  = Redisify('LUA', 'handle_heartbeat', MyNodeId, cmd);
  print ('   HEARTBEAT: myid: ' .. MyNodeId .. ' cmd: ' .. cmd);
  publish('sync', msg);
  if (AmBridge) then
    local myid = MyNodeId;
    cmd = cmd .. GenerateBridgeHBCommand();
    msg = Redisify('LUA', 'handle_bridge_heartbeat', myid, cmd);
    print ('   BRIDGE_HEARTBEAT: myid: ' .. myid .. ' cmd: ' .. cmd);
    publish('sync_bridge', msg);
  end
end

local SyncedHW = {};
function handle_heartbeat(nodeid, hb_eval_cmd)
  print ('handle_heartbeat: inid: ' .. nodeid .. ' hb_cmd: ' .. hb_eval_cmd)
  local inid = tonumber(nodeid);
  assert(loadstring(hb_eval_cmd))() -- Lua eval - defines vars: [N_HW]
  for num, data in pairs(N_HW) do
    local inum = tonumber(num);
    if (inum == MyNodeId) then
      --print('X to NODE: SyncedHW[' .. inid .. ']: ' .. data);
      SyncedHW[inid] = tonumber(data);
    end
  end
  check_HB(inid, false);
end

local SyncedBridgeHW = {};
function handle_bridge_heartbeat(nodeid, hb_eval_cmd)
  local inid = tonumber(nodeid);
  print ('handle_bridge_heartbeat: inid: ' .. inid .. ' hb_cmd: ' ..hb_eval_cmd)
  assert(loadstring(hb_eval_cmd))() -- Lua eval - defines vars: [N_HW, B_HW]
  for num, data in pairs(B_HW) do
    local inum = tonumber(num);
    --print ('BRIDGE to BRIDGE: SyncedBridgeHW[' .. inum .. ']: ' .. data);
    SyncedBridgeHW[inum] = tonumber(data);
  end
  check_HB(inid, true);
end

-- NETWORK_DISCONNECT NETWORK_DISCONNECT NETWORK_DISCONNECT
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
function fetch_missing_ops(hw, inid, beg, fin)
  print('FETCH: inid: ' .. inid .. ' beg: ' .. beg ..  ' fin: ' .. fin);
  local cmd = Redisify('LUA', 'rexmit_ops', MyNodeId, beg, fin);
  RemoteMessage(NodeData[inid]["bip"], NodeData[inid]["bport"], cmd);
end

-- HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW HW
function mod_hw(nodeid, o_xactid, issync)
  local inid   = tonumber(nodeid);
  local xactid = tonumber(o_xactid);
  local hwname = getHWname(inid);
  local hw     = tonumber(redis("get", hwname));
  local fromb  = NodeData[inid]['isb'];
  if (issync) then        -- SYNC
      local beg;
      if (hw == nil) then -- CHERRY SYNC
        beg = giveInitialAutoInc(inid);
        if (beg == xactid) then redis("set", hwname, xactid); return true; end;
      else
        if (hw  == xactid) then redis("set", hwname, xactid); return true; end;
        if (NodeData[inid]['isb']) then beg = getNextBridgeAutoInc(hw);
        else                            beg = getNextAutoInc      (hw); end
      end
      fetch_missing_ops(hw, inid, beg, xactid); -- ONLY sync or resync fetches
  else                    -- NORMAL OP
    redis("set", hwname, xactid);
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

-- Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC Q_SYNC
local TrimHW = 0;
function trim_sync_Q(hw, fromb)
  if (TrimHW == hw) then return; end -- nothing changed
  local channel;
  if (fromb) then channel = 'Q_sync_bridge';
  else            channel = 'Q_sync';        end
  redis("zremrangebyscore", channel, "-inf", hw);
  TrimHW = hw;
end
function check_HB(inid, fromb)
  local syncedhw; local num_hbs;
  if (fromb)              then syncedhw = SyncedBridgeHW;
  else                         syncedhw = SyncedHW;       end
  if (AmBridge and fromb) then num_hbs  = NumHBBridges;
  else                         num_hbs  = NumHBPeers;     end
  local nnodes =  0;
  local lw     = -1;
  for num, data in pairs(syncedhw) do
    nnodes = nnodes +1;
    if     (lw == -1)  then lw = data;
    elseif (data < lw) then lw = data; end
  end
  if (nnodes ~= num_hbs) then return; end
  trim_sync_Q(lw, fromb);
end

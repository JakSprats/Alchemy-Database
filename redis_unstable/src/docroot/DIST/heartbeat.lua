
function getGenerationName(nodeid)
  return 'GENERATION_' .. nodeid;
end
function getHWname(nodeid, qname)
  return 'HW_' .. nodeid .. '_Q_' .. qname;
end

function GenerateHeartbeatCommand()
  local node_hw_cmd  = 'node_hw  = {'; -- this command will be remotely EVALed
  local node_gnr_cmd = 'node_gnr = {'; -- this command will be remotely EVALed
  for inid, data in pairs(NodeData) do
    if (inid == MyNodeId) then
      node_hw_cmd  = node_hw_cmd  .. AutoInc['Next_sync_TransactionId'];
      node_gnr_cmd = node_gnr_cmd .. MyGeneration;
    else
      local hw     = redis("get", getHWname(inid, 'sync'));
      if (hw == nil) then hw = '0'; end
      node_hw_cmd  = node_hw_cmd .. hw;
      local gnr    = redis("get", getGenerationName(inid));
      if (gnr == nil) then gnr = '0'; end
      node_gnr_cmd = node_gnr_cmd .. gnr;
    end
    if (inid ~= NumNodes) then
      node_hw_cmd  = node_hw_cmd  .. ',';
      node_gnr_cmd = node_gnr_cmd .. ',';
    end
  end
  node_hw_cmd  = node_hw_cmd  .. '};';
  node_gnr_cmd = node_gnr_cmd .. '};';
  local bridge_gnr_cmd; -- this command will be remotely EVALed
  if (MyNodeId == -1) then
    bridge_gnr_cmd = 'brdg_gnr = ' .. MyGeneration .. ';';
  else
    bridge_gnr_cmd = '';
  end
  return node_hw_cmd .. node_gnr_cmd .. bridge_gnr_cmd;
end
function HeartBeat() -- lua_cron function, called every second
  if (PingSyncAllNodes() == false) then return; end -- wait until synced
  local cmd = GenerateHeartbeatCommand();
  print ('HeartBeat: cmd: ' .. cmd);
  local msg = Redisify('LUA', 'handle_heartbeat', MyNodeId, MyGeneration, cmd);
  redis("publish", 'sync', msg);
end

-- OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING
local GlobalRemoteHW   = {}; GlobalRemoteHW['sync'] = 0;
function trim_Q(qname, hw)
  if (GlobalRemoteHW[qname] == hw) then return; end
  redis("zremrangebyscore", 'Q_' .. qname, "-inf", hw);
  GlobalRemoteHW[qname] = hw;
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
  local data     = GetNode(ifromnode);
  RemoteMessage(data["ip"], data["port"], pipeline);
end
local RemoteHW         = {}; local LastHB_HW        = {};
function natural_net_recovery(hw)
  for num, data in pairs(RemoteHW) do
    if (tonumber(data) ~= tonumber(hw)) then
      --print('natural_net_recovery: node: ' .. num .. ' nhw: ' .. data ..
                                   --' hw: ' .. hw);
      handle_ooo(num, data, (hw + 1));
    end
  end
end

local GenerationSet = {};
function handle_heartbeat(nodeid, generation, hb_eval_cmd)
  print ('handle_heartbeat: nodeid: ' .. nodeid .. ' hb_eval_cmd: ' .. hb_eval_cmd);
  assert(loadstring(hb_eval_cmd))() -- Lua's eval - "node_hw, node_gnr" defined
  for num, data in pairs(node_hw) do
    if (num == MyNodeId or 
        (MyNodeId == -1 and num == nodeid)) then
      RemoteHW[nodeid] = data;
      if (LastHB_HW[nodeid] == nil) then LastHB_HW[nodeid] = data; end
    end
  end
  for nid, gnr in pairs(node_gnr) do
    local inid = tonumber(nid);
    if (inid == tonumber(nodeid)) then
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
  local nnodes = 0;
  local lw     = -1;
  for num, data in pairs(RemoteHW) do
    nnodes = nnodes +1;
    if     (lw == -1)  then lw = data;
    elseif (data < lw) then lw = data; end
  end
  if (nnodes ~= (NumNodes - 1)) then return; end
  trim_Q('sync', lw);
  if (tonumber(RemoteHW[nodeid]) < tonumber(LastHB_HW[nodeid])) then
    natural_net_recovery(LastHB_HW[nodeid]); 
  end
  LastHB_HW[nodeid] = AutoInc['Next_sync_TransactionId'];
end

function update_remote_hw(qname, nodeid, xactid)
  local inid   = tonumber(nodeid);
  local hwname = getHWname(nodeid, qname)
  local hw     = tonumber(redis("get", hwname));
  local dbg = hw; if (hw == nil) then dbg = "(nil)"; end
  print('update_remote_hw: nodeid: ' .. nodeid ..  ' xactid: ' .. xactid ..
                         ' HW: '     .. dbg);
  if     (hw == nil) then
    redis("set", hwname, xactid);
  elseif (hw == getPreviousAutoInc(xactid)) then
    redis("set", hwname, xactid);
  else
    local mabove = 'HW_' .. nodeid .. '_mabove';
    local mbelow = 'HW_' .. nodeid .. '_mbelow';
    local mav    = tonumber(redis("get", mabove));
    if (mav ~= nil) then
      local mbv = tonumber(redis("get", mbelow));
      if (tonumber(mav) == tonumber(getPreviousAutoInc(xactid))) then
        if (tonumber(xactid) == tonumber(getPreviousAutoInc(mbv))) then
          redis("del", mabove, mbelow); -- OOO done
        else
          redis("set", mabove, xactid); -- some more OOO left
        end
      end
    else
      local cmd  = Redisify('LUA', 'handle_ooo', MyNodeId, hw, xactid);
      local data = GetNode(inid);
      RemoteMessage(data["ip"], data["port"], cmd);
      redis("set", mabove, tostring(hw));
      redis("set", mbelow, xactid);
      redis("set", hwname, xactid); -- [mabove,mbelow] will catch OOO
    end
  end
end

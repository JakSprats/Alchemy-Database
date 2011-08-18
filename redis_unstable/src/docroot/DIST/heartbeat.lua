
local MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

function getHWname(nodeid, qname)
  return 'HW_' .. nodeid .. '_Q_' .. qname;
end
function HeartBeat() -- lua_cron function, called every second
  if (RsubscribeAlchemySync() == false) then return; end -- wait until synced
  local hw_eval_cmd = 'hw = {'; -- this command will be remotely EVALed
  for num, data in pairs(NodeData) do
    if (num == MyNodeId) then
      hw_eval_cmd = hw_eval_cmd .. AutoInc['Next_sync_TransactionId'];
    else
      local hw = redis("get", getHWname(num, 'sync'));
      if (hw == nil) then hw = '0'; end
      hw_eval_cmd = hw_eval_cmd .. hw;
    end
    if (num ~= NumNodes) then hw_eval_cmd = hw_eval_cmd .. ','; end
  end
  hw_eval_cmd = hw_eval_cmd .. '};';
  --print ('HeartBeat: hw_eval_cmd: .. ' .. hw_eval_cmd);
  local msg = Redisify('LUA', 'handle_heartbeat', MyNodeId, MyGeneration,
                                                  hw_eval_cmd);
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
  RemoteMessage(NodeData[ifromnode]["ip"], NodeData[ifromnode]["port"],
                pipeline);
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
function handle_heartbeat(nodeid, generation, hw_eval_cmd)
  assert(loadstring(hw_eval_cmd))() -- Lua's eval - "hw" is defined
  for num, data in pairs(hw) do
    if (num == MyNodeId) then
      RemoteHW[nodeid] = data;
      if (LastHB_HW[nodeid] == nil) then LastHB_HW[nodeid] = data; end
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
  local inodeid = tonumber(nodeid);
  local hwname = getHWname(nodeid, qname)
  local hw     = tonumber(redis("get", hwname));
  local dbg = hw; if (hw == nil) then dbg = "(nil)"; end
  --print('update_remote_hw: nodeid: ' .. nodeid ..  ' xactid: ' .. xactid ..
                         --' HW: '     .. dbg);
  if     (hw == nil) then
    redis("set", hwname, xactid);
  elseif (hw == getPreviousAutoInc(xactid)) then
    redis("set", hwname, xactid);
  else
    local mabove = 'HW_' .. nodeid .. '_mabove';
    local mbelow = 'HW_' .. nodeid .. '_mbelow';
    local mav    = redis("get", mabove);
    if (mav ~= nil) then
      local mbv = redis("get", mbelow);
      if (tonumber(mav) == tonumber(getPreviousAutoInc(xactid))) then
        if (tonumber(xactid) == tonumber(getPreviousAutoInc(mbv))) then
          redis("del", mabove, mbelow); -- OOO done
        else
          redis("set", mabove, xactid); -- some more OOO left
        end
      end
    else
      local cmd = Redisify('LUA', 'handle_ooo', MyNodeId, hw, xactid);
      RemoteMessage(NodeData[inodeid]["ip"], NodeData[inodeid]["port"], cmd);
      redis("set", mabove, tostring(hw));
      redis("set", mbelow, xactid);
      redis("set", hwname, xactid); -- [mabove,mbelow] will catch OOO
    end
  end
end

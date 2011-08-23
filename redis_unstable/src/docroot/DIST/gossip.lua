
local AllSynced    = false;
local NumSynced    = 0;
local PipeFD       = {}; -- {pipeid -> fd}
local SyncedPipeFD = {}; -- {fd     -> nodeid}

local Pipe_id      = 1;
local StartSync    = os.time();

local NumNonResponses = {20, 5, 15, 35};
function simulate_non_responsive_node() --DEBUG simulate non responsive node
  if (NumNonResponses[MyNodeId] == 0) then return false; end
  NumNonResponses[MyNodeId] = NumNonResponses[MyNodeId] - 1;
  print ('SIMULATED NON RESPONSE: ' .. NumNonResponses[MyNodeId] .. ' left');
  return true;
end

function GetNode(inid)
  print ('GetNode: inid: ' .. inid);
  if (inid < 1) then return BridgeData;
  else               return NodeData[inid]; end
end

function sync_ping(nodeid, channel)  print('sync_ping to nodeid: ' .. nodeid .. ' channel: ' .. channel);
  local inid = tonumber(nodeid);
  local data = GetNode(inid)
  local cmd  = Redisify('LUA', 'sync_pong', MyNodeId, Pipe_id, channel);
  --print ('PING command: ' .. cmd);
  local fd   = RemotePipe(data['ip'], data['port'], cmd);
  if (fd == -1) then return; end
  print ('PING to nodeid: ' .. nodeid .. '  fd: ' .. fd .. ' port: ' .. data['port']);
  table.insert(PipeFD, Pipe_id, fd);
  Pipe_id    = Pipe_id + 1;
end
function sync_pong(other_node_id, pipeid, channel)
  print ('PONG: ON nodeid: ' .. MyNodeId .. ' to: ' .. other_node_id);
  local ncmd  = Redisify('LUA', 'sync_pong_handler', pipeid, MyNodeId, channel);
  --print ('ncmd: ' .. ncmd);
  local cmd   = Redisify('MESSAGE', channel, ncmd);
  --print ('PONG command: ' .. cmd);
  local inid  = tonumber(other_node_id);
  local odata = GetNode(inid);
  --if (simulate_non_responsive_node()) then return; end
  print ('RemoteMessage: port: ' .. odata['port']);
  RemoteMessage(odata['ip'], odata['port'], cmd);
end
function sync_pong_handler(o_pipeid, o_nodeid, o_channel)
  local inid      = tonumber(o_nodeid);
  local data      = GetNode(inid);
  if (data['synced'] == 0) then
    local pipeid  = tonumber(o_pipeid);
    local fd      = tostring(PipeFD[pipeid]);
    local channel = tostring(o_channel);
    print ('sync_pong_handler inid: ' .. inid .. ' fd: ' .. fd);
    SubscribeFD(fd, channel);       -- redis("publish", channel) -> fd
    SyncedPipeFD[tonumber(fd)] = inid; -- means keep this one around
    data['synced']             = 1;
    NumSynced                  = NumSynced + 1;
    print ('NumSynced: ' .. NumSynced);
    if (NumSynced == NumToSync) then
      if (AllSynced == false) then
        AllSynced = true;
        print ('AllSynced in ' .. (os.time() - StartSync) .. ' secs');
        for k,v in pairs(PipeFD) do -- remove all PipeFD not in SyncedPipeFD
          local fd = tonumber(v);
          if (SyncedPipeFD[fd] == nil) then CloseFD(fd); end
        end
        PipeFD = {};
      end
    end
  end
end

function resync_ping(inid, o_channel)
  print('resync_ping: inid: ' .. inid);
  StartSync      = os.time()
  local data     = GetNode(inid);
  data['synced'] = 0;
  NumSynced      = NumSynced - 1;
  AllSynced      = false;
  for k,v in pairs(SyncedPipeFD) do
    if (tonumber(v) == inid) then
      local fd      = tostring(k);
      local channel = tostring(o_channel);
      print ('UnsubscribeFD: ' .. fd .. ' channel: ' .. channel);
      UnsubscribeFD(fd, channel);
      table.remove(SyncedPipeFD, k);
    end
  end
  sync_ping(inid, 'sync'); -- ASYNC
end

function PingSyncAllNodes() --print ('PingSyncAllNodes');
  if (AllSynced) then return true; end
  for k, inid in pairs(PeerData) do
    if (inid ~= MyNodeId) then
      local data = GetNode(inid);
      if (data['synced'] == 0) then
        local channel;
        if (MyNodeId == -1) then channel = 'sync_' .. GetNetwork(inid);
        else                     channel = 'sync';                      end
        sync_ping(inid, channel);
      end
    end
  end
  if (NumSynced == NumToSync) then return true;
  else                            return false; end
end

-- defines SimulateNetworkPartition
--dofile "./docroot/DIST/debug.lua";


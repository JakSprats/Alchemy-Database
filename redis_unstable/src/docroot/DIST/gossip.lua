
local AllSynced    = false;
local NumSynced    = 1; -- NOTE: already synce w/ myself
local PipeFD       = {};
local SyncedPipeFD = {};
local Pipe_id      = 1;
local StartSync    = os.time();

local NumNonResponses = {20, 5, 15, 35};
function simulate_non_responsive_node() --DEBUG simulate non responsive node
  if (NumNonResponses[MyNodeId] == 0) then return false; end
  NumNonResponses[MyNodeId] = NumNonResponses[MyNodeId] - 1;
  print ('SIMULATED NON RESPONSE: ' .. NumNonResponses[MyNodeId] .. ' left');
  return true;
end

function sync_ping(nodeid, channel) -- print('sync_ping to nodeid: ' .. nodeid);
  local data   = NodeData[nodeid];
  local cmd    = Redisify('LUA', 'sync_pong', MyNodeId, Pipe_id, channel);
  --print ('PING command: ' .. cmd);
  local fd = RemotePipe(data['ip'], data['port'], cmd);
  if (fd == -1) then return; end
  print ('PING to nodeid: ' .. nodeid .. '  fd: ' .. fd);
  table.insert(PipeFD, Pipe_id, fd);
  Pipe_id      = Pipe_id + 1;
end
function sync_pong(other_node_id, pipeid, channel)
  print ('PONG: ON nodeid: ' .. MyNodeId .. ' to: ' .. other_node_id);
  local ncmd  = Redisify('LUA', 'sync_pong_handler', pipeid, MyNodeId, channel);
  --print ('ncmd: ' .. ncmd);
  local cmd   = Redisify('MESSAGE', 'sync', ncmd);
  --print ('PONG command: ' .. cmd);
  local nodeid = tonumber(other_node_id);
  local odata = NodeData[nodeid];
  --if (simulate_non_responsive_node()) then return; end
  RemoteMessage(odata['ip'], odata['port'], cmd);
end
function sync_pong_handler(o_pipeid, o_nodeid, o_channel)
  local nodeid    = tonumber(o_nodeid);
  local data      = NodeData[nodeid];
  if (data['synced'] == 0) then
    local pipeid  = tonumber(o_pipeid);
    local fd      = tostring(PipeFD[pipeid]);
    local channel = tostring(o_channel);
    print ('sync_pong_handler nodeid: ' .. nodeid .. ' pipeid: ' .. pipeid);
    SubscribeFD(fd, channel);       -- redis("publish", channel) -> fd
    SyncedPipeFD[tonumber(fd)] = 1; -- means keep this one around
    data['synced']             = 1;
    NumSynced                  = NumSynced + 1;
    print ('NumSynced: ' .. NumSynced);
    if (NumSynced == NumNodes) then
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

function RsubscribeAlchemySync() --print ('RsubscribeAlchemySync');
  if (AllSynced) then return true; end
  for nodeid, data in pairs(NodeData) do
    if (nodeid ~= MyNodeId and data['synced'] == 0) then
      sync_ping(nodeid, 'sync'); -- ASYNC
    end
  end
  if (NumSynced == NumNodes) then return true;
  else                            return false; end
end

-- defines SimulateNetworkPartition
--dofile "./docroot/DIST/debug.lua";


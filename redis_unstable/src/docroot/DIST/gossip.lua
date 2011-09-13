
  -- GOSSIP GLOBALS
AllSynced       = false;               -- USED in global_promote_slave()
NumSynced       = 0;                   -- USED in global_promote_slave()
SyncedPipeFD    = {}; -- {fd     -> inid} USED in publication.lua:publish()
local PipeFD    = {}; -- {pipeid -> fd}
local StartSync = os.time();

local Pipe_id   = 1; -- used as incrementing counter

function sync_ping(nodeid, channel, sync_xactid)
  print('sync_ping to nodeid: '      .. nodeid .. ' channel: ' .. channel ..
                    ' sync_xactid: ' .. sync_xactid);
  local inid = tonumber(nodeid);
  local myid = MyNodeId;
  local cmd  = Redisify('LUA', 'sync_pong',
                                myid, Pipe_id, channel, sync_xactid);
  --print ('PING command: ' .. cmd);
  local fd;
  if (AmBridge and NodeData[inid]['isb']) then
    fd = RemotePipe(NodeData[inid]['bip'], NodeData[inid]['bport'], cmd);
  else
    fd = RemotePipe(NodeData[inid]['fip'], NodeData[inid]['fport'], cmd);
  end
  if (fd == -1) then return; end
  print ('PING to nodeid: ' .. nodeid .. ' fd: ' .. fd);
  PipeFD[Pipe_id] = fd;
  Pipe_id         = Pipe_id + 1;
end
function sync_pong(other_node_id, pipeid, channel, sync_xactid)
  local myid  = MyNodeId;
  print ('PONG: ON nodeid: ' .. myid .. ' to: '.. other_node_id ..
                 ' sync_xactid: ' .. sync_xactid);
  sync_hw(other_node_id, sync_xactid);
  local ncmd  = Redisify('LUA', 'sync_pong_handler',
                                pipeid, myid, channel);
  --print ('ncmd: ' .. ncmd);
  local cmd   = Redisify('MESSAGE', channel, ncmd);
  --print ('PONG command: ' .. cmd);
  local inid  = tonumber(other_node_id);
  RemoteMessage(NodeData[inid]['bip'], NodeData[inid]['bport'], cmd);
end
function sync_pong_handler(o_pipeid, o_nodeid, o_channel)
  local inid      = tonumber(o_nodeid);
  if (NodeData[inid]['synced'] == false) then
    local pipeid  = tonumber(o_pipeid);
    local fd      = tostring(PipeFD[pipeid]);
    local channel = tostring(o_channel);
    print ('sync_pong_handler inid: ' .. inid .. ' fd: ' .. fd);
    SubscribeFD(fd, channel);          -- redis("publish", channel) -> fd
    SyncedPipeFD[tonumber(fd)] = inid; -- means keep this one around
    NodeData[inid]['synced']   = true;
    NumSynced                  = NumSynced + 1;
    print ('NumSynced: ' .. NumSynced);
    if (NumSynced == NumPeers) then
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

function resync_ping(inid, o_channel) -- Narks node as down for PingSyncAllNodes
  if (NodeData[inid]['synced'] == false) then return; end
  print('resync_ping: inid: ' .. inid);
  StartSync                = os.time()
  NodeData[inid]['synced'] = false;
  NumSynced                = NumSynced - 1;
  AllSynced                = false;
  local channel            = tostring(o_channel);
  for k,v in pairs(SyncedPipeFD) do
    if (tonumber(v) == inid) then
      local fd      = tostring(k);
      print ('UnsubscribeFD: ' .. fd .. ' channel: ' .. channel);
      UnsubscribeFD(fd, channel);
      table.remove(SyncedPipeFD, k);
    end
  end
  local sync_xactid;
  if (AmBridge) then sync_xactid = 0;
  else               sync_xactid = AutoInc['In_Xactid']; end
end

function PingSyncAllNodes()
  if (AllSynced) then return true; end
  local sync_xactid;
  if (AmBridge) then sync_xactid = AutoInc['Out_Xactid'];
  else               sync_xactid = AutoInc['In_Xactid'];  end
  for k, inid in pairs(PeerData) do -- First Sync w/ Peers
    if (inid ~= MyNodeId) then
      if (NodeData[inid]['synced'] == false) then
        sync_ping(inid, 'sync', sync_xactid);
      end
    end
  end
  if (AmBridge) then                -- Second if Bridge, sync w/ BridgePeers
    sync_xactid = AutoInc['In_Xactid'];
    for k, inid in pairs(BridgeData) do
      if (inid ~= MyNodeId) then
        if (NodeData[inid]['synced'] == false) then
          sync_ping(inid, 'sync_bridge', sync_xactid);
        end
      end
    end
  end
  if (NumSynced > 1) then return true;
  else                    return false; end
end

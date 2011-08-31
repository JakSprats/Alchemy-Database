
local AllSynced    = false;
local NumSynced    = 0;
local PipeFD       = {}; -- {pipeid -> fd}
local SyncedPipeFD = {}; -- {fd     -> nodeid}

local Pipe_id      = 1;
local StartSync    = os.time();

function GetNode(id, o_isb)
  local isb = tonumber(o_isb);
  if     (id < 1)   then return BridgeData[BridgeId];
  elseif (isb == 1) then return BridgeData[id];
  else                   return NodeData[id];   end
end
function GetMyId(o_isb)
  local isb = tonumber(o_isb);
  if (isb == 1) then return MyBridgeId;
  else               return MyNodeId; end
end

function sync_ping(nodeid, channel, isb, sync_xactid)
  print('sync_ping to nodeid: ' .. nodeid .. ' channel: '     .. channel ..
                    ' isb: '    .. isb    .. ' sync_xactid: ' .. sync_xactid);
  local inid = tonumber(nodeid);
  local data = GetNode(inid, isb)
  local myid = GetMyId(isb);
  local cmd  = Redisify('LUA', 'sync_pong',
                                myid, Pipe_id, channel, isb, sync_xactid);
  --print ('PING command: ' .. cmd);
  local fd   = RemotePipe(data['ip'], data['port'], cmd);
  if (fd == -1) then return; end
  print ('PING to nodeid: ' .. nodeid .. '  fd: ' .. fd ..
                ' port: '   .. data['port']);
  table.insert(PipeFD, Pipe_id, fd);
  Pipe_id    = Pipe_id + 1;
end
function sync_pong(other_node_id, pipeid, channel, isb, sync_xactid)
  local myid  = GetMyId(isb);
  print ('PONG: ON nodeid: ' .. myid .. ' to: '          .. other_node_id ..
                 ' isb: '    .. isb  .. ' sync_xactid: ' .. sync_xactid);
  --TODO: this is a 1off ERROR - this is not the NEXT msg
  update_remote_hw(other_node_id, sync_xactid);
  local ncmd  = Redisify('LUA', 'sync_pong_handler',
                                pipeid, myid, channel, isb);
  --print ('ncmd: ' .. ncmd);
  local cmd   = Redisify('MESSAGE', channel, ncmd);
  --print ('PONG command: ' .. cmd);
  local inid  = tonumber(other_node_id);
  local odata = GetNode(inid, isb);
  print ('RemoteMessage: port: ' .. odata['port']);
  RemoteMessage(odata['ip'], odata['port'], cmd);
end
function sync_pong_handler(o_pipeid, o_nodeid, o_channel, isb)
  local inid      = tonumber(o_nodeid);
  local data      = GetNode(inid, isb);
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

function resync_ping(inid, o_channel, isb)
  print('resync_ping: inid: ' .. inid .. ' isb: ' .. isb);
  StartSync      = os.time()
  local data     = GetNode(inid, isb);
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
  local sync_xactid;
  if (MyBridgeId ~= -1) then sync_xactid = 0;
  else                       sync_xactid = AutoInc['Next_sync_XactId']; end
  sync_ping(inid, 'sync', isb, sync_xactid);
end

function PingSyncAllNodes()
  if (AllSynced) then return true; end
  local sync_xactid;
  if (MyBridgeId ~= -1) then sync_xactid = 0;
  else                       sync_xactid = AutoInc['Next_sync_XactId']; end
  for k, inid in pairs(PeerData) do -- First Sync w/ Peers
    if (inid ~= MyNodeId) then
      local data = GetNode(inid, 0);
      if (data['synced'] == 0) then
        sync_ping(inid, 'sync', 0, sync_xactid);
      end
    end
  end
  if (MyBridgeId ~= -1) then       -- Second if Bridge, sync w/ BridgePeers
    for ibid, data in pairs(BridgeData) do
      if (ibid ~= MyBridgeId) then
        if (data['synced'] == 0) then
          sync_ping(ibid, 'sync_bridge', 1, sync_xactid);
        end
      end
    end
  end
  if (NumSynced == NumToSync) then return true;
  else                             return false; end
end

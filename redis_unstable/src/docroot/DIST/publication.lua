
function local_publish(channel, pmsg) -- Assumes no failure (no Qing)
  redis("publish", channel, pmsg);
end

function checkFDsAgainstSyncedPipeFDs(fds, channel, fromb)
  local copySPFD = {};
  for k,v in pairs(SyncedPipeFD) do copySPFD[k] = v; end -- DEEP COPY
  if (fds ~= nil) then
    for k, fd in ipairs(fds) do
      if (copySPFD[fd] ~= nil) then copySPFD[fd] = nil; end -- ERASE Synced FDs
    end
  end
  for fd, inid in pairs(copySPFD) do -- Still in copySPFD -> NOT SYNCED
    if (fromb) then
      if (NodeData[inid]["isb"])          then resync_ping(inid, channel); end
    else
      if (NodeData[inid]["isb"] == false) then resync_ping(inid, channel); end
    end
  end
end

function publish(channel, pmsg) -- May fail, if so failing node NOT synced
  redis("publish", channel, pmsg);
  local fds = GetFDForChannel(channel)
  if (channel == 'sync') then
    checkFDsAgainstSyncedPipeFDs(fds, channel, false);
  elseif (channel == 'sync_bridge') then
    checkFDsAgainstSyncedPipeFDs(fds, channel, true);
  end
end

function publish_queue_sync(fname, ...)
  local channel = 'sync';
  local xactid  = IncrementAutoInc('In_Xactid');
  local pmsg    = Redisify('LUA', fname, MyNodeId, xactid, ...);
  --print ('publish_queue_sync: pmsg: ' .. pmsg);
  publish(channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end

function call_sync(func, fname, ...)
  local ret = func(0, 0, ...); -- LOCALLY: [nodeid, xactid] -> [0.0]
  publish_queue_sync(fname, ...);
  return ret;
end

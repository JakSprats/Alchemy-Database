
function publish_queue_sql(tbl, sqlbeg, sqlend)
  local channel = 'sql';
  local xactid  = IncrementAutoInc('Next_' .. channel .. '_' .. 
                                              tbl     .. '_XactId');
  local pmsg    = sqlbeg .. xactid .. sqlend;
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end

function publish_queue_sync(fname, ...)
  local channel = 'sync';
  local xactid  = IncrementAutoInc('In_Xactid');
  local pmsg    = Redisify('LUA', fname, MyNodeId, xactid, ...);
  --print ('publish_queue_sync: pmsg: ' .. pmsg);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end

function call_sync(func, fname, ...)
  local ret = func(0, 0, ...); -- LOCALLY: [nodeid, xactid] -> 0
  publish_queue_sync(fname, ...);
  return ret;
end


io.stdout:setvbuf("no"); -- flush stdout

-- INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES
dofile "includes.lua";

function GetNetwork(inid)
  print ('GetNetwork: inid: ' .. inid);
  assert(NetworkData[inid] ~= nil);
  return NetworkData[inid];
end
function GetForwardNetwork(inid)
  print ('GetForwardNetwork: inid: ' .. inid);
  assert(ForwardNetworkData[inid] ~= nil);
  return ForwardNetworkData[inid];
end

function queue_global_op(nodeid, xactid, func, name, ...)
  local inid    = tonumber(nodeid);
  local channel = 'sync_' .. GetForwardNetwork(inid);
  local pmsg    = Redisify('LUA', name, MyNodeId, xactid, ...);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end

global_register = function(nodeid, xactid, my_userid, username)
  print ('global_register: my_userid: ' .. my_userid);
  update_remote_hw('sync', nodeid, xactid);
  queue_global_op(nodeid, xactid, global_register, 'global_register',
                                                    my_userid, username);
end

global_logout = function(nodeid, xactid, my_userid)
  update_remote_hw('sync', nodeid, xactid);
  queue_global_op(nodeid, xactid, global_logout, 'global_logout', my_userid);
end

global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  print ('global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg);
  update_remote_hw('sync', nodeid, xactid);
  queue_global_op(nodeid, xactid, global_post, 'global_post',
                                                my_userid, postid, ts, msg);
end

global_follow = function(nodeid, xactid, my_userid, userid, follow)
  print ('global_follow: my_userid: ' .. my_userid .. ' userid: ' .. userid ..
         ' follow: ' .. follow);
  update_remote_hw('sync', nodeid, xactid);
  queue_global_op(nodeid, xactid, global_follow, 'global_follow',
                                                  my_userid, userid, follow);
end

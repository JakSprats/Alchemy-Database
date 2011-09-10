
AutoIncRange = 20; -- TODO testing OVERRIDE

io.stdout:setvbuf("no"); -- flush stdout

-- INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES
dofile "../includes.lua";

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
InitBridgeAutoInc('In_Xactid');
print ('In_Xactid: ' .. AutoInc['In_Xactid']);
InitBridgeAutoInc('Out_Xactid');
print ('Out_Xactid: ' .. AutoInc['Out_Xactid']);

-- FORWARDERS FORWARDERS FORWARDERS FORWARDERS FORWARDERS FORWARDERS
function bridge_global_caller(nodeid, fname, orig_bxactid, ...)
  local nbxactid = IncrementBridgeAutoInc('Out_Xactid'); -- BRIDGE REORDER (OUT)
  print ('bridge_global_caller: nodeid: '  .. nodeid .. ' fname: ' .. fname ..
               ' orig_bxactid: ' .. orig_bxactid .. ' nbxactid: ' .. nbxactid);
  update_bridge_hw(nodeid, orig_bxactid);
  local channel = 'sync';
  local pmsg    = Redisify('LUA', fname, MyNodeId, nbxactid, ...);
  --print ('bridge_global_caller: pmsg: ' .. pmsg);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, nbxactid, pmsg);
end

function queue_global_op(nodeid, xactid, fname, ...)
  update_hw(nodeid, xactid);
  local bxactid = IncrementBridgeAutoInc('In_Xactid'); -- BRIDGE REORDER (IN)
  local channel = 'sync_bridge';
  print ('B: Q_global_op: bxactid: ' .. bxactid ..  ' -> xactid: '  .. xactid);
  local pmsg    = Redisify('LUA', 'bridge_global_caller',
                                  MyNodeId, fname, bxactid, ...);
  --print ('pmsg: ' .. pmsg);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, bxactid, pmsg);
end

-- SKELETONS SKELETONS SKELETONS SKELETONS SKELETONS SKELETONS SKELETONS

-- TODO these all do the same thing
--      1.) Alchemy config option: bridge_mode yes
--      2.) any "MESSAGE -> LUA global_*" call will just call: queue_global_op
global_register = function(nodeid, xactid, my_userid, username)
  print ('B: global_register: my_userid: ' .. my_userid ..
                            ' xactid: '    .. xactid);
  queue_global_op(nodeid, xactid, 'global_register', my_userid, username);
end

global_logout = function(nodeid, xactid, my_userid)
  queue_global_op(nodeid, xactid, 'global_logout', my_userid);
end

global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  print ('B: global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg .. ' xactid: ' .. xactid);
  queue_global_op(nodeid, xactid, 'global_post', my_userid, postid, ts, msg);
end

global_follow = function(nodeid, xactid, my_userid, userid, follow)
  print ('B: global_follow: my_userid: ' .. my_userid ..
                          ' userid: '    .. userid ..  ' follow: ' .. follow);
  queue_global_op(nodeid, xactid, 'global_follow', my_userid, userid, follow);
end

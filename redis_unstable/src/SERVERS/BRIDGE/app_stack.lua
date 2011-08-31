
AutoIncRange = 20; -- TODO testing OVERRIDE

io.stdout:setvbuf("no"); -- flush stdout

-- INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES
dofile "../includes.lua";

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
InitBridgeAutoInc('Next_sync_XactId');
print ('Next_sync_XactId: ' .. AutoInc['Next_sync_XactId']);


function bridge_global_caller(bid, fname, bxactid, ...) -- NOTE: NO reordering
  print ('XXXXXXXXXXXXXXXX: bridge_global_caller: bid: '   .. bid .. 
                                                ' fname: ' .. fname ..
                                                ' bxactid: ' .. bxactid);
  update_bridge_hw(bid, bxactid);
  local channel = 'sync';
  local pmsg    = Redisify('LUA', fname, -1, bxactid, ...);
  --print ('bridge_global_caller: pmsg: ' .. pmsg);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end

function queue_global_op(xactid, fname, ...)
  --TODO handle OOO PEER xactids
  local bxactid = IncrementBridgeAutoInc('Next_sync_XactId'); -- BRIDGE REORDER
  local channel = 'sync_bridge';
  print ('B: queue_global_op: bxactid: ' .. bxactid ..
                         ' -> xactid: '  .. xactid);
  local pmsg    = Redisify('LUA', 'bridge_global_caller',
                                  MyBridgeId, fname, bxactid, ...);
  --print ('pmsg: ' .. pmsg);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, bxactid, pmsg);
end

-- TODO these all do the same thing
--      1.) Alchemy config option: bridge_mode yes
--      2.) any "MESSAGE -> LUA global_*" call will just call:
--                                    [update_bridge_remote_hw, queue_global_op]
global_register = function(nodeid, xactid, my_userid, username)
  print ('B: global_register: my_userid: ' .. my_userid);
  queue_global_op(xactid, 'global_register', my_userid, username);
end

global_logout = function(nodeid, xactid, my_userid)
  queue_global_op(xactid, 'global_logout', my_userid);
end

global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  print ('B: global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg);
  queue_global_op(xactid, 'global_post', my_userid, postid, ts, msg);
end

global_follow = function(nodeid, xactid, my_userid, userid, follow)
  print ('B: global_follow: my_userid: ' .. my_userid ..
                          ' userid: '    .. userid ..  ' follow: ' .. follow);
  queue_global_op(xactid, 'global_follow', my_userid, userid, follow);
end

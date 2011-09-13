
AutoIncRange = 20; -- TODO testing OVERRIDE

io.stdout:setvbuf("no"); -- flush stdout

-- INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES
dofile "./docroot/DIST/includes.lua";

-- INIT_SERVER INIT_SERVER INIT_SERVER INIT_SERVER INIT_SERVER INIT_SERVER

-- NOTE ALL GLOBAL VARIABLES MUST BE DECLARED HERE -> to exist on slaves
local MasterConnection = false;
function InitServerState()
  -- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
  -- one GLOBAL AutoInc for "sync"
  InitAutoInc('In_Xactid');
  print ('In_Xactid: '); dump(AutoInc['In_Xactid']);

  -- USERLAND_GLOBALS USERLAND_GLOBALS USERLAND_GLOBALS USERLAND_GLOBALS
  InitAutoInc('NextUserId');
  InitAutoInc('NextPostId');

  if (AmSlave) then
    CheckSlaveLuaFunctions(); CheckSlaveToMasterConnection();
  end
end
InitServerState();

-- INIT_PER_REQUEST INIT_PER_REQUEST INIT_PER_REQUEST INIT_PER_REQUEST
function InitRequest(rw) -- inits global vars included via "includes.lua"
  initPerRequestIsLoggedIn();
  initPerRequestIsDeflatable();
  initPerRequestInlineCache();
  HandleRW(rw);
end

-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
load_image('docroot/img/logo.png',    'logo.png');
load_image('docroot/img/sfondo.png',  'sfondo.png');
load_image('docroot/img/favicon.ico', 'favicon.ico');
load_text( 'docroot/css/style.css',   'css/style.css');
load_text( 'docroot/js/helper.js',    'helper.js');

-- SHARED_FUNCS SHARED_FUNCS SHARED_FUNCS SHARED_FUNCS SHARED_FUNCS
function commit_follow(my_userid, userid, follow)
  local f = tonumber(follow);
  if (f == 1) then
    redis("sadd", 'uid:' .. my_userid .. ':following', userid);
    redis("sadd", 'uid:' .. userid    .. ':followers', my_userid);
  else 
    redis("srem", 'uid:' .. my_userid .. ':following', userid);
    redis("srem", 'uid:' .. userid    .. ':followers', my_userid);
  end
end

-- LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS
function local_register(my_userid, username, password)
  redis("set",  'uid:'      .. my_userid   .. ':password', password);
  -- keep a global list of local users (used later for MIGRATEs)
  redis("lpush", "local_user_id",                          my_userid);
end

function local_post(my_userid, postid, msg, ts)
  redis("zadd", 'uid:' .. my_userid .. ':posts',   ts, postid); -- U follow U
  redis("zadd", 'uid:' .. my_userid .. ':myposts', ts, postid); -- for /profile
end

function local_follow(my_userid, userid, follow)
  commit_follow(my_userid, userid, follow);
end

-- SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS
function perform_auth_change(my_userid, oldauthsecret, newauthsecret)
  redis("set",    'uid:'  .. my_userid .. ':auth',     newauthsecret);
  redis("set",    'auth:' .. newauthsecret,            my_userid);
  if (oldauthsecret~= nil) then redis("delete", 'auth:' .. oldauthsecret); end
end

-- NOTE: register & logout can be OOO, shared vars MUST be stateless
global_register = function(nodeid, xactid, my_userid, username)
  print ('global_register: my_userid: ' .. my_userid ..
                         ' username: '  .. username .. ' xactid: ' .. xactid);
  -- do one-time SET's - this data is needed
  if (xactid ~= 0 and update_hw(nodeid, xactid) == false) then return; end
  redis("set",  'username:' .. username  .. ':id',       my_userid);
  redis("set",  'uid:'      .. my_userid .. ':username', username);
  redis("sadd", "global:users",                          my_userid);
  local oldauthsecret = get_sha1_variable('nlogouts', my_userid);
  if (oldauthsecret == nil) then
    local newauthsecret = init_sha1_variable('nlogouts', my_userid);
    perform_auth_change(my_userid, nil, newauthsecret);
    return newauthsecret;
  else
    return 0; -- handle OOO logout
  end
end

global_logout = function(nodeid, xactid, my_userid)
  if (xactid ~= 0 and update_hw(nodeid, xactid) == false) then return; end
  local oldauthsecret = get_sha1_variable('nlogouts', my_userid);
  -- combinatorial INCR operation governs flow
  local newauthsecret = incr_sha1_variable('nlogouts', my_userid);
  perform_auth_change(my_userid, oldauthsecret, newauthsecret);
  initPerRequestIsLoggedIn(); -- log out for internal redirects
end

local GlobalPostIterThreshold = 1000;
local GlobalPostIterInterval  = 1000;
-- GlobalPostIterThreshold = 2; -- DEBUG VALUES
-- GlobalPostIterInterval  = 2;
function iter_global_post(o_tot, o_interval, o_progress,
                          my_userid, postid, ts, msg)
  local tot       = tonumber(o_tot);
  local interval  = tonumber(o_interval);
  local progress  = tonumber(o_progress);
  --print ('iter_global_post: tot: ' .. tot .. ' progress: ' .. progress);
  local post      = my_userid .. '|' .. ts .. '|' .. msg;
  redis("set", 'post:' .. postid, post);
  local followers = redis("smembers", 'uid:' .. my_userid .. ':followers');
  local count     = 0;
  local max       = progress + interval;
  for k,v in pairs(followers) do
    if (count >= progress and count < max) then
      redis("zadd", 'uid:' .. v .. ':posts', ts, postid);
    end
    count = count + 1;
  end
  progress = max;
  if (progress >= tot) then return; end
  local pmsg = Redisify('LUA', 'iter_global_post',
                        tot, interval, progress, my_userid, postid, ts, msg);
  local_publish("echo", pmsg);
end

global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  print ('global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg .. ' xactid: ' .. xactid);
  if (xactid ~= 0 and update_hw(nodeid, xactid) == false) then return; end
  local post      = my_userid .. '|' .. ts .. '|' .. msg;
  redis("set", 'post:' .. postid, post);
  local followers = redis("smembers", 'uid:' .. my_userid .. ':followers');
  if (#followers > GlobalPostIterThreshold) then
    iter_global_post(#followers, GlobalPostIterInterval, 0,
                     my_userid, postid, ts, msg);
  else
    for k,v in pairs(followers) do
      redis("zadd", 'uid:' .. v .. ':posts', ts, postid);
    end
  end
  -- Push post to timeline
  redis("zadd",            "global:timeline", ts, postid);
end

global_follow = function(nodeid, xactid, my_userid, userid, follow)
  print ('global_follow: my_userid: ' .. my_userid .. ' userid: ' .. userid ..
         ' follow: ' .. follow);
  if (xactid ~= 0 and update_hw(nodeid, xactid) == false) then return; end
  if (UserNode(userid) == MyNodeId) then -- only for users ON THIS SHARD
    commit_follow(my_userid, userid, follow);
  end
end

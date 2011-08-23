
io.stdout:setvbuf("no"); -- flush stdout

-- CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT
dofile "./docroot/DIST/.instance_settings.lua";

-- INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES INLCUDES
dofile "./docroot/DIST/includes.lua";

-- INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT INIT
function InitRequest() -- inits global vars included via "includes.lua"
  initPerRequestIsLoggedIn();
  initPerRequestIsDeflatable();
  initPerRequestInlineCache();
end

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
-- one PK AutoInc for EACH table
InitAutoInc('Next_sql_users_TransactionId');
print ('Next_sql_users_TransactionId: ' .. AutoInc['Next_sql_users_TransactionId']);
InitAutoInc('Next_sql_posts_TransactionId');
print ('Next_sql_posts_TransactionId: ' .. AutoInc['Next_sql_posts_TransactionId']);
InitAutoInc('Next_sql_follows_TransactionId');
print ('Next_sql_follows_TransactionId: ' .. AutoInc['Next_sql_follows_TransactionId']);
-- one GLOBAL AutoInc for "sync"
InitAutoInc('Next_sync_TransactionId');
print ('Next_sync_TransactionId: ' .. AutoInc['Next_sync_TransactionId']);

-- USERLAND_GLOBALS USERLAND_GLOBALS USERLAND_GLOBALS USERLAND_GLOBALS
InitAutoInc('NextUserId');
print ('NextUserId: ' .. AutoInc['NextUserId']);
InitAutoInc('NextPostId');
print ('NextPostId: ' .. AutoInc['NextPostId']);

-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
load_image('docroot/img/logo.png',    'logo.png');
load_image('docroot/img/sfondo.png',  'sfondo.png');
load_image('docroot/img/favicon.ico', 'favicon.ico');
load_text( 'docroot/css/style.css',   'css/style.css');
load_text( 'docroot/js/helper.js',    'helper.js');

-- PUBLISH_REPLICATION PUBLISH_REPLICATION PUBLISH_REPLICATION
function publish_queue_sql(tbl, sqlbeg, sqlend)
  local channel = 'sql';
  local xactid  = IncrementAutoInc('Next_' .. channel .. '_' .. 
                                              tbl     .. '_TransactionId');
  local pmsg    = sqlbeg .. xactid .. sqlend;
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end
function publish_queue_sync(name, ...)
  local channel = 'sync';
  local xactid  = IncrementAutoInc('Next_' .. channel .. '_TransactionId');
  local pmsg    = Redisify('LUA', name, MyNodeId, xactid, ...);
  redis("publish", channel, pmsg);
  redis("zadd", 'Q_' .. channel, xactid, pmsg);
end
function call_sync(func, name, ...)
  local ret = func(0, 0, ...); -- LOCALLY: [nodeid, xactid] -> 0
  publish_queue_sync(name, ...);
  return ret;
end

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
  local sqlbeg = "INSERT INTO users (pk, id, name, passwd) VALUES (";
  local sqlend = "," .. my_userid .. ",'" ..
                        username .. "','" .. password .. "');";
  publish_queue_sql('users', sqlbeg, sqlend);
end

function local_post(my_userid, postid, msg, ts)
  redis("lpush", 'uid:' .. my_userid .. ':posts',   postid); -- U follow U
  redis("lpush", 'uid:' .. my_userid .. ':myposts', postid); -- for /profile
  local sqlbeg = "INSERT INTO posts (pk, userid, ts, msg) VALUES (";
  local sqlend = "," .. my_userid .. "," .. ts .. ",'" .. msg .. "');";
  publish_queue_sql('posts', sqlbeg, sqlend);
end

function local_follow(my_userid, userid, follow)
  commit_follow(my_userid, userid, follow);
  local sqlbeg = 
         "INSERT INTO follows (pk, from_userid, to_userid, type) VALUES (";
  local sqlend = "," .. my_userid .. "," .. userid .. "," .. follow .. ");";
  publish_queue_sql('follows', sqlbeg, sqlend);
end

-- SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS
function perform_auth_change(my_userid, oldauthsecret, newauthsecret)
  redis("set",    'uid:'  .. my_userid .. ':auth',     newauthsecret);
  redis("set",    'auth:' .. newauthsecret,            my_userid);
  if (oldauthsecret~= nil) then redis("delete", 'auth:' .. oldauthsecret); end
end

-- NOTE: register & logout can be OOO, shared vars MUST be stateless
global_register = function(nodeid, xactid, my_userid, username)
  print ('global_register: my_userid: ' .. my_userid);
  -- do one-time SET's - this data is needed
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
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
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
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
      redis("lpush", 'uid:' .. v .. ':posts', postid); -- TODO lpush NOT commutv
    end
    count = count + 1;
  end
  progress = max;
  if (progress >= tot) then return; end
  local pmsg = Redisify('LUA', 'iter_global_post',
                        tot, interval, progress, my_userid, postid, ts, msg);
  redis("publish", "echo", pmsg);
end

global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  print ('global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg);
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
  local post      = my_userid .. '|' .. ts .. '|' .. msg;
  redis("set", 'post:' .. postid, post);
  local followers = redis("smembers", 'uid:' .. my_userid .. ':followers');
  if (#followers > GlobalPostIterThreshold) then
    iter_global_post(#followers, GlobalPostIterInterval, 0,
                     my_userid, postid, ts, msg);
  else
    for k,v in pairs(followers) do
      redis("lpush", 'uid:' .. v .. ':posts', postid); -- TODO lpush NOT commutv
    end
  end
  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid); -- TODO lpush NOT commutative
  redis("ltrim", "global:timeline", 0, 1000);
end

global_follow = function(nodeid, xactid, my_userid, userid, follow)
  print ('global_follow: my_userid: ' .. my_userid .. ' userid: ' .. userid ..
         ' follow: ' .. follow);
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
  if (UserNode(userid) == MyNodeId) then -- only for users ON THIS SHARD
    commit_follow(my_userid, userid, follow);
  end
end

-- Retwis for Alchemy's Short Stack - Helper Functions (PRIVATE FUNCTIONS)
package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
local lz = require("zlib");

io.stdout:setvbuf("no"); -- flush stdout

-- CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT
dofile "./docroot/dist_alc_twitter/.instance_settings.lua";
NumNodes = #NodeData;

-- ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC
local AllSynced = false;
function RsubscribeAlchemySync()
  --print ('RsubscribeAlchemySync');
  local nsync = 0;
  for num, data in pairs(NodeData) do
    if (num ~= MyNodeId and data['synced'] == 0) then
      print ('RSUBSCRIBE ip: ' .. data['ip'] .. ' port: ' .. data['port']);
      local ret = redis("RSUBSCRIBE", data['ip'], data['port'], 'sync');
      if (ret["err"] == nil) then
        data['synced'] = 1; 
        nsync = nsync + 1;
        print ('SYNCED ip: ' .. data['ip'] .. ' port: ' .. data['port']);
      else
        data['synced'] = 0;
      end
    else
      nsync = nsync + 1;
    end
  end
  if (nsync == NumNodes) then
    if (AllSynced == false) then
      AllSynced = true;
      print ('AllSynced');
    end
    return true;
  end
  return false;
end

-- defines SimulateNetworkPartition
--dofile "./docroot/dist_alc_twitter/debug.lua";

-- HEARTBEAT HEARTBEAT HEARTBEAT HEARTBEAT HEARTBEAT HEARTBEAT HEARTBEAT
local MyGeneration = redis("get", "alchemy_generation");
if (MyGeneration == nil) then MyGeneration = 0; end
MyGeneration = MyGeneration + 1; -- This is the next generation
redis("set", "alchemy_generation", MyGeneration);
print('MyGeneration: ' .. MyGeneration);

function getHWname(nodeid, qname)
  return 'HW_' .. nodeid .. '_Q_' .. qname;
end
function HeartBeat() -- lua_cron function, called every second
  if (RsubscribeAlchemySync() == false) then return; end -- wait until synced
  local hw_eval_cmd = 'hw = {'; -- this command will be remotely EVALed
  for num, data in pairs(NodeData) do
    if (num == MyNodeId) then
      hw_eval_cmd = hw_eval_cmd .. AutoInc['Next_sync_TransactionId'];
    else
      local hw = redis("get", getHWname(num, 'sync'));
      if (hw == nil) then hw = '0'; end
      hw_eval_cmd = hw_eval_cmd .. hw;
    end
    if (num ~= NumNodes) then hw_eval_cmd = hw_eval_cmd .. ','; end
  end
  hw_eval_cmd = hw_eval_cmd .. '};';
  --print ('HeartBeat: hw_eval_cmd: .. ' .. hw_eval_cmd);
  local msg = Redisify('LUA', 'handle_heartbeat', MyNodeId, MyGeneration,
                                                  hw_eval_cmd);
  redis("publish", 'sync', msg);
end

-- OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING OOO_HANDLING
local GlobalRemoteHW   = {};
GlobalRemoteHW['sync'] = 0;
local RemoteHW         = {};
local LastHB_HW        = {};

function trim_Q(qname, hw)
  if (GlobalRemoteHW[qname] == hw) then return; end
  redis("zremrangebyscore", 'Q_' .. qname, "-inf", hw);
  GlobalRemoteHW[qname] = hw;
end
function handle_ooo(fromnode, hw, xactid)
  local ifromnode = tonumber(fromnode);
  --print ('handle_ooo: fromnode: ' .. fromnode .. ' hw: ' .. hw .. 
                    --' xactid: ' .. xactid);
  local beg      = tonumber(hw)     + 1;
  local fin      = tonumber(xactid) - 1;
  local msgs     = redis("zrangebyscore", "Q_sync", beg, fin);
  local pipeline = '';
  for k,v in pairs(msgs) do pipeline = pipeline .. v; end
  RemoteMessage(NodeData[ifromnode]["ip"], NodeData[ifromnode]["port"],
                pipeline);
end
function natural_net_recovery(hw)
  for num, data in pairs(RemoteHW) do
    if (tonumber(data) ~= tonumber(hw)) then
      --print('natural_net_recovery: node: ' .. num .. ' nhw: ' .. data ..
                                   --' hw: ' .. hw);
      handle_ooo(num, data, (hw + 1));
    end
  end
end
function handle_heartbeat(nodeid, generation, hw_eval_cmd)
  assert(loadstring(hw_eval_cmd))() -- Lua's eval
  for num, data in pairs(hw) do
    if (num == MyNodeId) then
      RemoteHW[nodeid] = data;
      if (LastHB_HW[nodeid] == nil) then LastHB_HW[nodeid] = data; end
    end
  end
  local nnodes = 0;
  local lw     = -1;
  for num, data in pairs(RemoteHW) do
    nnodes = nnodes +1;
    if     (lw == -1)  then lw = data;
    elseif (data < lw) then lw = data; end
  end
  if (nnodes ~= (NumNodes - 1)) then return; end
  trim_Q('sync', lw);
  if (tonumber(RemoteHW[nodeid]) < tonumber(LastHB_HW[nodeid])) then
    natural_net_recovery(LastHB_HW[nodeid]); 
  end
  LastHB_HW[nodeid] = AutoInc['Next_sync_TransactionId'];
end

function update_remote_hw(qname, nodeid, xactid)
  local inodeid = tonumber(nodeid);
  local hwname = getHWname(nodeid, qname)
  local hw     = tonumber(redis("get", hwname));
  local dbg = hw; if (hw == nil) then dbg = "(nil)"; end
  --print('update_remote_hw: nodeid: ' .. nodeid ..  ' xactid: ' .. xactid ..
                         --' HW: '     .. dbg);
  if     (hw == nil) then
    redis("set", hwname, xactid);
  elseif (hw == getPreviousAutoInc(xactid)) then
    redis("set", hwname, xactid);
  else
    local mabove = 'HW_' .. nodeid .. '_mabove';
    local mbelow = 'HW_' .. nodeid .. '_mbelow';
    local mav    = redis("get", mabove);
    if (mav ~= nil) then
      local mbv = redis("get", mbelow);
      if (tonumber(mav) == tonumber(getPreviousAutoInc(xactid))) then
        if (tonumber(xactid) == tonumber(getPreviousAutoInc(mbv))) then
          redis("del", mabove, mbelow); -- OOO done
        else
          redis("set", mabove, xactid); -- some more OOO left
        end
      end
    else
      local cmd = Redisify('LUA', 'handle_ooo', MyNodeId, hw, xactid);
      RemoteMessage(NodeData[inodeid]["ip"], NodeData[inodeid]["port"], cmd);
      redis("set", mabove, tostring(hw));
      redis("set", mbelow, xactid);
      redis("set", hwname, xactid); -- [mabove,mbelow] will catch OOO
    end
  end
end

-- AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER
AutoInc = {};
function InitAutoInc(name)
  local inc = AutoIncRange * (MyNodeId - 1);
  local id  = redis("get", 'global:' .. name);
  if (id == nil) then
      id = inc;
      redis("set", 'global:' .. name, id);
  end
  AutoInc[name] = id;
end
function IncrementAutoInc(name)
  --local was = AutoInc[name];
  AutoInc[name] = AutoInc[name] + 1;
  if ((AutoInc[name] % AutoIncRange) == 0) then
    AutoInc[name] = AutoInc[name] + (AutoIncRange * (NumNodes - 1));
  end
  redis("set", 'global:' .. name, AutoInc[name]);
  --print ('Autoinc[' .. name .. '] was: ' .. was .. ' is: ' .. AutoInc[name]);
  return AutoInc[name];
end
function getPreviousAutoInc(num)
  local was = num;
  num = tonumber(num) - 1;
  if ((num % AutoIncRange) == (AutoIncRange -1)) then
    num = num - (AutoIncRange * (NumNodes - 1));
  end
  return num;
end

-- STATELESS_VARS STATELESS_VARS STATELESS_VARS STATELESS_VARS STATELESS_VARS
local SALT0 = "#&^#sgDFTY|{$%^|@#$%^PSE2562346tgjgsdfgjads";
local SALT1 = "a;sdklrj	2e;lkfrn2<F2>o;4hqeofrh";
local SALT2 = "SDG:SD%:#$R:WEF:SER:@#:$@#:R$VSDSDR:!@#$5;";
local SALT3 = "2345KLJ234LKGNERKL7JL56KEJUKLYHJKL76J8LJ6HKLGNMJFOPUKI]89-"
function __create_sha1_variable(name, my_userid, val)                -- private
  if (my_userid == nil or val == nil) then
    print ('USAGE: get_sha1_variable(name, my_userid)');
    return 0;
  end
  return SHA1(SALT0 .. val .. SALT1 .. name .. SALT2 .. my_userid .. SALT3);
end
function __create_sha1_name(name, my_userid)                         -- private
  return 'uid:' .. my_userid .. ':' .. name;
end
function __set_sha1_variable(name, my_userid, val)                   -- private
  local rname = __create_sha1_name(name, my_userid);
  local rval  = __create_sha1_variable(name, my_userid, val);
  redis("set", rname, val);
  return rval;
end
function init_sha1_variable(name, my_userid)                          -- PUBLIC
  return __set_sha1_variable(name, my_userid, 0);
end
function get_sha1_variable(name, my_userid)                           -- PUBLIC
  local rname = __create_sha1_name(name, my_userid);
  return redis("get", rname);
end
function incr_sha1_variable(name, my_userid)                          -- PUBLIC
  local rval = get_sha1_variable(name, my_userid);
  return __set_sha1_variable(name, my_userid, (rval + 1))
end

-- DISTRIBUTED_LINKING DISTRIBUTED_LINKING DISTRIBUTED_LINKING
function GetNode(num)
  return (math.floor(tonumber(num) / AutoIncRange) % NumNodes) + 1;
end
function GetHttpDomainPort(num)
  local which = GetNode(num); --print ('num: ' .. num .. ' which: ' .. which);
  return 'http://' .. NodeData[which]["domain"] .. ':' .. 
                      NodeData[which]["port"]   .. '/';
end
function IsCorrectNode(num)
  local which = GetNode(num);
  return (which == MyNodeId);
end

function build_link(my_userid, page, arg1, arg2, arg3)
  local path = '';
  if (arg1 ~= nil) then     path =         '/' .. arg1;
    if (arg2 ~= nil) then   path = path .. '/' .. arg2;
      if (arg3 ~= nil) then path = path .. '/' .. arg3;
      end
    end
  end
  return GetHttpDomainPort(my_userid) .. page .. path;
end

-- OUTPUT_BUFFER+DEFLATE OUTPUT_BUFFER+DEFLATE OUTPUT_BUFFER+DEFLATE
-- this approach is explained here: http://www.lua.org/pil/11.6.html
-- the 3 functions [init_output, output, flush_output] could be 
--   1.) written in C
--   2.) pushed up into the server (i.e. OutputBuffer append to c->reply)
OutputBuffer = {};
function init_output()
  OutputBuffer = {};
end
function output(line)
  table.insert(OutputBuffer, line)
end
function flush_output()
  local out      = table.concat(OutputBuffer);
  local deflater = false;
  if (HTTP_HEADER['Accept-Encoding'] ~= nil and
      string.find(HTTP_HEADER['Accept-Encoding'], "deflate")) then
    deflater = true;
  end
  if (deflater) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
    return lz.deflate()(out, "finish")
  else
    return out;
   end
end

-- HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS
function gettime()
  return os.time(); -- TODO check speed, override w/ C call (no OS call)
end

math.randomseed(gettime() )
function getrand()
  return math.random(99999999999999);
end

function explode(div,str) -- credit: http://richard.warburton.it
  if (div == '') then return false end
  local pos,arr = 0,{}
  -- for each divider found
  for st,sp in function() return string.find(str,div,pos,true) end do
    table.insert(arr,string.sub(str,pos,st-1)) -- Attach chars left of curr div
    pos = sp + 1 -- Jump past current divider
  end
  table.insert(arr,string.sub(str,pos)) -- Attach chars right of last divider
  return arr
end

function is_empty(var)
  if (var == nil or string.len(var) == 0) then return true;
  else                                         return false; end
end

function url_decode(str) -- TODO find a C library for url_decode,url_encode
  str = string.gsub (str, "+", " ")
  str = string.gsub (str, "%%(%x%x)",
      function(h) return string.char(tonumber(h,16)) end)
  str = string.gsub (str, "\r\n", "\n")
  return str
end
function url_encode(str)
  if (str) then
    str = string.gsub (str, "\n", "\r\n")
    str = string.gsub (str, "([^%w ])",
        function (c) return string.format ("%%%02X", string.byte(c)) end)
    str = string.gsub (str, " ", "+")
  end
  return str	
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

-- HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML
dofile "./docroot/dist_alc_twitter/html.lua";

-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
function load_image(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  redis("SET", 'STATIC/' .. name, data);
end
load_image('docroot/img/logo.png',    'logo.png');
load_image('docroot/css/style.css',   'css/style.css');
load_image('docroot/img/sfondo.png',  'sfondo.png');
load_image('docroot/img/favicon.ico', 'favicon.ico');
load_image('docroot/js/helper.js',    'helper.js');

-- PUBLISH_REPLICATION PUBLISH_REPLICATION PUBLISH_REPLICATION
function publish_queue_sql(tbl, sqlbeg, sqlend)
  local channel = 'sql';
  local xactid  = IncrementAutoInc('Next_' .. channel .. '_' .. 
                                              tbl     .. '_TransactionId');
  local msg     = sqlbeg .. xactid .. sqlend;
  redis("publish", channel, msg);
  redis("zadd", 'Q_' .. channel, xactid, msg);
end
function publish_queue_sync(name, ...)
  local channel = 'sync';
  local xactid  = IncrementAutoInc('Next_' .. channel .. '_TransactionId');
  local msg     = Redisify('LUA', name, MyNodeId, xactid, ...);
  redis("publish", channel, msg);
  redis("zadd", 'Q_' .. channel, xactid, msg);
end
function call_sync(func, name, ...)
  local ret = func(0, 0, ...); -- LOCALLY: [nodeid, xactid] -> 0
  publish_queue_sync(name, ...);
  return ret;
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
  redis("lpush", 'uid:' .. my_userid .. ':posts', postid);   -- U follow U
  redis("lpush", 'uid:' .. my_userid .. ':myposts', postid); -- for /profile
  local sqlbeg = "INSERT INTO posts (pk, userid, ts, msg) VALUES (";
  local sqlend = "," .. my_userid .. "," .. ts .. ",'" .. msg .. "');";
  publish_queue_sql('posts', sqlbeg, sqlend);
end
function local_follow(my_userid, userid, follow)
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
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
  -- do one-time SET's - this data is needed
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
end
global_post = function(nodeid, xactid, my_userid, postid, ts, msg)
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
  print ('global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts ..
         ' msg: ' .. msg);
  local post   = my_userid .. '|' .. ts .. '|' .. msg;
  redis("set", 'post:' .. postid, post);
  -- global_post just does follower, not self
  local followers = redis("smembers", 'uid:' .. my_userid .. ':followers');
  -- todo only local followers
  for k,v in pairs(followers) do
    redis("lpush", 'uid:' .. v .. ':posts', postid); -- TODO lpush NOT combntrl
  end
  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid); -- TODO lpush NOT combntrl
  redis("ltrim", "global:timeline", 0, 1000);
end
global_follow = function(nodeid, xactid, my_userid, userid, follow)
  if (xactid ~= 0) then update_remote_hw('sync', nodeid, xactid); end
  local f = tonumber(follow);
  if (f == 1) then
    redis("sadd", 'uid:' .. userid    .. ':followers', my_userid);
    redis("sadd", 'uid:' .. my_userid .. ':following', userid);
  else 
    redis("srem", 'uid:' .. userid    .. ':followers', my_userid);
    redis("srem", 'uid:' .. my_userid .. ':following', userid);
  end
end

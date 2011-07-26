-- Retwis for Alchemy's Short Stack - Helper Functions (PRIVATE FUNCTIONS)
package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
local lz = require("zlib");

-- CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT
dofile "./docroot/dist_alc_twitter/instance_settings.lua";
NumNodes = #NodeData;

-- ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC ALCHEMY_SYNC
function RsubscribeAlchemySync() -- lua_cron function, called every second
  --print ('RsubscribeAlchemySync');
  for num,data in pairs(NodeData) do
    if (num ~= NodeId and data['synced'] == 0) then
        print ('RSUBSCRIBE to ip: ' .. data['ip'] .. ' port: ' .. data['port']);
        ret = redis("RSUBSCRIBE", data['ip'], data['port'],
                                                        'channel:alchemy_sync');
        if (ret["err"] == nil) then data['synced'] = 1; 
        else                        data['synced'] = 0; end
    end
  end
end

-- AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER
AutoInc = {};
function AutoIncInit(name)
  local inc = AutoIncRange * (NodeId - 1);
  local id  = redis("get", "global:" .. name);
  if (id == nil) then
      id = inc;
      redis("set", "global:" .. name, id);
  end
  AutoInc[name] = id;
end
function IncrementAutoInc(name)
  AutoInc[name] = AutoInc[name] +1;
  if ((AutoInc[name] % AutoIncRange) == 0) then
    AutoInc[name] = AutoInc[name] + (AutoIncRange * (NumNodes - 1));
  end
  redis("set", "global:SavedStartUserId", StartUserId);
  redis("set", "global:" .. name, AutoInc[name]);
  return AutoInc[name];
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
  return "uid:" .. my_userid .. ":" .. name;
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

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
AutoIncInit('NextUserId');
print ('NextUserId: ' .. AutoInc['NextUserId']);
AutoIncInit('NextPostId');
print ('NextPostId: ' .. AutoInc['NextPostId']);

function GetNode(num)
  return (math.floor(tonumber(num) / AutoIncRange) % NumNodes) + 1;
end
function GetHttpDomainPort(num)
  local which = GetNode(num);
  --print ('num: ' .. num .. ' which: ' .. which);
  return "http://" .. NodeData[which]["domain"] .. ":" .. 
                      NodeData[which]["port"]   .. "/";
end
function IsCorrectNode(num)
  local which = GetNode(num);
  return (which == NodeId);
end
-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
function load_image(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  redis("SET", "STATIC/" .. name, data);
end
load_image('docroot/img/logo.png',    'logo.png');
load_image('docroot/css/style.css',   'css/style.css');
load_image('docroot/img/sfondo.png',  'sfondo.png');
load_image('docroot/img/favicon.ico', 'favicon.ico');
load_image('docroot/js/helper.js',    'helper.js');

-- HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS HELPERS
math.randomseed( os.time() )
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
  local deflater = string.find(HTTP_HEADER['Accept-Encoding'], "deflate")
  if (deflater) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
    return lz.deflate()(out, "finish")
  else
    return out;
   end
end

function build_link(my_userid, page, arg1, arg2, arg3)
  local path = '';
  if (arg1 ~= nil) then
    path = '/' .. arg1;
    if (arg2 ~= nil) then
      path = path .. '/' .. arg2;
      if (arg3 ~= nil) then
        path = path .. '/' .. arg3;
      end
    end
  end
  return GetHttpDomainPort(my_userid) .. page .. path;
end
-- HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML
function create_navbar(my_userid)
  local domain = GetHttpDomainPort(my_userid)
  output([[<div id="navbar">
       <a href="]] .. build_link(my_userid, 'index_page') .. [[">home</a> |
       <a href="]] .. build_link(my_userid, 'timeline')   ..  '">timeline</a>');
  if (isLoggedIn()) then
    output('| <a href="' .. build_link(my_userid, 'logout', my_userid) ..
                                                              '">logout</a>');
  end
  output('</div>');
end

function create_header(my_userid)
output([[<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html lang="it">
<head>
<script src="/STATIC/helper.js"></script>
</script>
<link rel="shortcut icon" href="/STATIC/favicon.ico" />
<meta content="text/html; charset=UTF-8" http-equiv="content-type">
<title>Retwis - Example Twitter clone based on Alchemy DB</title>
<link href="/STATIC/css/style.css" rel="stylesheet" type="text/css">
</head>
<body>
<div id="page">
<div id="header">
<img style="border:none" src="/STATIC/logo.png" width="192" height="85" alt="Retwis">]]);
create_navbar(my_userid);
output('</div>');
end

function create_footer()
  output('<div id="footer"> <a href="http://code.google.com/p/alchemydatabase/">Alchemy Database</a> is a A Hybrid Relational-Database/NOSQL-Datastore</div> </div> </body> </html>');
end

function create_welcome()
  output([[ 
<div id="welcomebox">
<div id="registerbox">
<h2>Register!</h2>
<b>Want to try Retwis? Create an account!</b>
<form method="GET" onsubmit="return passwords_match(this.elements['password'].value, this.elements['password2'].value) && form_action_rewrite_url('register', encodeURIComponent(this.elements['username'].value), encodeURIComponent(this.elements['password'].value))">
<table>
<tr> <td>Username</td><td><input type="text" name="username"></td> </tr>
<tr> <td>Password</td><td><input type="password" name="password"></td> </tr>
<tr> <td>Password (again)</td><td><input type="password" name="password2"></td> </tr>
<tr> <td colspan="2" align="right"><input type="submit" name="doit" value="Create an account"></td> </tr>
</table>
</form>
<h2>Already registered? Login here</h2>
<form method="GET" onsubmit="return form_action_rewrite_url('login', encodeURIComponent(this.elements['username'].value), encodeURIComponent(this.elements['password'].value))">
<table>
<tr> <td>Username</td><td><input type="text" name="username"></td> </tr>
<tr> <td>Password</td><td><input type="password" name="password"></td> </tr>
<tr> <td colspan="2" align="right"><input type="submit" name="doit" value="Login"></td> </tr>
</table>
</form>
</div>
Hello! Retwis is a very simple clone of <a href="http://twitter.com">Twitter</a>, as a demo for the <a href="http://code.google.com/p/alchemydatabase/wiki/ShortStack">Alchemy's Short-Stack</a>
</div>
]]);
end

function strElapsed(t)
  local d = os.time() - t;
  if (d < 60) then return d .. " seconds"; end
  if (d < 3600) then
      local m = d/60;
      return m .. " minutes";
  end
  if (d < 3600*24) then
      local h = d/3600;
      return h .. " hours";
  end
  d = d/(3600*24);
  return d .. " days";
end

function showPost(id)
  local postdata = redis("get", "post:" .. id);
  if (postdata == nil) then return false; end
  local aux      = explode("|", postdata);
  local userid   = aux[1];
  local time     = aux[2];
  local username = redis("get", "uid:" .. userid .. ":username");
  local post     = aux[3];
  local elapsed  = strElapsed(time);
  local userlink = 
  output('<div class="post">' ..
         '<a class="username" href="' ..
                            build_link(userid, "profile", userid) ..  '">' ..
           username .. "</a>" ..
         ' ' .. post .."<br>" .. '<i>posted '..
         elapsed ..' ago via web</i></div>');
  return true;
end

function showUserPosts(key, start, count)
  local posts = redis("lrange", key, start, (start + count));
  local c     = 0;
  for k,v in pairs(posts) do
      if (showPost(v)) then c = c + 1; end
      if (c == count) then break; end
  end
end

function showUserPostsWithPagination(thispage, username, userid, start, count)
  local navlink  = "";
  local nextc    = start + 10;
  local prevc    = start - 10;
  local nextlink = "";
  local prevlink = "";
  if (prevc < 0) then prevc = 0; end
  local key, u;
  if (username) then
      u   = userid;
      key = "uid:" .. userid .. ":myposts";
  else
      u   = 0;
      key = "uid:" .. userid .. ":posts";
  end

  showUserPosts(key, start, count);
  local nposts = redis("llen", key);
  if (nposts ~= nil and nposts > start + count) then
      nextlink = '<a href="' .. build_link(userid, thispage, u, nextc) ..
                         '">&raquo; Older posts </a>';
  end
  if (start > 0) then
      prevlink = '<a href="' .. build_link(userid, thispage, u, prevc) ..
                        '">Newer posts &laquo;</a>';
  end
  local divider;
  if (string.len(nextlink) and string.len(prevlink)) then divider = ' --- ';
  else                                                    divider = ' '; end
  if (string.len(nextlink) or string.len(prevlink)) then
      output('<div class="rightlink">' .. prevlink .. divider ..
                                          nextlink .. '</div>');
  end
end

function showLastUsers()
  local users = redis("sort", "global:users", "GET", "uid:*:username", "DESC", "LIMIT", 0, 10);
  output('<div>');
  for k,v in pairs(users) do
    local userid = redis("get", "username:" .. v .. ":id");
    output('<a class="username" href="' ..
                   build_link(userid, "profile", v) ..  '">' .. v .. '</a> ');
  end
  output('</div><br>');
end

function create_home(thispage, my_userid, start)
  local nfollowers = redis("scard", "uid:" .. my_userid .. ":followers");
  local nfollowing = redis("scard", "uid:" .. my_userid .. ":following");
  local s          = 0;
  if (start ~= nil) then s = start; end
  output([[
<div id="postform">
<form method="GET"
  onsubmit="return form_action_rewrite_url('post', ]] .. my_userid .. [[, encodeURIComponent(this.elements['status'].value));" >]]);
  output(User['username'] ..', what you are doing?');
  output([[
<br>
<table>
<tr><td><textarea cols="70" rows="3" name="status"></textarea></td></tr>
<tr><td align="right"><input type="submit" name="doit" value="Update"></td></tr>
</table>
</form>
<div id="homeinfobox">
]]);
  output(nfollowers .. ' followers<br>' .. nfollowing .. ' following<br></div></div>');
  showUserPostsWithPagination(thispage, false, User['id'], s, 10);
end

function goback(my_userid, msg)
  create_header(my_userid);
  output('<div id ="error">' .. msg .. '<br><a href="javascript:history.back()">Please return back and try again</a></div>');
  create_footer();
end

User     = {};
LoggedIn = false;
function loadUserInfo(userid)
  User['id']       = userid;
  User['username'] = redis("get", "uid:" .. userid .. ":username");
end

function isLoggedIn()
  LoggedIn = false;
  local authcookie = COOKIE['auth'];
  if (authcookie ~= nil) then
    local userid = redis("get", "auth:" .. authcookie);
    if (userid ~= nil) then
      if (redis("get", "uid:" .. userid .. ":auth") ~= authcookie) then
        return false;
      end
      loadUserInfo(userid); LoggedIn = true; return true;
    end
  end
  return false;
end

-- LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS
function local_register(my_userid, password, authsecret)
  redis("set",  "uid:"      .. my_userid   .. ":password", password);
  -- keep a global list of local users (used later for MIGRATEs)
  redis("lpush", "local_user_id",                          my_userid);
  -- TODO publish to "channel:sql"
end
function local_post(my_userid, postid, ts, msg)
  redis("lpush", "uid:" .. my_userid .. ":posts", postid);   -- U follow U
  redis("lpush", "uid:" .. my_userid .. ":myposts", postid); -- for /profile
  local sqlmsg = "INSERT INTO tweets (userid, ts, msg) VALUES (" ..
                  my_userid .. "," .. ts .. ",'" .. msg .. "');";
  redis("publish", "channel:sql", sqlmsg); -- for pubsub_pipe replication
end
function local_follow(my_userid, userid, follow)
  -- TODO publish to "channel:sql"
end

-- SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS
function call_sync(func, name, ...)
  local ret = func(...);
  local pubmsg = Redisify('LUA', name, ...);
  redis("publish", "channel:alchemy_sync", pubmsg); -- pubsub_pipe replication
  return ret;
end

-- NOTE, register & logout have no guaranteed order
global_register = function(my_userid, username)
 -- do one-time SET's - this data is needed
  redis("set",  "username:" .. username  .. ":id",       my_userid);
  redis("set",  "uid:"      .. my_userid .. ":username", username);
  redis("sadd", "global:users",                          my_userid);
  local nlgt = redis("get",  "uid:" .. my_userid .. ":nlogouts");
  if (nlgt == nil) then
    local authsecret = init_sha1_variable('nlogouts', my_userid);
    redis("set",  "uid:"      .. my_userid   .. ":auth",     authsecret);
    redis("set",  "auth:"     .. authsecret,                 my_userid);
    return authsecret;
  else
    return 0; -- handle OOO logout
  end
end

global_logout = function(my_userid)
  local oldauthsecret = get_sha1_variable('nlogouts', my_userid);
  -- combinatorial INCR operation governs flow
  local newauthsecret = incr_sha1_variable('nlogouts', my_userid);
  redis("set",    "uid:"  .. my_userid .. ":auth",     newauthsecret);
  redis("set",    "auth:" .. newauthsecret,            my_userid);
  redis("delete", "auth:" .. oldauthsecret);
end

global_post = function(my_userid, postid, ts, msg)
  print ('global_post: my_userid: ' .. my_userid .. ' ts: ' .. ts .. ' msg: ' .. msg);
  local post   = my_userid .. "|" .. ts .. "|" .. msg;
  redis("set", "post:" .. postid, post);
  -- global_post just does follower, not self
  local followers = redis("smembers", "uid:" .. my_userid .. ":followers");
  -- todo only local followers
  for k,v in pairs(followers) do
    redis("lpush", "uid:" .. v .. ":posts", postid);
  end
  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid);
  redis("ltrim", "global:timeline", 0, 1000);
end

global_follow = function(my_userid, userid, follow)
  local f = tonumber(follow);
  if (f == 1) then
    redis("sadd", "uid:" .. userid    .. ":followers", my_userid);
    redis("sadd", "uid:" .. my_userid .. ":following", userid);
  else 
    redis("srem", "uid:" .. userid    .. ":followers", my_userid);
    redis("srem", "uid:" .. my_userid .. ":following", userid);
  end
end

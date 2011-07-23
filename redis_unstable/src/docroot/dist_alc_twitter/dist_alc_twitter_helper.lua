-- Retwis for Alchemy's Short Stack - Helper Functions (PRIVATE FUNCTIONS)
package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
local lz = require("zlib");

-- CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT CLUSTER_INIT
dofile "./docroot/dist_alc_twitter/instance_settings.lua";
NumNodes = #NodeData;

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

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
AutoIncInit('NextUserId');
print ('NextUserId: ' .. AutoInc['NextUserId']);
AutoIncInit('PostUserId');
print ('PostUserId: ' .. AutoInc['PostUserId']);

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

function GetNode(num)
  return (math.floor(tonumber(num) / NumNodes) % NumNodes) + 1;
end
function GetHttpDomainPort(num)
  local which = GetNode(num);
print ('num: ' .. num .. ' which: ' .. which);
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
function getrand() -- NOTE: this is lazy, should be more random
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

-- HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML
function create_navbar(my_userid)
  local domain = GetHttpDomainPort(my_userid)
  output('<div id="navbar"> <a href="' .. domain .. 'index_page">home</a> | <a href="' .. domain .. 'timeline">timeline</a>');
  if (isLoggedIn()) then
    output('| <a href="' .. domain .. 'logout">logout</a>');
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
  local uid      = aux[1];
  local time     = aux[2];
  local username = redis("get", "uid:" .. uid .. ":username");
  local post     = aux[3]; -- TODO: more
  local elapsed  = strElapsed(time);
  local userlink = 
  output('<div class="post">' ..
         "<a class=\"username\" href=\"" .. GetHttpDomainPort(uid) ..
                                           "profile/" .. uid .. "\">" ..
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
      u   = "/"    .. userid .. "/";
      key = "uid:" .. userid .. ":myposts";
  else
      u   = "/";
      key = "uid:" .. userid .. ":posts";
  end

  showUserPosts(key, start, count);
  local nposts = redis("llen", key);
  if (nposts ~= nil and nposts > start + count) then
      nextlink = "<a href=\"" ..
                       GetHttpDomainPort(userid) .. thispage .. u .. nextc ..
                         "\">&raquo; Older posts </a>";
  end
  if (start > 0) then
      prevlink = "<a href=\"" ..
                       GetHttpDomainPort(userid) .. thispage .. u .. prevc ..
                        "\">Newer posts &laquo;</a>";
  end
  local divider;
  if (string.len(nextlink) and string.len(prevlink)) then divider = ' --- ';
  else                                                    divider = ' '; end
  if (string.len(nextlink) or string.len(prevlink)) then
      output("<div class=\"rightlink\">" .. prevlink .. divider ..
                                            nextlink .. "</div>");
  end
end

function showLastUsers()
  local users = redis("sort", "global:users", "GET", "uid:*:username", "DESC", "LIMIT", 0, 10);
  output("<div>");
  for k,v in pairs(users) do
    local uid = redis("get", "username:" .. v .. ":id");
    output("<a class=\"username\" href=\"" .. GetHttpDomainPort(uid) ..
                                      "profile/" .. v .. "\">" .. v .. "</a> ");
  end
  output("</div><br>");
end

function create_home(thispage, start)
  local nfollowers = redis("scard", "uid:" .. User['id'] .. ":followers");
  local nfollowing = redis("scard", "uid:" .. User['id'] .. ":following");
  local s          = 0;
  if (start ~= nil) then s = start; end
  output([[
<div id="postform">
<form method="GET"
  onsubmit="return form_action_rewrite_url('post', encodeURIComponent(this.elements['status'].value));" >]]);
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
  output(nfollowers .. " followers<br>" .. nfollowing .. " following<br></div></div>");
  showUserPostsWithPagination(thispage, false, User['id'], s, 10);
end

function goback(msg)
  create_header(0);
  output('<div id ="error">' .. msg .. '<br><a href="javascript:history.back()">Please return back and try again</a></div>');
  create_footer();
end

User = {};
function loadUserInfo(userid)
  User['id']       = userid;
  User['username'] = redis("get", "uid:" .. userid .. ":username");
end

function isLoggedIn()
  local authcookie = COOKIE['auth'];
  if (authcookie ~= nil) then
    local userid = redis("get", "auth:" .. authcookie);
    if (userid ~= nil) then
      if (redis("get", "uid:" .. userid .. ":auth") ~= authcookie) then
        return false;
      end
      loadUserInfo(userid); return true;
    end
  end
  return false;
end

-- LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS LOCAL_FUNCS
function local_register(my_userid, password, authsecret)
  redis("set",  "uid:"      .. my_userid   .. ":password", password);
end
function local_post(my_userid, postid, ts, msg)
  redis("lpush", "uid:" .. my_userid .. ":posts", postid);   -- U follow U
  redis("lpush", "uid:" .. my_userid .. ":myposts", postid); -- for /profile
  local sqlmsg = "INSERT INTO tweets (userid, ts, msg) VALUES (" ..
                  my_userid .. "," .. ts .. ",'" .. msg .. "');";
  redis("publish", "channel:sql", sqlmsg); -- for pubsub_pipe replication
end

-- SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS SYNC_FUNCS
function call_sync(func, name, ...)
  func(...);
  local pubmsg = Redisify('LUA', name, ...);
  redis("publish", "channel:alchemy_sync", pubmsg); -- pubsub_pipe replication
end

-- NOTE, register & logout make new auth's globally, so login can be local_()
sync_register = function(my_userid, username, authsecret)
  redis("set",  "username:" .. username  .. ":id",       my_userid);
  redis("set",  "uid:"      .. my_userid .. ":username", username);
  redis("sadd", "global:users",                          my_userid);
  redis("set",  "uid:"      .. my_userid   .. ":auth",     authsecret);
  redis("set",  "auth:"     .. authsecret,                 my_userid);
end

sync_logout = function(my_userid, oldauthsecret, newauthsecret)
  redis("set",    "uid:"  .. my_userid .. ":auth", newauthsecret);
  redis("set",    "auth:" .. newauthsecret,        my_userid);
  redis("delete", "auth:" .. oldauthsecret);
end

sync_post = function(my_userid, postid, ts, msg)
  print ('sync_post: my_userid: ' .. my_userid .. ' ts: ' .. ts .. ' msg: ' .. msg);
  local post   = my_userid .. "|" .. ts .. "|" .. msg;
  redis("set", "post:" .. postid, post);
  -- sync_post just does follower, not self
  local followers = redis("smembers", "uid:" .. my_userid .. ":followers");
  -- todo only local followers
  for k,v in pairs(followers) do
    redis("lpush", "uid:" .. v .. ":posts", postid);
  end
  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid);
  redis("ltrim", "global:timeline", 0, 1000);
end

sync_follow = function(my_userid, userid, follow)
  local f = tonumber(follow);
  if (f == 1) then
    redis("sadd", "uid:" .. userid    .. ":followers", my_userid);
    redis("sadd", "uid:" .. my_userid .. ":following", userid);
  else 
    redis("srem", "uid:" .. userid    .. ":followers", my_userid);
    redis("srem", "uid:" .. my_userid .. ":following", userid);
  end
end

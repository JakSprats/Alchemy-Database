-- Retwis for Alchemy's Short Stack - about 400 lines
function load_image(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  redis("SET", "STATIC/" .. name, data);
end

-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
load_image('external/retwis-0.3/logo.png',      'logo.png');
load_image('external/retwis-0.3/css/style.css', 'css/style.css');
load_image('external/retwis-0.3/sfondo.png',    'sfondo.png');
load_image('external/retwis-0.3/favicon.ico',   'favicon.ico');
load_image('external/retwis-0.3/helper.js',     'helper.js');

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
-- STRINGS STRINGS STRINGS STRINGS STRINGS STRINGS STRINGS STRINGS
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
  return table.concat(OutputBuffer);
end

-- RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS
-- RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS RETWIS
function create_navbar()
  output('<div id="navbar"> <a href="/index_page">home</a> | <a href="/timeline">timeline</a>');
  if (isLoggedIn()) then
    output('| <a href="/logout">logout</a>');
  end
  output('</div>');
end

function create_header()
output([[<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html lang="it">
<head>
<script src="/STATIC/helper.js"></script>
<link rel="shortcut icon" href="/STATIC/favicon.ico" />
<meta content="text/html; charset=UTF-8" http-equiv="content-type">
<title>Retwis - Example Twitter clone based on Alchemy DB</title>
<link href="/STATIC/css/style.css" rel="stylesheet" type="text/css">
</head>
<body>
<div id="page">
<div id="header">
<img style="border:none" src="/STATIC/logo.png" width="192" height="85" alt="Retwis">]]);
create_navbar();
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
<form method="GET" onsubmit="return passwords_match(this.elements['password'].value, this.elements['password2'].value) && form_action_rewrite_url('/register', encodeURIComponent(this.elements['username'].value), encodeURIComponent(this.elements['password'].value))">
<table>
<tr> <td>Username</td><td><input type="text" name="username"></td> </tr>
<tr> <td>Password</td><td><input type="password" name="password"></td> </tr>
<tr> <td>Password (again)</td><td><input type="password" name="password2"></td> </tr>
<tr> <td colspan="2" align="right"><input type="submit" name="doit" value="Create an account"></td> </tr>
</table>
</form>
<h2>Already registered? Login here</h2>
<form method="GET" onsubmit="return form_action_rewrite_url('./login', encodeURIComponent(this.elements['username'].value), encodeURIComponent(this.elements['password'].value))">
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
         "<a class=\"username\" href=\"/profile/" .. username .. "\">" ..
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
  local u;
  if (username) then u = username;
  else               u = ""; end

  local key    = "uid:" .. userid .. ":posts";
  showUserPosts(key, start, count);
  local nposts = redis("llen", key);
  if (nposts ~= nil and nposts > start + count) then
      nextlink = "<a href=\"" .. thispage .. "/" .. nextc .. "/" .. u .."\">Older posts &raquo;</a>";
  end
  if (start > 0) then
      prevlink = "<a href=\"" .. thispage .. "/" .. prevc .. "/" .. u .."\">Newer posts &laquo;</a>";
  end
  if (string.len(nextlink) or string.len(prevlink)) then
      output("<div class=\"rightlink\">" .. prevlink .. " " ..  nextlink .. "</div>");
  end
end

function showLastUsers()
  local users = redis("sort", "global:users", "GET", "uid:*:username", "DESC", "LIMIT", 0, 10);
  output("<div>");
  for k,v in pairs(users) do
    output("<a class=\"username\" href=\"/profile/" .. v .. "\">" .. v .. "</a> ");
  end
  output("</div><br>");
end

function create_home(thispage, start)
  local nfollowers = redis("scard", "uid:" .. User['id'] .. ":followers");
  local nfollowing = redis("scard", "uid:" .. User['id'] .. ":following");
  local s          = 0;
  if (start ~= nil) then s = start; end
  output('<div id="postform"><form method="GET" onsubmit="return form_action_rewrite_url(\'/post\', encodeURIComponent(this.elements[\'status\'].value), \'\');">');
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
  create_header();
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
      loadUserInfo(userid);
      return true;
    end
  end
  return false;
end

-- PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API
-- PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API PUBLIC_API
module("whitelist", package.seeall);

function index_page(start) 
  init_output();
  local thispage = '/index_page';
  local s        = tonumber(start);
  create_header();
  if (isLoggedIn()) then
    create_home(thispage, s);
  else 
    create_welcome();
  end
  create_footer();
  return flush_output();
end

function register(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback("Either Username or Password is Empty");
    return flush_output();
  end
  if (redis("get", "username:" .. username .. ":id")) then
    goback("Sorry the selected username is already in use.");
    return flush_output();
  end

  -- Everything is ok, Register the user!
  local userid = redis("incr", "global:nextUserId");
  redis("set", "username:" .. username .. ":id",       userid);
  redis("set", "uid:"      .. userid   .. ":username", username);
  redis("set", "uid:"      .. userid   .. ":password", password);

  local authsecret = getrand();
  redis("set", "uid:" .. userid .. ":auth", authsecret);
  redis("set", "auth:" .. authsecret,       userid);

  -- Manage a Set with all the users, may be userful in the future
  redis("sadd", "global:users", userid);

  -- User registered! Login this guy
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');

  create_header();
  output('<h2>Welcome aboard!</h2> Hey ' .. username .. ', now you have an account, <a href="/index_page">a good start is to write your first message!</a>.');
  create_footer();
  return flush_output();
end

function logout()
  local isl = isLoggedIn();
  if (isl == false) then
    SetHttpRedirect('/index_page'); return;
  end

  local newauthsecret = getrand();
  local userid        = User['id'];
  local oldauthsecret = redis("get", "uid:" .. userid .. ":auth");

  redis("set", "uid:" .. userid .. ":auth", newauthsecret);
  redis("set", "auth:" .. newauthsecret, userid);
  redis("delete", "auth:" .. oldauthsecret);
  SetHttpRedirect('/index_page');
end

function login(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback("You need to enter both username and password to login.");
    return flush_output();
  end
  local userid = redis("get", "username:" .. username ..":id");
  if (userid == nil) then
    goback("Wrong username or password");
    return flush_output();
  end
  local realpassword = redis("get", "uid:" .. userid .. ":password");
  if (realpassword ~= password) then
    goback("Wrong useranme or password");
    return flush_output();
  end

  -- Username / password OK, set the cookie and redirect to index.php
  local authsecret = redis("get", "uid:" .. userid .. ":auth");
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  SetHttpRedirect('/index_page');
end

function post(msg)
  local isl = isLoggedIn();
  if (isl == false or is_empty(msg)) then
    SetHttpRedirect('/index_page'); return;
  end

  local postid = redis("incr", "global:nextPostId");
  local post   =  User['id'] .. "|" .. os.time() .. "|" .. msg;
  redis("set", "post:" .. postid, post);
  local followers = redis("smembers", "uid:" .. User['id'] .. ":followers");
  table.insert(followers, User['id']); -- Add the post to our own posts too 

  for k,v in pairs(followers) do
    redis("lpush", "uid:" .. v .. ":posts", postid);
  end
  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid);
  redis("ltrim", "global:timeline", 0, 1000);
  SetHttpRedirect('/index_page');
end

function timeline()
  init_output();
  create_header();
  showLastUsers();
  output('<i>Latest 50 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 50);
  create_footer();
  return flush_output();
end

function profile(username, start)
  local thispage = '/profile';
  if (is_empty(username)) then
    SetHttpRedirect('/index_page'); return;
  end
  local userid = redis("get", "username:" .. username .. ":id")
  if (userid == nil) then
    SetHttpRedirect('/index_page'); return;
  end

  init_output();
  create_header();
  output("<h2 class=\"username\">" .. username .. "</h2>");
  local isl  = isLoggedIn();
  if (isl and User['id'] ~= userid) then
    local isfollowing = redis("sismember", "uid:" .. User['id'] .. ":following", userid);
    if (isfollowing == 1) then
      output("<a href=\"/follow/" .. userid .. "/0\" class=\"button\">Stop following</a>");
    else
      output("<a href=\"/follow/" .. userid .. "/1\" class=\"button\">Follow this user</a>");
    end
  end
  local s = tonumber(start);
  if (s == nil) then s = 0; end
  showUserPostsWithPagination(thispage, false, userid, s, 10);
  create_footer();
  return flush_output();
end

function follow(userid, follow)
  if (is_empty(userid) or is_empty(follow)) then
    SetHttpRedirect('/index_page'); return;
  end
  local isl = isLoggedIn();
  if (isl == false) then
    SetHttpRedirect('/index_page'); return;
  end
  if (userid ~= User['id']) then
    local f = tonumber(follow);
    if (f == 1) then
      redis("sadd", "uid:" .. userid     .. ":followers", User['id']);
      redis("sadd", "uid:" .. User['id'] .. ":following", userid);
    else 
      redis("srem", "uid:" .. userid     .. ":followers", User['id']);
      redis("srem", "uid:" .. User['id'] ..":following",  userid);
    end
  end
  local username = redis("get", "uid:" .. userid .. ":username");
  SetHttpRedirect('/profile/' .. username);
end

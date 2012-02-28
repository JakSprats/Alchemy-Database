-- Retwis for Alchemy's Short Stack - Helper Functions (PRIVATE FUNCTIONS)
package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
local lz = require("zlib");

-- STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES STATIC_FILES
function load_image(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  alchemy("SET", "STATIC/" .. name, data);
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

-- TODO find a C library for url_decode,url_encode
function url_decode(str)
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

-- DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE
local IsSet_IsDeflatable = false; -- reset every request
local IsDeflatable       = false;
function set_is_deflatable()
  if (IsSet_IsDeflatable) then return IsDeflatable; end
  if (HTTP_HEADER['Accept-Encoding'] ~= nil and
      string.find(HTTP_HEADER['Accept-Encoding'], "deflate")) then
    IsDeflatable = true;
  else
    IsDeflatable = false;
  end
  IsSet_IsDeflatable = true;
  return IsDeflatable;
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
  SetHttpResponseHeader('Content-Type', 'text/html; charset=utf-8');
  local out          = table.concat(OutputBuffer);
  local deflater     = set_is_deflatable();
  IsSet_IsDeflatable = false;
  if (deflater) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
    return lz.deflate()(out, "finish")
  else
    return out;
  end
end

-- CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE
local CacheExpireTime = 600; -- 10 minutes
--CacheExpireTime = 5; -- DEBUG

function getCacheKey(...)
  local key = 'PAGE_CACHE';
  for i,v in ipairs(arg) do     key = key .. '_' .. tostring(v); end
  if (set_is_deflatable()) then key = 'gzip_' .. key;            end
  return key;
end
function CacheExists(...)
  local key = getCacheKey(...);
  local hit = alchemy("exists", key);
  --print ('CacheExists: key: ' .. key .. ' hit: ' .. hit);
  if (hit == 0) then return false;
  else               return true;  end
end
function CacheGet(...)
  SetHttpResponseHeader('Content-Type', 'text/html; charset=utf-8');
  if (set_is_deflatable()) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
  end
  local key = getCacheKey(...);
  IsSet_IsDeflatable = false;
  --print ('CacheGet key: ' .. key);
  alchemy("expire", key,            CacheExpireTime); -- live a little longer
  alchemy("expire", key .. '_BLOB', CacheExpireTime); -- live a little longer
  return alchemy("get", key .. '_BLOB');
end
function CachePutOutput(...)
  local key          = getCacheKey(...);
  IsSet_IsDeflatable = false;
  --print ('CachePutOutput key: ' .. key);
  local out          = table.concat(OutputBuffer);
  local deflater     = set_is_deflatable();
  if (deflater) then out = lz.deflate()(out, "finish") end
  alchemy("setex", key,            CacheExpireTime, 1);
  alchemy("setex", key .. '_BLOB', CacheExpireTime, out);
end

-- ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG
function CheckEtag(...)
  local ekey = 'ETAG';
  for i,v in ipairs(arg) do     ekey = ekey .. '_' .. tostring(v); end
  --print('CheckEtag: key: ' .. ekey);
  if (HTTP_HEADER['If-None-Match'] ~= nil) then
    if (HTTP_HEADER['If-None-Match'] == ekey) then
      SetHttp304();
      return true;
    end
  end
  SetHttpResponseHeader('Etag', ekey);
  return false;
end
-- HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML
function create_navbar()
  if (User['id'] ~= nil) then
    output('<div id="navbar"> <a href="/home">home</a> | <a href="/timeline">timeline</a>| <a href="/logout">logout</a>');
  else
    output('<div id="navbar"> <a href="/index_page">home</a> | <a href="/timeline">timeline</a>');
  end
  output('</div>');
end

function create_header()
output([[
<html>
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
<form action="/register" method="POST" onsubmit="return passwords_match(this.elements['password'].value, this.elements['password2'].value)">
<table>
<tr> <td>Username</td><td><input type="text" name="username"></td> </tr>
<tr> <td>Password</td><td><input type="password" name="password"></td> </tr>
<tr> <td>Password (again)</td><td><input type="password" name="password2"></td> </tr>
<tr> <td colspan="2" align="right"><input type="submit" name="doit" value="Create an account"></td> </tr>
</table>
</form>
<h2>Already registered? Login here</h2>
<form action="/login" method="POST" >
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

function create_follow() -- Button controlled by Cookie sent w/ Response
--TODO using cookies[1,2] is dangerous, use explicit names [userid, following]
output([[
<script>
  var each_cookie = process_cookies();
  if (each_cookie.length < 3) { return; }
  var my_userid = each_cookie[1].split("=")[1];
  var following = each_cookie[2].split("=")[1];
  var userid    = each_cookie[3].split("=")[1];
  if (following == 1) {
    document.write('<a href="/follow/' + my_userid + '/' + userid + '/0" class="button">Stop following</a>');
  } else if (following == 0) {
    document.write('<a href="/follow/' + my_userid + '/' + userid + '/1" class="button">Follow this user</a>');
  }
</script>
]]);
end

function set_auth_cookie(authsecret, userid)
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  SetHttpResponseHeader('Set-Cookie', 'userid=' .. userid .. '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
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
  local postdata = alchemy("get", "post:" .. id);
  if (postdata == nil) then return false; end
  local aux      = explode("|", postdata);
  local uid      = aux[1];
  local time     = aux[2];
  local username = alchemy("get", "uid:" .. uid .. ":username");
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
  local posts = alchemy("lrange", key, start, (start + count));
  local c     = 0;
  for k,v in pairs(posts) do
      if (showPost(v)) then c = c + 1; end
      if (c == count) then break; end
  end
end

function showUserPostsWithPagination(thispage, nposts, username, userid, start, count)
  local navlink  = "";
  local nextc    = start + 10;
  local prevc    = start - 10;
  local nextlink = "";
  local prevlink = "";
  if (prevc < 0) then prevc = 0; end
  local key, u;
  if (username) then
      u = "/" .. username .. "/";
      key = "uid:" .. userid .. ":myposts";
  else
      u = "/";
      key = "uid:" .. userid .. ":posts";
  end

  showUserPosts(key, start, count);
  if (nposts ~= nil and nposts > start + count) then
      nextlink = "<a href=\"" .. thispage .. u .. nextc .."\">Older posts &raquo;</a>";
  end
  if (start > 0) then
      prevlink = "<a href=\"" .. thispage .. u .. prevc .."\">Newer posts &laquo;</a>";
  end
  if (string.len(nextlink) or string.len(prevlink)) then
      output("<div class=\"rightlink\">" .. prevlink .. " " ..  nextlink .. "</div>");
  end
end

function showLastUsers()
  local users = alchemy("sort", "global:users", "GET", "uid:*:username", "DESC", "LIMIT", 0, 10);
  output("<div>");
  for k,v in pairs(users) do
    output("<a class=\"username\" href=\"/profile/" .. v .. "\">" .. v .. "</a> ");
  end
  output("</div><br>");
end

function create_home(start, nposts, nfollowers, nfollowing)
  local thispage   = '/home';
  local s          = 0;
  if (start ~= nil) then s = start; end
  -- post needs "my_userid" as 2nd arg for haproxy
  output('<div id="postform"><form action="/post/' .. User['id'] .. '" method="POST">');
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
  showUserPostsWithPagination(thispage, nposts, false, User['id'], tonumber(s), 10);
end

function goback(msg)
  create_header();
  output('<div id ="error">' .. msg .. '<br><a href="javascript:history.back()">Please return back and try again</a></div>');
  create_footer();
end

User = {};
function loadUserInfo(userid)
  User['id']       = userid;
  User['username'] = alchemy("get", "uid:" .. userid .. ":username");
end

function isLoggedIn()
  User = {};
  local authcookie = COOKIE['auth'];
  if (authcookie ~= nil) then
    local userid = alchemy("get", "auth:" .. authcookie);
    if (userid ~= nil) then
      if (alchemy("get", "uid:" .. userid .. ":auth") ~= authcookie) then
        return false;
      end
      loadUserInfo(userid);
      return true;
    end
  end
  return false;
end

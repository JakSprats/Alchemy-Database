-- Retwis for Alchemy's Short Stack - Helper Functions (PRIVATE FUNCTIONS)

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

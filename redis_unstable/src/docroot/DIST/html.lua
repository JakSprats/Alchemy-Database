-- HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML HTML
function create_navbar(my_userid)
  local domain = GetHttpDomainPort(my_userid)
  if (LoggedIn) then
    output([[<div id="navbar">
      <a href="]] .. build_link(my_userid, 'home')     .. [[">home</a> |
      <a href="]] .. build_link(my_userid, 'timeline') .. [[">timeline</a> |
      <a href="]] .. build_link(my_userid, 'logout', my_userid) .. 
                                                          '">logout</a></div>');
  else
    output([[<div id="navbar">
      <a href="]] .. build_link(my_userid, 'index_page') .. [[">home</a> |
      <a href="]] .. build_link(my_userid, 'timeline')   ..
                                                        '">timeline</a></div>');
  end
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
--TODO using cookies[1,2] is dangerous, use explicit names [auth, userid, following]
output([[
<script>
  var each_cookie = process_cookies();
  if (each_cookie.length < 4) { return; }
  var my_userid   = each_cookie[1].split("=")[1];
  var following   = each_cookie[2].split("=")[1];
  var userid      = each_cookie[3].split("=")[1];
  //alert('my_userid: ' + my_userid + ' following: ' + following + ' userid: ' + userid);
  if (following == 1) {
    document.write('<a href="/follow/' + my_userid + '/' + userid + '/0" class="button">Stop following</a>');
  } else if (following == 0) {
    document.write('<a href="/follow/' + my_userid + '/' + userid + '/1" class="button">Follow this user</a>');
  }
</script>
]]);
end

function scriptElapsed(t)
  return '<script> output_elapsed(' .. t .. ');</script>';
end

function showPost(id)
  local postdata = redis("get", "post:" .. id);
  if (postdata == nil) then return false; end
  local aux      = explode("|", postdata);
  local userid   = aux[1];
  local time     = aux[2];
  local username = redis("get", "uid:" .. userid .. ":username");
  local post     = aux[3];
  local userlink = 
  output('<div class="post">' ..
         '<a class="username" href="' ..
                            build_link(userid, "profile", userid) ..  '">' ..
           username .. "</a>" ..  ' ' .. post .."<br>" .. '<i>posted '..
           scriptElapsed(time) ..' ago via web</i></div>');
  return true;
end

function showUserPosts(key, start, count)
  output([[
<script>
var AlchemyNow  = new Date();
var AlchemyNows = (AlchemyNow.getTime()/1000);
</script>
]]);
  local posts = redis("lrange", key, start, (start + count));
  local c     = 0;
  for k,v in pairs(posts) do
      if (showPost(v)) then c = c + 1; end
      if (c == count) then break; end
  end
end

function showUserPostsWithPagination(thispage, nposts, username, userid,
                                     start, count)
  local navlink  = "";
  local nextc    = start + 10;
  local prevc    = start - 10;
  local nextlink = "";
  local prevlink = "";
  if (prevc < 0) then prevc = 0; end
  local key, u;
  if (username) then u   = userid; key = "uid:" .. userid .. ":myposts";
  else               u   = 0;      key = "uid:" .. userid .. ":posts"; end
  showUserPosts(key, start, count);
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
                build_link(userid, "profile", userid) ..  '">' .. v .. '</a> ');
  end
  output('</div><br>');
end

function create_home(my_userid, my_username, start, nposts,
                     nfollowers, nfollowing)
  local thispage   = '/home';
  local s          = 0;
  if (start ~= nil) then s = tonumber(start); end
  output('<div id="postform"><form action="/post/' .. my_userid ..
                              '" method="POST">');
  output(my_username ..', what you are doing?');
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
  showUserPostsWithPagination(thispage, nposts, false, my_userid, s, 10);
end

function goback(my_userid, msg)
  create_header(my_userid);
  output('<div id ="error">' .. msg .. '<br><a href="javascript:history.back()">Please return back and try again</a></div>');
  create_footer();
end

MyUserid = 0;
LoggedIn = false;
function resetIsLoggedIn()
  MyUserid = 0;
  LoggedIn = false;
end
function setIsLoggedIn(userid)
  MyUserid = userid;
  LoggedIn = true;
end
function isLoggedIn()
  resetIsLoggedIn();
  local authcookie = COOKIE['auth'];
  if (authcookie ~= nil) then
    local userid = redis("get", "auth:" .. authcookie);
    if (userid ~= nil) then
      if (redis("get", "uid:" .. userid .. ":auth") ~= authcookie) then
        return false;
      end
      MyUserid = userid; LoggedIn = true; return true;
    end
  end
  return false;
end

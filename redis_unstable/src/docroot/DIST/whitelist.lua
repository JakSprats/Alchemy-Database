-- Retwis for Alchemy's App Stack - PUBLIC API
dofile "./docroot/DIST/app_stack.lua";

function I_index_page(redirect, start) 
  local inline;
  if (redirect == false) then inline = (VirginInlineCache and CanInline);
  else                        inline = false;       end
  if (CheckEtag('index_page',     inline))   then return; end
  if (CacheExists('index_page',   inline)) then
    return CacheGet('index_page', inline);
  end
  local my_userid;
  if (LoggedIn) then my_userid = MyUserid;
  else               my_userid = 0;        end
  init_output();
  create_header(inline, my_userid); create_welcome(); create_footer(inline);
  CachePutOutput('index_page', inline);
  return flush_output();
end
function WL_index_page(rw, start)
  if (isLoggedIn()) then SetHttpRedirect(build_link(0, 'home')); return; end
  InitRequest();
  return I_index_page(false, start);
end

function I_home(my_userid, my_username, s)
  setIsLoggedIn(my_userid); -- used for internal redirects
  local nposts    = redis("llen",  "uid:" .. my_userid .. ":posts");
  local nflwers   = redis("scard", "uid:" .. my_userid .. ":followers");
  local nflwing   = redis("scard", "uid:" .. my_userid .. ":following");
  local my_userid = MyUserid;
  if (CheckEtag('home', my_userid, nposts, nflwers, nflwing)) then return; end
  init_output();
  create_header(false, my_userid);
  create_home(my_userid, my_username, s, nposts, nflwers, nflwing);
  create_footer(false);
  return flush_output();
end
function WL_home(rw, s)
  if (isLoggedIn() == false) then
    SetHttpRedirect(build_link(0, 'index_page')); return;
  else
    local my_userid   = MyUserid;
    if (IsCorrectNode(my_userid) == false) then -- home ONLY to shard-node
      SetHttpRedirect(build_link(my_userid, 'home')); return;
    end
    InitRequest();
    local my_username = redis("get", "uid:" .. my_userid .. ":username");
    return I_home(my_userid, my_username, s);
  end
end

function D_profile(isl, my_userid, userid)
  local f   = -1;
  if (isl and my_userid ~= userid) then
    local isf = redis("sismember", "uid:" .. my_userid .. ":following", userid);
    if (isf == 1) then f = 1;
    else               f = 0; end
  end
  SetHttpResponseHeader('Set-Cookie', 'following='   .. f ..
                                      '; Max-Age=1; path=/;');
  SetHttpResponseHeader('Set-Cookie', 'other_userid=' .. userid ..
                                      '; Max-Age=1; path=/;');
  return f;
end
function I_profile(userid, username, s)
  local isl = isLoggedIn(); -- populates MyUserid
  local my_userid;
  if (LoggedIn) then my_userid = MyUserid;
  else               my_userid = 0;       end
  local f = D_profile(isl, my_userid, userid);
  local nposts = redis("llen", "uid:" .. userid .. ":myposts")
  if (CheckEtag('profile', isl, userid, nposts, s)) then return; end
  s = tonumber(s);
  if (s == nil) then s = 0; end
  if (s == 0) then -- CACHE only 1st page
    if (CacheExists('profile', isl, userid, nposts)) then
      return CacheGet('profile', isl, userid, nposts); end end

  init_output();
  create_header(false, my_userid);
  output("<h2 class=\"username\">" .. username .. "</h2>");
  create_follow(my_userid);
  showUserPostsWithPagination('profile', nposts, username, userid, s, 10);
  create_footer(false);
  CachePutOutput('profile', isl, userid, nposts);
  return flush_output();
end
function WL_profile(rw, userid, start)
  if (is_empty(userid)) then
    SetHttpRedirect(build_link(0, 'index_page')); return;
  end
  if (IsCorrectNode(userid) == false) then -- profile ONLY to shard-node
    SetHttpRedirect(build_link(userid, 'profile', userid, start)); return;
  end
  local username = redis("get", "uid:" .. userid .. ":username");
  if (username == nil) then -- FOR: hackers doing userid scanning
    SetHttpRedirect(build_link(0, 'index_page')); return;
  end
  InitRequest();
  return I_profile(userid, username, start);
end

function WL_follow(rw, muserid, userid, follow) -- muserid used ONLY by haproxy
  if (is_empty(userid) or is_empty(follow) or isLoggedIn() == false) then
    SetHttpRedirect(build_link(0, 'index_page')); return;
  end
  local my_userid = MyUserid;
  if (userid == my_userid) then -- FOR: URL hackers
    SetHttpRedirect(build_link(my_userid, 'home')); return;
  end
  if (IsCorrectNode(my_userid) == false) then -- follow ONLY to shard-node
    SetHttpRedirect(build_link(my_userid, 'follow', my_userid, userid, follow));
    return;
  end
  local username = redis("get", "uid:" .. userid .. ":username");
  if (username == nil) then -- FOR: hackers doing userid scanning
    SetHttpRedirect(build_link(0, 'home')); return;
  end
  InitRequest();
  local_follow(my_userid, userid, follow);
  call_sync(global_follow, 'global_follow', my_userid, userid, follow);
  if (IsCorrectNode(userid)) then -- profile ONLY to shard-node
    return I_profile(userid, username, 0); -- internal redirect
  else
    D_profile(true, my_userid, userid); -- add [following, other_userid] cookies
    SetHttpRedirect(build_link(userid, 'profile', userid)); return;
  end
end

function WL_register(rw, o_username, o_password)
  init_output();
  if (is_empty(o_username) or is_empty(o_password)) then
    goback(0, "Username or Password is Empty"); return flush_output();
  end
  local username   = url_decode(o_username);
  local password   = url_decode(o_password);
  if (redis("get", "username:" .. username .. ":id")) then
    goback(0, "Sorry the selected username is already in use.");
    return flush_output();
  end
  -- "username" assigned via global pool, so it must be sharded data
  local node = GetUsernameNode(username);
  if (node ~= MyNodeId) then -- register ONLY to shard-node
    SetHttpRedirect(build_link_node(node, 'register', o_username, o_password));
    return;
  end
  InitRequest();
  -- Everything is ok, Register the user!
  local my_userid  = IncrementAutoInc('NextUserId');
  local authsecret = call_sync(global_register, 'global_register',
                               my_userid,       username);
  local_register(my_userid, username, password);

  -- User registered -> Log him in
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. 
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');

  create_header(false, my_userid);
  output('<h2>Welcome aboard!</h2> Hey ' .. username ..
             ', now you have an account, <a href=' .. 
            build_link(my_userid, "index_page") .. 
             '>a good start is to write your first message!</a>.');
  create_footer(false);
  return flush_output();
end

function WL_logout(rw, muserid) -- muserid used for URL haproxy LoadBalancing
  if (isLoggedIn() == false) then
    SetHttpRedirect(build_link(muserid, 'index_page')); return;
  end
  local my_userid     = MyUserid;
  if (IsCorrectNode(my_userid) == false) then -- logout BETTER at shard-node
    SetHttpRedirect(build_link(my_userid, 'logout', my_userid));
    return;
  end
  InitRequest();
  call_sync(global_logout, 'global_logout', my_userid);
  return I_index_page(true, 0); -- internal redirect
end

function WL_login(rw, o_username, o_password)
  init_output();
  if (is_empty(o_username) or is_empty(o_password)) then
    goback(0, "Enter both username and password to login.");
    return flush_output();
  end
  local my_username  = url_decode(o_username);
  local password     = url_decode(o_password);
  local my_userid    = redis("get", "username:" .. my_username ..":id");
  if (my_userid == nil) then
    goback(0, "Wrong username or password"); return flush_output();
  end
  if (IsCorrectNode(my_userid) == false) then -- login ONLY 2 shard-node
    SetHttpRedirect(build_link(my_userid, 'login', o_username, o_password));
    return;
  end
  local realpassword = redis("get", "uid:" .. my_userid .. ":password");
  if (realpassword ~= password) then
    goback(0, "Wrong username or password"); return flush_output();
  end
  InitRequest();
  -- Username / password OK, set the cookie and internal redirect to home
  local authsecret   = redis("get", "uid:" .. my_userid .. ":auth");
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret ..
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  return I_home(my_userid, my_username, 0); -- internal redirect
end

function WL_post(rw, muserid, o_msg) -- muserid for URL haproxy LoadBalancing
  if (is_empty(o_msg) or isLoggedIn() == false) then
    SetHttpRedirect(build_link(muserid, 'index_page')); return;
  end
  local my_userid = MyUserid;
  if (IsCorrectNode(my_userid) == false) then -- post ONLY to shard-node
    SetHttpRedirect(build_link(my_userid, 'post', my_userid, o_msg)); return;
  end
  InitRequest();
  local msg         = url_decode(o_msg);
  local ts          = gettime();
  local postid      = IncrementAutoInc('NextPostId');
  call_sync(global_post, 'global_post', my_userid, postid, ts, msg);
  local_post(my_userid, postid, msg, ts);
  local my_username = redis("get", "uid:" .. my_userid .. ":username");
  return I_home(my_userid, my_username, 0); -- internal redirect
end

function WL_timeline(rw)
  local n_users   = redis("scard",  "global:users");
  local last_post = redis("lindex", "global:timeline", 0);
  if (CheckEtag('timeline',     n_users, last_post)) then return; end
  if (CacheExists('tineline',   n_users, last_post)) then
    return CacheGet('timeline', n_users, last_post);
  end
  InitRequest();
  local my_userid;
  if (isLoggedIn()) then my_userid = MyUserid;
  else                   my_userid = 0;        end
  init_output();
  create_header(false, my_userid);
  showLastUsers();
  output('<i>Latest 20 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 20);
  create_footer(false);
  CachePutOutput('timeline', n_users, last_post);
  return flush_output();
end

-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
function WL_hello_world() return 'HELLO WORLD'; end

redis("set", 'HELLO WORLD', 'HELLO WORLD');
function WL_hello_world_data() return redis("get", 'HELLO WORLD'); end

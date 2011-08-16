dofile "./docroot/retwis_helper.lua";
-- Retwis for Alchemy's Short Stack - PUBLIC API

function I_index_page(start) 
  if (CheckEtag('index_page')) then   return;                        end
  if (CacheExists('index_page')) then return CacheGet('index_page'); end
  init_output();
  create_header(); create_welcome(); create_footer();
  CachePutOutput('index_page');
  return flush_output();
end
function WL_index_page(start) 
  if (isLoggedIn()) then SetHttpRedirect('/home'); return; end
  return I_index_page(start);
end

function I_home(s) 
  local nflwers   = redis("scard", "uid:" .. User['id'] .. ":followers");
  local nflwing   = redis("scard", "uid:" .. User['id'] .. ":following");
  local nposts    = redis("llen",  "uid:" .. User['id'] .. ":posts");
  local my_userid = User['id'];
  if (CheckEtag('home', my_userid, nposts, nflwers, nflwing)) then return; end
  init_output();
  create_header(); create_home(s, nposts, nflwers, nflwing); create_footer();
  return flush_output();
end
function WL_home(s) 
  if (isLoggedIn() == false) then SetHttpRedirect('/index_page'); return; end
  return I_home(s);
end

function I_profile(username, s) -- NOTE: username is NOT encoded
  local page      = '/profile';
  local userid    = redis("get", "username:" .. username .. ":id")
  if (userid == nil) then SetHttpRedirect('/index_page'); return; end
  local nposts    = redis("llen", "uid:" .. userid .. ":myposts");
  local isl       = isLoggedIn(); -- populates User[]
  local my_userid = User['id'];
  local f         = -1;
  if (isl and my_userid ~= userid) then
    local isf = redis("sismember", "uid:" .. my_userid .. ":following",userid);
    if (isf == 1) then f = 1;
    else               f = 0; end
  end
  SetHttpResponseHeader('Set-Cookie', 'following=' .. f ..
                                      '; Max-Age=1; path=/;');
  SetHttpResponseHeader('Set-Cookie', 'otheruser=' .. userid ..
                                      '; Max-Age=1; path=/;');
  if (CheckEtag('profile', userid, nposts, s)) then return; end
  if (s == nil or tonumber(s) == 0) then -- CACHE only 1st page
    if (CacheExists('profile', isl, userid, nposts)) then
      return CacheGet('profile', isl, userid, nposts);
    end
  end

  init_output();
  create_header();
  output("<h2 class=\"username\">" .. username .. "</h2>");
  create_follow();
  s = tonumber(s);
  if (s == nil) then s = 0; end
  showUserPostsWithPagination(page, nposts, username, userid, s, 10);
  create_footer();
  CachePutOutput('profile', isl, userid, nposts);
  return flush_output();
end
function WL_profile(username, s)
  if (is_empty(username)) then SetHttpRedirect('/index_page'); return; end
  username = url_decode(username);
  isLoggedIn(); -- this call effects links
  return I_profile(username, s);
end

function WL_follow(my_userid, userid, follow) -- my_userid only for proxying
  if (is_empty(userid) or is_empty(follow)) then
    SetHttpRedirect('/index_page'); return;
  end
  if (isLoggedIn() == false) then SetHttpRedirect('/index_page'); return; end
  if (userid ~= User['id']) then
    local f = tonumber(follow);
    if (f == 1) then
      redis("sadd", "uid:" .. userid     .. ":followers", User['id']);
      redis("sadd", "uid:" .. User['id'] .. ":following", userid);
    else 
      redis("srem", "uid:" .. userid     .. ":followers", User['id']);
      redis("srem", "uid:" .. User['id'] .. ":following", userid);
    end
  end
  local username = redis("get", "uid:" .. userid .. ":username");
  return I_profile(username); -- internal redirect
end

function WL_register(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback("Either Username or Password is Empty"); return flush_output();
  end
  username = url_decode(username);
  password = url_decode(password);
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

  -- User registered -> Log him in
  set_auth_cookie(authsecret, userid);
  loadUserInfo(userid);

  create_header();
  output('<h2>Welcome aboard!</h2> Hey ' .. username .. ', now you have an account, <a href="/index_page">a good start is to write your first message!</a>.');
  create_footer();
  return flush_output();
end

function WL_logout()
  if (isLoggedIn() == false) then SetHttpRedirect('/index_page'); return; end

  local newauthsecret = getrand();
  local userid        = User['id'];
  local oldauthsecret = redis("get", "uid:" .. userid .. ":auth");

  redis("set", "uid:" .. userid .. ":auth", newauthsecret);
  redis("set", "auth:" .. newauthsecret, userid);
  redis("delete", "auth:" .. oldauthsecret);
  return I_index_page(0); -- internal redirect
end

function WL_login(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback("You need to enter both username and password to login.");
    return flush_output();
  end
  username = url_decode(username);
  password = url_decode(password);
  local userid = redis("get", "username:" .. username ..":id");
  if (userid == nil) then
    goback("Wrong username or password"); return flush_output();
  end
  local realpassword = redis("get", "uid:" .. userid .. ":password");
  if (realpassword ~= password) then
    goback("Wrong username or password"); return flush_output();
  end

  -- Username / password OK, set the cookie and internal redirect to index
  local authsecret = redis("get", "uid:" .. userid .. ":auth");
  set_auth_cookie(authsecret, userid);
  loadUserInfo(userid); -- log user in
  return I_home(); -- internal redirect
end

function WL_post(my_userid, msg)
  if (isLoggedIn() == false or is_empty(msg)) then
    SetHttpRedirect('/index_page'); return;
  end
  msg = url_decode(msg);

  local ts = os.time();
  local postid = redis("incr", "global:nextPostId");
  local post   = User['id'] .. "|" .. ts .. "|" .. msg;
  redis("set", "post:" .. postid, post);
  local followers = redis("smembers", "uid:" .. User['id'] .. ":followers");
  table.insert(followers, User['id']); -- Add the post to our own posts too 

  for k,v in pairs(followers) do
    redis("lpush", "uid:" .. v .. ":posts", postid);
  end
  redis("lpush", "uid:" .. User['id'] .. ":myposts", postid); -- for /profile

  -- Push post to timeline, and trim timeline to newest 1000 elements.
  redis("lpush", "global:timeline", postid);
  redis("ltrim", "global:timeline", 0, 1000);

  return I_home(); -- internal redirect
end

function WL_timeline()
  -- dependencies: n_global_users, n_global_timeline
  -- page is too volatile to cache -> NO CACHING
  isLoggedIn(); -- this call effects links
  init_output();
  create_header();
  showLastUsers();
  output('<i>Latest 20 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 20);
  create_footer();
  return flush_output();
end

-- DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
function WL_hello_world()
  return 'HELLO WORLD';
end
redis("set", 'HELLO WORLD', 'HELLO WORLD');
function WL_hello_world_data()
  return redis("get", 'HELLO WORLD');
end

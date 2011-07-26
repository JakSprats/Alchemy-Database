dofile "./docroot/retwis_helper.lua";
-- Retwis for Alchemy's Short Stack - PUBLIC API
function WL_index_page(start) 
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

function WL_register(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback("Either Username or Password is Empty");
    return flush_output();
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
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');

  create_header();
  output('<h2>Welcome aboard!</h2> Hey ' .. username .. ', now you have an account, <a href="/index_page">a good start is to write your first message!</a>.');
  create_footer();
  return flush_output();
end

function WL_logout()
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

function WL_post(msg)
  local isl = isLoggedIn();
  if (isl == false or is_empty(msg)) then
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

  SetHttpRedirect('/index_page');
end

function WL_timeline()
  init_output();
  create_header();
  showLastUsers();
  output('<i>Latest 50 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 50);
  create_footer();
  return flush_output();
end

function WL_profile(username, start)
  local thispage = '/profile';
  if (is_empty(username)) then
    SetHttpRedirect('/index_page'); return;
  end
  username = url_decode(username);
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
  showUserPostsWithPagination(thispage, username, userid, s, 10);
  create_footer();
  return flush_output();
end

function WL_follow(userid, follow)
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
  SetHttpRedirect('/profile/' .. url_encode(username));
end

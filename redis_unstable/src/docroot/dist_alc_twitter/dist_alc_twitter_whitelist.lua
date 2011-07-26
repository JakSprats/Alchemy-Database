dofile "./docroot/dist_alc_twitter/dist_alc_twitter_helper.lua";
-- Retwis for Alchemy's Short Stack - PUBLIC API

function WL_index_page(start) 
  init_output();
  local thispage = 'index_page';
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = getrand();  end
  create_header(my_userid);
  if (LoggedIn) then
    if (IsCorrectNode(my_userid) == false) then -- home ONLY to shard-node
      SetHttpRedirect(build_link(my_userid, 'index_page'));
      return;
    end
    create_home(thispage, my_userid, tonumber(start));
  else 
    create_welcome();
  end
  create_footer();
  return flush_output();
end

function WL_register(username, password)
  init_output();
  if (is_empty(username) or is_empty(password)) then
    goback(getrand(), "Either Username or Password is Empty");
    return flush_output();
  end
  username         = url_decode(username);
  password         = url_decode(password);
  if (redis("get", "username:" .. username .. ":id")) then
    goback(getrand(), "Sorry the selected username is already in use.");
    return flush_output();
  end
  -- Everything is ok, Register the user!
  local my_userid  = IncrementAutoInc('NextUserId');
  local authsecret = call_sync(global_register, 'global_register',
                               my_userid,       username);
  local_register(my_userid, username, password);

  -- User registered -> Log him in
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. 
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');

  create_header(my_userid);
  output('<h2>Welcome aboard!</h2> Hey ' .. username ..
             ', now you have an account, <a href=' .. 
            build_link(my_userid, "index_page") .. 
             '>a good start is to write your first message!</a>.');
  create_footer();
  return flush_output();
end

function WL_login(o_username, o_password)
  init_output();
  if (is_empty(o_username) or is_empty(o_password)) then
    goback(getrand(),
                      "You need to enter both username and password to login.");
    return flush_output();
  end
  username           = url_decode(o_username);
  password           = url_decode(o_password);
  local my_userid    = redis("get", "username:" .. username ..":id");
  if (my_userid == nil) then
    goback(getrand(), "Wrong username or password");
    return flush_output();
  end
  if (IsCorrectNode(my_userid) == false) then -- login ONLY 2 shard-node
    SetHttpRedirect(build_link(my_userid, 'login', o_username, o_password));
    return;
  end
  local realpassword = redis("get", "uid:" .. my_userid .. ":password");
  if (realpassword ~= password) then
    goback(getrand(), "Wrong username or password");
    return flush_output();
  end
  -- Username / password OK, set the cookie and redirect to index.php
  local authsecret   = redis("get", "uid:" .. my_userid .. ":auth");
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret ..
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  SetHttpRedirect(build_link(my_userid, 'index_page'));
end

function WL_logout(muserid) -- muserid used for URL haproxy LoadBalancing
  if (isLoggedIn() == false) then
    SetHttpRedirect(build_link(muserid, 'index_page')); return;
  end
  local my_userid     = User['id'];
  if (IsCorrectNode(my_userid) == false) then -- logout BETTER at shard-node
    SetHttpRedirect(build_link(my_userid, 'logout', my_userid));
    return;
  end
  call_sync(global_logout, 'global_logout', my_userid);
  SetHttpRedirect(build_link(getrand(), 'index_page'));
end

function WL_post(muserid, o_msg) -- muserid used for URL haproxy LoadBalancing
  if (is_empty(o_msg) or isLoggedIn() == false) then
    SetHttpRedirect(build_link(muserid, 'index_page')); return;
  end
  local my_userid = User['id'];
  if (IsCorrectNode(my_userid) == false) then -- post ONLY to shard-node
    SetHttpRedirect(build_link(my_userid, 'post', my_userid, o_msg)); return;
  end
  msg             = url_decode(o_msg);
  print ("post: " .. msg);
  local ts        = os.time();
  local postid    = IncrementAutoInc('NextPostId');
  local my_userid = User['id'];

  call_sync(global_post, 'global_post', my_userid, postid, ts, msg);
  local_post(my_userid, postid, ts, msg);
  SetHttpRedirect(build_link(my_userid, 'index_page'));
end

function WL_timeline()
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = getrand();  end
  init_output();
  create_header(my_userid);
  showLastUsers();
  output('<i>Latest 50 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 50);
  create_footer();
  return flush_output();
end

function WL_profile(userid, start)
  local thispage = 'profile';
  if (is_empty(userid)) then
    SetHttpRedirect(build_link(getrand(), 'index_page')); return;
  end
  if (IsCorrectNode(userid) == false) then -- profile ONLY to shard-node
    SetHttpRedirect(build_link(userid, 'profile', userid, start)); return;
  end
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = getrand();  end
  local username = redis("get", "uid:" .. userid .. ":username");
  init_output();
  create_header(my_userid);
  output('<h2 class="username">' .. username .. '</h2>');
  if (LoggedIn and my_userid ~= userid) then
    local isfollowing = redis("sismember",
                              "uid:" .. User['id'] .. ":following", userid);
    if (isfollowing == 1) then
      output('<a href="' ..
              build_link(my_userid, "follow", my_userid, userid, "0") ..
                                    '" class="button">Stop following</a>');
    else
      output('<a href="' ..
              build_link(my_userid, "follow", my_userid, userid, "1") ..
                                    '" class="button">Follow</a>');
    end
  end
  local s = tonumber(start);
  if (s == nil) then s = 0; end
  showUserPostsWithPagination(thispage, username, userid, s, 10);
  create_footer();
  return flush_output();
end

function WL_follow(muserid, userid, follow) -- muserid used ONLY by haproxy
  if (is_empty(userid) or is_empty(follow) or isLoggedIn() == false) then
    SetHttpRedirect(build_link(getrand(), 'index_page')); return;
  end
  local my_userid = User['id'];
  if (IsCorrectNode(my_userid) == false) then -- follow ONLY to shard-node
    SetHttpRedirect(build_link(my_userid, 'follow', my_userid, userid, follow));
    return;
  end
  if (userid ~= my_userid) then
    call_sync(global_follow, 'global_follow', my_userid, userid, follow);
    local_follow(my_userid, userid, follow);
  end
  SetHttpRedirect(build_link(userid, 'profile', userid));
end

-- Retwis for Alchemy's Short Stack - PUBLIC API
module("whitelist", package.seeall);

function index_page(start) 
  init_output();
  local thispage = 'index_page';
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = 0;          end
  create_header(my_userid);
  if (my_userid ~= 0) then
    create_home(thispage, tonumber(start));
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
  username         = url_decode(username);
  password         = url_decode(password);
  if (redis("get", "username:" .. username .. ":id")) then
    goback("Sorry the selected username is already in use.");
    return flush_output();
  end
  -- Everything is ok, Register the user!
  local my_userid  = IncrementAutoInc('NextUserId');
  local authsecret = getrand();
  call_sync(sync_register, 'sync_register', my_userid, username, authsecret);
  local_register(my_userid, password, authsecret);

  -- User registered -> Log him in
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret .. 
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');

  create_header(my_userid);
  output('<h2>Welcome aboard!</h2> Hey ' .. username .. ', now you have an account, <a href="/index_page">a good start is to write your first message!</a>.');
  create_footer();
  return flush_output();
end

function login(o_username, o_password)
  init_output();
  if (is_empty(o_username) or is_empty(o_password)) then
    goback("You need to enter both username and password to login.");
    return flush_output();
  end
  username           = url_decode(o_username);
  password           = url_decode(o_password);
  local my_userid    = redis("get", "username:" .. username ..":id");
  if (my_userid == nil) then
    goback("Wrong username or password");
    return flush_output();
  end
  if (IsCorrectNode(my_userid) == false) then -- login ONLY to shard-node
    print ('login: wrong node: redirect to: ' .. GetHttpDomainPort(my_userid));
    SetHttpRedirect(GetHttpDomainPort(my_userid) .. 'login/' ..
                    o_username .. '/' .. o_password);
    return;
  end
  local realpassword = redis("get", "uid:" .. my_userid .. ":password");
  if (realpassword ~= password) then
    goback("Wrong username or password");
    return flush_output();
  end
  -- Username / password OK, set the cookie and redirect to index.php
  local authsecret   = redis("get", "uid:" .. my_userid .. ":auth");
  SetHttpResponseHeader('Set-Cookie', 'auth=' .. authsecret ..
                            '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  SetHttpRedirect('/index_page');
end

function logout()
  if (isLoggedIn() == false) then
    SetHttpRedirect('/index_page'); return;
  end
  local newauthsecret = getrand();
  local my_userid     = User['id'];
  local oldauthsecret = redis("get", "uid:" .. my_userid .. ":auth");
  call_sync(sync_logout, 'sync_logout', my_userid, oldauthsecret,
                                        newauthsecret);
  SetHttpRedirect('/index_page');
end

function post(msg) -- usename is used for consisten hashing -> ignore
  if (is_empty(msg) or isLoggedIn() == false) then
    SetHttpRedirect('/index_page'); return;
  end
  msg = url_decode(msg);
  print ("post: " .. msg);

  local ts        = os.time();
  local postid    = IncrementAutoInc('PostUserId');
  local my_userid = User['id'];

  call_sync(sync_post, 'sync_post', my_userid, postid, ts, msg);
  local_post(my_userid, postid, ts, msg);

  SetHttpRedirect('/index_page');
end

function timeline()
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = 0;          end
  init_output();
  create_header(my_userid);
  showLastUsers();
  output('<i>Latest 50 messages from users aroud the world!</i><br>');
  showUserPosts("global:timeline", 0, 50);
  create_footer();
  return flush_output();
end

function profile(userid, start)
  local thispage = 'profile';
  if (is_empty(userid)) then
    SetHttpRedirect('/index_page'); return;
  end
  local my_userid;
  if (isLoggedIn()) then my_userid = User['id'];
  else                   my_userid = 0;          end
  local username = redis("get", "uid:" .. userid .. ":username");
  init_output();
  create_header(my_userid);
  output("<h2 class=\"username\">" .. username .. "</h2>");
  if (my_userid ~= 0 and my_userid ~= userid) then
    local isfollowing = redis("sismember",
                              "uid:" .. User['id'] .. ":following", userid);
    if (isfollowing == 1) then
      output("<a href=\"/follow/" .. userid ..
                                    "/0\" class=\"button\">Stop following</a>");
    else
      output("<a href=\"/follow/" .. userid ..
                                     "/1\" class=\"button\">Follow</a>");
    end
  end
  local s = tonumber(start);
  if (s == nil) then s = 0; end
  showUserPostsWithPagination(thispage, username, userid, s, 10);
  create_footer();
  return flush_output();
end

function follow(userid, follow)
  if (is_empty(userid) or is_empty(follow) or isLoggedIn() == false) then
    SetHttpRedirect('/index_page'); return;
  end
  local my_userid = User['id'];
  if (userid ~= my_userid) then
    call_sync(sync_follow, 'sync_follow', my_userid, userid, follow);
  end
  SetHttpRedirect('/profile/' .. userid);
end

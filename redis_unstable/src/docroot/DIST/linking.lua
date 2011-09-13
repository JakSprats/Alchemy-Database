
-- PAGE read/write OPS (CONSTANTS)
GLOBAL_OP = 1;
READ_OP   = 2;
WRITE_OP  = 3;
PageOperations = {};
PageOperations['register']   = WRITE_OP;
PageOperations['logout']     = WRITE_OP;
PageOperations['post']       = WRITE_OP;
PageOperations['follow']     = WRITE_OP;
PageOperations['home']       = READ_OP;
PageOperations['profile']    = READ_OP;
PageOperations['login']      = READ_OP;
PageOperations['index_page'] = GLOBAL_OP;
PageOperations['timeline']   = GLOBAL_OP;

function UserNode(o_num)
  local num   = tonumber(o_num);
  local bid   = (math.floor(num  / AutoIncRange) % #BridgeData);
  local lnid  = (num % AutoIncRange) % NumPeers;
  if (lnid == 0) then lnid = NumPeers; end
  return (bid * NumPeers) + lnid; -- TODO need a table FirstPeer[bid];
end

local LastGlobalRead = 0;
function GetHttpDomainPort(rw, num)
  local which;
  if     (rw == GLOBAL_OP) then -- ROUND-ROBIN ALL NODES
    LastGlobalRead = LastGlobalRead + 1;
    if (LastGlobalRead > #GlobalReadData) then LastGlobalRead = 1; end
    which          = LastGlobalRead;
  else
    which = UserNode(num); 
  end
  --print ('GetHttpDomainPort: num: ' .. num .. ' which: ' .. which);
  return 'http://' .. NodeData[which]["lbdomain"] .. ':' .. 
                      NodeData[which]["lbport"]   .. '/';
end
function IsCorrectNode(rw, num)
  if     (rw == GLOBAL_OP) then return true;
  elseif (rw == WRITE_OP)  then return (UserNode(num) == MyNodeId);
  elseif (rw == READ_OP)   then
     local unode = UserNode(num);
     if (unode                == MyNodeId) then return true;  end
     if (MasterData[MyNodeId] == nil)      then return false; end
     return (unode == MasterData[MyNodeId]);
  end
end
function GetUsernameNode(username)
  local uns = SHA1(username);
  local tot = 0;
  for i = 1, #uns do
    local c = string.byte(uns, i);
    tot     = tot + tonumber(c);
  end 
  --print ('GetUsernameNode: username: ' .. username .. ' tot: ' .. tot);
  return UserNode(tot);
end

function build_op_path(rw, page)
  local oppath;
  if     (rw == GLOBAL_OP) then oppath = "/g";
  elseif (rw == WRITE_OP)  then oppath = "/w";
  elseif (rw == READ_OP)   then oppath = "/r";
  else                          assert(false, "Page: " .. page .. 
                                              " not in PageOperations[]");
  end
  return oppath;
end
function build_path(arg1, arg2, arg3)
  local path = '';
  if (arg1 ~= nil) then     path =         '/' .. arg1;
    if (arg2 ~= nil) then   path = path .. '/' .. arg2;
      if (arg3 ~= nil) then path = path .. '/' .. arg3; end end end
  return path;
end
function build_link(my_userid, page, arg1, arg2, arg3)
  local rw     = PageOperations[page];
  local path   = build_path(arg1, arg2, arg3);
  local oppath = build_op_path(rw, page)
  return GetHttpDomainPort(rw, my_userid) .. page .. oppath .. path;
end
function build_link_node(node, page, arg1, arg2, arg3)
  local rw     = PageOperations[page];
  local path   = build_path(arg1, arg2, arg3);
  local oppath = build_op_path(rw, page)
  return 'http://' .. NodeData[node]["fdomain"] .. ':' .. 
                      NodeData[node]["lbport"]   .. '/' .. 
                          page .. oppath .. path;
end

MyUserid   = 0;
LoggedIn   = false;
function initPerRequestIsLoggedIn()
  MyUserid = 0;
  LoggedIn = false;
end
function setIsLoggedIn(userid)
  MyUserid = userid;
  LoggedIn = true;
end
function isLoggedIn()
  initPerRequestIsLoggedIn();
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

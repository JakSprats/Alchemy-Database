
-- GLOBAL_AUTO_INC_COUNTER GLOBAL_AUTO_INC_COUNTER GLOBAL_AUTO_INC_COUNTER
AutoInc = {};

function giveInitialAutoInc(myid)
  if (NodeData[myid]['isb']) then
    local bid = tonumber(NodeData[myid]['bridge']) -- NOTE B2B IDs -> computed
    return (1 + (bid - 1) * AutoIncRange);
  else
    local id = 1 + (BridgeId - 1) * AutoIncRange;
    for inid, bid in pairs(IslandData) do
      if (myid == inid)     then break; end
      if (bid  == BridgeId) then id = id + 1; end
    end
    return id;
  end
end
function InitAutoInc(name)
  local id = redis("get", 'global:' .. name);
  if (id == nil) then
    id = giveInitialAutoInc(MyNodeId);
    redis("set", 'global:' .. name, id);
  end
  AutoInc[name] = id;
end
function getNextAutoInc(num)
  local bid  = (math.floor(num / AutoIncRange) % #BridgeData + 1);
  num        = num + AutoIncStep;
  local nbid = (math.floor(num / AutoIncRange) % #BridgeData + 1);
  if (bid ~= nbid) then num = num + AutoIncRange; end
  return num;
end
function IncrementAutoInc(name)
  AutoInc[name] = getNextAutoInc(AutoInc[name]);
  redis("set", 'global:' .. name, AutoInc[name]);
  return AutoInc[name];
end
function getPreviousAutoInc(o_num)
  local num = tonumber(o_num);
  --local was = num;
  local bid  = (math.floor(num  / AutoIncRange) % #BridgeData + 1);
  local prev = tonumber(num) - AutoIncStep;
  local nbid = (math.floor(prev / AutoIncRange) % #BridgeData + 1);
  if (bid ~= nbid) then prev = prev - AutoIncRange; end
  --print ('getPreviousAutoInc: was: ' .. was .. ' prev: ' .. prev);
  return prev;
end
-- BRIDGE_AUTO_INC_COUNTER BRIDGE_AUTO_INC_COUNTER BRIDGE_AUTO_INC_COUNTER
function InitBridgeAutoInc(name)
  local id = redis("get", 'global:' .. name);
  if (id == nil) then
    id = giveInitialAutoInc(MyNodeId);
    redis("set", 'global:' .. name, id);
  end
  AutoInc[name] = id;
end
function getNextBridgeAutoInc(num)
  num       = num + 1;
  local bid = (num % AutoIncRange);
  if (bid == 0) then num = num + AutoIncRange; end
  return num;
end
function IncrementBridgeAutoInc(name)
  AutoInc[name] = getNextBridgeAutoInc(AutoInc[name]);
  redis("set", 'global:' .. name, AutoInc[name]);
  return AutoInc[name];
end
function getPreviousBridgeAutoInc(o_num)
  local num = tonumber(o_num);
  if (num == 1) then return 1; end
  --local was = num;
  num = num - 1;
  local bid = (num % AutoIncRange);
  if (bid == 0) then num = num - AutoIncRange; end
  --print ('getPreviousBridgeAutoInc: was: ' .. was .. ' num: ' .. num);
  return num;
end

-- STATELESS_VARS STATELESS_VARS STATELESS_VARS STATELESS_VARS STATELESS_VARS
local SALT0 = "#&^#sgDFTY|{$%^|@#$%^PSE2562346tgjgsdfgjads";
local SALT1 = "a;sdklrj	2e;lkfrn2<F2>o;4hqeofrh";
local SALT2 = "SDG:SD%:#$R:WEF:SER:@#:$@#:R$VSDSDR:!@#$5;";
local SALT3 = "2345KLJ234LKGNERKL7JL56KEJUKLYHJKL76J8LJ6HKLGNMJFOPUKI]89-"
function __create_sha1_variable(name, my_userid, val)                -- private
  if (my_userid == nil or val == nil) then
    print ('USAGE: get_sha1_variable(name, my_userid)');
    return 0;
  end
  return SHA1(SALT0 .. val .. SALT1 .. name .. SALT2 .. my_userid .. SALT3);
end
function __create_sha1_name(name, my_userid)                         -- private
  return 'uid:' .. my_userid .. ':' .. name;
end
function __set_sha1_variable(name, my_userid, val)                   -- private
  local rname = __create_sha1_name(name, my_userid);
  local rval  = __create_sha1_variable(name, my_userid, val);
  redis("set", rname, val);
  return rval;
end
function init_sha1_variable(name, my_userid)                          -- PUBLIC
  return __set_sha1_variable(name, my_userid, 0);
end
function get_sha1_variable(name, my_userid)                           -- PUBLIC
  local rname = __create_sha1_name(name, my_userid);
  return redis("get", rname);
end
function incr_sha1_variable(name, my_userid)                          -- PUBLIC
  local rval = get_sha1_variable(name, my_userid);
  return __set_sha1_variable(name, my_userid, (rval + 1))
end

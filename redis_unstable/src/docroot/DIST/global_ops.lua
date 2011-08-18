
-- AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER AUTO_INC_COUNTER
AutoInc = {};
function InitAutoInc(name)
  local inc = AutoIncRange * (MyNodeId - 1);
  local id  = redis("get", 'global:' .. name);
  if (id == nil) then
      id = inc;
      redis("set", 'global:' .. name, id);
  end
  AutoInc[name] = id;
end
function IncrementAutoInc(name)
  --local was = AutoInc[name];
  AutoInc[name] = AutoInc[name] + 1;
  if ((AutoInc[name] % AutoIncRange) == 0) then
    AutoInc[name] = AutoInc[name] + (AutoIncRange * (NumNodes - 1));
  end
  redis("set", 'global:' .. name, AutoInc[name]);
  --print ('Autoinc[' .. name .. '] was: ' .. was .. ' is: ' .. AutoInc[name]);
  return AutoInc[name];
end
function getPreviousAutoInc(num)
  local was = num;
  num = tonumber(num) - 1;
  if ((num % AutoIncRange) == (AutoIncRange -1)) then
    num = num - (AutoIncRange * (NumNodes - 1));
  end
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

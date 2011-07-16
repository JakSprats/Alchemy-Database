module("whitelist", package.seeall);

function index_page() 
  return '<html>HI I AM THE INDEX PAGE</html>\n';
end

function debug() 
  local text = '';
  text = text .. 'User-Agent: ' .. HTTP_HEADER['User-Agent:'] .. '   ';
  text = text .. 'Cookie: mycookie: ' .. COOKIE['mycookie'];
  SetHttpResponseHeader('Set-Cookie', 'mycookie=NEW');
  SetHttpResponseHeader('Set-Cookie', 'cookie2=NEWER');
  return '<html>DEBUG: ' .. text .. '</html>\n';
end

function register(user, passwd) 
  local exists = redis('get', 'user_' .. user);
  if (exists ~= nil) then
    return '<html>ERROR: USER: "' .. user .. '" already exists</html>\n';
  else
    redis('set', 'user_' .. user, passwd);
    return '<html>USER: "' .. user .. '" REGISTERED</html>\n';
  end
end

function login(user, passwd) 
  local ok     = 0;
  local exists = redis('get', 'user_' .. user);
  if (exists ~= nil and exists == passwd) then
    ok = 1;
  end
  if (ok == 1) then
    return '<html>USER: "' .. user .. '" NOW LOGGED IN</html>\n';
  else
    return '<html>ERROR: USER: "' .. user .. '" login error</html>\n';
  end
end

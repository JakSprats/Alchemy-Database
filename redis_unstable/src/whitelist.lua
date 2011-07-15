module("whitelist", package.seeall);

function index_page() 
  return '<html>HI I AM THE INDEX PAGE</html>\n';
end

function register(user, passwd) 
  exists = redis('get', 'user_' .. user);
  if (exists ~= nil) then
    return '<html>ERROR: USER: "' .. user .. '" already exists</html>\n';
  else
    redis('set', 'user_' .. user, passwd);
    return '<html>USER: "' .. user .. '" REGISTERED</html>\n';
  end
end

function login(user, passwd) 
  ok     = 0;
  exists = redis('get', 'user_' .. user);
  if (exists ~= nil and exists == passwd) then
    ok = 1;
  end
  if (ok == 1) then
    return '<html>USER: "' .. user .. '" NOW LOGGED IN</html>\n';
  else
    return '<html>ERROR: USER: "' .. user .. '" login error</html>\n';
  end
end

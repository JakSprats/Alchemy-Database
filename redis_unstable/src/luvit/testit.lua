-- Load our native module
local alchemy = require('./alchemy')
alchemy.call("SET", "TEST_KEY", "PURE_LUA");
local res = alchemy.call("GET", "TEST_KEY");
print('GET: res: ' .. res);

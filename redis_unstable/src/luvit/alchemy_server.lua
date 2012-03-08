local table   = require('table');
local string  = require('string');
local http    = require("http");
local alchemy = require('alchemy');

function split(str, pat)
   local t         = {}
   local fpat      = "(.-)" .. pat
   local last_end  = 1
   local s, e, cap = str:find(fpat, 1)
   while s do
      if s ~= 1 or cap ~= "" then table.insert(t,cap) end
      last_end  = e+1
      s, e, cap = str:find(fpat, last_end)
   end
   if last_end <= #str then
      cap = str:sub(last_end)
      table.insert(t, cap)
   end
   return t
end

http.createServer(function (req, res)
  local url  = req.url;
  local cmd  = split(url, "/");
  local ares = alchemy.call(unpack(cmd));
  local body;
  if (type(ares) == "table") then body = table.concat(ares);
  else                            body = ares;               end
  body = body .. "\n";
  res:writeHead(200, {
    ["Content-Type"]   = "text/plain",
    ["Content-Length"] = string.len(body);
  })
  res:finish(body)
end):listen(8080)

print("LuvitAlchemy Server listening at http://localhost:8080/")

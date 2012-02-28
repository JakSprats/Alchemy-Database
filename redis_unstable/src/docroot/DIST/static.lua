package.cpath = package.cpath .. ";./extra/base64/?.so"
require "base64"
package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
local LZ = require("zlib");

function load_image(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  alchemy("SET", 'STATIC/' .. name, data);
  local b64_data = base64.encode(data);
  alchemy("SET", 'BASE64/STATIC/' .. name, b64_data);
end

function load_text(ifile, name)
  local inp = assert(io.open(ifile, "rb"))
  local data = inp:read("*all")
  alchemy("SET", 'STATIC/' .. name, data);
  local deflate_data = LZ.deflate()(data, "finish")
  alchemy("SET", 'DEFLATE/STATIC/' .. name, deflate_data);
end

package.cpath = package.cpath .. ";./extra/lua-zlib/?.so"
LZ = require("zlib");

-- DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE DEFLATE
local IsSet_IsDeflatable = false; -- reset every request
local IsDeflatable       = false;
function set_is_deflatable()
  if (IsSet_IsDeflatable) then return IsDeflatable; end
  if (HTTP_HEADER['Accept-Encoding'] ~= nil and
      string.find(HTTP_HEADER['Accept-Encoding'], "deflate")) then
    IsDeflatable = true;
  else
    IsDeflatable = false;
  end
  IsSet_IsDeflatable = true;
  return IsDeflatable;
end

-- OUTPUT_BUFFER+DEFLATE OUTPUT_BUFFER+DEFLATE OUTPUT_BUFFER+DEFLATE
-- this approach is explained here: http://www.lua.org/pil/11.6.html
-- the 3 functions [init_output, output, flush_output] could be 
--   1.) written in C
--   2.) pushed up into the server (i.e. OutputBuffer append to c->reply)
OutputBuffer = {};
function init_output()
  OutputBuffer = {};
end
function output(line)
  table.insert(OutputBuffer, line)
end
function flush_output()
  SetHttpResponseHeader('Content-Type', 'text/html; charset=utf-8');
  local out          = table.concat(OutputBuffer);
  local deflater     = set_is_deflatable();
  IsSet_IsDeflatable = false;
  if (deflater) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
    return LZ.deflate()(out, "finish")
  else
    return out;
  end
end


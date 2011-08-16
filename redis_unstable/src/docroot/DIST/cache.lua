
-- CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE CACHE
local CacheExpireTime = 600; -- 10 minutes
--CacheExpireTime = 5; -- DEBUG

function clearCache() -- NOTE: used when cache format changes
  local cached  = redis("keys", "PAGE_CACHE*");
  for k,v in pairs(cached) do  redis("del", v); end
  local gcached = redis("keys", "GZIP_PAGE_CACHE*");
  for k,v in pairs(gcached) do redis("del", v); end
end
clearCache(); -- NOTE: good idea to clear cache while developing

function getCacheKey(...)
  local key = 'PAGE_CACHE';
  for i,v in ipairs(arg) do     key = key .. '_' .. tostring(v); end
  if (set_is_deflatable()) then key = 'GZIP_' .. key;            end
  return key;
end
function CacheExists(...)
  local key = getCacheKey(...);
  local hit = redis("exists", key);
  --print ('CacheExists: key: ' .. key .. ' hit: ' .. hit);
  if (hit == 0) then return false;
  else               return true;  end
end
function CacheGet(...)
  SetHttpResponseHeader('Content-Type', 'text/html; charset=utf-8');
  if (set_is_deflatable()) then
    SetHttpResponseHeader('Content-Encoding', 'deflate');
  end
  local key = getCacheKey(...);
  IsSet_IsDeflatable = false;
  --print ('CacheGet key: ' .. key);
  redis("expire", key,            CacheExpireTime); -- live a little longer
  redis("expire", key .. '_BLOB', CacheExpireTime); -- live a little longer
  return redis("get", key .. '_BLOB');
end
function CachePutOutput(...)
  local key          = getCacheKey(...);
  IsSet_IsDeflatable = false;
  --print ('CachePutOutput key: ' .. key);
  local out          = table.concat(OutputBuffer);
  local deflater     = set_is_deflatable();
  if (deflater) then out = LZ.deflate()(out, "finish") end
  redis("setex", key,            CacheExpireTime, 1);
  redis("setex", key .. '_BLOB', CacheExpireTime, out);
end

-- ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG ETAG
function CheckEtag(...)
  local ekey = 'ETAG';
  for i,v in ipairs(arg) do     ekey = ekey .. '_' .. tostring(v); end
  --print('CheckEtag: key: ' .. ekey);
  if (HTTP_HEADER['If-None-Match'] ~= nil) then
    if (HTTP_HEADER['If-None-Match'] == ekey) then
      SetHttp304();
      return true;
    end
  end
  SetHttpResponseHeader('Etag', ekey);
  return false;
end

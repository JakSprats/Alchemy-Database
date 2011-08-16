package.cpath = package.cpath .. ";./extra/base64/?.so"
require "base64"

-- only certain browsers can inline Images
-- REF: http://en.wikipedia.org/wiki/Data_URI_scheme
function is_user_agent_inlineable()
  local ua = HTTP_HEADER['User-Agent'];
  if (ua == nil) then return false; end
  ua = string.lower(ua);
  --print ('ua: ' .. ua);
  if ((string.find(ua, 'msie \[9') ~= nil)      or
      ((string.find(ua, 'mozilla')    ~= nil) and
       (string.find(ua, 'compatible') == nil))  or
      (string.find(ua, 'webkit')  ~= nil)       or
      (string.find(ua, 'Opera')   ~= nil)) then
        return true;
  else
        return false;
  end  
end

CanInline = false;
InlinedJS = {}; InlinedCSS = {}; InlinedPNG = {};
function setInlineable(virgin, isl)
  InlinedJS = {}; InlinedCSS = {}; InlinedPNG = {};
  CanInline = false;
  if (virgin and isl == false) then CanInline = true; end
  --print ('setInlineable: virgin: ' .. tostring(virgin) .. 
  --       ' isl: ' .. tostring(isl) .. ' CanInline: ' .. tostring(CanInline));
  return CanInline;
end

function inline_include_js(js)
  if (CanInline) then
    table.insert(InlinedJS, js)  
    --for k,v in pairs(InlinedJS)  do print('InlinedJS: ' .. v); end
    local body = redis("get", js);
    return '<script>' .. body .. '</script>';
  else
    return '<script src="/' .. js .. '"></script>';
  end
end

function inline_include_css(css)
  if (CanInline) then
    table.insert(InlinedCSS, css)  
    --for k,v in pairs(InlinedCSS)  do print('InlinedCSS: ' .. v); end
    local body = redis("get", css);
    return '<style type="text/css">' .. body .. '</style>';
  else
    return '<link href="/' .. css .. '" rel="stylesheet" type="text/css" />';
  end
end

function inline_include_png_src(isrc)
  if (CanInline) then
    table.insert(InlinedPNG, isrc)  
    for k,v in pairs(InlinedPNG)  do print('InlinedPNG: ' .. v); end
    local body     = redis("get", isrc);
    local b64_body = base64.encode(body);
    return 'data:image/png;base64,' .. b64_body;
  else
    return '/' .. isrc;
  end
end

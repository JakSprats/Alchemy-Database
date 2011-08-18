
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

VirginInlineCache = true;
CanInline         = false;
InlinedJS         = {}; InlinedCSS = {}; InlinedPNG = {}; InlinedPNG_EOD = {};

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
InitAutoInc('NextInlineCacheId');
print ('NextInlineCacheId: ' .. AutoInc['NextInlineCacheId']);

function initPerRequestInlineCache()
  InlinedJS = {}; InlinedCSS = {}; InlinedPNG = {}; InlinedPNG_EOD = {};
  local cachecookie = COOKIE['cacheid'];
  if (cachecookie == nil) then
    VirginInlineCache = true;
    SetHttpResponseHeader('Set-Cookie', 
                          'cacheid=' .. IncrementAutoInc('NextInlineCacheId') ..
                          '; Expires=Wed, 09 Jun 2021 10:18:14 GMT; path=/;');
  else
    VirginInlineCache = false;
  end
  CanInline = is_user_agent_inlineable();
  --print ('VirginInlineCache: ' .. tostring(VirginInlineCache) .. ' CanInline: ' .. tostring(CanInline));
end

function inline_include_js(js)
  if (VirginInlineCache and CanInline) then
    table.insert(InlinedJS, js)  
    --for k,v in pairs(InlinedJS)  do print('InlinedJS: ' .. v); end
    local body = redis("get", js);
    return '<script>' .. body .. '</script>';
  else
    return '<script src="/' .. js .. '"></script>';
  end
end

function inline_include_css(css)
  if (VirginInlineCache and CanInline) then
    table.insert(InlinedCSS, css)  
    --for k,v in pairs(InlinedCSS)  do print('InlinedCSS: ' .. v); end
    local body = redis("get", css);
    return '<style type="text/css">' .. body .. '</style>';
  else
    return '<link href="/' .. css .. '" rel="stylesheet" type="text/css" />';
  end
end

function inline_include_png_src(isrc)
  if (VirginInlineCache and CanInline) then
    table.insert(InlinedPNG, isrc)  
    --for k,v in pairs(InlinedPNG)  do print('InlinedPNG: ' .. v); end
    local b64_body = redis("get", 'BASE64/' .. isrc);
    return 'data:image/png;base64,' .. b64_body;
  else
    return '/' .. isrc;
  end
end

function inline_include_png_at_EOD(ihtml_beg, isrc)
  if (VirginInlineCache and CanInline) then
    itbl = {ihtml_beg, isrc};
    table.insert(InlinedPNG_EOD, itbl)  
    --for k,v in pairs(InlinedPNG_EOD)  do print('InlinedPNG_EOD: ' .. v); end
    return '<div id="alc_png_eod_' .. #InlinedPNG_EOD .. '"></div>';
  else
    return ihtml_beg .. ' src="/' .. isrc .. '"> ';
  end
end

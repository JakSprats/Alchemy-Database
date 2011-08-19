
-- USER_AGENT USER_AGENT USER_AGENT USER_AGENT USER_AGENT USER_AGENT USER_AGENT
-- only certain browsers can inline Images
-- REF: http://en.wikipedia.org/wiki/Data_URI_scheme
function sf(a, b) return string.find(a,b); end -- like a C #define
function is_user_agent_inlineable()
  local ua = HTTP_HEADER['User-Agent'];
  if (ua == nil) then return false; end
  ua = string.lower(ua); --print ('ua: ' .. ua);
  if ((sf(ua, 'msie \[9') ~= nil)                                    or
      ((sf(ua, 'mozilla') ~= nil) and (sf(ua, 'compatible') == nil)) or
      (sf(ua,  'webkit')  ~= nil)                                    or
      (sf(ua,  'Opera')   ~= nil)) then
        return true;
  else
        return false;
  end  
end

VirginInlineCache = true;
CanInline         = false;
InlinedJS         = {}; InlinedJS_EOD  = {};
InlinedCSS        = {};
InlinedPNG        = {}; InlinedPNG_EOD = {};

-- GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS GLOBALS
InitAutoInc('NextInlineCacheId');
print ('NextInlineCacheId: ' .. AutoInc['NextInlineCacheId']);

function initPerRequestInlineCache()
  InlinedJS  = {}; InlinedJS_EOD  = {};
  InlinedCSS = {};
  InlinedPNG = {}; InlinedPNG_EOD = {};
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
  --print ('VirginInlineCache: ' .. tostring(VirginInlineCache) ..
  --       ' CanInline: ' .. tostring(CanInline));
end

-- INLINE INLINE INLINE INLINE INLINE INLINE INLINE INLINE INLINE INLINE INLINE
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

-- EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD EOD
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

function inline_include_js_at_EOD(js)
  if (VirginInlineCache and CanInline) then
    table.insert(InlinedJS_EOD, js)  
    --for k,v in pairs(InlinedJS_EOD)  do print('InlinedJS_EOD: ' .. v); end
    return ''; -- nothing to return, js will be inlined at EOD
  else
    return '<script src="/' .. js .. '"></script>';
  end
end

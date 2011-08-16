
function passwords_match(p1, p2) {
  if (!p1 || p1 != p2) {
      alert("PASSWORDS DONT MATCH");
      return false;
  } else return true;
}

function process_cookies() {
  var whole_cookie = unescape(document.cookie);
  return whole_cookie.split(";");
}

function output_elapsed(when) {
  var secs = Math.floor(AlchemyNows - when);
  if (secs > 86400) {
    document.write(Math.floor(secs/86400) +  ' days ago');
  } else if (secs > 3600) {
    document.write(Math.floor(secs/3600)  + ' hours ago');
  } else if (secs > 60) {
    document.write(Math.floor(secs/60)    +  ' minutes ago');
  } else {
    document.write(secs                   +  ' seconds ago');
  }
}

function include_alc(script_filename) {
  var html_doc = document.getElementsByTagName('head').item(0);
  var js    = document.createElement('script');
  js.setAttribute('language', 'javascript');
  js.setAttribute('type', 'text/javascript');
  js.setAttribute('src', script_filename);
  html_doc.appendChild(js);
}

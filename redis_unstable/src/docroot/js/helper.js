function form_action_rewrite_url(action, a, b) {
  var href = '/' + action + '/' + a;
  if (b) href += '/' + b;
  //alert('redirect to : ' + href);
  window.location.href = href;
  return false;
}

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

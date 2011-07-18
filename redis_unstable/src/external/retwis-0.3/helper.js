  function form_action_rewrite_url(action, a, b) {
    var href = action + '/' + encodeURIComponent(a);
    if (b) href += '/' + encodeURIComponent(b);
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


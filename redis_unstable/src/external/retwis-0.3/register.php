<?
include("retwis.php");

# Form sanity checks
if (!gt("username") || !gt("password") || !gt("password2"))
    goback("Every field of the registration form is needed!");
if (gt("password") != gt("password2"))
    goback("The two password fileds don't match!");

# The form is ok, check if the username is available
$username = gt("username");
$password = gt("password");
$r = redisLink();
if ($r->get("username:$username:id"))
    goback("Sorry the selected username is already in use.");

# Everything is ok, Register the user!
$userid = $r->incr("global:nextUserId");
$r->set("username:$username:id",$userid);
$r->set("uid:$userid:username",$username);
$r->set("uid:$userid:password",$password);

$authsecret = getrand();
$r->set("uid:$userid:auth",$authsecret);
$r->set("auth:$authsecret",$userid);

# Manage a Set with all the users, may be userful in the future
$r->sadd("global:users",$userid);

# User registered! Login this guy
setcookie("auth",$authsecret,time()+3600*24*365);

include("header.php");
?>
<h2>Welcome aboard!</h2>
Hey <?=utf8entities($username)?>, now you have an account, <a href="index.php">a good start is to write your first message!</a>.
<?
include("footer.php")
?>

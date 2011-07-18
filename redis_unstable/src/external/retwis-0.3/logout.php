<?
include("retwis.php");

if (!isLoggedIn()) {
    header("Location: index.php");
    exit;
}

$r = redisLink();
$newauthsecret = getrand();
$userid = $User['id'];
$oldauthsecret = $r->get("uid:$userid:auth");

$r->set("uid:$userid:auth",$newauthsecret);
$r->set("auth:$newauthsecret",$userid);
$r->delete("auth:$oldauthsecret");

header("Location: index.php");
?>

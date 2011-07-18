<?
include("retwis.php");

$r = redisLink();
if (!isLoggedIn() || !gt("uid") || gt("f") === false ||
    !($username = $r->get("uid:".gt("uid").":username"))) {
    header("Location:index.php");
    exit;
}

$f = intval(gt("f"));
$uid = intval(gt("uid"));
if ($uid != $User['id']) {
    if ($f) {
        $r->sadd("uid:".$uid.":followers",$User['id']);
        $r->sadd("uid:".$User['id'].":following",$uid);
    } else {
        $r->srem("uid:".$uid.":followers",$User['id']);
        $r->srem("uid:".$User['id'].":following",$uid);
    }
}
header("Location: profile.php?u=".urlencode($username));
?>

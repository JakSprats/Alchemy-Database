<?
include("retwis.php");

if (!isLoggedIn() || !gt("status")) {
    header("Location:index.php");
    exit;
}

$r = redisLink();
$postid = $r->incr("global:nextPostId");
$status = str_replace("\n"," ",gt("status"));
$post = $User['id']."|".time()."|".$status;
$r->set("post:$postid",$post);
$followers = $r->smembers("uid:".$User['id'].":followers");
if ($followers === false) $followers = Array();
$followers[] = $User['id']; /* Add the post to our own posts too */

foreach($followers as $fid) {
    $r->push("uid:$fid:posts",$postid,false);
}
# Push the post on the timeline, and trim the timeline to the
# newest 1000 elements.
$r->push("global:timeline",$postid,false);
$r->ltrim("global:timeline",0,1000);

header("Location: index.php");
?>

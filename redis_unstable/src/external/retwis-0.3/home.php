<?
include("retwis.php");
if (!isLoggedIn()) {
    header("Location: index.php");
    exit;
}
include("header.php");
$r = redisLink();
?>
<div id="postform">
<form method="POST" action="post.php">
<?=utf8entities($User['username'])?>, what you are doing?
<br>
<table>
<tr><td><textarea cols="70" rows="3" name="status"></textarea></td></tr>
<tr><td align="right"><input type="submit" name="doit" value="Update"></td></tr>
</table>
</form>
<div id="homeinfobox">
<?=$r->scard("uid:".$User['id'].":followers")?> followers<br>
<?=$r->scard("uid:".$User['id'].":following")?> following<br>
</div>
</div>
<?
$start = gt("start") === false ? 0 : intval(gt("start"));
showUserPostsWithPagination(false,$User['id'],$start,10);
include("footer.php")
?>

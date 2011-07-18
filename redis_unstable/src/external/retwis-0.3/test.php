<?
include("header.php");
include("retwis.php");

$r = redisLink();
var_dump($r->smembers("foobarzzz"));

include("footer.php");
?>

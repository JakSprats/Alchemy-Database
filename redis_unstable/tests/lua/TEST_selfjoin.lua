package.path = package.path .. ";;test/?.lua"
require "is_external"

local pkname = "id";

function init_word2item_schema()
    local in_s;
    local now = os.time();

    local tbl1    = "words";
    drop_table(tbl1);
    create_table(tbl1, "id INT, name TEXT");
    in_s = '1,\'IPOD\'';    insert(tbl1, in_s);
    in_s = '2,\'NANO\'';    insert(tbl1, in_s);
    in_s = '3,\'CLASSIC\''; insert(tbl1, in_s);
    in_s = '4,\'BLACK\'';   insert(tbl1, in_s);
    in_s = '5,\'WHITE\'';   insert(tbl1, in_s);
    in_s = '6,\'ADIDAS\'';  insert(tbl1, in_s);
    in_s = '7,\'NIKE\'';    insert(tbl1, in_s);
    in_s = '8,\'PUMA\'';    insert(tbl1, in_s);
    in_s = '9,\'FILM\'';    insert(tbl1, in_s);

    local tbl2    = "item";
    drop_table(tbl2);
    create_table(tbl2, "id INT, name TEXT");
    in_s = '10,\'IPOD NANO BLACK\'';    insert(tbl2, in_s);
    in_s = '20,\'IPOD NANO WHITE\'';    insert(tbl2, in_s);
    in_s = '30,\'IPOD CLASSIC BLACK\''; insert(tbl2, in_s);
    in_s = '40,\'IPOD CLASSIC WHITE\''; insert(tbl2, in_s);
    in_s = '50,\'WHITE ADIDAS\'';       insert(tbl2, in_s);
    in_s = '60,\'WHITE NIKE\'';         insert(tbl2, in_s);
    in_s = '70,\'WHITE PUMA\'';         insert(tbl2, in_s);
    in_s = '80,\'WHITE CLASSIC NIKE\''; insert(tbl2, in_s);
    in_s = '90,\'WHITE NANO FILM\'';    insert(tbl2, in_s);

    local tbl3    = "word2item";
    drop_table(tbl3);
    create_table(tbl3, "id INT, seller_id INT, word_id INT, item_id INT, x INT)");
    create_unique_index("ind_w2i_sw2i", tbl3, "seller_id, word_id, item_id");
    in_s = '100, 99,1,10,1'; insert(tbl3, in_s);  --IPOD   ->IPOD NANO BLACK
    in_s = '200, 99,1,20,2'; insert(tbl3, in_s);  --IPOD   ->IPOD NANO WHITE
    in_s = '300, 99,1,30,3'; insert(tbl3, in_s);  --IPOD   ->IPOD CLASSIC BLACK
    in_s = '400, 99,1,40,4'; insert(tbl3, in_s);  --IPOD   ->IPOD CLASSIC WHTIE
    in_s = '500, 99,2,10,5'; insert(tbl3, in_s);  --NANO   ->IPOD NANO BLACK
    in_s = '600, 99,2,20,6'; insert(tbl3, in_s);  --NANO   ->IPOD NANO WHITE
    in_s = '700, 99,3,30,7'; insert(tbl3, in_s);  --CLASSIC->IPOD CLASSIC BLACK
    in_s = '800, 99,3,40,8'; insert(tbl3, in_s);  --CLASSIC->IPOD CLASSIC WHITE
    in_s = '900, 99,4,10,9'; insert(tbl3, in_s);  --BLACK  ->IPOD NANO BLACK
    in_s = '1000,99,4,30,10'; insert(tbl3, in_s); --BLACK  ->IPOD CLASSIC BLACK
    in_s = '1100,99,5,20,11'; insert(tbl3, in_s); --WHITE  ->IPOD NANO WHITE
    in_s = '1200,99,5,40,12'; insert(tbl3, in_s); --WHITE  ->IPOD CLASSIC WHITE

    in_s = '1300,99,5,50,13'; insert(tbl3, in_s); --WHITE  ->WHITE ADIDAS
    in_s = '1400,99,5,60,14'; insert(tbl3, in_s); --WHITE  ->WHITE NIKE
    in_s = '1500,99,5,70,15'; insert(tbl3, in_s); --WHITE  ->WHITE PUMA
    in_s = '1600,99,6,50,16'; insert(tbl3, in_s); --ADIDAS ->WHITE ADIDAS
    in_s = '1700,99,7,60,17'; insert(tbl3, in_s); --NIKE   ->WHITE NIKE
    in_s = '1800,99,8,70,18'; insert(tbl3, in_s); --PUMA   ->WHITE PUMA

    in_s = '1900,99,5,80,19'; insert(tbl3, in_s); --WHITE  ->WHITE CLASSIC NIKE
    in_s = '2000,99,3,80,20'; insert(tbl3, in_s); --CLASSIC->WHITE CLASSIC NIKE
    in_s = '2100,99,7,80,21'; insert(tbl3, in_s); --NIKE   ->WHITE CLASSIC NIKE

    in_s = '2200,99,5,90,22'; insert(tbl3, in_s); --WHITE  ->WHITE NANO FILM
    in_s = '2300,99,2,90,23'; insert(tbl3, in_s); --NANO   ->WHITE NANO FILM
    in_s = '2400,99,9,90,24'; insert(tbl3, in_s); --FILM   ->WHITE NANO FILM

    return "+OK";
end

if is_external.yes == 1 then
    print (init_word2item_schema());
end


package.path = package.path .. ";;test/?.lua"
require "is_external"

local pkname = "id";

function init_star_schema()
    local in_s;
    local now = os.time();

    local tbl1    = "cust";
    drop_table(tbl1);
    create_table(tbl1, "id INT, name TEXT");
    in_s = '1,\'bob\'';  insert(tbl1, in_s);
    in_s = '2,\'bill\''; insert(tbl1, in_s);
    in_s = '3,\'RUSS\''; insert(tbl1, in_s);
    in_s = '4,\'jane\''; insert(tbl1, in_s);
    in_s = '5,\'dick\''; insert(tbl1, in_s);

    local tbl2    = "order";
    drop_table(tbl2);
    create_table(tbl2, "id INT, cust_id INT, date INT");
    create_index("i_order_c", tbl2, "cust_id");
    in_s = '10,1,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '20,1,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '30,2,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '40,2,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '50,3,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '60,3,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '70,3,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '80,4,' .. now;  insert(tbl2, in_s); now = now + 1;
    in_s = '90,4,' .. now;  insert(tbl2, in_s); now = now + 1;

    local tbl3    = "order_item";
    drop_table(tbl3);
    create_table(tbl3, "id INT, order_id INT, description TEXT");
    create_index("i_orderitem_o", tbl3, "order_id");
    in_s = '100,10,\'ipod\'';    insert(tbl3, in_s);
    in_s = '200,20,\'tshirt\'';  insert(tbl3, in_s);
    in_s = '300,30,\'stereo\'';  insert(tbl3, in_s);
    in_s = '400,40,\'kindle\'';  insert(tbl3, in_s);
    in_s = '500,50,\'laptop\'';  insert(tbl3, in_s);
    in_s = '600,60,\'pc\'';      insert(tbl3, in_s);
    in_s = '700,70,\'car\'';     insert(tbl3, in_s);
    in_s = '800,80,\'camera\'';  insert(tbl3, in_s);

    local tbl4    = "order_item_note";
    drop_table(tbl4);
    create_table(tbl4, "id INT, order_item_id INT, note TEXT");
    create_index("i_orderitemnote_oi", tbl4, "order_item_id");
    in_s = '1000,100,\'order recieved\'';    insert(tbl4, in_s);
    in_s = '1001,100,\'order shipped\'';     insert(tbl4, in_s);
    in_s = '2000,200,\'order saved\'';       insert(tbl4, in_s);
    in_s = '2001,200,\'order cancelled\'';   insert(tbl4, in_s);
    in_s = '3000,300,\'saved in cart\'';     insert(tbl4, in_s);
    in_s = '4000,400,\'order cancelled\'';   insert(tbl4, in_s);
    in_s = '5000,500,\'order recieved\'';    insert(tbl4, in_s);
    in_s = '5001,500,\'order paid\'';        insert(tbl4, in_s);
    in_s = '5002,500,\'order shipped\'';     insert(tbl4, in_s);

    return "+OK";
end

function check_star_schema3()
    local res;
    local cnts = {2,2,3,1,0};
    for cid = 1, 5 do
        local res = select_count('cust, order, order_item',
                                    'cust.id = ' .. cid .. ' AND ' .. 
                                    ' cust.id = order.cust_id AND ' .. 
                                    ' order.id = order_item.order_id');
        if (res == nil) then
            print ('JOIN: [cust, order, order_item] missing: ' .. i);
        else
            if (tonumber(res) ~= cnts[cid]) then
                print ('ERROR: expected: ' .. cnts[cid] .. ' got: ' .. res);
            end
        end
    end
    return "OK: JOIN: [cust, order, order_item]";
end

function check_star_schema4()
    local res;
    local cnts = {4,2,3,0,0};
    for cid = 1, 5 do
        local res = select_count('cust, order, order_item, order_item_note',
                             'cust.id = ' .. cid .. ' AND ' .. 
                             ' cust.id = order.cust_id AND ' .. 
                             ' order.id = order_item.order_id AND ' ..
                             ' order_item.id = order_item_note.order_item_id');
        if (res == nil) then
            print ('JOIN: [cust, order, order_item, order_item_note] missing: ' .. i);
        else
            if (tonumber(res) ~= cnts[cid]) then
                print ('ERROR: expected: ' .. cnts[cid] .. ' got: ' .. res);
            end
        end
    end
    return "OK: JOIN: [cust, order, order_item, order_item_note]";
end

-- LOOP 1 (no Q)
-- $CLI SELECT \* FROM "cust, order, order_item, order_item_note" WHERE "cust.id BETWEEN 1 AND 3 AND cust.id = order.cust_id AND order.id = order_item.order_id AND  order_item.id = order_item_note.order_item_id ORDER BY cust.id LIMIT 2 OFFSET X"

-- LOOP 2 (queued)
-- $CLI SELECT \* FROM "cust, order, order_item, order_item_note" WHERE "cust.id BETWEEN 1 AND 3 AND cust.id = order.cust_id AND order.id = order_item.order_id AND  order_item.id = order_item_note.order_item_id ORDER BY order_item.description LIMIT 2 OFFSET X"

if is_external.yes == 1 then
    print (init_star_schema());
    print (check_star_schema3());
    print (check_star_schema4());
end


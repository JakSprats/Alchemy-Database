package.path = package.path .. ";;test/?.lua"
require "is_external"

local c    = 200;
local req  = 1000000;
local mods = "127,61,43,29,17,7" -- primes
local tbl  = "customer_profile";

local debug_iters = 250;
local max_iters   = 1000;
--max_iters = 10000;

function init_customer_profile()
    drop_table(tbl);
    create_table(tbl, "thread_sub_id INTEGER  NOT NULL, page_sub_id INTEGER  NOT NULL, page_id INTEGER  NOT NULL, thread_id INTEGER  NOT NULL, subject_id INTEGER  NOT NULL, corr_id INTEGER, cp_name CHARACTER VARYING(255), last_msg_id INTEGER, msg_cnt INTEGER, retrieved_cnt INTEGER");
    create_index("index_tsub_psub", tbl, "page_sub_id");
    create_index("index_tsub_p",    tbl, "page_id");
    create_index("index_tsub_thrd", tbl, "thread_id");
    create_index("index_tsub_subj", tbl, "subject_id");
    create_index("index_tsub_corp", tbl, "corr_id");

    local icmd = '../gen-benchmark -q -c ' .. c ..' -n ' .. req ..
                 ' -s -m ' .. mods .. ' -A OK ' .. 
                 ' -Q INSERT INTO customer_profile VALUES ' .. 
                 '"(00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,00000000000001,\'pagename_00000000000001\',0,0,0)"'; -- > /dev/null';
    local x   = socket.gettime()*1000;
    print ('executing: (' .. icmd .. ')');
    os.execute(icmd);
    is_external.print_diff_time('time: (' .. icmd .. ')', x);
    local x   = socket.gettime()*1000;
    --print ('save()');
    --print (diff_time('time: save()', x));
    --save();
    return "+OK";
end

function do_t_sp_cp()
    local x = 0;
    for k = 1, 42 do
        for l = 1, 28 do
            for m = 1, 16 do
                local cnt = select("COUNT(*)", tbl,
                  'thread_id = ' .. k ..
                  ' AND subject_id = ' .. l ..
                  ' AND corr_id = ' .. m);
                if (cnt ~= 47 and cnt ~= 48) then
                    print('thread_id, subject_id, corr_id expected [47,48] got: ' .. cnt .. ' for k=' .. k .. ',l=' .. l .. ',m=' .. m);
                end
                if (x == max_iters) then return x; end
                if (x % debug_iters == 0) then print ('loops: ' .. x); end
                x = x + 1;
            end
        end
    end
    return x;
end
function validate_t_sp_cp_integrity()
    print ('running TEST: thread_id, subject_id, corr_id');
    local x = do_t_sp_cp();
    print ('TEST: FINISHED. ' .. x .. ' SELECTs w/ filter examined');
    return "+OK";
end


function do_p_t_sp()
    local x = 0;
    for j = 1, 60 do
        for k = 1, 42 do
            for l = 1, 28 do
                local cnt = select("COUNT(*)", tbl,
                  'page_id = ' .. j ..
                  ' AND thread_id = ' .. k ..
                  ' AND subject_id = ' .. l);
                if (cnt ~= 13 and cnt ~= 14) then
                    print('page_id, thread_id, subject_id, expected [13,14] got: ' .. cnt .. ' for j=' .. j .. 'k=' .. k .. ',l=' .. l);
                end
                if (x == max_iters) then return x; end
                if (x % debug_iters == 0) then print ('loops: ' .. x); end
                x = x + 1;
            end
        end
    end
    return x;
end
function validate_p_t_sp_integrity()
    print ('running TEST: page_id, thread_id, subject_id');
    local x = do_p_t_sp();
    print ('TEST: FINISHED. ' .. x .. ' SELECTs w/ filter examined');
    return "+OK";
end

function do_ps_p_c()
    local x = 0;
    for i = 1, 126 do
        for j = 1, 60 do
            for m = 1, 16 do
                local cnt = select("COUNT(*)", tbl,
                  'page_sub_id = ' .. i ..
                  ' AND page_id = ' .. j ..
                  ' AND corr_id = ' .. m);
                if (cnt ~= 7 and cnt ~= 8) then
                    print('page_sub_id, page_id, corr_id, expected [7,8] got: ' .. cnt .. ' for i=' .. i .. 'j=' .. j .. ',m=' .. m);
                end
                if (x == max_iters) then return x; end
                if (x % debug_iters == 0) then print ('loops: ' .. x); end
                x = x + 1;
            end
        end
    end
    return x;
end
function validate_ps_p_c_integrity()
    print ('running TEST: page_sub_id, page_id, corr_id');
    local x = do_ps_p_c();
    print ('TEST: FINISHED. ' .. x .. ' SELECTs w/ filter examined');
    return "+OK";
end


function do_ps_p_sp()
    local x = 0;
    for i = 1, 126 do
        for j = 1, 60 do
            for l = 1, 28 do
                local cnt = select("COUNT(*)", tbl,
                  'page_sub_id = ' .. i ..
                  ' AND page_id = ' .. j ..
                  ' AND subject_id = ' .. l);
                if (cnt ~= 4 and cnt ~= 5) then
                    print('page_sub_id, page_id, subject_id, expected [4,5] got: ' .. cnt .. ' for i=' .. i .. 'j=' .. j .. ',l=' .. l);
                end
                if (x == max_iters) then return x; end
                if (x % debug_iters == 0) then print ('loops: ' .. x); end
                x = x + 1;
            end
        end
    end
    return x;
end
function validate_ps_p_sp_integrity()
    print ('running TEST: page_sub_id, page_id, subject_id');
    local x = do_ps_p_sp()
    print ('TEST: FINISHED. ' .. x .. ' SELECTs w/ filter examined');
    return "+OK";
end

function do_ps_p_t()
    local x = 0;
    for i = 1, 126 do
        for j = 1, 60 do
            for k = 1, 42 do
                local cnt = select("COUNT(*)", tbl,
                  'page_sub_id = ' .. i ..
                  ' AND page_id = ' .. j ..
                  ' AND thread_id = ' .. k);
                if (cnt ~= 3 and cnt ~= 4) then
                    print('page_sub_id, page_id, thread_id, expected [3] got: ' .. cnt .. ' for i=' .. i .. 'j=' .. j .. ',k=' .. k);
                end
                if (x == max_iters) then return x; end
                if (x % debug_iters == 0) then print ('loops: ' .. x); end
                x = x + 1;
            end
        end
    end
    return x;
end
function validate_ps_p_t_integrity()
    print ('TEST: page_sub_id, page_id, thread_id');
    local x = do_ps_p_t()
    print ('TEST: FINISHED. ' .. x .. ' SELECTs w/ filter examined');
    return "+OK";
end

function validate_operators()
    local ocnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1");
    local cnt  = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND subject_id >= 0");
    if (cnt ~= ocnt) then
        print ('ERROR: expected ' .. ocnt .. ' from "page_id = 1 AND thread_id = 1 AND subject_id >= 0", got: ' .. cnt)
    end
    local lcnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND subject_id = 0");
    cnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND subject_id <= 0");
    if (cnt ~= lcnt) then
        print ('ERROR: expected ' .. lcnt .. ' from "page_id = 1 AND thread_id = 1 AND subject_id <= 0", got: ' .. cnt)
    end
    cnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND subject_id < 1");
    if (cnt ~= lcnt) then
        print ('ERROR: expected ' .. lcnt .. ' from "page_id = 1 AND thread_id = 1 AND subject_id <= 0", got: ' .. cnt)
    end
    cnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND subject_id > 0");
    if ((cnt + lcnt) ~= ocnt) then
        print ('ERROR: expected ' .. ocnt .. ' from "page_id = 1 AND thread_id = 1 AND subject_id <= 0", got: (' .. cnt .. ' + ' .. lcnt .. ') -> ' .. (cnt +lcnt))
    end
    return "+OK";
end

function validate_select_filter()
    local ocnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1");
    local cnt  = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND corr_id IN ( 0, 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10 ,11, 12, 13, 14, 15, 16)");
    if (cnt ~= ocnt) then
        print ('ERROR: expected ' .. ocnt .. ' from "page_id = 1 AND thread_id = 1 AND corr_id IN ( 0, 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10 ,11, 12, 13, 14, 15, 16)", got: ' .. cnt)
    end
    local lcnt = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND corr_id = 0");
    local ecnt = 0;
    local in_s = '';
    for m = 0, 16 do
        in_s = in_s .. m .. ', ';
        cnt  = select("COUNT(*)", tbl, "page_id = 1 AND thread_id = 1 AND corr_id IN ( " .. in_s .. ")");
        if (cnt ~= (ecnt + lcnt) and cnt ~= (ecnt + lcnt + 1)) then
            print ("ERROR: validate_select_filter: IN ( " .. in_s .. ") cnt: " .. cnt .. " ecnt: " .. ecnt .. " lcnt: " .. lcnt .. " (ecnt + lcnt): " .. (ecnt + lcnt));
        end
        ecnt = cnt;
    end
    return "+OK";
end

function validate_scan_filter()
    local ocnt = scanselect("COUNT(*)", tbl, "cp_name IN ( 'pagename_00000000000001', 'pagename_00000000000002')");
    local cnt  = scanselect("COUNT(*)", tbl, "cp_name IN ( 'pagename_00000000000001', 'pagename_00000000000002') AND corr_id IN ( 0, 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10 ,11, 12, 13, 14, 15, 16)");
    if (cnt ~= ocnt) then
        print ('ERROR: expected ' .. ocnt .. ' from "cp_name IN ( \'pagename_00000000000001\', \'pagename_00000000000002\') AND corr_id IN ( 0, 1, 2, 3, 4, 5, 6, 7, 8 , 9, 10 ,11, 12, 13, 14, 15, 16)", got: ' .. cnt)
    end
    local lcnt = scanselect("COUNT(*)", tbl, "cp_name IN ( 'pagename_00000000000001', 'pagename_00000000000002') AND corr_id = 0");
    local ecnt = 0;
    local in_s = '';
    for m = 0, 16 do
        in_s = in_s .. m .. ', ';
        cnt  = scanselect("COUNT(*)", tbl, "cp_name IN ( 'pagename_00000000000001', 'pagename_00000000000002') AND corr_id IN ( " .. in_s .. ")");
        if (cnt ~= (ecnt + lcnt)     and
            cnt ~= (ecnt + lcnt + 1) and
            cnt ~= (ecnt + lcnt - 1)) then
            print ("ERROR: validate_scan_filter: IN ( " .. in_s .. ") cnt: " .. cnt .. " ecnt: " .. ecnt .. " lcnt: " .. lcnt .. " (ecnt + lcnt): " .. (ecnt + lcnt));
        end
        ecnt = cnt;
    end
    return "+OK";
end

function validate_customer_profile_integrity()
    validate_t_sp_cp_integrity();
    validate_p_t_sp_integrity();
    validate_ps_p_c_integrity();
    validate_ps_p_sp_integrity();
    validate_ps_p_t_integrity();
end

if is_external.yes == 1 then
    print (init_customer_profile());
    print (validate_operators());
    print (validate_select_filter());
    print (validate_scan_filter());
    print (validate_customer_profile_integrity());
end

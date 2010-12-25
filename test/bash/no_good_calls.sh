#!/bin/bash 

CLI="./redisql-cli"

function bad_storer() {
  echo TEST: bad_storer
  $CLI DROP TABLE bad_store
  $CLI CREATE TABLE bad_store "(id INT, val TEXT)"
  $CLI INSERT INTO bad_store VALUES "(1, 1.11111)"
  $CLI INSERT INTO bad_store VALUES "(2, TEXT)"
  echo OK ISTORE
  $CLI SELECT id,val FROM bad_store WHERE id BETWEEN 1 AND 2 STORE ZADD Z_bad_store
  echo BAD ISTORE
  $CLI SELECT val,id FROM bad_store WHERE id BETWEEN 1 AND 2 STORE ZADD Z_bad_store

  echo
  $CLI DROP TABLE bad_store_sub_pk
  $CLI CREATE TABLE bad_store_sub_pk "(id INT, user_id INT, ts INT, val TEXT)"
  $CLI INSERT INTO bad_store_sub_pk VALUES "(1, 1, 888, 1.11111)"
  $CLI INSERT INTO bad_store_sub_pk VALUES "(2, 1, 999, TEXT)"
  echo OK ISTORE w/ SUB_PK
  $CLI SELECT user_id,ts,val FROM bad_store_sub_pk WHERE id BETWEEN 1 AND 2 STORE ZADD Z_bad_store_sub_pk$
  echo BAD ISTORE w/ SUB_PK
  $CLI SELECT user_id,val,ts FROM bad_store_sub_pk WHERE id BETWEEN 1 AND 2 STORE ZADD Z_bad_store_sub_pk$

  echo
  echo OK JOIN
  $CLI SELECT bad_store.id,bad_store_sub_pk.val FROM bad_store,bad_store_sub_pk WHERE bad_store.id = bad_store_sub_pk.id AND bad_store_sub_pk.id BETWEEN 1 AND 2 STORE ZADD Z_bad_store_join
  echo BAD JOIN
  $CLI SELECT bad_store.val,bad_store_sub_pk.id FROM bad_store,bad_store_sub_pk WHERE bad_store.id = bad_store_sub_pk.id AND bad_store_sub_pk.id BETWEEN 1 AND 2 STORE ZADD Z_bad_store_join

  echo
  echo OK JOIN w/ ORDER BY
  $CLI SELECT bad_store.id,bad_store_sub_pk.val FROM bad_store,bad_store_sub_pk WHERE bad_store.id = bad_store_sub_pk.id AND bad_store_sub_pk.id BETWEEN 1 AND 2 ORDER BY bad_store_sub_pk.ts STORE ZADD Z_bad_store_join
  echo BAD JOIN w/ ORDER BY
  $CLI SELECT bad_store.val,bad_store_sub_pk.id FROM bad_store,bad_store_sub_pk WHERE bad_store.id = bad_store_sub_pk.id AND bad_store_sub_pk.id BETWEEN 1 AND 2 ORDER BY bad_store_sub_pk.ts DESC STORE ZADD Z_bad_store_join
}

function bad_inner() {
  echo TEST: bad_inner
  echo BAD ISTORE IN
  $CLI SELECT \* FROM customer WHERE "id IN (\$LRANGE LINDEX_cust_id)"
  echo BAD JOIN IN
  $CLI SELECT "division.id,division.name,division.location,external.name,external.salary" FROM "division,external" WHERE "division.id=external.division AND division.id IN (\$LRANGE L_IND_div_i)"
}

function bad_normer() {
  echo TEST: ./test/bash/pop_BAD_NORM.sh
  ./test/bash/pop_BAD_NORM.sh
}

function bad_create_tables_as_selecter() {
  echo TEST: bad_create_tables_as_selecter
  echo BAD RANGE QUERY
  $CLI CREATE TABLE bad_copy "AS SELECT id,hobby,name,employee FROM customer WHERE hobby BETWEEN a AND "
  echo BAD JOIN
  $CLI CREATE TABLE bad_worker_health "AS SELECT worker.id,worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND "
}

function bad_create_table_genner() {
  echo TEST: CREATE TABLE 
  echo "CREATE TABLE toomanycols"
  $CLI CREATE TABLE toomanycols "(id INT, col_0 INT, col_1 INT, col_2 INT, col_3 INT, col_4 INT, col_5 INT, col_6 INT, col_7 INT, col_8 INT, col_9 INT, col_10 INT, col_11 INT, col_12 INT, col_13 INT, col_14 INT, col_15 INT, col_16 INT, col_17 INT, col_18 INT, col_19 INT, col_20 INT, col_21 INT, col_22 INT, col_23 INT, col_24 INT, col_25 INT, col_26 INT, col_27 INT, col_28 INT, col_29 INT, col_30 INT, col_31 INT, col_32 INT, col_33 INT, col_34 INT, col_35 INT, col_36 INT, col_37 INT, col_38 INT, col_39 INT, col_40 INT, col_41 INT, col_42 INT, col_43 INT, col_44 INT, col_45 INT, col_46 INT, col_47 INT, col_48 INT, col_49 INT, col_50 INT, col_51 INT, col_52 INT, col_53 INT, col_54 INT, col_55 INT, col_56 INT, col_57 INT, col_58 INT, col_59 INT, col_60 INT, col_61 INT, col_62 INT, col_63 INT, col_64 INT, col_65 INT, col_66 INT, col_67 INT, col_68 INT, col_69 INT, col_70 INT, col_71 INT, col_72 INT, col_73 INT, col_74 INT, col_75 INT, col_76 INT, col_77 INT, col_78 INT, col_79 INT, col_80 INT, col_81 INT, col_82 INT, col_83 INT, col_84 INT, col_85 INT, col_86 INT, col_87 INT, col_88 INT, col_89 INT, col_90 INT, col_91 INT, col_92 INT, col_93 INT, col_94 INT, col_95 INT, col_96 INT, col_97 INT, col_98 INT, col_99)"
}

function bad_inserter() {
  echo TEST: INSERT
  $CLI CREATE TABLE bad_insert \(id INT, i INT, f FLOAT, t TEXT\)
  echo ONE OK
  $CLI INSERT INTO bad_insert VALUES \(1,1,1.11111111,ONE\)
  echo REPEAT PK
  $CLI INSERT INTO bad_insert VALUES \(1,1,1.11111111,ONE\)
  echo NEGATIVE PK
  $CLI INSERT INTO bad_insert VALUES \(-1,1,1.11111111,ONE\)
  echo NEGATIVE UINT
  $CLI INSERT INTO bad_insert VALUES \(2,-1,1.11111111,ONE\)
  echo TOO FEW COLS
  $CLI INSERT INTO bad_insert VALUES \(2,1,1.11111111\)
  echo TEXT AS UINT
  $CLI INSERT INTO bad_insert VALUES \(2,TEXT,1.11111111,TWO\)
  echo TEXT AS FLOAT
  $CLI INSERT INTO bad_insert VALUES \(3,1,TEXT,THREE\)
  echo ALL TEXT
  $CLI INSERT INTO bad_insert VALUES \(TEXT,TEXT,TEXT,TEXT\)
}

function bad_select_where_clause() {
  echo TEST: SELECT
  echo OK
  echo SELECT \* FROM customer WHERE employee=4 ORDER BY hobby DESC LIMIT 2 OFFSET 1
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMIT 2 OFFSET 1"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMIT 2 OFFSET "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMIT 2 OFFSE"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMIT 2 "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMIT "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC LIMI "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DESC "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby DES"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobby"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY hobb"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER BY "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER B"
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDER "
  $CLI SELECT \* FROM customer WHERE "employee=4 ORDE"
  $CLI SELECT \* FROM customer WHERE "employee=4"
  $CLI SELECT \* FROM customer WHERE "employee="
  $CLI SELECT \* FROM customer WHERE "employee"
  $CLI SELECT \* FROM customer WHERE "employe"
  $CLI SELECT \* FROM customer WHERE ""
  $CLI SELECT \* FROM customer WHERE "" ""
  $CLI SELECT \* FROM customer WHERE "" "" ""
  $CLI SELECT \* FROM customer WHERE "" "" "" ""
  $CLI SELECT \* FROM customer WHERE
  $CLI SELECT \* FROM customer WHER
  $CLI SELECT \* FROM customer 
  $CLI SELECT \* FROM custome
  $CLI SELECT \* FROM 
  $CLI SELECT \* FRO
}

function bad_scanselect_where_clause() {
  echo TEST: SCANSELECT
  echo OK
  echo SCANSELECT \* FROM customer WHERE name BETWEEN a AND z ORDER BY hobby DESC LIMIT 3 OFFSET 2
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMIT 3 OFFSET 2"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMIT 3 OFFSET "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMIT 3 OFFSE"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMIT 3"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMIT "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC LIMI"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DES"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobb"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER B"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDE"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AN"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN "
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEE"
  $CLI SCANSELECT \* FROM customer WHERE "name "
  $CLI SCANSELECT \* FROM customer WHERE "name"
  $CLI SCANSELECT \* FROM customer WHERE ""
  $CLI SCANSELECT \* FROM customer WHERE 
  $CLI SCANSELECT \* FROM customer WHER
  $CLI SCANSELECT \* FROM customer
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMIT 3 OFFSET 2"
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMIT 3 OFFSET "
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMIT 3 OFFSE"
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMIT 3 "
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMIT "
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC LIMI"
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DESC "
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby DES"
  $CLI SCANSELECT \* FROM customer "ORDER BY hobby"
  $CLI SCANSELECT \* FROM customer "ORDER BY hobb"
  $CLI SCANSELECT \* FROM customer "ORDER BY "
  $CLI SCANSELECT \* FROM customer "ORDER B"
  $CLI SCANSELECT \* FROM customer "ORDER "
  $CLI SCANSELECT \* FROM customer "ORDER"
  $CLI SCANSELECT \* FROM customer ""
  $CLI SCANSELECT \* FROM customer
}
function bad_insert_return_size() {
  echo TEST: INSERT RETURN SIZE
  $CLI INSERT INTO external VALUES \(5,22,1,9999.99,"slim"\) RETURN
  $CLI INSERT INTO external VALUES \(5,22,1,9999.99,"slim"\) RETURN SIZ
  $CLI INSERT INTO external VALUES \(5,22,1,9999.99,"slim"\) RETURN SIZE
}


function bad_join_syntax() {
  echo TEST: JOIN
  echo MULTI INDEX
  $CLI SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND subdivision.id = worker.id AND division.id BETWEEN 11 AND 33
  echo OK
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT 2 OFFSET 1"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT 2 OFFSET "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT 2 OFFSET"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT 2 OFFSE"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT 2 "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMIT"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC LIMI"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DESC "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when DES"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.when "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY user_action.whe"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER BY "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER B"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDER "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id ORDE"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_id "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = user_action.user_i"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id = "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.id "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND user.i"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AND "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 AN"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = 4 "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id = "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.id "
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE "user.i"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE ""
}

function bad_tests() {
  $CLI FLUSHALL
  populate
  populate_new_join

  bad_select_where_clause
  echo
  bad_scanselect_where_clause
  echo
  bad_storer
  echo
  bad_inner
  echo
  bad_create_tables_as_selecter
  echo
  bad_create_table_genner
  echo
  bad_inserter
  echo
  bad_insert_return_size
  echo
  bad_join_syntax
  echo
  bad_normer
}

function do_bad_sql_queries() {
  bad_select_where_clause
  echo
  bad_scanselect_where_clause
  echo
  bad_inserter
  echo
  bad_insert_return_size
  echo
  bad_join_syntax
  echo
}

function bad_sql_tests() {
  $CLI FLUSHALL
  populate
  populate_new_join

  do_bad_sql_queries
}

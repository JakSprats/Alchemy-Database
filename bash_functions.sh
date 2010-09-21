#!/bin/bash 

CLI="./alsosql-cli"

T2P="tr \, \| "

function T() {
  echo -ne "     "
}
function NL() {
  echo
}

function init_external() {
  $CLI CREATE TABLE external \(id int primary key, division int, health int, salary TEXT, name TEXT\)
  $CLI CREATE INDEX external:division:index ON \(external.division\)
  $CLI CREATE INDEX external:health:index   ON \(external.health\)
}
function init_healthplan() {
  $CLI CREATE TABLE healthplan \(id int primary key, name TEXT\)
}
function init_division() {
  $CLI CREATE TABLE division \(id int primary key, name TEXT, location TEXT\)
  $CLI CREATE INDEX division:name:index ON \(division.name\)
}
function init_subdivision() {
  $CLI CREATE TABLE subdivision \(id int primary key, division int, name TEXT\)
  $CLI CREATE INDEX subdivision:division:index ON \(subdivision.division\)
}
function init_employee() {
  $CLI CREATE TABLE employee \(id int primary key, division int, salary TEXT, name TEXT\)
  $CLI CREATE INDEX employee:name:index     ON \(employee.name\)
  $CLI CREATE INDEX employee:division:index ON \(employee.division\)
}
function init_customer() {
  $CLI CREATE TABLE customer \(id int primary key, employee int, name TEXT, hobby TEXT\)
  $CLI CREATE INDEX customer:employee:index ON \(customer.employee\)
  $CLI CREATE INDEX customer:hobby:index    ON \(customer.hobby\)
}
function init_worker() {
  $CLI CREATE TABLE worker \(id int primary key, division int, health int, salary TEXT, name TEXT\)
  $CLI CREATE INDEX worker:division:index ON \(worker.division\)
  $CLI CREATE INDEX worker:health:index   ON \(worker.health\)
}

function insert_external() {
  $CLI INSERT INTO external VALUES \(1,66,1,15000.99,"marieanne"\)
  $CLI INSERT INTO external VALUES \(2,33,3,75000.77,"rosemarie"\)
  $CLI INSERT INTO external VALUES \(3,11,2,55000.55,"johnathan"\)
  $CLI INSERT INTO external VALUES \(4,22,1,25000.99,"bartholemew"\)
}
function insert_healthplan() {
  $CLI INSERT INTO healthplan VALUES \(1,"none"\)
  $CLI INSERT INTO healthplan VALUES \(2,"kaiser"\)
  $CLI INSERT INTO healthplan VALUES \(3,"general"\)
  $CLI INSERT INTO healthplan VALUES \(4,"extended"\)
  $CLI INSERT INTO healthplan VALUES \(5,"foreign"\)
}
function insert_subdivision() {
  $CLI INSERT INTO subdivision VALUES \(1,11,"middle-management"\)
  $CLI INSERT INTO subdivision VALUES \(2,11,"top-level"\)
  $CLI INSERT INTO subdivision VALUES \(3,44,"trial"\)
  $CLI INSERT INTO subdivision VALUES \(4,44,"research"\)
  $CLI INSERT INTO subdivision VALUES \(5,22,"factory"\)
  $CLI INSERT INTO subdivision VALUES \(6,22,"field"\)
}
function insert_division() {
  $CLI INSERT INTO division VALUES \(11,"bosses","N.Y.C"\)
  $CLI INSERT INTO division VALUES \(22,"workers","Chicago"\)
  $CLI INSERT INTO division VALUES \(33,"execs","Dubai"\)
  $CLI INSERT INTO division VALUES \(55,"bankers","Zurich"\)
  $CLI INSERT INTO division VALUES \(66,"janitors","Detroit"\)
  $CLI INSERT INTO division VALUES \(44,"lawyers","L.A."\)
}
function insert_employee() {
  $CLI INSERT INTO employee VALUES \(1,11,10000.99,"jim"\)
  $CLI INSERT INTO employee VALUES \(2,22,2000.99,"jack"\)
  $CLI INSERT INTO employee VALUES \(3,33,30000.99,"bob"\)
  $CLI INSERT INTO employee VALUES \(4,22,3000.99,"bill"\)
  $CLI INSERT INTO employee VALUES \(5,22,5000.99,"tim"\)
  $CLI INSERT INTO employee VALUES \(6,66,60000.99,"jan"\)
  $CLI INSERT INTO employee VALUES \(7,77,7000.99,"beth"\)
  $CLI INSERT INTO employee VALUES \(8,88,80000.99,"kim"\)
  $CLI INSERT INTO employee VALUES \(9,99,9000.99,"pam"\)
  $CLI INSERT INTO employee VALUES \(11,111,111000.99,"sammy"\)
}
function insert_customer() {
  $CLI INSERT INTO customer VALUES \(1,2,"johnathan","sailing"\)
  $CLI INSERT INTO customer VALUES \(2,3,"bartholemew","fencing"\)
  $CLI INSERT INTO customer VALUES \(3,3,"jeremiah","yachting"\)
  $CLI INSERT INTO customer VALUES \(4,4,"christopher","curling"\)
  $CLI INSERT INTO customer VALUES \(6,4,"jennifer","stamps"\)
  $CLI INSERT INTO customer VALUES \(7,4,"marieanne","painting"\)
  $CLI INSERT INTO customer VALUES \(8,5,"rosemarie","violin"\)
  $CLI INSERT INTO customer VALUES \(9,5,"bethany","choir"\)
  $CLI INSERT INTO customer VALUES \(10,6,"gregory","dance"\)
}
function insert_worker() {
  $CLI INSERT INTO worker VALUES \(1,11,2,60000.66,"jim"\)
  $CLI INSERT INTO worker VALUES \(2,22,1,30000.33,"jack"\)
  $CLI INSERT INTO worker VALUES \(3,33,4,90000.99,"bob"\)
  $CLI INSERT INTO worker VALUES \(4,44,3,70000.77,"bill"\)
  $CLI INSERT INTO worker VALUES \(6,66,1,12000.99,"jan"\)
  $CLI INSERT INTO worker VALUES \(7,66,1,11000.99,"beth"\)
  $CLI INSERT INTO worker VALUES \(8,11,2,68888.99,"mac"\)
  $CLI INSERT INTO worker VALUES \(9,22,1,31111.99,"ken"\)
  $CLI INSERT INTO worker VALUES \(10,33,4,111111.99,"seth"\)
}

function initer() {
  init_worker
  init_customer
  init_employee
  init_division
  init_subdivision
  init_healthplan
  init_external
}
function inserter() {
  insert_worker
  insert_customer
  insert_employee
  insert_division
  insert_subdivision
  insert_healthplan
  insert_external
}

function selecter() {
  echo "SELECT"
  echo division -------------------------------------------
  T; $CLI SELECT "*" FROM division WHERE id = 22                 | $T2P; NL
  T; $CLI SELECT name, location FROM division WHERE id = 22      | $T2P; NL
  echo employee -------------------------------------------
  T; $CLI SELECT "*" FROM employee WHERE id = 2                  | $T2P; NL
  T; $CLI SELECT name,salary FROM employee WHERE id = 2          | $T2P; NL
  echo customer -------------------------------------------
  T; $CLI SELECT "*" FROM customer WHERE id = 2                  | $T2P; NL
  T; $CLI SELECT name FROM customer WHERE id = 2                 | $T2P; NL
  echo worker ---------------------------------------------
  T; $CLI SELECT "*" FROM worker WHERE id = 7                    | $T2P; NL
  T; $CLI SELECT name, salary, division FROM worker WHERE id = 7 | $T2P; NL
  echo subdivision ----------------------------------------
  T; $CLI SELECT "*" FROM subdivision WHERE id = 2               | $T2P; NL
  T; $CLI SELECT name,division FROM subdivision WHERE id = 2     | $T2P; NL
  echo healthplan -----------------------------------------
  T; $CLI SELECT "*" FROM healthplan WHERE id = 2                | $T2P; NL
  T; $CLI SELECT name FROM healthplan WHERE id = 2               | $T2P; NL
  echo external -------------------------------------------
  T; $CLI SELECT "*" FROM external WHERE id = 3                  | $T2P; NL
  T; $CLI SELECT name,salary,division FROM external WHERE id = 3 | $T2P; NL
}

function updater() {
  echo "UPDATE"
  echo SELECT "*" FROM employee WHERE id = 1
  T; $CLI SELECT "*" FROM employee WHERE id = 1                  | $T2P; NL
  echo UPDATE employee SET "salary=50000,name=NEWNAME,division=66" WHERE id = 1
  $CLI UPDATE employee SET "salary=50000,name=NEWNAME,division=66" WHERE id = 1
  echo SELECT "*" FROM employee WHERE id = 1
  T; $CLI SELECT "*" FROM employee WHERE id = 1                  | $T2P; NL
  echo UPDATE employee SET id=100 WHERE "id = 1"
  $CLI UPDATE employee SET id=100 WHERE id = 1
  echo SELECT "*" FROM employee WHERE id = 100
  T; $CLI SELECT "*" FROM employee WHERE id = 100                | $T2P; NL
}

function delete_employee() {
  echo SELECT name,salary FROM employee WHERE id = 3
  T; $CLI SELECT name,salary FROM employee WHERE id = 3          | $T2P; NL
  echo DELETE FROM employee WHERE id = 3
  $CLI DELETE FROM employee WHERE id = 3
  echo SELECT name,salary FROM employee WHERE id = 3
  T; $CLI SELECT name,salary FROM employee WHERE id = 3          | $T2P; NL
}
function delete_customer() {
  echo SELECT name, hobby FROM customer WHERE id = 7
  T; $CLI SELECT name, hobby FROM customer WHERE id = 7          | $T2P; NL
  echo DELETE FROM customer WHERE id = 7
  $CLI DELETE FROM customer WHERE id = 7
  echo SELECT name, hobby FROM customer WHERE id = 7
  T; $CLI SELECT name, hobby FROM customer WHERE id = 7          | $T2P; NL
}
function delete_division() {
  echo SELECT name, location FROM division WHERE id = 33
  T; $CLI SELECT name, location FROM division WHERE id = 33      | $T2P; NL
  echo DELETE FROM division WHERE id = 33
  $CLI DELETE FROM division WHERE id = 33
  echo SELECT name, location FROM division WHERE id = 33
  T; $CLI SELECT name, location FROM division WHERE id = 33      | $T2P; NL
}
  
function deleter() {
  echo "DELETE"
  delete_employee
  delete_customer
  delete_division
}

function iselecter_division() {
  echo SELECT id,name,location FROM division WHERE name BETWEEN a AND z
  $CLI SELECT id,name,location FROM division WHERE name BETWEEN a AND z
}
function iselecter_employee() {
  echo SELECT id,name,salary,division FROM employee WHERE division BETWEEN 11 AND 55
  $CLI SELECT id,name,salary,division FROM employee WHERE division BETWEEN 11 AND 55
}
function iselecter_customer() {
  echo SELECT hobby,id,name,employee FROM customer WHERE hobby BETWEEN a AND z
  $CLI SELECT hobby,id,name,employee FROM customer WHERE hobby BETWEEN a AND z
}
function iselecter_customer_employee() {
  echo SELECT employee,name,id FROM customer WHERE employee BETWEEN 3 AND 6
  $CLI SELECT employee,name,id FROM customer WHERE employee BETWEEN 3 AND 6
}
function iselecter_worker() {
  echo SELECT id,health,name,salary,division FROM worker WHERE health BETWEEN 1 AND 3
  $CLI SELECT id,health,name,salary,division FROM worker WHERE health BETWEEN 1 AND 3
}
function iselecter() {
  echo "ISELECT"
  iselecter_division
  echo
  iselecter_employee
  echo
  iselecter_customer
}
function iupdater_customer() {
  echo UPDATE customer SET hobby="fishing",employee=6 WHERE hobby BETWEEN v AND z
  $CLI UPDATE customer SET hobby="fishing",employee=6 WHERE hobby BETWEEN v AND z
}
function iupdater_customer_rev() {
  echo UPDATE customer SET hobby="ziplining",employee=7 WHERE hobby BETWEEN f AND g
  $CLI UPDATE customer SET hobby="ziplining",employee=7 WHERE hobby BETWEEN f AND g
}
function ideleter_customer() {
  echo DELETE FROM customer WHERE employee BETWEEN 4 AND 5
  $CLI DELETE FROM customer WHERE employee BETWEEN 4 AND 5
}


function join_div_extrnl() {
  echo externals
  echo SELECT division.name,division.location,external.name,external.salary FROM division,external WHERE division.id=external.division AND division.id BETWEEN 11 AND 80
  $CLI SELECT division.name,division.location,external.name,external.salary FROM division,external WHERE division.id=external.division AND division.id BETWEEN 11 AND 80
}

function join_div_wrkr() {
  echo workers
  echo SELECT division.name,division.location,worker.name,worker.salary FROM division,worker WHERE division.id = worker.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT division.name,division.location,worker.name,worker.salary FROM division,worker WHERE division.id = worker.division AND division.id BETWEEN 11 AND 33
}

function join_wrkr_health() {
  echo workers w/ healthcare
  echo SELECT worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5
  $CLI SELECT worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5
  echo reverse
  echo SELECT healthplan.name,worker.name,worker.salary FROM healthplan,worker WHERE healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5
  $CLI SELECT healthplan.name,worker.name,worker.salary FROM healthplan,worker WHERE healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5
}

function join_div_wrkr_sub() {
  echo workers w/ subdivision
  echo SELECT division.name,division.location,worker.name,worker.salary,subdivision.name FROM division,worker,subdivision WHERE division.id = worker.division AND division.id = subdivision.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT division.name,division.location,worker.name,worker.salary,subdivision.name FROM division,worker,subdivision WHERE division.id = worker.division AND division.id = subdivision.division AND division.id BETWEEN 11 AND 33

}

function join_div_sub_wrkr() {
  echo 3 way
  echo SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33
}

function joiner() {
  echo "JOINS"
  echo
  join_div_extrnl
  echo
  join_div_wrkr
  echo
  join_wrkr_health
  echo
  join_div_wrkr_sub
  echo
  join_div_sub_wrkr
}

function populate() {
  initer
  inserter
}

function works() {
  populate
  selecter
  iselecter
  updater
  iselecter_employee
  deleter
  iselecter
  iupdater_customer
  iselecter_customer
  ideleter_customer
  iselecter_customer_employee
  joiner
}

function single_join_div_extrnl() {
  init_division
  insert_division
  init_external
  insert_external
  join_div_extrnl
}

function single_join_wrkr_health_rev() {
  init_worker
  insert_worker
  init_healthplan
  insert_healthplan
  $CLI SELECT healthplan.name,worker.name,worker.salary FROM healthplan,worker WHERE healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5
}

function single_join_wrkr_health() {
  init_worker
  insert_worker
  init_healthplan
  insert_healthplan
  $CLI SELECT worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health=healthplan.id AND healthplan.id BETWEEN 1 AND 5
}

function single_join_sub_wrkr() {
  init_division
  insert_division
  init_worker
  insert_worker
  init_subdivision
  insert_subdivision
  join_div_sub_wrkr
}

function scan_external() {
  echo SCANSELECT name,salary FROM external WHERE salary BETWEEN 15000.99 AND 25001.01
  $CLI SCANSELECT name,salary FROM external WHERE salary BETWEEN 15000.99 AND 25001.01
}
function scan_healthpan() {
   $CLI SCANSELECT "*" FROM healthplan WHERE name BETWEEN a AND k
}

function istore_worker_name_list() {
  echo SELECT name FROM worker WHERE division BETWEEN 11 AND 33 STORE RPUSH l_worker_name
  $CLI SELECT name FROM worker WHERE division BETWEEN 11 AND 33 STORE RPUSH l_worker_name
  echo LRANGE l_worker_name 0 -1
  $CLI LRANGE l_worker_name 0 -1
}

function istore_customer_hobby_denorm_to_many_lists() {
  echo SELECT employee, hobby FROM customer WHERE employee BETWEEN 3 aND 6 STORE RPUSH employee_hobby_list$
  $CLI SELECT employee, hobby FROM customer WHERE employee BETWEEN 3 aND 6 STORE RPUSH employee_hobby_list$
  echo LRANGE employee_hobby_list:4 0 -1
  $CLI LRANGE employee_hobby_list:4 0 -1
}

function istore_emp_div_sal_denorm_to_many_zset() {
  echo select division, salary, name FROM worker WHERE id BETWEEN 1 AND 8 STORE ZADD worker_div$
  $CLI select division, salary, name FROM worker WHERE id BETWEEN 1 AND 8 STORE ZADD worker_div$
  echo ZRANGE worker_div:66 0 -1
  $CLI ZRANGE worker_div:66 0 -1
}

function init_actionlist() {
  $CLI CREATE TABLE actionlist \(id INT PRIMARY KEY, user_id INT, timestamp INT, action TEXT\)
}
function insert_actionlist() {
  $CLI INSERT INTO actionlist VALUES \(1,1,12345,"account created"\)
  $CLI INSERT INTO actionlist VALUES \(2,1,12346,"first login"\)
  $CLI INSERT INTO actionlist VALUES \(3,1,12347,"became paid member"\)
  $CLI INSERT INTO actionlist VALUES \(4,1,12348,"posted picture"\)
  $CLI INSERT INTO actionlist VALUES \(5,1,12349,"filled in profile"\)
  $CLI INSERT INTO actionlist VALUES \(6,1,12350,"signed out"\)
  $CLI INSERT INTO actionlist VALUES \(7,2,22345,"signed in"\)
  $CLI INSERT INTO actionlist VALUES \(8,2,22346,"updated picture"\)
  $CLI INSERT INTO actionlist VALUES \(9,2,22347,"checked email"\)
  $CLI INSERT INTO actionlist VALUES \(10,2,22348,"signed in"\)
  $CLI INSERT INTO actionlist VALUES \(11,3,32348,"signed in"\)
  $CLI INSERT INTO actionlist VALUES \(12,3,32349,"contacted customer care"\)
  $CLI INSERT INTO actionlist VALUES \(13,3,32350,"upgraded account"\)
  $CLI INSERT INTO actionlist VALUES \(14,3,32351,"uploaded video"\)
}
function denorm_actionlist_to_many_zsets() {
  echo select user_id, timestamp, action FROM actionlist WHERE id BETWEEN 1 AND 20 STORE ZADD user_action_zset$
  $CLI select user_id, timestamp, action FROM actionlist WHERE id BETWEEN 1 AND 20 STORE ZADD user_action_zset$
  echo ZREVRANGE user_action_zset:1 0 1
  $CLI ZREVRANGE user_action_zset:1 0 1
}



function istore_worker_hash_name_salary() {
  echo SELECT name,salary FROM worker WHERE division BETWEEN 11 AND 33 STORE HSET h_worker_name_to_salary
  $CLI SELECT name,salary FROM worker WHERE division BETWEEN 11 AND 33 STORE HSET h_worker_name_to_salary
  echo HGETALL h_worker_name_to_salary
  $CLI HGETALL h_worker_name_to_salary
}

function jstore_div_subdiv() {
  echo SELECT subdivision.id,subdivision.name,division.name FROM subdivision,division WHERE subdivision.division = division.id AND division.id BETWEEN 11 AND 44 STORE INSERT normal_div_subdiv
  $CLI SELECT subdivision.id,subdivision.name,division.name FROM subdivision,division WHERE subdivision.division = division.id AND division.id BETWEEN 11 AND 44 STORE INSERT normal_div_subdiv
  echo DUMP normal_div_subdiv
  $CLI DUMP normal_div_subdiv
}

function jstore_worker_location_hash() {
  echo SELECT worker.name,division.location FROM worker,division WHERE worker.division=division.id AND division.id BETWEEN 11 AND 80 STORE HSET worker_city_hash
  $CLI SELECT worker.name,division.location FROM worker,division WHERE worker.division=division.id AND division.id BETWEEN 11 AND 80 STORE HSET worker_city_hash
  echo HGETALL worker_city_hash
  $CLI HGETALL worker_city_hash
}

function jstore_worker_location_table() {
  echo SELECT external.name,division.location FROM external,division WHERE external.division=division.id AND division.id BETWEEN 11 AND 80 STORE INSERT w_c_tbl
  $CLI SELECT external.name,division.location FROM external,division WHERE external.division=division.id AND division.id BETWEEN 11 AND 80 STORE INSERT w_c_tbl
  echo dump w_c_tbl
  $CLI dump w_c_tbl
}

function jstore_city_wrkr_denorm_to_many_hash() {
  echo SELECT division.location, worker.name, worker.salary FROM worker,division WHERE division.id=worker.division AND worker.division BETWEEN 11 AND 66 STORE HSET city_wrkr$
  $CLI SELECT division.location, worker.name, worker.salary FROM worker,division WHERE division.id=worker.division AND worker.division BETWEEN 11 AND 66 STORE HSET city_wrkr$
  echo HGETALL city_wrkr:N.Y.C
  $CLI HGETALL city_wrkr:N.Y.C
}

function create_table_as_select_customer() {
  $CLI CREATE TABLE copy AS SELECT id,hobby,name,employee FROM customer WHERE hobby BETWEEN a AND z
  $CLI DESC copy
  $CLI DUMP copy
}

function create_table_as_select_join_worker_health() {
  $CLI CREATE TABLE worker_health AS SELECT worker.id,worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5
  $CLI DESC worker_health
  $CLI DUMP worker_health
}


# BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS
# BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS
function init_test_table() {
  $CLI CREATE TABLE test \(id int primary key, field TEXT, name TEXT\)
  #$CLI CREATE INDEX test:name:index  ON \(test.name\)
  #$CLI CREATE INDEX test:field:index ON \(test.field\)
}
function init_join_table() {
  $CLI CREATE TABLE join \(id int primary key, field TEXT, name TEXT\)
}
function init_third_join_table() {
  $CLI CREATE TABLE third_join \(id int primary key, field TEXT, name TEXT\)
}
function init_ten_join_tables() {
  I=0
  while [ $I -lt 10 ]; do
    $CLI CREATE TABLE join_$I \(id int primary key, field TEXT, name TEXT\)
    I=$[${I}+1];
  done
}
# STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK
function init_string_test_table() {
  $CLI CREATE TABLE stest \(id TEXT, field TEXT, name TEXT\)
}
function init_string_join_table() {
  $CLI CREATE TABLE join \(id TEXT, field TEXT, name TEXT\)
}
function init_string_third_join_table() {
  $CLI CREATE TABLE third_join \(id TEXT, field TEXT, name TEXT\)
}

function init_address_table() {
  $CLI CREATE TABLE address \(id INT, street TEXT, city TEXT, state INT, zip INT\)
}

function init_bigrow_table() {
  $CLI CREATE TABLE bigrow \(id int primary key, field TEXT\)
}

# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS

function init_FK_table() {
  if [ -n "$STRING_OVERRIDE" ]; then
    echo CREATE TABLE FK \(id TEXT, fk TEXT, value TEXT\)
    $CLI CREATE TABLE FK \(id TEXT, fk TEXT, value TEXT\)
  else
    echo CREATE TABLE FK \(id int primary key, fk int, value TEXT\)
    $CLI CREATE TABLE FK \(id int primary key, fk int, value TEXT\)
  fi
  $CLI CREATE INDEX FK:fk:index ON \(FK.fk\)
}
function init_FK2_table() {
  if [ -n "$STRING_OVERRIDE" ]; then
    echo CREATE TABLE FK2 \(id TEXT, fk TEXT, value TEXT\)
    $CLI CREATE TABLE FK2 \(id TEXT, fk TEXT, value TEXT\)
  else
    echo CREATE TABLE FK2 \(id int primary key, fk int, value TEXT\)
    $CLI CREATE TABLE FK2 \(id int primary key, fk int, value TEXT\)
  fi
  $CLI CREATE INDEX FK2:fk:index ON \(FK2.fk\)
}

function test_fk_range_queries() {
  NUM=1000
  M=10
  R=$[${NUM}/${M}]
  Q=2
  init_FK_table
  taskset -c 1 ./alsosql-benchmark -n $NUM -r $NUM -PF -M $M -c 20
  taskset -c 1 ./alsosql-benchmark -n $NUM -r $R -F -Q $Q -c 20
}
function test_fk_joins() {
  NUM=1000
  M=2
  R=$[${NUM}/${M}]
  Q=1
  echo NUM: $NUM M: $M R: $R Q: $Q
  init_FK_table
  init_FK2_table
  taskset -c 1 ./alsosql-benchmark -n $NUM -r $NUM -PF  -M $M -c 20
  taskset -c 1 ./alsosql-benchmark -n $NUM -r $NUM -PF2 -M $M -c 20
  taskset -c 1 ./alsosql-benchmark -n $NUM -r $R -FJ -Q $Q -c 20
}

function test_fk_all() {
  test_fk_range_queries
  test_fk_joins
}

function secondary_range_query_test() {
  init_string_test_table
  $CLI CREATE INDEX stest:name:index  ON \(stest.name\)
  $CLI CREATE INDEX stest:field:index ON \(stest.field\)
  I=1;
  NUM=3
  NUM=100
  while [ $I -le $NUM ] ; do
    $CLI INSERT INTO stest VALUES \(012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_$I,F_$I,N_$I\);
  I=$[${I}+1];
done
}
function secondary_range_query_scan_pk() {
  $CLI SELECT \* FROM stest WHERE id BETWEEN 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_80 AND 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_82
}
function secondary_range_query_scan_name() {
  $CLI SELECT \* FROM stest WHERE name BETWEEN N_80 AND N_82
}
function secondary_range_query_scan_field() {
  $CLI SELECT \* FROM stest WHERE field BETWEEN F_80 AND F_82
}
function secondary_many_range_query_test() {
  J=0;
  K=10;
  while [ $J -lt 99 ]; do
    J=$[${K}+3];
    echo -ne "$K $J: ";
    $CLI SELECT \* FROM stest WHERE name BETWEEN N_$K AND N_$J | \
      tr \. "\n" |wc -l;
    K=$[${K}+1];
  done
}

function istorer() {
  istore_worker_name_list
  istore_customer_hobby_denorm_to_many_lists
  istore_emp_div_sal_denorm_to_many_zset
  istore_worker_hash_name_salary
}

function jstorer() {
  jstore_div_subdiv
  jstore_worker_location_hash
  jstore_worker_location_table
  jstore_city_wrkr_denorm_to_many_hash
}

function basic_tests() {
  works
  scan_external
  scan_healthpan

  istorer
  jstorer

  create_table_as_select_customer
  create_table_as_select_join_worker_health
}

function all_tests() {
  works
  scan_external
  scan_healthpan

  istorer
  jstorer

  create_table_as_select_customer
  create_table_as_select_join_worker_health

  test_fk_joins

  secondary_range_query_test
  secondary_range_query_scan_pk
  secondary_range_query_scan_name
  secondary_range_query_scan_field
  secondary_many_range_query_test
}

function all_tests_and_benchmarks() {
  all_tests
  ./Benchmark_Range_Query_Lengths.sh
  ./Benchmark_Range_Query_Lengths.sh JOIN
  ./Benchmark_Range_Query_Lengths.sh 3WAY
  ./Benchmark_Range_Query_Lengths.sh 10WAY
}

# UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS
# UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS UNIT_TESTS

function simple_test() {
  init_string_test_table
  $CLI INSERT INTO stest VALUES \(key1,1,1\)
  $CLI INSERT INTO stest VALUES \(key2_____________________________________________________________________________________________________________________________________end,2,2\)
  $CLI CREATE TABLE itest \(id INT,name TEXT\)
  $CLI INSERT INTO itest VALUES \(40,I_40\)
  $CLI INSERT INTO itest VALUES \(35000,I_35000\)
}

function populate_itest() {
  $CLI CREATE TABLE itest \(id INT,name TEXT\)
  I=1;
  N=1000
  if [ -n "$2" ]; then
    I=$1
    N=$2
  fi
  while [ $I -lt $N ]; do
    $CLI INSERT INTO itest VALUES \(${I},NAME_${I}\)
    echo $CLI INSERT INTO itest VALUES \(${I},NAME_${I}\)
    I=$[${I}+1];
  done
}

function size_test() {
  $CLI CREATE TABLE size_test \(id int primary key, num int, str TEXT\)
  #$CLI CREATE INDEX size_test:num:index ON \(size_test.num\)
  I=1;
  N=100000;
  J=0;
  while [ $I -lt $N ]; do
    $CLI INSERT INTO size_test VALUES \(${I},${J},STRING_OF_LENGTH_MORE_THAN_32_BYTES_${I}\);
    echo $CLI INSERT INTO size_test VALUES \(${I},${J},STRING_OF_LENGTH_MORE_THAN_32_BYTES_${I}\);
    I=$[${I}+1];
    MOD=$[${I}%10]
    if [ $MOD -eq 0 ]; then
        J=$[${J}+1];
    fi
  done
}

function init_ints_table() {
  $CLI CREATE TABLE ints \(id int primary key, x TEXT, a int, y TEXT, b int, z TEXT, c int\)
}
function insert_ints() {
  $CLI INSERT INTO ints VALUES \(1,A,1,B,256,C,35000\)
  $CLI INSERT INTO ints VALUES \(2,D,2,E,500,F,600000000\)
  $CLI INSERT INTO ints VALUES \(40000,G,3,H,3,I,3\)
  $CLI INSERT INTO ints VALUES \(40001,J,3,K,3,L,3\)
  $CLI INSERT INTO ints VALUES \(300000000,M,3,N,3,O,3\)
}

function populate_join_fk_test() {
  $CLI CREATE TABLE master     \(id INT, val TEXT\)

  $CLI CREATE TABLE reference1 \(id INT, master_id INT, val TEXT\)
  $CLI CREATE INDEX reference1:master_id:index ON \(reference1.master_id\)

  $CLI CREATE TABLE reference2 \(id INT, master_id INT, val TEXT\)
  $CLI CREATE INDEX reference2:master_id:index ON \(reference2.master_id\)

  $CLI INSERT INTO master VALUES \(1,"MASTER_ONE"\)
  $CLI INSERT INTO master VALUES \(2,"MASTER_TWO"\)
  $CLI INSERT INTO master VALUES \(3,"MASTER_THREE"\)

  $CLI INSERT INTO reference1 VALUES \(1,1,"R1_M_ONE_CHILD_ONE"\)
  $CLI INSERT INTO reference1 VALUES \(2,1,"R1_M_ONE_CHILD_TWO"\)
  $CLI INSERT INTO reference1 VALUES \(3,1,"R1_M_ONE_CHILD_THREE"\)
  $CLI INSERT INTO reference1 VALUES \(4,2,"R1_M_TWO_CHILD_ONE"\)
  $CLI INSERT INTO reference1 VALUES \(5,2,"R1_M_TWO_CHILD_TWO"\)
  $CLI INSERT INTO reference1 VALUES \(6,2,"R1_M_TWO_CHILD_THREE"\)
  $CLI INSERT INTO reference1 VALUES \(7,3,"R1_M_THREE_CHILD_ONE"\)
  $CLI INSERT INTO reference1 VALUES \(8,3,"R1_M_THREE_CHILD_TWO"\)
  $CLI INSERT INTO reference1 VALUES \(9,3,"R1_M_THREE_CHILD_THREE"\)

  $CLI INSERT INTO reference2 VALUES \(1,1,"R2_M_ONE_CHILD_ONE"\)
  $CLI INSERT INTO reference2 VALUES \(2,1,"R2_M_ONE_CHILD_TWO"\)
  $CLI INSERT INTO reference2 VALUES \(3,1,"R2_M_ONE_CHILD_THREE"\)
  $CLI INSERT INTO reference2 VALUES \(4,2,"R2_M_TWO_CHILD_ONE"\)
  $CLI INSERT INTO reference2 VALUES \(5,2,"R2_M_TWO_CHILD_TWO"\)
  $CLI INSERT INTO reference2 VALUES \(6,2,"R2_M_TWO_CHILD_THREE"\)
  $CLI INSERT INTO reference2 VALUES \(7,3,"R2_M_THREE_CHILD_ONE"\)
  $CLI INSERT INTO reference2 VALUES \(8,3,"R2_M_THREE_CHILD_TWO"\)
  $CLI INSERT INTO reference2 VALUES \(9,3,"R2_M_THREE_CHILD_THREE"\)

  echo SELECT reference1.val, reference2.val FROM reference1, reference2 WHERE reference1.master_id = reference2.master_id AND reference1.master_id BETWEEN 1 AND 3
  $CLI SELECT reference1.val, reference2.val FROM reference1, reference2 WHERE reference1.master_id = reference2.master_id AND reference1.master_id BETWEEN 1 AND 3
}

function index_size_test() {
  $CLI CREATE TABLE istest \(id INT, fk1 INT, fk2 INT, val TEXT\)
  $CLI CREATE INDEX istest:fk1:index ON \(istest.fk1\)
  $CLI CREATE INDEX istest:fk2:index ON \(istest.fk2\)
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(1,101,201,"ONE"\)
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(2,102,202,"TWO"\)
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(3,103,203,"THREE"\)
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(4,104,204,"FOUR"\) 
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(5,104,204,"FIVE"\) 
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(6,104,204,"SIX"\) 
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(7,104,204,"SEBN"\) 
  $CLI desc istest
  $CLI INSERT INTO istest VALUES \(8,104,204,"ATE"\) 
  $CLI desc istest
}

function pk_tester() {
  $CLI CREATE TABLE pktest \(id INT, val TEXT\)
  $CLI INSERT INTO pktest VALUES \(1,"1"\)
  $CLI INSERT INTO pktest VALUES \(200,"200"\)
  $CLI INSERT INTO pktest VALUES \(536870911,"536870911"\)
  $CLI INSERT INTO pktest VALUES \(536870914,"536870914"\)
  $CLI INSERT INTO pktest VALUES \(4294967295,"4294967295"\)
  $CLI INSERT INTO pktest VALUES \(4294967298,"4294967298"\)
  $CLI INSERT INTO pktest VALUES \(2,"2"\)
  $CLI INSERT INTO pktest VALUES \(2147483648,"2147483648"\)
  $CLI INSERT INTO pktest VALUES \(2147483647,"2147483647"\)
  $CLI INSERT INTO pktest VALUES \(-5,"2147483647"\)
  $CLI CREATE TABLE pktest_string \(id TEXT, val TEXT\)
  $CLI INSERT INTO pktest_string VALUES \("1","1"\)
  $CLI INSERT INTO pktest_string VALUES \("21","21"\)
  $CLI INSERT INTO pktest_string VALUES \("sXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXs","sXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXs"\)
  echo PK LEN 232
  $CLI INSERT INTO pktest_string VALUES \("USHRTXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT","USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT"\)
  echo PK LEN 300
    $CLI INSERT INTO pktest_string VALUES \("USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT","USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT"\)
}


# MYSQL
function dump_alsosql_table_to_mysql() {
  if [ -z "$2" ]; then
    echo "Usage: $0 database-name table-name"
    return 2;
  fi

  DB="$1"
  TBL="$2"

  $CLI dump "$TBL" TO MYSQL| tr \; "\n" | cut -b 4- | \
  while read a; do
    echo "$a;"
  done | mysql -uroot ${DB}
}

function dump_mysql_table_to_alsosql() {
  if [ -z "$2" ]; then
    echo "Usage: $0 database-name table-name"
    return 2;
  fi

  DB="$1"
  TBL="$2"
  (
    I=0
    mysqldump -d --compact -uroot "$DB" "$TBL" | \
        grep -v \; | tr -d \` | grep -v "PRIMARY KEY" | \
        cut -f 1 -d \( | tr -d \, | sed "s/varchar/text/g" | \
    while read a; do
      if [ $I -eq 0 ]; then
        echo -ne "${a} (";
      elif [ $I -eq 1 ]; then
        echo -ne "${a}";
      else
        echo -ne ",${a}";
      fi
      I=$[${I}+1]
    done
    echo ")"
  ) | $CLI

  (
    mysqldump -t --compact -uroot "$DB" "$TBL" | \
       tr \) "\n" | cut -f 2- -d \( | tr -d \' | while read a; do
      if [ "$a" != ";" ]; then
        echo "INSERT INTO $TBL VALUES ("${a}")"
      fi
    done
  ) | $CLI
  
}

#!/bin/bash 

CLI="./redisql-cli"

T2P="tr \, \| "

function T() {
  echo -ne "     "
}
function NL() {
  echo
}

function init_external() {
  $CLI CREATE TABLE external "(id int primary key, division int, health int, salary FLOAT, name TEXT)"
  $CLI CREATE INDEX external:division:index ON external \(division\)
  $CLI CREATE INDEX external:health:index   ON external \(health\)
}
function init_healthplan() {
  $CLI CREATE TABLE healthplan "(id int primary key, name TEXT)"
}
function init_division() {
  $CLI CREATE TABLE division "(id int primary key, name TEXT, location TEXT)"
  $CLI CREATE INDEX division:name:index ON division \(name\)
}
function init_subdivision() {
  $CLI CREATE TABLE subdivision "(id int primary key, division int, name TEXT)"
  $CLI CREATE INDEX subdivision:division:index ON subdivision \(division\)
}
function init_employee() {
  $CLI CREATE TABLE employee "(id int primary key, division int, salary FLOAT, name TEXT)"
  $CLI CREATE INDEX employee:name:index     ON employee \(name\)
  $CLI CREATE INDEX employee:division:index ON employee \(division\)
}
function init_customer() {
  $CLI CREATE TABLE customer "(id int primary key, employee int, name TEXT, hobby TEXT)"
  $CLI CREATE INDEX customer:employee:index ON customer \(employee\)
  $CLI CREATE INDEX customer:hobby:index    ON customer \(hobby\)
}
function init_worker() {
  $CLI CREATE TABLE worker "(id int primary key, division int, health int, salary FLOAT, name TEXT)"
  $CLI CREATE INDEX worker:division:index ON worker \(division\)
  $CLI CREATE INDEX worker:health:index   ON worker \(health\)
}

function insert_external() {
  $CLI INSERT INTO external VALUES \(1,66,1,15000.99,"marieanne"\) RETURN SIZE
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
  $CLI INSERT INTO employee VALUES \(2,22,12000.99,"jack"\)
  $CLI INSERT INTO employee VALUES \(3,33,30000.99,"bob"\)
  $CLI INSERT INTO employee VALUES \(4,22,23000.99,"bill"\)
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
function dropper() {
  echo "DROPPER"
  $CLI DROP TABLE worker
  $CLI DROP TABLE customer
  $CLI DROP TABLE employee
  $CLI DROP TABLE division
  $CLI DROP TABLE subdivision
  $CLI DROP TABLE healthplan
  $CLI DROP TABLE external
}

function selecter() {
  echo "SELECT"
  echo division -------------------------------------------
  T; $CLI SELECT "*" FROM division WHERE "id = 22"                    | $T2P; NL
  T; $CLI SELECT "name, location" FROM division WHERE "id = 22"       | $T2P; NL
  echo employee -------------------------------------------
  T; $CLI SELECT "*" FROM employee WHERE "id=2"                       | $T2P; NL
  T; $CLI SELECT "name,salary" FROM employee WHERE "id=2"             | $T2P; NL
  echo customer -------------------------------------------
  T; $CLI SELECT "*" FROM customer WHERE "id =2"                      | $T2P; NL
  T; $CLI SELECT name FROM customer WHERE "id =2"                     | $T2P; NL
  echo worker ---------------------------------------------
  T; $CLI SELECT "*" FROM worker WHERE "id = 7"                       | $T2P; NL
  T; $CLI SELECT "name ,salary , division" FROM worker WHERE "id = 7" | $T2P; NL
  echo subdivision ----------------------------------------
  T; $CLI SELECT "*" FROM subdivision WHERE "id = 2"                  | $T2P; NL
  T; $CLI SELECT "name,division" FROM subdivision WHERE "id = 2"      | $T2P; NL
  echo healthplan -----------------------------------------
  T; $CLI SELECT "*" FROM healthplan WHERE "id = 2"                   | $T2P; NL
  T; $CLI SELECT name FROM healthplan WHERE "id = 2"                  | $T2P; NL
  echo external -------------------------------------------
  T; $CLI SELECT "*" FROM external WHERE "id = 3 "                    | $T2P; NL
  T; $CLI SELECT "name,salary,division" FROM external WHERE "id = 3"  | $T2P; NL
}

function updater() {
  echo "UPDATE"
  echo SELECT "*" FROM employee WHERE id = 1
  T; $CLI SELECT "*" FROM employee WHERE "id = 1"                  | $T2P; NL
  echo UPDATE employee SET "salary=5.55,name=NEWNAME,division=66" WHERE id = 1
  $CLI UPDATE employee SET "salary=5.55,name=NEWNAME,division=66" WHERE "id=1"
  echo SELECT "*" FROM employee WHERE id = 1
  T; $CLI SELECT "*" FROM employee WHERE "id = 1"                  | $T2P; NL
  echo UPDATE employee SET id=100 WHERE "id = 1"
  $CLI UPDATE employee SET id=100 WHERE "id = 1"
  echo SELECT "*" FROM employee WHERE id = 100
  T; $CLI SELECT "*" FROM employee WHERE "id = 100"                | $T2P; NL
}

function delete_employee() {
  echo SELECT name,salary FROM employee WHERE id = 3
  T; $CLI SELECT "name,salary" FROM employee WHERE "id = 3"          | $T2P; NL
  echo DELETE FROM employee WHERE id = 3
  $CLI DELETE FROM employee WHERE "id = 3"
  echo SELECT name,salary FROM employee WHERE id = 3
  T; $CLI SELECT "name,salary" FROM employee WHERE "id = 3"          | $T2P; NL
}
function delete_customer() {
  echo SELECT name, hobby FROM customer WHERE id = 7
  T; $CLI SELECT "name, hobby" FROM customer WHERE "id = 7"          | $T2P; NL
  echo DELETE FROM customer WHERE id = 7
  $CLI DELETE FROM customer WHERE "id = 7"
  echo SELECT "name, hobby" FROM customer WHERE id = 7
  T; $CLI SELECT "name, hobby" FROM customer WHERE "id = 7"          | $T2P; NL
}
function delete_division() {
  echo SELECT name, location FROM division WHERE id = 33
  T; $CLI SELECT "name, location" FROM division WHERE "id = 33"      | $T2P; NL
  echo DELETE FROM division WHERE id = 33
  $CLI DELETE FROM division WHERE "id = 33"
  echo SELECT "name, location" FROM division WHERE id = 33
  T; $CLI SELECT "name, location" FROM division WHERE "id = 33"      | $T2P; NL
}
  
function deleter() {
  echo "DELETE"
  delete_employee
  delete_customer
  delete_division
}

function iselecter_division() {
  echo SELECT id,name,location FROM division WHERE name BETWEEN a AND z
  $CLI SELECT "id,name,location" FROM division WHERE "name BETWEEN a AND z"
}
function iselecter_employee() {
  echo SELECT id,name,salary,division FROM employee WHERE division BETWEEN 11 AND 55
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "division BETWEEN 11 AND 55"
}
function iselecter_customer() {
  echo SELECT hobby,id,name,employee FROM customer WHERE hobby BETWEEN a AND z
  $CLI SELECT "hobby,id,name,employee" FROM customer WHERE "hobby BETWEEN a AND z"
}
function iselecter_customer_employee() {
  echo SELECT employee,name,id FROM customer WHERE employee BETWEEN 3 AND 6
  $CLI SELECT "employee,name,id" FROM customer WHERE "employee BETWEEN 3 AND 6"
}
function iselecter_worker() {
  echo SELECT id,health,name,salary,division FROM worker WHERE health BETWEEN 1 AND 3
  $CLI SELECT "id,health,name,salary,division" FROM worker WHERE "health BETWEEN 1 AND 3"
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
  echo UPDATE customer SET hobby=FISHING,employee=6 WHERE hobby BETWEEN v AND z
  $CLI UPDATE customer SET "hobby =FISHING,employee=6" WHERE "hobby BETWEEN v AND z"
}
function iupdater_customer_rev() {
  echo UPDATE customer SET hobby=ziplining,employee=7 WHERE hobby BETWEEN f AND g
  $CLI UPDATE customer SET "hobby=ziplining,employee=7" WHERE "hobby BETWEEN f AND g"
}
function ideleter_customer() {
  echo DELETE FROM customer WHERE employee BETWEEN 4 AND 5
  $CLI DELETE FROM customer WHERE "employee BETWEEN 4 AND 5"
}


function join_div_extrnl() {
  echo externals
  echo SELECT division.name,division.location,external.name,external.salary FROM division,external WHERE division.id=external.division AND division.id BETWEEN 11 AND 80
  $CLI SELECT "division.name , division.location ,external.name, external.salary" FROM "division,external" WHERE "division.id=external.division AND division.id BETWEEN 11 AND 80"
}

function join_div_wrkr() {
  echo workers
  echo SELECT division.name,division.location,worker.name,worker.salary FROM division,worker WHERE division.id = worker.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT "division.name,division.location,worker.name,worker.salary" FROM "division, worker" WHERE "division.id = worker.division AND division.id BETWEEN 11 AND 33"
}

function join_wrkr_health() {
  echo workers w/ healthcare
  echo SELECT worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5
  $CLI SELECT "worker.name,worker.salary,healthplan.name" FROM "worker, healthplan" WHERE "worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5"
  echo reverse
  echo SELECT healthplan.name,worker.name,worker.salary FROM healthplan,worker WHERE healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5
  $CLI SELECT "healthplan.name,worker.name ,worker.salary" FROM "healthplan,worker" WHERE "healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5"
}

function join_div_wrkr_sub() {
  echo workers w/ subdivision
  echo SELECT division.name,division.location,worker.name,worker.salary,subdivision.name FROM division,worker,subdivision WHERE division.id = worker.division AND division.id = subdivision.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT "division.name,division.location,worker.name,worker.salary,subdivision.name" FROM "division, worker , subdivision" WHERE "division.id = worker.division AND division.id = subdivision.division AND division.id BETWEEN 11 AND 33"
}

function join_div_sub_wrkr() {
  echo 3 way
  echo SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33
  $CLI SELECT "division.name,division.location,subdivision.name,worker.name,worker.salary" FROM "division,subdivision,worker" WHERE "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33"
}

function order_by_test() {
  echo "ORDER BY TEST"
  echo SELECT id,name,salary,division FROM employee WHERE id BETWEEN 4 AND 9 ORDER BY id LIMIT 4 OFFSET 1;
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "id BETWEEN 4 AND 9 ORDER BY id LIMIT 4 OFFSET 1"

  echo SELECT id,name,salary,division FROM employee WHERE id BETWEEN 4 AND 9 ORDER BY id DESC LIMIT 4;
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "id BETWEEN 4 AND 9 ORDER BY id DESC LIMIT 4"

  echo SELECT id,name,salary,division FROM employee WHERE id BETWEEN 4 AND 9 ORDER BY name LIMIT 4;
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "id BETWEEN 4 AND 9 ORDER BY name LIMIT 4"

  echo SELECT id,name,salary,division FROM employee WHERE division BETWEEN 22 AND 77 ORDER BY division
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "division BETWEEN 22 AND 77 ORDER BY division"

  echo SELECT id,name,salary,division FROM employee WHERE division BETWEEN 22 AND 77 ORDER BY name LIMIT 4
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "division BETWEEN 22 AND 77 ORDER BY name LIMIT 4"

  echo SELECT id,name,salary,division FROM employee WHERE division BETWEEN 22 AND 77 ORDER BY name DESC LIMIT 4
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "division BETWEEN 22 AND 77 ORDER BY name DESC LIMIT 4"

  echo SELECT "id,name,salary,division" FROM employee WHERE "division = 22 ORDER BY name DESC LIMIT 2"
  $CLI SELECT "id,name,salary,division" FROM employee WHERE "division = 22 ORDER BY name DESC LIMIT 2"
}

function order_by_limit_offset_test() {
    echo "PK O(PK)"
    $CLI SELECT id,name,salary,division FROM employee WHERE id BETWEEN 1 AND 9 ORDER BY id
    echo LIMIT 3
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY id LIMIT 3
    echo LIMIT 3 OFFSET 1
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY id LIMIT 3 OFFSET 1

    echo
    echo "PK O(name)"
    $CLI SELECT id,name,salary,division FROM employee WHERE id BETWEEN 1 AND 11 ORDER BY name
    echo LIMIT 3
    $CLI SELECT id,name,salary,division FROM employee WHERE id BETWEEN 1 AND 11 ORDER BY name LIMIT 3
    echo LIMIT 3 OFFSET 1
    $CLI SELECT id,name,salary,division FROM employee WHERE id BETWEEN 1 AND 11 ORDER BY name LIMIT 3 OFFSET 1

    echo
    echo "FK O(FK) - FK=name"
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY name
    echo LIMIT 3
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY name LIMIT 3
    echo LIMIT 3 OFFSET 1
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY name LIMIT 3 OFFSET 1

    echo
    echo "FK O(salary) - FK=name"
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY salary
    echo LIMIT 3
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY salary LIMIT 3
    echo LIMIT 3 OFFSET 1
    $CLI SELECT id,name,salary,division FROM employee WHERE name BETWEEN a AND z ORDER BY salary LIMIT 3 OFFSET 1
}

function order_by_test_joins() {
  echo SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.salary
  $CLI SELECT "division.name,division.location,subdivision.name,worker.name,worker.salary" FROM "division,subdivision,worker" WHERE "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.salary"
  echo SELECT division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.salary DESC
  $CLI SELECT "division.name,division.location,subdivision.name,worker.name,worker.salary" FROM "division,subdivision,worker" WHERE "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.salary DESC"
  echo SELECT worker.health,division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.health
  $CLI SELECT "worker.health,division.name,division.location,subdivision.name,worker.name,worker.salary" FROM "division,subdivision,worker" WHERE "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.health"
  echo SELECT worker.health,division.name,division.location,subdivision.name,worker.name,worker.salary FROM division,subdivision,worker WHERE division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.health DESC
  $CLI SELECT "worker.health,division.name,division.location,subdivision.name,worker.name,worker.salary" FROM "division,subdivision,worker" WHERE "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33 ORDER BY worker.health DESC"
}

function istore_customer_hobby_order_by_denorm_to_many_lists() {
  echo SELECT employee, hobby FROM customer WHERE employee BETWEEN 3 AND 6 ORDER BY hobby STORE RPUSH employee_ordered_hobby_list$
  $CLI SELECT "employee, hobby" FROM customer WHERE "employee BETWEEN 3 AND 6 ORDER BY hobby STORE RPUSH employee_ordered_hobby_list$"
  echo LRANGE employee_ordered_hobby_list:4 0 -1
  $CLI LRANGE employee_ordered_hobby_list:4 0 -1
}

function sql_orderby_test() {
  order_by_test
  order_by_limit_offset_test
  order_by_test_joins
}

function orderbyer() {
  echo ORDERBYER
  sql_orderby_test
  istore_customer_hobby_order_by_denorm_to_many_lists
}

function select_count_range() {
  $CLI SELECT "COUNT(*)" FROM customer WHERE "hobby BETWEEN b AND s"
}
function select_count_join() {
  $CLI SELECT "COUNT(*)" FROM healthplan,worker WHERE healthplan.id=worker.health AND healthplan.id BETWEEN 2 AND 5
}
function select_count_fk() {
  $CLI SELECT "COUNT(*)" FROM customer WHERE employee = 4
}
function select_counter() {
  select_count_range
  select_count_join
  select_count_fk
}

function sql_in_test_cust_id() {
  echo SELECT \* FROM customer WHERE "id IN (1,2,3,4)"
  $CLI SELECT \* FROM customer WHERE "id IN (1,2,3,4)"
}
function in_test_cust_id() {
  sql_in_test_cust_id
  $CLI DEL LINDEX_cust_id
  $CLI LPUSH LINDEX_cust_id 4
  $CLI LPUSH LINDEX_cust_id 2
  $CLI LPUSH LINDEX_cust_id 1
  $CLI LPUSH LINDEX_cust_id 3
  echo SELECT \* FROM customer WHERE "id IN (\$LRANGE LINDEX_cust_id 0 3)"
  $CLI SELECT \* FROM customer WHERE "id IN (\$LRANGE LINDEX_cust_id 0 3)"
}
function sql_in_test_select() {
  echo SELECT \* FROM customer WHERE "id IN (\$SELECT id FROM customer WHERE id between 1 AND 3) ORDER BY name"
  $CLI SELECT \* FROM customer WHERE "id IN (\$SELECT id FROM customer WHERE id between 1 AND 3) ORDER BY name"
}
function sql_in_test_cust_hobby() {
  echo SELECT \* FROM customer WHERE "hobby IN (yachting,painting,violin)" ORDER BY name
  $CLI SELECT \* FROM customer WHERE "hobby IN (yachting,painting,violin)" ORDER BY name
}
function in_test_cust_hobby() {
  sql_in_test_cust_hobby
  $CLI DEL list_index_customer_hobby
  $CLI LPUSH list_index_customer_hobby yachting
  $CLI LPUSH list_index_customer_hobby painting
  $CLI LPUSH list_index_customer_hobby violin
  $CLI LPUSH list_index_customer_hobby choir
  echo SELECT \* FROM customer WHERE "hobby IN (\$LRANGE list_index_customer_hobby 0 2)" ORDER BY name
  $CLI SELECT \* FROM customer WHERE "hobby IN (\$LRANGE list_index_customer_hobby 0 2)" ORDER BY name
}

function in_test_join_nonrelational() {
  echo SELECT division.id,division.name,division.location,external.name,external.salary FROM division,external WHERE division.id=external.division AND division.id IN "(44,55,33,11,22)"
  $CLI SELECT "division.id,division.name,division.location,external.name,external.salary" FROM "division,external" WHERE "division.id=external.division AND division.id IN (44,55,33,11,22)"
  $CLI DEL L_IND_div_id
  echo LPUSH L_IND_div_id 22
  $CLI LPUSH L_IND_div_id 22
  echo LPUSH L_IND_div_id 11
  $CLI LPUSH L_IND_div_id 11
  echo LPUSH L_IND_div_id 33
  $CLI LPUSH L_IND_div_id 33
  echo LPUSH L_IND_div_id 55
  $CLI LPUSH L_IND_div_id 55
  echo LPUSH L_IND_div_id 44
  $CLI LPUSH L_IND_div_id 44
  echo SELECT division.id,division.name,division.location,external.name,external.salary FROM division,external WHERE division.id=external.division AND division.id IN "(\$LRANGE L_IND_div_id 0 -1)"
  $CLI SELECT "division.id,division.name,division.location,external.name,external.salary" FROM "division,external" WHERE "division.id=external.division AND division.id IN (\$LRANGE L_IND_div_id 0 -1)"
}

function sql_in_tester() {
  sql_in_test_cust_id
  sql_in_test_select
  sql_in_test_cust_hobby
}
function in_tester() {
  echo IN_TESTER
  in_test_cust_id
  sql_in_test_select
  in_test_cust_hobby
  in_test_join_nonrelational
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

function sql_test() {
  dropper
  works
  scanner
  sql_in_tester
  sql_orderby_test
  select_counter
  sql_create_table_as_tester
  sql_float_tests
  init_test_table
  NUM=$(echo "math.randomseed(os.time()); print (math.random(1000));" | lua)
  ./redisql-benchmark -n $NUM -r $NUM -c 100 -T
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
  order_by_test
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
  $CLI SELECT "healthplan.name,worker.name,worker.salary" FROM "healthplan,worker" WHERE "healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5"
}

function single_join_wrkr_health() {
  init_worker
  insert_worker
  init_healthplan
  insert_healthplan
  $CLI SELECT "worker.name,worker.salary,healthplan.name" FROM "worker,healthplan" WHERE "worker.health=healthplan.id AND healthplan.id BETWEEN 1 AND 5"
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
  $CLI SCANSELECT "name,salary" FROM external WHERE "salary BETWEEN 15000.99 AND 25001.01"
}
function scan_healthpan() {
   echo SCANSELECT "*" FROM healthplan WHERE name BETWEEN a AND k
   $CLI SCANSELECT "*" FROM healthplan WHERE "name BETWEEN a AND k"
   echo SCANSELECT \* FROM healthplan WHERE "name in (none,general,foreign)"
   $CLI SCANSELECT \* FROM healthplan WHERE "name in (none,general,foreign)"
   echo SCANSELECT \* FROM healthplan WHERE "name=kaiser"
   $CLI SCANSELECT \* FROM healthplan WHERE "name=kaiser"
   echo SCANSELECT \* FROM healthplan WHERE "name in (none)"
   $CLI SCANSELECT \* FROM healthplan WHERE "name in (none)"
}

function scan_customer() {
  echo SCANSELECT \* FROM customer ORDER BY hobby
  $CLI SCANSELECT \* FROM customer ORDER BY hobby
  echo SCANSELECT \* FROM customer WHERE "name in (bethany,gregory,jennifer) ORDER BY hobby"
  $CLI SCANSELECT \* FROM customer WHERE "name in (bethany,gregory,jennifer) ORDER BY hobby"
  echo SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC"
  $CLI SCANSELECT \* FROM customer WHERE "name BETWEEN a AND z ORDER BY hobby DESC"
}

function scanner() {
  echo SCANNER
  scan_customer
  scan_external
  scan_healthpan
}

function init_x3() {
  $CLI CREATE TABLE X3  "(id int, f float, t text, i int)";
}
function insert_x3() {
  $CLI INSERT INTO X3 VALUES "(1,1.11,text1,33)";
  $CLI INSERT INTO X3 VALUES "(2,2.22,text2,22)";
  $CLI INSERT INTO X3 VALUES "(3,3.33,text3,11)"
}
function scan_x3() {
  echo SCANSELECT \* FROM X3 WHERE i BETWEEN 11 AND 33
  $CLI SCANSELECT \* FROM X3 WHERE i BETWEEN 11 AND 33
  echo SCANSELECT \* FROM X3 WHERE i BETWEEN 12 AND 33
  $CLI SCANSELECT \* FROM X3 WHERE i BETWEEN 12 AND 33
  echo SCANSELECT \* FROM X3 WHERE i BETWEEN 12 AND 32
  $CLI SCANSELECT \* FROM X3 WHERE i BETWEEN 12 AND 32
  echo SCANSELECT \* FROM X3 WHERE f BETWEEN 1.1 AND 3.33
  $CLI SCANSELECT \* FROM X3 WHERE f BETWEEN 1.1 AND 3.33
  echo SCANSELECT \* FROM X3 WHERE f BETWEEN 1.2 AND 3.2
  $CLI SCANSELECT \* FROM X3 WHERE f BETWEEN 1.2 AND 3.2
  echo SCANSELECT \* FROM X3 WHERE f BETWEEN 1.2 AND 3.1
  $CLI SCANSELECT \* FROM X3 WHERE f BETWEEN 1.2 AND 3.1

  echo
  echo SCANSELECT \* FROM X3 ORDER BY f DESC
  $CLI SCANSELECT \* FROM X3 ORDER BY f DESC
  echo SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2
  $CLI SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2
  echo SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2 OFFSET 1
  $CLI SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2 OFFSET 1
  echo SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2 OFFSET 2
  $CLI SCANSELECT \* FROM X3 ORDER BY f DESC LIMIT 2 OFFSET 2

}

function test_x3() {
  init_x3
  insert_x3
  scan_x3
}

function init_x4() {
  $CLI CREATE TABLE X4 "(id int, f float, t text)";
  $CLI CREATE INDEX X4:f:index ON X4 "(f)"
}
function insert_x4() {
  $CLI INSERT INTO X4 VALUES "(1,1.11,text1)";
  $CLI INSERT INTO X4 VALUES "(2,2.22,text2)";
  $CLI INSERT INTO X4 VALUES "(3,3.33,text3)"
  $CLI INSERT INTO X4 VALUES "(4,4.44,text4)"
  $CLI INSERT INTO X4 VALUES "(5,5.55,text5)"
  $CLI INSERT INTO X4 VALUES "(6,6.66,text6)"
  $CLI INSERT INTO X4 VALUES "(7,7.77,text7)"
  $CLI INSERT INTO X4 VALUES "(15,5.55,text15)"
  $CLI INSERT INTO X4 VALUES "(16,6.66,text16)"
  $CLI INSERT INTO X4 VALUES "(17,7.77,text17)"
  $CLI INSERT INTO X4 VALUES "(25,5.55,text25)"
  $CLI INSERT INTO X4 VALUES "(26,6.66,text26)"
  $CLI INSERT INTO X4 VALUES "(27,7.77,text27)"
  $CLI INSERT INTO X4 VALUES "(35,5.55,text35)"
  $CLI INSERT INTO X4 VALUES "(36,6.66,text36)"
  $CLI INSERT INTO X4 VALUES "(37,7.77,text37)"
}
function select_x4() {
  echo SELECT \* FROM X4 WHERE id BETWEEN 2 AND 5
  $CLI SELECT \* FROM X4 WHERE id BETWEEN 2 AND 5
  echo SELECT \* FROM X4 WHERE f BETWEEN 2 AND 5
  $CLI SELECT \* FROM X4 WHERE f BETWEEN 2 AND 5
  echo SELECT \* FROM X4 WHERE f BETWEEN 2 AND 5 ORDER BY f DESC
  $CLI SELECT \* FROM X4 WHERE f BETWEEN 2 AND 5 ORDER BY f DESC
  echo SELECT \* FROM X4 WHERE f = 5.550000191
  $CLI SELECT \* FROM X4 WHERE f = 5.550000191
  echo SELECT \* FROM X4 WHERE f = 5.550000191 order by t LIMIT 2 OFFSET 1
  $CLI SELECT \* FROM X4 WHERE f = 5.550000191 order by t LIMIT 2 OFFSET 1
}

function test_x4() {
  init_x4
  insert_x4
  select_x4
}

function init_x5() {
  $CLI CREATE TABLE X5  "(id float, t text, i int)";
}
function insert_x5() {
  $CLI INSERT INTO X5 VALUES "(1.11,text1,33)";
  $CLI INSERT INTO X5 VALUES "(2.22,text2,22)";
  $CLI INSERT INTO X5 VALUES "(3.33,text3,11)"
}
function update_x5() {
  $CLI DUMP X5
  echo UPDATE X5 SET id=4.4 WHERE id = 2.2200000286102295
  $CLI UPDATE X5 SET id=4.4 WHERE id = 2.2200000286102295
  $CLI DUMP X5
}
function test_x5() {
  init_x5
  insert_x5
  update_x5
}

function test_float_nrl() {
  echo test_float_nrl
  $CLI ZADD Z 1.11 1; $CLI ZADD Z 2.22 2; $CLI ZADD Z 3.33 3
  $CLI SELECT \* FROM X4 WHERE "f IN (\$ZRANGE Z 0 -1 WITHSCORES)"
}

function sql_float_tests() {
  $CLI DROP TABLE X3
  $CLI DROP TABLE X4
  $CLI DROP TABLE X5
  test_x3
  test_x4
  test_x5
}
function float_tests() {
  sql_float_tests
  test_float_nrl
}

function istore_worker_name_list() {
  echo SELECT name FROM worker WHERE division BETWEEN 11 AND 33 STORE RPUSH l_worker_name
  $CLI SELECT name FROM worker WHERE "division BETWEEN 11 AND 33 STORE RPUSH l_worker_name"
  echo LRANGE l_worker_name 0 -1
  $CLI LRANGE l_worker_name 0 -1
}

function istore_customer_hobby_denorm_to_many_lists() {
  echo SELECT employee, hobby FROM customer WHERE employee BETWEEN 3 AND 6 STORE RPUSH employee_hobby_list$
  $CLI SELECT "employee, hobby" FROM customer WHERE "employee BETWEEN 3 AND 6 STORE RPUSH employee_hobby_list$"
  echo LRANGE employee_hobby_list:6 0 -1
  $CLI LRANGE employee_hobby_list:6 0 -1
}

function istore_emp_div_sal_denorm_to_many_zset() {
  echo SELECT division, salary, name FROM worker WHERE id BETWEEN 1 AND 8 STORE ZADD worker_div$
  $CLI SELECT "division, salary, name" FROM worker WHERE "id BETWEEN 1 AND 8 STORE ZADD worker_div$"
  echo ZRANGE worker_div:66 0 -1
  $CLI ZRANGE worker_div:66 0 -1
}

function init_actionlist() {
  $CLI CREATE TABLE actionlist "(id INT PRIMARY KEY, user_id INT, timestamp INT, action TEXT)"
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
  echo SELECT user_id, timestamp, action FROM actionlist WHERE id BETWEEN 1 AND 20 STORE ZADD user_action_zset$
  $CLI SELECT "user_id, timestamp, action" FROM actionlist WHERE "id BETWEEN 1 AND 20 STORE ZADD user_action_zset$"
  echo ZREVRANGE user_action_zset:1 0 1
  $CLI ZREVRANGE user_action_zset:1 0 1
}

function actionlist_user1_different_order_by_lists() {
  $CLI CREATE INDEX actionlist:user_id:index ON actionlist \(user_id\)
  echo SELECT action FROM actionlist WHERE user_id = 1 ORDER BY timestamp STORE RPUSH actl1
  $CLI SELECT action FROM actionlist WHERE "user_id = 1 ORDER BY timestamp STORE RPUSH actl1"
  echo SELECT action FROM actionlist WHERE user_id = 1 ORDER BY timestamp DESC STORE RPUSH actl1_desc
  $CLI SELECT action FROM actionlist WHERE "user_id = 1 ORDER BY timestamp DESC STORE RPUSH actl1_desc"
  echo LRANGE actl1 0 -1
  $CLI LRANGE actl1 0 -1
  echo LRANGE actl1_desc 0 -1
  $CLI LRANGE actl1_desc 0 -1
}

function istore_worker_hash_name_salary() {
  echo SELECT name,salary FROM worker WHERE division BETWEEN 11 AND 33 STORE HSET h_worker_name_to_salary
  $CLI SELECT "name,salary" FROM worker WHERE "division BETWEEN 11 AND 33 STORE HSET h_worker_name_to_salary"
  echo HGETALL h_worker_name_to_salary
  $CLI HGETALL h_worker_name_to_salary
}


function join_store_worker_location_hash() {
  echo SELECT worker.name,division.location FROM worker,division WHERE worker.division=division.id AND division.id BETWEEN 11 AND 80 STORE HSET worker_city_hash
  $CLI SELECT "worker.name,division.location" FROM "worker,division" WHERE "worker.division=division.id AND division.id BETWEEN 11 AND 80 STORE HSET worker_city_hash"
  echo HGETALL worker_city_hash
  $CLI HGETALL worker_city_hash
  echo "TEST JOIN ORDER BY STORE"
  echo SELECT "worker.name,division.location" FROM "worker,division" WHERE "worker.division=division.id AND division.id BETWEEN 11 AND 80 ORDER BY division.location STORE HSET worker_city_hash2"
  $CLI SELECT "worker.name,division.location" FROM "worker,division" WHERE "worker.division=division.id AND division.id BETWEEN 11 AND 80 ORDER BY division.location STORE HSET worker_city_hash2"
  echo HGETALL worker_city_hash2
  $CLI HGETALL worker_city_hash2
}


function join_store_city_wrkr_denorm_to_many_hash() {
  echo SELECT division.location, worker.name, worker.salary FROM worker,division WHERE division.id=worker.division AND worker.division BETWEEN 11 AND 66 STORE HSET city_wrkr$
  $CLI SELECT "division.location, worker.name, worker.salary" FROM "worker,division" WHERE "division.id=worker.division AND worker.division BETWEEN 11 AND 66 STORE HSET city_wrkr$"
  echo HGETALL city_wrkr:N.Y.C
  $CLI HGETALL city_wrkr:N.Y.C
}

function create_table_as_select_customer() {
  echo CREATE TABLE copy AS SELECT id,hobby,name,employee FROM customer WHERE hobby BETWEEN a AND z
  $CLI CREATE TABLE copy "AS SELECT id,hobby,name,employee FROM customer WHERE hobby BETWEEN a AND z"
  $CLI DESC copy
  $CLI DUMP copy
}

function create_table_as_select_join_worker_health() {
  echo CREATE TABLE worker_health AS SELECT worker.id,worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5
  $CLI CREATE TABLE worker_health "AS SELECT worker.id,worker.name,worker.salary,healthplan.name FROM worker,healthplan WHERE worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5"
  $CLI DESC worker_health
  $CLI DUMP worker_health
}

function create_table_as_obj() {
  $CLI RPUSH LLL 1
  $CLI RPUSH LLL 2
  $CLI RPUSH LLL 3
  $CLI CREATE TABLE copy_LLL "AS DUMP LLL"
  $CLI DESC copy_LLL
  $CLI DUMP copy_LLL
  $CLI CREATE TABLE copy_part_LLL "AS LRANGE LLL 0 1"
  $CLI DUMP copy_part_LLL

  $CLI SADD SSS 1
  $CLI SADD SSS 2
  $CLI SADD SSS 3
  $CLI SADD SSS 4
  $CLI SADD SSS 5
  $CLI CREATE TABLE copy_SSS "AS DUMP SSS"
  $CLI DESC copy_SSS
  $CLI DUMP copy_SSS

  $CLI ZADD ZZZ 1 ONE
  $CLI ZADD ZZZ 2 TWO
  $CLI ZADD ZZZ 3 THREE
  $CLI CREATE TABLE copy_ZZZ "AS DUMP ZZZ"
  $CLI DESC copy_ZZZ
  $CLI DUMP copy_ZZZ
  $CLI CREATE TABLE copy_part_ZZZ "AS ZRANGE ZZZ 0 1"
  $CLI DUMP copy_part_ZZZ

  $CLI HSET HHH col1 col_ONE
  $CLI HSET HHH col2 col_TWO
  $CLI HSET HHH col3 col_THREE
  $CLI HSET HHH col4 col_FOUR
  $CLI CREATE TABLE copy_HHH "AS DUMP HHH"
  $CLI DESC copy_HHH
  $CLI DUMP copy_HHH

  $CLI CREATE TABLE copy2 "AS SELECT id,hobby,name,employee FROM customer WHERE hobby BETWEEN a AND f"
  $CLI CREATE TABLE copy3 "AS DUMP copy2"
  $CLI DUMP copy3
}

# PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL
# PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL PROTOCOL
function protocol_example_sql() {
  $CLI CREATE TABLE foo "(id INT, val FLOAT, name TEXT)"
  $CLI CREATE INDEX foo_val_index ON foo \(val\)
  $CLI INSERT INTO foo VALUES \(1,1.1111111,one\)
  $CLI INSERT INTO foo VALUES \(2,2.2222222,two\)
  $CLI SELECT "*" FROM foo WHERE "id = 1"
  $CLI SCANSELECT "*" FROM foo
  $CLI UPDATE foo SET val=9.999999 WHERE "id = 1"
  $CLI DELETE FROM foo WHERE "id = 2"
  $CLI DROP INDEX foo_val_index
  $CLI DESC foo
  $CLI DUMP foo
}

function protocol_example_lua() {
  $CLI CONFIG SET luafilename helper.lua
  $CLI LUA "return select('*','foo','id = 1');"
}
function protocol_example_norm() {
  $CLI SET user:1:name bill
  $CLI SET user:1:age 33
  $CLI SET user:1:address:city "capitol city"

  $CLI SET user:2:age 22
  $CLI SET user:2:name jane
  $CLI SET user:2:address:city "houston"

  $CLI NORM user address
  $CLI denorm user "user:*"
}
alias TCPDUMP_ALCHEMY="sudo tcpdump -l -q -A -s 1500 -i lo 'tcp port 6379'"

# BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS
# BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS BENCHMARK_HELPERS
function init_test_table() {
  $CLI CREATE TABLE test "(id int primary key, field TEXT, name TEXT)"
}
function init_join_table() {
  $CLI CREATE TABLE join "(id int primary key, field TEXT, name TEXT)"
}
function init_third_join_table() {
  $CLI CREATE TABLE third_join "(id int primary key, field TEXT, name TEXT)"
}
function init_ten_join_tables() {
  I=0
  while [ $I -lt 10 ]; do
    $CLI CREATE TABLE join_$I "(id int primary key, field TEXT, name TEXT)"
    I=$[${I}+1];
  done
}
# STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK
function init_string_test_table() {
  $CLI CREATE TABLE stest "(id TEXT, field TEXT, name TEXT)"
}
function init_string_join_table() {
  $CLI CREATE TABLE join "(id TEXT, field TEXT, name TEXT)"
}
function init_string_third_join_table() {
  $CLI CREATE TABLE third_join "(id TEXT, field TEXT, name TEXT)"
}

function init_address_table() {
  $CLI CREATE TABLE address "(id INT, street TEXT, city TEXT, state INT, zip INT)"
}

function init_bigrow_table() {
  $CLI CREATE TABLE bigrow "(id int primary key, field TEXT)"
}

function init_message_list_table() {
  $CLI CREATE TABLE message_list "(id int primary key, fk int, count INT)"
  $CLI CREATE INDEX message_list:fk:index ON message_list \(fk\)
}
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS
# TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS TESTS

function init_FK_table() {
  if [ -n "$STRING_OVERRIDE" ]; then
    echo CREATE TABLE FK \(id TEXT, fk TEXT, value TEXT\)
    $CLI CREATE TABLE FK "(id TEXT, fk TEXT, value TEXT)"
  else
    echo CREATE TABLE FK \(id int primary key, fk int, value TEXT\)
    $CLI CREATE TABLE FK "(id int primary key, fk int, value TEXT)"
  fi
  $CLI CREATE INDEX FK:fk:index ON FK \(fk\)
}
function init_FK2_table() {
  if [ -n "$STRING_OVERRIDE" ]; then
    echo CREATE TABLE FK2 \(id TEXT, fk TEXT, value TEXT\)
    $CLI CREATE TABLE FK2 "(id TEXT, fk TEXT, value TEXT)"
  else
    echo CREATE TABLE FK2 \(id int primary key, fk int, value TEXT\)
    $CLI CREATE TABLE FK2 "(id int primary key, fk int, value TEXT)"
  fi
  $CLI CREATE INDEX FK2:fk:index ON FK2 \(fk\)
}

function test_fk_range_queries() {
  NUM=1000
  M=10
  R=$[${NUM}/${M}]
  Q=2
  init_FK_table
  taskset -c 1 ./redisql-benchmark -n $NUM -r $NUM -PF -M $M -c 20
  taskset -c 1 ./redisql-benchmark -n $NUM -r $R -F -Q $Q -c 20
}
function test_fk_joins() {
  NUM=1000
  M=2
  R=$[${NUM}/${M}]
  Q=1
  echo NUM: $NUM M: $M R: $R Q: $Q
  init_FK_table
  init_FK2_table
  taskset -c 1 ./redisql-benchmark -n $NUM -r $NUM -PF  -M $M -c 20
  taskset -c 1 ./redisql-benchmark -n $NUM -r $NUM -PF2 -M $M -c 20
  taskset -c 1 ./redisql-benchmark -n $NUM -r $R -FJ -Q $Q -c 20
}

function test_fk_all() {
  test_fk_range_queries
  test_fk_joins
}

function secondary_range_query_test() {
  init_string_test_table
  $CLI CREATE INDEX stest:name:index  ON stest \(name\)
  $CLI CREATE INDEX stest:field:index ON stest \(field\)
  I=1;
  NUM=3
  NUM=100
  while [ $I -le $NUM ] ; do
    $CLI INSERT INTO stest VALUES \(012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_$I,F_$I,N_$I\);
  I=$[${I}+1];
done
}
function secondary_range_query_scan_pk() {
  $CLI SELECT \* FROM stest WHERE "id BETWEEN 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_80 AND 012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_82"
}
function secondary_range_query_scan_name() {
  $CLI SELECT \* FROM stest WHERE "name BETWEEN N_80 AND N_82"
}
function secondary_range_query_scan_field() {
  $CLI SELECT \* FROM stest WHERE "field BETWEEN F_80 AND F_82"
}
function secondary_many_range_query_test() {
  J=0;
  K=10;
  while [ $J -lt 99 ]; do
    J=$[${K}+3];
    echo -ne "$K $J: ";
    $CLI SELECT \* FROM stest WHERE "name BETWEEN N_$K AND N_$J" | \
      tr \. "\n" |wc -l;
    K=$[${K}+1];
  done
}

function init_complex() {
  $CLI DROP TABLE complex
  $CLI CREATE TABLE complex "(id int primary key, state int, message TEXT)"
  $CLI CREATE INDEX nrl_ind_complex_scan ON complex "SCANSELECT * FROM complex"
}
function insert_complex() {
  echo INSERT INTO complex VALUES "(1,1,ONE)"
  $CLI INSERT INTO complex VALUES "(1,1,ONE)"
  echo INSERT INTO complex VALUES "(2,2,TWO)"
  $CLI INSERT INTO complex VALUES "(2,2,TWO)"
  echo INSERT INTO complex VALUES "(3,3,THREE)"
  $CLI INSERT INTO complex VALUES "(3,3,THREE)"
  echo INSERT INTO complex VALUES "(4,4,FOUR)"
  $CLI INSERT INTO complex VALUES "(4,4,FOUR)"
  echo INSERT INTO complex VALUES "(5,5,FIVE)"
  $CLI INSERT INTO complex VALUES "(5,5,FIVE)"
}

function non_rel_index_test() {
  $CLI DROP TABLE nrl
  $CLI DROP TABLE T_NRL
  $CLI CREATE TABLE nrl "(id int primary key, state int, message TEXT)"
  $CLI CREATE INDEX nrl:pub:index ON nrl "PUBLISH NRL:\$state message=\$message -END"
  $CLI CREATE INDEX nrl:zadd:index ON nrl "ZADD Z_NRL \$state \$id"
  # yes this does a lua transform and then publishes it (per insert)
  $CLI CREATE INDEX nrl:lua_pub:index ON nrl "LUA len=string.len('\$message'); return client('PUBLISH', 'NRL:\$state', 'LUA:message=\$message ' .. ' len=' .. len .. ' date: ' .. os.date('%c'));"
  $CLI CREATE TABLE T_NRL "(id int primary key, message TEXT)"
  $CLI CREATE INDEX nrl:insert:index ON nrl "INSERT INTO T_NRL VALUES (\$id,\$message)"

  $CLI INSERT INTO nrl VALUES "(1,1,hi state 1)"
  $CLI INSERT INTO nrl VALUES "(2,1,state 1 is great)"
  $CLI INSERT INTO nrl VALUES "(3,2,lets not forget state 2)"
  $CLI INSERT INTO nrl VALUES "(4,2,state 2 rocks)"
  $CLI SELECT \* FROM nrl WHERE "id IN (\$ZREVRANGE Z_NRL 0 -1)"
}

function istorer() {
  echo ISTORER
  istore_worker_name_list
  istore_customer_hobby_denorm_to_many_lists
  istore_emp_div_sal_denorm_to_many_zset
  istore_worker_hash_name_salary
  istore_customer_hobby_order_by_denorm_to_many_lists
}

function join_storer() {
  echo JSTORER
  join_store_worker_location_hash
  join_store_city_wrkr_denorm_to_many_hash
}

function sql_create_table_as_tester() {
  create_table_as_select_customer
  create_table_as_select_join_worker_health
}

function create_table_as_tester() {
  echo CREATE_TABLE_AS
  sql_create_table_as_tester
  create_table_as_obj
}

function basic_tests() {
  works
  scanner

  istorer
  join_storer

  create_table_as_tester
}

function all_tests() {
  $CLI FLUSHALL
  works
  dropper

  float_tests
  populate
  scanner
  in_tester
  orderbyer
  select_counter

  istorer
  join_storer
  create_table_as_tester

  non_rel_index_test
ieturn 0;

  test_fk_joins

  secondary_range_query_test
  secondary_range_query_scan_pk
  secondary_range_query_scan_name
  secondary_range_query_scan_field
  secondary_many_range_query_test

  ./test/pop_denorm.sh
  ./test/pop_denorm.sh 1
  ./test/pop_H_denorm.sh
  ./test/pop_H_denorm.sh 1
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
  $CLI CREATE TABLE itest "(id INT,name TEXT)"
  $CLI INSERT INTO itest VALUES \(40,I_40\)
  $CLI INSERT INTO itest VALUES \(35000,I_35000\)
}

function populate_itest() {
  $CLI CREATE TABLE itest "(id INT,name TEXT)"
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
  $CLI CREATE TABLE size_test "(id int primary key, num int, str TEXT)"
  #$CLI CREATE INDEX size_test:num:index ON size_test \(num\)
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
  $CLI CREATE TABLE ints "(id int primary key, x TEXT, a int, y TEXT, b int, z TEXT, c int)"
}
function insert_ints() {
  $CLI INSERT INTO ints VALUES \(1,A,1,B,256,C,35000\)
  $CLI INSERT INTO ints VALUES \(2,D,2,E,500,F,600000000\)
  $CLI INSERT INTO ints VALUES \(40000,G,3,H,3,I,3\)
  $CLI INSERT INTO ints VALUES \(40001,J,3,K,3,L,3\)
  $CLI INSERT INTO ints VALUES \(300000000,M,3,N,3,O,3\)
}

function populate_join_fk_test() {
  $CLI CREATE TABLE master     "(id INT, val TEXT)"

  $CLI CREATE TABLE reference1 "(id INT, master_id INT, val TEXT)"
  $CLI CREATE INDEX reference1:master_id:index ON reference1 \(master_id\)

  $CLI CREATE TABLE reference2 "(id INT, master_id INT, val TEXT)"
  $CLI CREATE INDEX reference2:master_id:index ON reference2 \(master_id\)

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
  $CLI SELECT "reference1.val, reference2.val" FROM "reference1, reference2" WHERE "reference1.master_id = reference2.master_id AND reference1.master_id BETWEEN 1 AND 3"
}

function index_size_test() {
  $CLI CREATE TABLE istest "(id INT, fk1 INT, fk2 INT, val TEXT)"
  $CLI CREATE INDEX istest:fk1:index ON istest \(fk1\)
  $CLI CREATE INDEX istest:fk2:index ON istest \(fk2\)
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
  $CLI CREATE TABLE pktest "(id INT, val TEXT)"
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
  $CLI CREATE TABLE pktest_string "(id TEXT, val TEXT)"
  $CLI INSERT INTO pktest_string VALUES \("1","1"\)
  $CLI INSERT INTO pktest_string VALUES \("21","21"\)
  $CLI INSERT INTO pktest_string VALUES \("sXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXs","sXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXs"\)
  echo PK LEN 232
  $CLI INSERT INTO pktest_string VALUES \("USHRTXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT","USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT"\)
  echo PK LEN 300
    $CLI INSERT INTO pktest_string VALUES \("USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT","USHRTXXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXUSHRT"\)
}


# MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL MYSQL
function dump_redisql_table_to_mysql() {
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

function dump_mysql_table_to_redisql() {
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

# LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA LUA
RECONF="$CLI CONFIG SET luafilename helper.lua"

function lua_return_value_test() {
  $RECONF
  echo LUA "NOT LUA ERROR"
  echo -ne "  "; $CLI LUA "NOT LUA ERROR"
  echo "No return"
  echo -ne "  "; $CLI LUA "i=2+2;"
  echo "return 4"
  echo -ne "  "; $CLI LUA "i=2+2; return i;"
  echo "ERR: Unknown command"
  echo -ne "  "; $CLI LUA "return client('XXX','XXX');"
  echo "ERR wrong number of arguments"
  echo -ne "  "; $CLI LUA "return client('SET');"
  echo "DEL X"
  echo -ne "  "; $CLI LUA "return client('DEL', 'X');"
  echo "GET X"
  echo -ne "  "; $CLI LUA "return client('GET', 'X');"
  echo "SET X valX"
  echo -ne "  "; $CLI LUA "return client('SET', 'X', 'valX');"
  echo "GET X"
  echo -ne "  "; $CLI LUA "return client('GET', 'X');"
  echo "SET X valX"
  echo -ne "  "; $CLI LUA "return client('SET', 'X', '5');"
  echo "GET X"
  echo -ne "  "; $CLI LUA "return client('GET', 'X');"
  $CLI ZADD ZZZ 1 ONE
  $CLI ZADD ZZZ 2 TWO
  $CLI ZADD ZZZ 3 THREE
  $CLI CREATE TABLE copy_ZZZ "AS DUMP ZZZ"
  $CLI LUA "return client('ZRANGE', 'ZZZ', 0, -1);"
  $CLI DESC copy_ZZZ
  $CLI DUMP copy_ZZZ
  $CLI LUA "return client('SELECT','*','FROM','copy_ZZZ','WHERE','pk BETWEEN 1 AND 2');"

  echo "t= {'X', 1}; return client('GET',t[1]); -> GET X"
  echo -ne "  "; $CLI LUA "t= {'X', 1}; return client('GET',t[1]);"
  echo "t= {'X', 1}; return client('GET',t[2]); -> GET 1"
  echo -ne "  "; $CLI LUA "t= {'X', 1}; return client('GET',t[2]);"
  echo "t= {'X', 1}; return client('GET',t[0]); -> GET undefined -> ERROR"
  echo -ne "  "; $CLI LUA "t= {'X', 1}; return client('GET',t[0]);"

}

function init_messages_table() {
  $CLI CREATE TABLE messages "(id int primary key, cat INT, text TEXT)"
  $CLI CREATE INDEX nrl:messages:index ON messages "PUBLISH MSG:\$cat message=\$text"
}


# STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK STRING_PK
function init_string_pk_one() {
  $CLI CREATE TABLE s_one "(id TEXT, val TEXT)"
}
function init_string_pk_two() {
  $CLI CREATE TABLE s_two "(id TEXT, val TEXT)"
}
function init_string_pk_three() {
  $CLI CREATE TABLE s_three "(id TEXT, val TEXT)"
}
function insert_string_pk_one() {
  $CLI INSERT INTO s_one VALUES \("1","1_1"\)
  $CLI INSERT INTO s_one VALUES \("2","1_2"\)
  $CLI INSERT INTO s_one VALUES \("3","1_3"\)
  $CLI INSERT INTO s_one VALUES \("4","1_4"\)
  $CLI INSERT INTO s_one VALUES \("5","1_5"\)
  $CLI INSERT INTO s_one VALUES \("6","1_6"\)
  $CLI INSERT INTO s_one VALUES \("7","1_7"\)
  $CLI INSERT INTO s_one VALUES \("8","1_8"\)
  $CLI INSERT INTO s_one VALUES \("9","1_9"\)
}
function insert_string_pk_two() {
  $CLI INSERT INTO s_two VALUES \("1","2_1"\)
  $CLI INSERT INTO s_two VALUES \("3","2_3"\)
  $CLI INSERT INTO s_two VALUES \("5","2_5"\)
  $CLI INSERT INTO s_two VALUES \("7","2_7"\)
  $CLI INSERT INTO s_two VALUES \("9","2_9"\)
}
function insert_string_pk_three() {
  $CLI INSERT INTO s_three VALUES \("2","3_2"\)
  $CLI INSERT INTO s_three VALUES \("4","3_4"\)
  $CLI INSERT INTO s_three VALUES \("6","3_6"\)
  $CLI INSERT INTO s_three VALUES \("8","3_8"\)
}

function pk_string_join_tests() {
  $CLI DROP TABLE s_one
  $CLI DROP TABLE s_two
  $CLI DROP TABLE s_three
  init_string_pk_one
  init_string_pk_two
  init_string_pk_three
  insert_string_pk_one
  insert_string_pk_two
  insert_string_pk_three
  echo $CLI SELECT s_one.val,s_two.val FROM s_one,s_two WHERE s_one.id = s_two.id AND s_one.id BETWEEN 1 AND 9
  $CLI SELECT "s_one.val,s_two.val" FROM "s_one,s_two" WHERE "s_one.id = s_two.id AND s_one.id BETWEEN 1 AND 9"

  echo $CLI SELECT s_one.val,s_two.val FROM s_one,s_two WHERE s_one.id = s_two.id AND s_one.id IN "(1,2,3,4,5,6,7,8,9)"
  $CLI SELECT "s_one.val,s_two.val" FROM "s_one,s_two" WHERE "s_one.id = s_two.id AND s_one.id IN (1,2,3,4,5,6,7,8,9)"

  echo $CLI SELECT s_one.val,s_three.val FROM s_one,s_three WHERE s_one.id = s_three.id AND s_one.id BETWEEN 1 AND 9
  $CLI SELECT s_one.val,s_three.val FROM "s_one,s_three" WHERE "s_one.id = s_three.id AND s_one.id BETWEEN 1 AND 9"

  echo $CLI SELECT s_one.val,s_three.val FROM s_one,s_three WHERE s_one.id = s_three.id AND s_one.id  IN "(1,2,3,4,5,6,7,8,9)"
  $CLI SELECT s_one.val,s_three.val FROM "s_one,s_three" WHERE "s_one.id = s_three.id AND s_one.id  IN (1,2,3,4,5,6,7,8,9)"
}




# FLOAT_PK FLOAT_PK FLOAT_PK FLOAT_PK FLOAT_PK FLOAT_PK FLOAT_PK FLOAT_PK
function init_float_pk_one() {
  $CLI CREATE TABLE f_one "(id FLOAT, val TEXT)"
}
function init_float_pk_two() {
  $CLI CREATE TABLE f_two "(id FLOAT, val TEXT)"
}
function init_float_pk_three() {
  $CLI CREATE TABLE f_three "(id FLOAT, val TEXT)"
}
function insert_float_pk_one() {
  $CLI INSERT INTO f_one VALUES \("1.1","1_1"\)
  $CLI INSERT INTO f_one VALUES \("2.2","1_2"\)
  $CLI INSERT INTO f_one VALUES \("3.3","1_3"\)
  $CLI INSERT INTO f_one VALUES \("4.4","1_4"\)
  $CLI INSERT INTO f_one VALUES \("5.5","1_5"\)
  $CLI INSERT INTO f_one VALUES \("6.6","1_6"\)
  $CLI INSERT INTO f_one VALUES \("7.7","1_7"\)
  $CLI INSERT INTO f_one VALUES \("8.8","1_8"\)
  $CLI INSERT INTO f_one VALUES \("9.9","1_9"\)
}
function insert_float_pk_two() {
  $CLI INSERT INTO f_two VALUES \("1.1","2_1"\)
  $CLI INSERT INTO f_two VALUES \("3.3","2_3"\)
  $CLI INSERT INTO f_two VALUES \("5.5","2_5"\)
  $CLI INSERT INTO f_two VALUES \("7.7","2_7"\)
  $CLI INSERT INTO f_two VALUES \("9.9","2_9"\)
}
function insert_float_pk_three() {
  $CLI INSERT INTO f_three VALUES \("2.2","3_2"\)
  $CLI INSERT INTO f_three VALUES \("4.4","3_4"\)
  $CLI INSERT INTO f_three VALUES \("6.6","3_6"\)
  $CLI INSERT INTO f_three VALUES \("8.8","3_8"\)
}

function pk_float_join_tests() {
  $CLI DROP TABLE f_one
  $CLI DROP TABLE f_two
  $CLI DROP TABLE f_three
  init_float_pk_one
  init_float_pk_two
  init_float_pk_three
  insert_float_pk_one
  insert_float_pk_two
  insert_float_pk_three
  echo $CLI SELECT f_one.val,f_two.val FROM f_one,f_two WHERE f_one.id = f_two.id AND f_one.id BETWEEN 1 AND 10 
  $CLI SELECT "f_one.val,f_two.val" FROM "f_one,f_two" WHERE "f_one.id = f_two.id AND f_one.id BETWEEN 1 AND 10"

  echo $CLI SELECT f_one.val,f_two.val FROM f_one,f_two WHERE f_one.id = f_two.id AND f_one.id IN "(1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9)"
  $CLI SELECT "f_one.val,f_two.val" FROM "f_one,f_two" WHERE "f_one.id = f_two.id AND f_one.id IN (1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9)"

  echo $CLI SELECT f_one.val,f_three.val FROM f_one,f_three WHERE f_one.id = f_three.id AND f_one.id BETWEEN 1 AND 10 
  $CLI SELECT f_one.val,f_three.val FROM "f_one,f_three" WHERE "f_one.id = f_three.id AND f_one.id BETWEEN 1 AND 10"

  echo $CLI SELECT f_one.val,f_three.val FROM f_one,f_three WHERE f_one.id = f_three.id AND f_one.id  IN "(1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9)"
  $CLI SELECT f_one.val,f_three.val FROM "f_one,f_three" WHERE "f_one.id = f_three.id AND f_one.id  IN (1.1,2.2,3.3,4.4,5.5,6.6,7.7,8.8,9.9)"
}

# HARDENING HARDENING HARDENING HARDENING HARDENING HARDENING
# HARDENING HARDENING HARDENING HARDENING HARDENING HARDENING
function update_variation_test() {
  $CLI CREATE TABLE updatee "(id INT, t1 TEXT, t2 TEXT, t3 TEXT)"
  $CLI INSERT INTO updatee VALUES "(1,ONE,TWO,THREE)"
  echo "ONE COL UPDATE -> ERROR "
  $CLI UPDATE updatee SET "t1 = \\\\\\ \, t2=2" WHERE id = 1; $CLI DUMP updatee
  echo "TWO COL UPDATE -> OK "
  $CLI UPDATE updatee SET "t1 = \\\\\\ , t2=2" WHERE id = 1; $CLI DUMP updatee
}

function init_new_joins() {
  $CLI DROP TABLE user
  $CLI DROP TABLE user_last_login
  $CLI DROP TABLE user_action
  $CLI CREATE TABLE user "(id INT, name TEXT, nickname TEXT)"
  $CLI CREATE TABLE user_last_login "(user_id INT, when INT)"
  $CLI CREATE TABLE user_action "(id INT, user_id INT, when INT, deed TEXT)"
  $CLI CREATE INDEX uact_uid_ind ON user_action \(user_id\)
}

function insert_new_joins() {
  $CLI INSERT INTO user VALUES \(1,\"bill\",\"crazy_dude\"\)
  $CLI INSERT INTO user VALUES \(2,\"ted\",\"excellence\"\)
  $CLI INSERT INTO user VALUES \(3,\"bert\",\"meanie\"\)
  $CLI INSERT INTO user VALUES \(4,\"ernie\",\"suave\"\)
  $CLI INSERT INTO user VALUES \(5,\"nolan\",\"speedy\"\)

  DATE=$(date +%s)
  ODATE=$DATE

  DATE=$ODATE
  $CLI INSERT INTO user_last_login VALUES \(1,$DATE\)
  $CLI INSERT INTO user_action VALUES \(1,1,$DATE,"logged in"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(2,1,$DATE,"bought a phone booth"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(3,1,$DATE,"unsubscribed"\)

  DATE=$ODATE
  $CLI INSERT INTO user_last_login VALUES \(2,$DATE\)
  $CLI INSERT INTO user_action VALUES \(4,2,$DATE,"signed up"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(5,2,$DATE,"filled out survey"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(6,2,$DATE,"emailed customer care"\)

  DATE=$[${DATE}+10]
  ODATE=$DATE # reset ODATE
  $CLI INSERT INTO user_last_login VALUES \(3,$DATE\)
  $CLI INSERT INTO user_action VALUES \(7,3,$DATE,"logged in"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(8,3,$DATE,"logged out"\)

  DATE=$ODATE
  $CLI INSERT INTO user_last_login VALUES \(4,$DATE\)
  $CLI INSERT INTO user_action VALUES \(9,4,$DATE,"logged in"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(10,4,$DATE,"searched for books"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(11,4,$DATE,"bookmarked 'Trainspotting'"\)

  DATE=$[${DATE}+10]
  ODATE=$DATE # reset ODATE
  $CLI INSERT INTO user_last_login VALUES \(5,$DATE\)
  $CLI INSERT INTO user_action VALUES \(12,5,$DATE,"logged in"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(13,5,$DATE,"searched for 'baseballs'"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(14,5,$DATE,"bought a cigar"\)
  DATE=$[${DATE}+10]
  $CLI INSERT INTO user_action VALUES \(15,5,$DATE,"logged out"\)
}

function new_join_test() {
  echo "SINGLE -> 1 ROW"
  $CLI SELECT user.name,user_last_login.when FROM user,user_last_login WHERE user.id = user_last_login.user_id AND user.id =1
  echo "SINGLE -> 2 ROWS"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE user.id = 4 AND user.id = user_action.user_id
  echo "RANGE(2)"
  $CLI SELECT user.name,user_action.when,user_action.deed FROM user,user_action WHERE user.id BETWEEN 1 AND 2 AND user.id = user_action.user_id
}

function populate_new_join() {
  init_new_joins
  insert_new_joins
}

function run_new_join_tests() {
  populate_new_join
  new_join_test
}

function wait_on_proc_net_tcp() {
  LIM=$1
  SLEEP_TIME=2
  PROC_TCP=99999
  while [ $PROC_TCP -gt $LIM ]; do
    PROC_TCP=$(wc -l /proc/net/tcp | cut -f 1 -d \ )
    if [ $PROC_TCP -lt $LIM ]; then
        break;
    fi
    echo $0: sleep $SLEEP_TIME - PROC_TCP=$PROC_TCP
    sleep $SLEEP_TIME
  done
}

function looping_sql_test() {
  while true; do
    $CLI flushall;
    sql_test;
    wait_on_proc_net_tcp 10000
  done
}

function large_update() {
  if [ -z "$4" ]; then
    echo "Usage: large_update statement limit variable sleep_throttle"
    return;
  fi
  STMT="$1"
  LIMIT="$2"
  VAR="$3"
  THRTL="$4"

  MOD_STMT="$STMT LIMIT $LIMIT OFFSET $VAR"
  time (
    while true; do
      RES=$($CLI $MOD_STMT | cut -f 2 -d \ )
      if [ "$RES" != "$LIMIT" ]; then echo "BREAK"; break; fi
      sleep $THRTL
    done
  )
}

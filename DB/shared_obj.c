/*
 * This file implements ALCHEMY_DATABASE's error messages
 *

AGPL License

Copyright (c) 2011 Russell Sullivan <jaksprats AT gmail DOT com>
ALL RIGHTS RESERVED 

   This file is part of ALCHEMY_DATABASE

   This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

 */

#include <strings.h>

#include "redis.h"

#include "xdb_hooks.h"

void DXDB_createSharedObjects() {
    shared.undefinedcolumntype = createObject(REDIS_STRING,sdsnew(
        "-ERR Column Type Unknown ALCHEMY_DATABASE uses[INT,LONG,FLOAT,TEXT,U128] and recognizes INT=[*INT],LONG=[BIGINT],FLOAT=[FLOAT,REAL,DOUBLE],TEXT=[*CHAR,TEXT,BLOB,BINARY,BYTE]\r\n"));
    shared.toofewcolumns = createObject(REDIS_STRING,sdsnew(
        "-ERR Too few columns (min 2)\r\n"));
    shared.nonuniquecolumns = createObject(REDIS_STRING,sdsnew(
        "-ERR Column name defined more than once\r\n"));
    shared.nonuniquetablenames = createObject(REDIS_STRING,sdsnew(
        "-ERR Table name already exists\r\n"));
    shared.nonuniqueindexnames = createObject(REDIS_STRING,sdsnew(
        "-ERR Index name already exists\r\n"));
    shared.indextargetinvalid = createObject(REDIS_STRING,sdsnew(
        "-ERR Index on Tablename.columnname target error\r\n"));
    shared.indexedalready = createObject(REDIS_STRING,sdsnew(
        "-ERR Tablename.Columnname is ALREADY indexed\r\n"));
    shared.index_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE INDEX' command\r\n"));
    shared.nonuniquekeyname = createObject(REDIS_STRING,sdsnew(
        "-ERR Key name already exists\r\n"));
    shared.trigger_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE TRIGGER' command\r\n"));
    shared.luatrigger_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR wrong number of arguments for 'CREATE LUATRIGGER' command\r\n"));

    shared.nonexistenttable = createObject(REDIS_STRING,sdsnew(
        "-ERR Table does not exist\r\n"));
    shared.insertcolumn = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (1234,'abc',,,)\r\n"));
    shared.insert_ovrwrt = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT on Existing Data - use REPLACE\r\n"));

    shared.uint_pkbig = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: PK greater than UINT_MAX(4GB)\r\n"));
    shared.col_uint_string_too_long = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: UINT Column longer than 32 bytes\r\n"));
    shared.u2big = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: UINT Column greater than UINT_MAX(4GB)\r\n"));
    shared.col_float_string_too_long = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT: FLOAT Column longer than 32 bytes\r\n"));

    shared.nonexistentcolumn = createObject(REDIS_STRING,sdsnew(
        "-ERR Column does not exist\r\n"));
    shared.nonexistentindex = createObject(REDIS_STRING,sdsnew(
        "-ERR Index does not exist\r\n"));
    shared.drop_virtual_index = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Primary Key Indices can not be dropped\r\n"));
    shared.drop_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: LRU Indices can not be dropped\r\n"));
    shared.drop_ind_on_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Index on SHARDKEY can not be dropped\r\n"));
    shared.drop_luatrigger = createObject(REDIS_STRING,sdsnew(
        "-ERR TARGET: DROP LUATRIGGER on wrong object\r\n"));
    shared.badindexedcolumnsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: JOIN WHERE tablename.columname ...\r\n"));
    shared.luat_decl_fmt = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LUATRIGGER name ON table ADD_FUNC [DEL_FUNC] - NOTE: no U128 support\r\n"));
    shared.luat_c_decl = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LUATRIGGER ... ADD_FUNC can ONLY contain column names and commas, e.g. \"luafunc(col1, col2, col3)\"\r\n"));

    shared.invalidupdatestring = createObject(REDIS_STRING,sdsnew(
        "-ERR UPDATE: SET error, syntax is col1=val1,col2=val2,....\r\n"));
    shared.invalidrange = createObject(REDIS_STRING,sdsnew(
        "-ERR RANGE: Invalid range\r\n"));

    shared.toomany_nob = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY columns MAX = 16\r\n"));

    shared.mci_on_pk = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Compound Indexes can NOT be on PrimaryKey\r\n"));
    shared.UI_SC = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX must be a Compound Index - e.g. ON (fk1, fk2)\r\n"));
    shared.two_uniq_mci = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: only ONE UNIQUE INDEX per table\r\n"));
    shared.uniq_mci_notint = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX final column must be INT\r\n"));
    shared.uniq_mci_pk_notint = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UNIQUE INDEX Primary Key must be INT\r\n"));

    shared.accesstypeunknown = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT ... WHERE x IN ([SELECT|SCAN])\r\n"));

    shared.createsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: \"CREATE TABLE tablename (columnname type,,,,)\" OR \"CREATE INDEX indexname ON tablename (columnname) [ORDER BY othercolumn] [OFFSET X]\" OR \"CREATE LRUINDEX ON tablename\" OR \"CREATE LUATRIGGER luatriggername ON tablename ADD_LUA_CALL DEL_LUA_CALL\"\r\n"));
    shared.createsyntax_dn = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE TABLE tablename (luaobj.x.y.z,,,) TYPE\r\n"));
    shared.dropsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DROP TABLE tablename OR DROP INDEX indexname OR DROP LUATRIGGER\r\n"));
    shared.altersyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: ALTER TABLE tablename ADD [COLUMN columname type[INT,LONG,FLOAT,TEXT,U128]] [SHARDKEY columname] [FOREIGN KEY (fk_name) REFERENCES othertable (other_table_indexed_column)] [HASHABILITY] - ALTER TABLE tablename SET DIRTY\r\n"));
    shared.alter_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: ALTER TABLE - CAN NOT be done on OPTIMISED 2 COLUMN TABLES\r\n"));
    shared.lru_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LRUINDEX ON tablename - CAN NOT be done on OPTIMISED 2 COLUMN TABLES\r\n"));
    shared.lru_repeat = createObject(REDIS_STRING,sdsnew(
        "-ERR LOGIC: LRUINDEX already exists on this table\r\n"));
    shared.col_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR KEYWORD: LRU is a keyword, can not be used as a columnname\r\n"));
    shared.update_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: LRU column can not be DIRECTLY UPDATED\r\n"));
    shared.insert_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: INSERT of LRU column DIRECTLY not kosher\r\n"));
    shared.insert_replace_update = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED:  REPLACE INTO tbl ... ON DUPLICATE KEY UPDATE\r\n"));

    shared.insertsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,)\r\n"));
    shared.insertsyntax_no_into = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,) - \"INTO\" keyword MISSING\r\n"));
    shared.insertsyntax_no_values = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO tablename VALUES (vals,,,,) - \"VALUES\" keyword MISSING\r\n"));
    shared.part_insert_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: INSERT INTO table with 2 values, both values must be specified - these tables are optimised and stored inside the BTREE and MUST have ALL values defined\r\n"));

    shared.key_query_mustbe_eq = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT WHERE fk != 4 - primary key or index lookup must use EQUALS (=) - OR use \"SCAN\"\r\n"));

    shared.whereclause_in_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col IN (...) - \"IN\" requires () delimited list\r\n"));
    shared.where_in_select = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col IN (SELECT col ....) INNER SELECT SYNTAX ERROR \r\n"));
    shared.whereclause_between = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE col BETWEEN x AND y\r\n"));

    shared.wc_orderby_no_by = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col - \"BY\" MISSING\r\n"));
    shared.order_by_col_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: ORDER BY columname - column does not exist\r\n"));
    shared.oby_lim_needs_num = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col [DESC] LIMIT N = \"N\" MISSING\r\n"));
    shared.oby_ofst_needs_num = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE ... ORDER BY col [DESC] LIMIT N OFFSET M = \"M\" MISSING\r\n"));
    shared.orderby_count = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT COUNT(*) ... WHERE ... ORDER BY col - \"ORDER BY\" and \"COUNT(*)\" dont mix, drop the \"ORDER BY\"\r\n"));

    shared.selectsyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT [col,,,,] FROM tablename WHERE [[indexed_column = val]|| [indexed_column BETWEEN x AND y] || [indexed_column IN (X,Y,Z,...)] || [indexed_column IN (nested sql statment)]] [ORDER BY [col [DESC/ASC]*,,] LIMIT n OFFSET m]\r\n"));
    shared.selectsyntax_nofrom = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT col,,,, FROM tablename WHERE indexed_column = val - \"FROM\" keyword MISSING\r\n"));
    shared.selectsyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT col,,,, FROM tablename WHERE indexed_column = val - \"WHERE\" keyword MISSING\r\n"));
    shared.rangequery_index_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - indexed_column either non-existent or not indexed\r\n"));

    shared.deletesyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DELETE FROM tablename WHERE indexed_column = val || WHERE indexed_column BETWEEN x AND y\r\n"));
    shared.deletesyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DELETE FROM tablename WHERE indexed_column = val - \"WHERE\" keyword MISSING\r\n"));

    shared.updatesyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE tablename SET col1=val1,col2=val2,,,, WHERE indexed_column = val\r\n"));
    shared.updatesyntax_nowhere = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE tablename SET col1=val1,col2=val2,,,, WHERE indexed_column = val \"WHERE\" keyword MISSING\r\n"));
    shared.update_pk_range_query = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE of PK not allowed with Range Query\r\n"));
    shared.update_pk_ovrw = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UPDATE of PK would overwrite existing row - USE \"REPLACE\"\r\n"));
    shared.update_expr = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: parse error\r\n"));
    shared.update_expr_col = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: SYNTAX: 'columname OP value' - OP=[+-*/%||] value=[columname,integer,float]\r\n"));
    shared.update_expr_div_0 = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression evaluation resulted in divide by zero\r\n"));
    shared.update_expr_mod = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: MODULO only possible on INT columns \r\n"));
    shared.update_expr_cat = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: String Concatenation only possible on TEXT columns \r\n"));
    shared.update_expr_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: TEXT columns do not support [+-*/%^] Operations\r\n"));
    shared.update_expr_empty_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: Concatenating Empty String\r\n"));
    shared.update_expr_math_str = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: INT or FLOAT columns can not be set to STRINGS\r\n"));
    shared.update_expr_col_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: UPDATE expression: SYNTAX: Left Hand Side column operations not possible on other columns in table\r\n"));
    shared.update_expr_float_overflow = createObject(REDIS_STRING,sdsnew(
        "-ERR MATH: UPDATE expression: Floating point arithmetic produced overflow or underflow [FLT_MIN,FLT_MAX]\r\n"));
    shared.up_on_mt_col = createObject(REDIS_STRING,sdsnew(
        "-ERR LOGIC: UPDATE expression against an empty COLUMN - behavior undefined\r\n"));
    shared.neg_on_uint = createObject(REDIS_STRING,sdsnew(
        "-ERR MATH: UPDATE expression: NEGATIVE value against UNSIGNED [INT,LONG,U128]\r\n"));

    shared.wc_col_not_found = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - Column does not exist\r\n"));
    shared.whereclause_col_not_indxd = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE indexed_column = val - Column must be indexed\r\n"));
    shared.whereclause_no_and = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: WHERE-CLAUSE: WHERE indexed_column BETWEEN start AND finish - \"AND\" MISSING\r\n"));

    shared.scansyntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SCAN col,,,, FROM tablename [WHERE [indexed_column = val]|| [indexed_column BETWEEN x AND y] [ORDER BY col LIMIT num offset] ]\r\n"));
    shared.cr8tbl_scan = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE TABLE AS SCAN not yet supported\r\n"));

    shared.toofewindicesinjoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: Too few indexed columns in join(min=2)\r\n"));
    shared.toomanyindicesinjoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: MAX indices in JOIN reached(64)\r\n"));
    shared.joincolumnlisterror = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: error in columnlist (select columns)\r\n"));
    shared.join_order_by_tbl = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - table does not exist\r\n"));
    shared.join_order_by_col = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - column does not exist\r\n"));
    shared.join_table_not_in_query = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: ORDER BY tablename.columname - table not in SELECT *\r\n"));
    shared.joinsyntax_no_tablename = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SELECT tbl.col,,,, FROM tbl1,tbl2 WHERE tbl1.indexed_column = tbl2.indexed_column AND tbl1.indexed_column BETWEEN x AND y - MISSING table-name in WhereClause\r\n"));
    shared.join_chain = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: TABLES in WHERE statement must form a chain to be a star schema (e.g. WHERE t1.pk = 3 AND t1.fk = t2.pk AND t2.pk = t3.pk AND t3.fk = t4.pk [tables were chained {t1,t2,t3,t4 each joined to one another}]\r\n"));
    shared.fulltablejoin = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: Join has a full table scan in it (maybe in the middle) - this is not supported via SELECT - use \"SCAN\"\r\n"));
    shared.joindanglingfilter = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: relationship not joined (i.e. a.x = 5 and a is not joined)\r\n"));
    shared.join_noteq = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: table joins only possible via the EQUALS operator (e.g. t1.fk = t2.fk2 ... not t1.fk < t2.fk2)\r\n"));
    shared.join_coltypediff = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: column types of joined columns do not match\r\n"));
    shared.join_col_not_indexed = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: joined column IS not indexed - USE \"SCAN\"\r\n"));
    shared.join_qo_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: query optimiser could not find a join plan\r\n"));
    shared.join_type_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: JOIN: joined column's types do not match\r\n"));

    shared.create_table_err = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE TABLE tablename (col INT,,,,,,) [SELECT ....]\r\n"));
    shared.create_table_as_count = createObject(REDIS_STRING,sdsnew(
        "-ERR TYPE: CREATE TABLE tbl AS SELECT COUNT(*) - is disallowed\r\n"));

    shared.dump_syntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: DUMP tablename [TO MYSQL [mysqltablename]],[TO FILE fname]\r\n"));
    shared.show_syntax = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: SHOW [TABLES|INDEXES]\r\n"));

    shared.alter_sk_rpt = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: TABLE can only have 1 shardkey\r\n"));
    shared.alter_sk_no_i = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: must be Indexed\r\n"));
    shared.alter_sk_no_lru = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: can not be on LRU column\r\n"));
    shared.alter_fk_not_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR ALTER TABLE ADD FOREIGN KEY: must point from this table's shard-key to the foreign table's shard-key\r\n"));
    shared.alter_fk_repeat = createObject(REDIS_STRING,sdsnew(
        "-ERR ALTER TABLE ADD FOREIGN KEY: table already has foreign key, drop foreign key first to redefine ... and caution if your data is already distributed\r\n"));
    shared.alter_sk_no_lfu = createObject(REDIS_STRING,sdsnew(
        "-ERR SHARDKEY: can not be on LFU column\r\n"));

    shared.select_on_sk = createObject(REDIS_STRING,sdsnew(
        "-ERR SELECT: NOT ON SHARDKEY\r\n"));
    shared.scan_sharded = createObject(REDIS_STRING,sdsnew(
        "-ERR SCAN: PROHIBITED on SHARDED TABLE -> USE \"DSCAN\"\r\n"));

    shared.constraint_wrong_nargs = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE CONSTRAINT constraintname ON table (column) RESPECTS INDEX (indexname) [ASC|DESC]\r\n"));
    shared.constraint_col_indexed = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: CREATE CONSTRAINT ON indexed (column) - column can NOT be indexed\r\n"));
    shared.constraint_not_num     = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: CREATE CONSTRAINT ON [INT|LONG] (column) RESPECT [INT|LONG] INDEX - both column and index must be [INT|LONG]\r\n"));
    shared.constraint_table_mismatch = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: CREATE CONSTRAINT ON table RESPECT INDEX index - index does not belong to table\r\n"));
    shared.constraint_nonuniq        = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: CREATE CONSTRAINT constraintname exists already\r\n"));
    shared.constraint_viol        = createObject(REDIS_STRING,sdsnew(
        "-ERR CONSTRAINT_VIOLATION: INSERT violated table's constraint\r\n"));

    shared.indexobcerr            = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE INDEX ... ORDER BY col - column not found\r\n"));
    shared.indexobcrpt            = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE INDEX ... ORDER BY PK - PK ordering is default, operation not needed\r\n"));
    shared.indexobcill            = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE INDEX ... ORDER BY col - Lots of constraints: No UniqueMultipleColumnIndexes, Both indexed_column & order_by_column must be [INT|LONG] and can not be the same column\r\n"));

    shared.indexcursorerr         = createObject(REDIS_STRING,sdsnew(
        "-ERR CREATE INDEX ... OFFEST NUM error - caveats: PK must be [INT|LONG], NUM must be positive\r\n"));

    shared.obindexviol            = createObject(REDIS_STRING,sdsnew(
        "-ERR ORDER BY INDEX - ordered by column has a repeat value, this is analagous to a duplicate PK and a violation\r\n"));

    shared.repeat_hash_cnames     = createObject(REDIS_STRING,sdsnew(
        "-ERR INSERT into TABLE with HASHABILITY - REPEATING column names\r\n"));

    shared.lfu_other = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE LFUINDEX ON tablename - CAN NOT be done on OPTIMISED 2 COLUMN TABLES\r\n"));
    shared.lfu_repeat = createObject(REDIS_STRING,sdsnew(
        "-ERR LOGIC: LFUINDEX already exists on this table\r\n"));
    shared.drop_lfu = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: LFU Indices can not be dropped\r\n"));
    shared.col_lfu = createObject(REDIS_STRING,sdsnew(
        "-ERR KEYWORD: LFU is a keyword, can not be used as a columnname\r\n"));

    shared.kw_cname = createObject(REDIS_STRING,sdsnew(
        "-ERR KEYWORD: ColumnName is a keyword [LRU,LFU]\r\n"));

    shared.u128_parse = createObject(REDIS_STRING,sdsnew(
        "-ERR PARSE: U128's are represented as \"high|low\" - the '|' is mandatory\r\n"));
    shared.update_u128_complex = createObject(REDIS_STRING,sdsnew(
        "-ERR PARSE: UPDATING U128 columns MUST be simple equality updates (e.g. SET u128col = 11111|2222222)\r\n"));
    shared.uniq_simp_index_nums = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Unique indexes are only on [INT,LONG,U128] columns on tables w/ [INT,LONG,U128] Primary Keys\r\n"));
    shared.updateipos           = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: UPDATING index.pos()\r\n"));

    shared.prepare_syntax       = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: PREPARE planname AS SELECT cols FROM tbls WHERE clause\r\n"));
    shared.supported_prepare    = createObject(REDIS_STRING,sdsnew(
        "-ERR SUPPORTED: PREPARE does not yet support [IN() clause, RangeQueries]\r\n"));
    shared.execute_argc         = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: EXECUTE number-of-args does NOT match PREPARE number-of-args\r\n"));
    shared.execute_miss         = createObject(REDIS_STRING,sdsnew(
        "-ERR NOT-FOUND: EXECUTE prepared statement not found\r\n"));

    shared.dirty_miss = createObject(REDIS_STRING,sdsnew(
        "-MISS: SELECT hit a MISSED row, unable to complete\r\n"));
    shared.deletemiss             = createObject(REDIS_STRING,sdsnew(
        "-MISS: DELETE hit a MISSED row, unable to complete\r\n"));
    shared.updatemiss             = createObject(REDIS_STRING,sdsnew(
        "-MISS: UPDATE hit a MISSED row, unable to complete\r\n"));
    shared.evict_other = createObject(REDIS_STRING,sdsnew(
        "-ERR: EVICT only supported on tables w/ [INT|LONG] PKs & 2+ columns\r\n"));

    shared.replace_dirty        = createObject(REDIS_STRING,sdsnew(
        "-ERR: REPLACE on a Table w/ SecondaryIndexes & EVICTIONS - PROHIBITED\r\n"));
    shared.insert_dirty_pkdecl = createObject(REDIS_STRING,sdsnew(
        "-ERR: INSERT on a Table w/ EVICTIONS can NOT declare PK values (auto-increment must be used)\r\n"));

    shared.evictnotdirty          = createObject(REDIS_STRING,sdsnew(
        "-ERR: EVICT can not be called on NON-DIRTY tables -> call \"ALTER table SET DIRTY\"\r\n"));

    shared.range_mciup            = createObject(REDIS_STRING,sdsnew(
        "-ERR: PROHIBITED: Range Updating a column that belongs to a compound index (Alchemy does NOT rollback)\r\n"));
    shared.range_u_up             = createObject(REDIS_STRING,sdsnew(
        "-ERR: PROHIBITED: Range Updating a column that belongs to a UNIQUE index (Alchemy does NOT rollback)\r\n"));
    shared.uviol                  = createObject(REDIS_STRING,sdsnew(
        "-ERR: VIOLATION: UNIQUE INDEX CONSTRAINT FAILED\r\n"));
    shared.dirtypk                = createObject(REDIS_STRING,sdsnew(
        "-ERR: PROHIBITED: ONLY tables w/ [INT|LONG|U128] PKs can be set to DIRTY\r\n"));

    shared.update_luaobj_complex = createObject(REDIS_STRING,sdsnew(
        "-ERR PARSE: UPDATING LUAOBJ columns MUST be done via pre defined boolean functions (e.g. SET bool_update_func(luaobjcol),,,\r\n"));
    shared.unsupported_pk        = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Invalid Primary Key Type. Supported Types: [INT,LONG,U128,FLOAT,TEXT]\r\n"));
    shared.order_by_luaobj       = createObject(REDIS_STRING,sdsnew(
        "-ERR UNDEFINED: sorting by a LUAOBJ requires a function\r\n"));
    shared.buildindexdirty       = createObject(REDIS_STRING,sdsnew(
        "-ERR PROHIBITED: Build Index on Dirty Table\r\n"));
    shared.cr8tablesyntax        = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE TABLE tblname (colname coltype,,,,,)\r\n"));
    shared.joindotnotation       = createObject(REDIS_STRING,sdsnew(
        "-ERR NOT_SUPPORTED: JOINs on DotNotationIndexes (i.e. luaobj.x) are not yet supported, if you have a good use-case, please email us\r\n"));

    shared.http_not_on           = createObject(REDIS_STRING,sdsnew(
        "-ERR CONFIGURATION: HTTP must be turned on, use [webserver_mode, rest_api_mode]\r\n"));

    shared.create_findex         = createObject(REDIS_STRING,sdsnew(
        "-ERR SYNTAX: CREATE INDEX indexname ON tablename (functionname()) TYPE constructor [destructor]\r\n"));
    shared.luafuncindex_rpt      = createObject(REDIS_STRING,sdsnew(
        "-ERR: CREATE INDEX indexname ON tablename (functionname()) TYPE constructor [destructor] - TABLE already has this functionname indexed\r\n"));
}

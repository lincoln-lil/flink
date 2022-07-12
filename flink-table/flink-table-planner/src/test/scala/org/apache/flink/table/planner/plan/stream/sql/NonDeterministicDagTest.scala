/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo

import org.junit.{Before, Test}

class NonDeterministicDagTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, String, Long)](
      "MyTable",
      'a,
      'b,
      'c,
      'proctime.proctime,
      'rowtime.rowtime)
    util.addTableSource[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)
    util.addTableSource[(Long, Int, String)]("T1", 'a, 'b, 'c)
    util.addTableSource[(Long, Int, String)]("T2", 'a, 'b, 'c)
    util.addTableSource(
      "MyTable1",
      Array[TypeInformation[_]](
        Types.BYTE,
        Types.SHORT,
        Types.INT,
        Types.LONG,
        Types.FLOAT,
        Types.DOUBLE,
        Types.BOOLEAN,
        Types.STRING,
        Types.LOCAL_DATE,
        Types.LOCAL_TIME,
        Types.LOCAL_DATE_TIME,
        DecimalDataTypeInfo.of(30, 20),
        DecimalDataTypeInfo.of(10, 5)
      ),
      Array(
        "byte",
        "short",
        "int",
        "long",
        "float",
        "double",
        "boolean",
        "string",
        "date",
        "time",
        "timestamp",
        "decimal3020",
        "decimal105")
    )
  }

  @Test
  def testNonDeterministicFunc(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.verifyExecPlan(s"""
                           |select
                           |  `day`, a, count(*) cnt, sum(b) qmt
                           |from (
                           |  select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc
                           |) t
                           |group by `day`, a
                           |""".stripMargin)
  }

  @Test
  def testNonDeterministicFuncOnPk(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | primary key (c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.verifyExecPlan(
      s"""
         |select
         |  `day`, count(*) cnt, sum(b) qmt
         |from (
         |  select *, concat(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) `day` from cdc
         |) t
         |group by `day`
         |""".stripMargin)
  }

  @Test
  def testNonDeterministicFuncProjectPk(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | primary key (c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.verifyExecPlan(s"""
                           |select
                           |  `day`, count(*) cnt, sum(b) qmt
                           |from (
                           |  select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc
                           |) t
                           |group by `day`
                           |""".stripMargin)
  }

  // TODO add a correlate, requireDeterminism no include left
  // TODO add a correlate, requireDeterminism no include right
  // TODO add a correlate, condition is nonDeterministic
  // TODO add a calc, condition is nonDeterministic
  // TODO add a lookup, only select lookup key field, expect not affect NDU
  // TODO add a lookup, only select lookup key field, but with ND-call, expect exception
  // TODO add a lookup, select non lookup key fields, expect exception
  // TODO add a lookup, join condition is nonDeterministic
  // TODO add a lookup, pushed projection is nonDeterministic, e.g., dim.ts>now()-interval 1'hour'

  // TODO add a cdc source, select metadata
  // TODO add a cdc source, select metadata with rename

  @Test
  def testView(): Unit = {
    // non-reused plan
    /*
     * Sink(table=[default_catalog.default_database.sink1], fields=[a, day, EXPR$2])
        +- GroupAggregate(groupBy=[a, day], select=[a, day, SUM_RETRACT(b) AS EXPR$2])
           +- Exchange(distribution=[hash[a, day]])
              +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b])
                 +- TableSourceScan(table=[[default_catalog, default_database, src1, project=[a, b], metadata=[]]], fields=[a, b])

        Sink(table=[default_catalog.default_database.sink2], fields=[a, day, b])
        +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b], where=[(b > 100)])
           +- TableSourceScan(table=[[default_catalog, default_database, src1, filter=[], project=[a, b], metadata=[]]], fields=[a, b])
     */

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      RelNodeBlockPlanBuilder.TABLE_OPTIMIZER_REUSE_OPTIMIZE_BLOCK_WITH_DIGEST_ENABLED,
      true)
    // reused plan
    /*
     *
    TableSourceScan(table=[[default_catalog, default_database, src1]], fields=[a, b, c, d])(reuse_id=[1])

    Sink(table=[default_catalog.default_database.sink1], fields=[a, day, EXPR$2])
    +- GroupAggregate(groupBy=[a, day], select=[a, day, SUM_RETRACT(b) AS EXPR$2])
       +- Exchange(distribution=[hash[a, day]])
          +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b])
             +- Reused(reference_id=[1])

    Sink(table=[default_catalog.default_database.sink2], fields=[a, day, b])
    +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b], where=[(b > 100)])
       +- Reused(reference_id=[1])
     */

    // after correction
    /*
      TableSourceScan(table=[[default_catalog, default_database, src1]], fields=[a, b, c, d])(reuse_id=[1])

      Sink(table=[default_catalog.default_database.sink1], fields=[a, day, EXPR$2])
      +- GroupAggregate(groupBy=[a, day], select=[a, day, SUM_RETRACT(b) AS EXPR$2])
         +- Exchange(distribution=[hash[a, day]])
            +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b], updateMaterialize=[true])
               +- Reused(reference_id=[1])

      Sink(table=[default_catalog.default_database.sink2], fields=[a, day, b])
      +- Calc(select=[a, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd') AS day, b], where=[(b > 100)], updateMaterialize=[true])
         +- Reused(reference_id=[1])

     */

    util.tableEnv.executeSql("""
                               |create temporary table src1 (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink2 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql(s"""
                                |create temporary view v1 as
                                |select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day`
                                |from src1
                                |""".stripMargin)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(s"""
                            |insert into sink1
                            |select a, `day`, sum(b)
                            |from v1
                            |group by a, `day`
                            |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink2
                            |select a, `day`, b
                            |from v1
                            |where b > 100
                            |""".stripMargin)
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testNestedSourceView(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE src (
         |  id int,
         |  deepNested row<nested1 row<name string, `value` int>,
         |    nested2 row<num int, flag boolean>>,
         |  name string,
         |  metadata_1 int metadata,
         |  metadata_2 string metadata,
         |  primary key(id, name) not enforced
         |) WITH (
         |  'connector' = 'values',
         |  'nested-projection-supported' = 'true',
         |  'changelog-mode' = 'I,UA,UB,D',
         |  'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
         |)
         |""".stripMargin
    util.tableEnv.executeSql(ddl)

    util.tableEnv.executeSql(
      """
        |create view v1 as
        |SELECT id,
        |       deepNested.nested2.num AS a,
        |       deepNested.nested1.name AS name,
        |       deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as b
        |FROM src
        |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink2 (
                               |  a int,
                               |  b string,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      s"""
         |insert into sink1
         |select a, `day`, sum(b)
         |from (select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day` from v1) t
         |group by a, `day`
         |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink2
                            |select a, name, b
                            |from v1
                            |where b > 100
                            |""".stripMargin)
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testJoinedView(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table src1 (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table src2 (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               |  a int,
                               |  b string,
                               |  c bigint,
                               |  d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink2 (
                               |  a int,
                               |  b string,
                               |  c bigint,
                               |  d string
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql(
      s"""
         |create temporary view v1 as
         |select
         |  t1.a as a, t1.`day` as `day`, t2.b as b, t2.c as c
         |from (
         |  select a, b, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') as `day`
         |  from src1
         | ) t1
         |join (
         |  select b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `day`, c, d
         |  from src2
         |) t2
         | on t1.a = t2.d
         |""".stripMargin)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(s"""
                            |insert into sink1
                            |select a, `day`, sum(b), count(distinct c)
                            |from v1
                            |group by a, `day`
                            |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink2
                            |select a, `day`, b, c
                            |from v1
                            |where b > 100
                            |""".stripMargin)
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testNestedAgg(): Unit = {
    util.addTableSource[(String, Long, String, Int)]("src", 'a, 'b, 'c, 'cnt)
    util.tableEnv.executeSql(s"""
                                |-- first group by
                                |create view v1 as
                                |select
                                |  concat(a, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `a-day`
                                |  ,sum(cnt) as total
                                |from src
                                |group by a
                                |""".stripMargin)
    val sql =
      s"""
         |-- second group by
         |select
         |  `a-day`
         |  ,sum(total)
         |from v1
         |group by `a-day`
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testOptimalRankPlan(): Unit = {
    util.addTableSource[(Int, Long, String, Int)]("src", 'a, 'b, 'c, 'd)
    util.tableEnv.executeSql(s"""
                                | create view v1 as
                                |  select a, b, sum(d) cnt
                                |  from src
                                |  group by a,b
                                | """.stripMargin)
    util.verifyExecPlan(s"""
                           |select * from (
                           | select
                           |  a, cnt, row_number() over (partition by a order by cnt desc) rn
                           | from v1
                           | ) t where t.rn < 4
                           |""".stripMargin)
  }

  @Test
  def testOptimalRankPlan1(): Unit = {
    util.addTableSource[(Int, Long, String, Int)]("src", 'a, 'b, 'c, 'd)
    util.tableEnv.executeSql(s"""
                                | create view v1 as
                                |  select a, b, sum(d) cnt
                                |  from src
                                |  group by a,b
                                | """.stripMargin)
    util.verifyExecPlan(s"""
                           |select * from (
                           | select
                           |  a, b, cnt, row_number() over (partition by a order by cnt desc) rn
                           | from v1
                           | ) t where t.rn < 4
                           |""".stripMargin)
  }

  @Test
  def testOptimalRankPlan2(): Unit = {
    util.tableEnv.executeSql("""
                               |create temporary table src (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)
    util.tableEnv.executeSql(s"""
                                | create view v1 as
                                |  select a, b, count(*) mc
                                |  from src
                                |  group by a,b
                                | """.stripMargin)
    util.verifyExecPlan(s"""
                           |select * from (
                           | select
                           |  a, mc, row_number() over (partition by a order by mc desc) rn
                           | from v1
                           | ) t where t.rn = 1
                           |""".stripMargin)
  }

  @Test
  def testJoin(): Unit = {
    util.tableEnv.executeSql("""
                               |create table src (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.verifyExecPlan(
      s"""
         |select t1.a, t1.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`
         |  from src
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from src
         |) t2
         |  on t1.a = t2.a
         |""".stripMargin)
  }

  @Test
  def testJoin2(): Unit = {
    // TODO add back pk for input, it can help state optimization
    // InputSideHasUniqueKey vs InputSideHasNoUniqueKey
    // save one get & shorten mapstate's key (friendly to kv-separate)
    util.tableEnv.executeSql("""
                               |create table src (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.verifyExecPlan(
      s"""
         |select t1.a, t1.c, t1.`c-day`, t2.b, t2.c, t2.d
         |from (
         |  select a, b, c, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`
         |  from src
         | ) t1
         |join (
         |  select a, b, c, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from src
         |) t2
         |  on t1.a = t2.a
         |""".stripMargin)
  }

}

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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.config.OptimizerConfigOptions.NonDeterministicUpdateHandling
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.planner.{JBoolean, JLong}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.{CountAccumulator, StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

import scala.util.Random

@RunWith(classOf[Parameterized])
class NonDeterministicDagTest(tryResolve: Boolean) extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()

  @Before
  def before(): Unit = {
    if (tryResolve) {
      util.tableConfig.getConfiguration.set(
        OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_HANDLING,
        NonDeterministicUpdateHandling.TRY_RESOLVE)
    }

    util.addTableSource[(Int, Long, String, Boolean)]("T", 'a, 'b, 'c, 'd)
    util.addDataStream[(Int, String, Long)]("T1", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

    util.tableEnv.executeSql("""
                               |create temporary table src (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table upsert_src (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_computed_col (
                               |  a int,
                               |  b bigint,
                               |  c string,
                               |  d int,
                               |  `day` as DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd'),
                               |  primary key(a, c) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D'
                               |)""".stripMargin)
    util.tableEnv.executeSql(
      """
        |create temporary table cdc_with_meta (
        | a int,
        | b bigint,
        | c string,
        | d boolean,
        | metadata_1 int metadata,
        | metadata_2 string metadata,
        | metadata_3 bigint metadata,
        | primary key (a) not enforced
        |) with (
        | 'connector' = 'values',
        | 'changelog-mode' = 'I,UA,UB,D',
        | 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'
        |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_watermark (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | op_ts timestamp_ltz(3),
                               | primary key (a) not enforced,
                               | watermark for op_ts as op_ts - interval '5' second
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'op_ts:timestamp_ltz(3)'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_meta_and_wm (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | op_ts timestamp_ltz(3) metadata,
                               | primary key (a) not enforced,
                               | watermark for op_ts as op_ts - interval '5' second
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'op_ts:timestamp_ltz(3)'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_with_composite_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | d bigint,
                               | primary key (a,d) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_with_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table sink_without_pk (
                               | a int,
                               | b bigint,
                               | c string
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table dim_with_pk (
                               | a int,
                               | b bigint,
                               | c string,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values'
                               |)""".stripMargin)

    util.tableEnv.executeSql("""
                               |create temporary table dim_without_pk (
                               | a int,
                               | b bigint,
                               | c string
                               |) with (
                               | 'connector' = 'values'
                               |)""".stripMargin)
    // builtin ND calls
    // 'CURRENT_TIMESTAMP', 'NOW', 'UUID', 'CURRENT_DATE', 'PROCTIME', 'UNIX_TIMESTAMP'
    // custom ND function
    util.tableEnv.createTemporaryFunction("ndFunc", TestNonDeterministicUdf)
    util.tableEnv.createTemporaryFunction("ndTableFunc", TestNonDeterministicUdtf)
    util.tableEnv.createTemporaryFunction("ndAggFunc", TestTestNonDeterministicUdaf)
    // deterministic table function
    util.tableEnv.createTemporaryFunction("str_split", new StringSplit())
  }

  @Test
  def testCdcWithMetaSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, metadata_3, c
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaRenameSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.tableEnv.executeSql("""
                               |create temporary table cdc_with_meta_rename (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | metadata_3 bigint metadata,
                               | e as metadata_3,
                               | primary key (a) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,UB,D',
                               | 'readable-metadata' = 'metadata_3:BIGINT'
                               |)""".stripMargin)

    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, e from cdc_with_meta_rename
                                 |""".stripMargin)
  }

  @Test
  def testSourceWithComputedColumnSinkWithPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }

    // can not infer pk from cdc source with computed column(s)
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, `day`
                                 |from cdc_with_computed_col
                                 |where b > 100
                                 |""".stripMargin)
  }

  @Test
  def testSourceWithComputedColumnMultiSink(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(s"""
                            |insert into sink_without_pk
                            |select a, sum(b), `day`
                            |from cdc_with_computed_col
                            |group by a, `day`
                            |""".stripMargin)
    stmtSet.addInsertSql(s"""
                            |insert into sink_with_pk
                            |select a, b, `day`
                            |from cdc_with_computed_col
                            |where b > 100
                            |""".stripMargin)
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncSinkWithPK(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  t1.a, t1.b, a1
                                 |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncNoLeftOutput(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndTableFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk(a)
                                 |select
                                 |  cast(a1 as integer) a
                                 |from cdc t1, lateral table(ndTableFunc(a)) as T(a1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateNonDeterministicFuncNoRightOutput(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from cdc t1 join lateral table(ndTableFunc(a)) as T(a1) on true
                                 |""".stripMargin)
  }

  @Test
  def testCdcCorrelateOnNonDeterministicCondition(): Unit = {
    // TODO update this after FLINK-7865 was fixed
    thrown.expectMessage("unexpected correlate variable $cor0 in the plan")
    thrown.expect(classOf[TableException])
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from cdc t1 join lateral table(str_split(c)) as T(c1) 
                                 | -- the join predicate can only be empty or literal true for now
                                 |  on ndFunc(b) > 100
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithMetaCorrelateSinkWithPk(): Unit = {
    // Under ignore mode, the generated execution plan may cause wrong result though
    // upsertMaterialize has been enabled in sink, because
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_1' in cdc source may cause wrong result or error on downstream operators")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.metadata_1, T.c1
                                 |from cdc_with_meta t1, lateral table(str_split(c)) as T(c1)
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFuncSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, ndFunc(b), c
                                 |from cdc 
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFuncSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$1(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, ndFunc(b), c
                                 |from cdc 
                                 |""".stripMargin)
  }

  @Test
  def testCdcWithNonDeterministicFilter(): Unit = {
    // TODO should throw error if tryResolve is true after FLINK-2xxxx was fixed
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t1.c
                                 |from cdc t1
                                 |where t1.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkSinkWithPk(): Unit = {
    // The lookup key contains the dim table's pk, there will be no materialization.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithoutPkSinkWithPk(): Unit = {
    // This case shows how costly is if the dim table does not define a pk.
    // The lookup key doesn't contain the dim table's pk, there will be two more costly
    // materialization compare to the one with pk.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_without_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcLeftJoinDimWithPkSinkWithPk(): Unit = {
    // The lookup key contains the dim table's pk, there will be no materialization.
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 left join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithoutPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_without_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkOnlySinkWithoutPk(): Unit = {
    // only select lookup key field, expect not affect NDU
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcLeftJoinDimWithoutPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_without_pk
         |select t1.a, t1.b, t2.c
         |from (
         |  select *, proctime() proctime from cdc
         |) t1 left join dim_without_pk for system_time as of t1.proctime as t2
         |on t1.a = t2.a
         |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkOutputNoPkSinkWithoutPk(): Unit = {
    // non lookup pk selected, expect materialize if tryResolve
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t2.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a 
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicFuncSinkWithoutPk(): Unit = {
    if (tryResolve) {
      // only select lookup key field, but with ND-call, expect exception
      thrown.expectMessage(
        "column(s): a(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select ndFunc(t2.a) a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicLocalCondition(): Unit = {
    // use user defined function
    if (tryResolve) {
      // not select lookup source field, but with NonDeterministicCondition, expect exception
      thrown.expectMessage(
        "exists non deterministic function: 'ndFunc' in condition: '>(ndFunc($1), 100)' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select t1.a, t1.b, t1.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a and ndFunc(t2.b) > 100
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimWithPkNonDeterministicLocalCondition2(): Unit = {
    // use builtin temporal function
    if (tryResolve) {
      thrown.expectMessage(
        "exists non deterministic function: 'UNIX_TIMESTAMP' in condition: '>($1, -(UNIX_TIMESTAMP(), 300))' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t2.b as version, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a 
                                 |  -- check dim table data's freshness
                                 |  and t2.b > UNIX_TIMESTAMP() - 300
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinDimNonDeterministicRemainingCondition(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "exists non deterministic function: 'ndFunc' in condition: '>($1, ndFunc($3))' which may cause wrong result")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select t1.a, t2.b, t2.c
                                 |from (
                                 |  select *, proctime() proctime from cdc
                                 |) t1 join dim_with_pk for system_time as of t1.proctime as t2
                                 |on t1.a = t2.a 
                                 |  -- non deterministic function in remaining condition 
                                 |  and t1.b > ndFunc(t2.b)
                                 |""".stripMargin)
  }

  @Test
  def testGroupByNonDeterministicFuncWithCdcSource(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  a, count(*) cnt, `day`
                                 |from (
                                 |  select *, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day` from cdc
                                 |) t
                                 |group by `day`, a
                                 |""".stripMargin)
  }

  @Test
  def testGroupByNonDeterministicUdfWithCdcSource(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): EXPR$0(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  ndFunc(a), count(*) cnt, c
                                 |from cdc
                                 |group by ndFunc(a), c
                                 |""".stripMargin)
  }

  @Test
  def testNestedAggWithNonDeterministicGroupingKeys(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a, sum(b) qmt, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') `day`
         |from (
         |  select *, row_number() over (partition by a order by PROCTIME() desc) rn from src
         |) t
         |where rn = 1
         |group by a, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')
         |""".stripMargin)
  }

  @Test
  def testGroupAggNonDeterministicFuncOnSourcePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(
      s"""
         |select
         |  `day`, count(*) cnt, sum(b) qmt
         |from (
         |  select *, concat(cast(a as varchar), DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) `day` from cdc
         |) t
         |group by `day`
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgs(): Unit = {
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from T
         |group by a
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgsOnCdcSource(): Unit = {
    if (tryResolve) {
      // though original pk was selected and same as the sink's pk, but the valid_uv was
      // non-deterministic, will raise an error
      thrown.expectMessage(
        "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from cdc
         |group by a
         |""".stripMargin)
  }

  @Test
  def testAggWithNonDeterministicFilterArgsOnCdcSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): $f2(generated by non-deterministic function: UNIX_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_without_pk
         |select
         |  a
         |  ,count(*) cnt
         |  ,cast(count(distinct c) filter (where b > UNIX_TIMESTAMP() - 180) as varchar) valid_uv
         |from cdc
         |group by a
         |""".stripMargin)
  }

  @Test
  def testNonDeterministicAggOnAppendSourceSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  a
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |group by a
                                 |""".stripMargin)
  }

  @Test
  def testNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  a
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |group by a
                                 |""".stripMargin)
  }

  @Test
  def testGlobalNonDeterministicAggOnAppendSourceSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select
                                 |  max(a)
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |""".stripMargin)
  }

  @Test
  def testGlobalNonDeterministicAggOnAppendSourceSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): ndCnt(generated by non-deterministic function: ndAggFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  max(a)
                                 |  ,ndAggFunc(b) ndCnt
                                 |  ,max(c) mc
                                 |from T
                                 |""".stripMargin)
  }

  @Test
  def testUpsertSourceSinkWithPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @Test
  def testUpsertSourceSinkWithoutPk(): Unit = {
    // contains normalize
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, b, c
                                 |from upsert_src
                                 |""".stripMargin)
  }

  @Test
  def testMultiOverWithNonDeterministicUdafSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_composite_pk
        |SELECT 
        |  a
        |  ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |  ,b
        |  ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |FROM T1
      """.stripMargin
    )
  }

  @Test
  def testOverWithNonDeterministicUdafSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT 
        |  a
        |  ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) 
        |  ,b
        |FROM T1
      """.stripMargin
    )
  }

  @Test
  def testAppendRankOnMultiOverWithNonDeterministicUdafSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_composite_pk
        |select a, uv, b, nd from (
        | select 
        |  a, uv, b, nd,
        |  row_number() over (partition by a order by uv desc) rn
        | from (
        |  SELECT 
        |    a
        |    ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |    ,b
        |    ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |  FROM T1
        |  ) 
        |) where rn = 1
      """.stripMargin
    )
  }

  @Test
  def testAppendRankOnMultiOverWithNonDeterministicUdafSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |select a, nd, b from (
        | select 
        |  a, uv, b, nd,
        |  row_number() over (partition by a order by uv desc) rn
        | from (
        |  SELECT 
        |    a
        |    ,COUNT(distinct b)  OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) uv
        |    ,b
        |    ,ndAggFunc(a) OVER (PARTITION BY a ORDER BY proctime
        |      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) nd
        |  FROM T1
        |  ) 
        |) where rn = 1
      """.stripMargin
    )
  }

  @Test
  def testUnionSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, d
                                 |from src
                                 |union
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testUnionAllSinkWithCompositePk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_with_composite_pk
                                 |select a, b, c, d
                                 |from src
                                 |union all
                                 |select a, b, c, metadata_3
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testUnionAllSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "metadata column(s): 'metadata_3' in cdc source may cause wrong result or error")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select a, b, c
                                 |from src
                                 |union all
                                 |select a, metadata_3, c 
                                 |from cdc_with_meta
                                 |""".stripMargin)
  }

  @Test
  def testCdcJoinWithNonDeterministicCondition(): Unit = {
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): $f4(generated by non-deterministic function: ndFunc ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(s"""
                                 |insert into sink_without_pk
                                 |select
                                 |  t1.a
                                 |  ,t2.b
                                 |  ,t1.c
                                 |from cdc t1 join cdc t2
                                 |  on ndFunc(t1.b) = ndFunc(t2.b)
                                 |""".stripMargin)
  }

  @Test
  def testProctimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.c, t1.b FROM T1 t1 JOIN T1 t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcProctimeIntervalJoinOnPkSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.b, t1.c FROM (
                                | select *, proctime() proctime from cdc) t1 JOIN 
                                | (select *, proctime() proctime from cdc) t2 ON
                                |  t1.a = t2.a AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcProctimeIntervalJoinOnNonPkSinkWithoutPk(): Unit = {
    if (tryResolve) {
      thrown.expectMessage("can not satisfy the determinism requirement")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert("""
                                |insert into sink_without_pk
                                |SELECT t2.a, t2.b, t1.c FROM (
                                | select *, proctime() proctime from cdc) t1 JOIN 
                                | (select *, proctime() proctime from cdc) t2 ON
                                |  t1.b = t2.b AND t1.proctime > t2.proctime - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcRowtimeIntervalJoinSinkWithoutPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_without_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testCdcRowtimeIntervalJoinSinkWithPk(): Unit = {
    util.verifyExecPlanInsert(
      """
        |insert into sink_with_pk
        |SELECT t2.a, t1.b, t2.c FROM cdc_with_watermark t1 JOIN cdc_with_watermark t2 ON
        |  t1.a = t2.a AND t1.op_ts > t2.op_ts - INTERVAL '5' SECOND
      """.stripMargin)
  }

  @Test
  def testJoinKeyContainsUk(): Unit = {
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.a = t2.a
         |""".stripMargin)
  }

  @Test
  def testJoinHasBothSidesUk(): Unit = {
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  @Test
  def testJoinHasBothSidesUkSinkWithoutPk(): Unit = {
    if (tryResolve) {
      // sink require all columns be deterministic though join has both side uk
      thrown.expectMessage(
        "column(s): c-day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_pk
         |select t1.a, t2.a, t2.`c-day`
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  @Test
  def testJoinHasSingleSideUk(): Unit = {
    if (tryResolve) {
      // the input side without uk requires all columns be deterministic
      thrown.expectMessage("can not satisfy the determinism requirement")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(
      s"""
         |select t1.a, t2.`c-day`, t2.b, t2.d
         |from (
         |  select a, b, c, d
         |  from cdc
         | ) t1
         |join (
         |  select a, b, CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd')) as `c-day`, d
         |  from cdc
         |) t2
         |  on t1.b = t2.b
         |""".stripMargin)
  }

  // a real case from FLINK-27369
  @Test
  def testCdcJoinWithNonDeterministicOutputSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_order (
                                | order_id INT,
                                | order_name STRING,
                                | product_id INT,
                                | user_id INT,
                                | PRIMARY KEY(order_id) NOT ENFORCED
                                |) WITH (
                                | 'connector' = 'values',
                                | 'changelog-mode' = 'I,UA,UB,D'
                                |)""".stripMargin)

    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_logistics (
                                | logistics_id INT,
                                | logistics_target STRING,
                                | logistics_source STRING,
                                | logistics_time TIMESTAMP(0),
                                | order_id INT,
                                | PRIMARY KEY(logistics_id) NOT ENFORCED
                                |) WITH (
                                |  'connector' = 'values',
                                | 'changelog-mode' = 'I,UA,UB,D'
                                |)""".stripMargin)

    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE t_join_sink (
                                | order_id INT,
                                | order_name STRING,
                                | logistics_id INT,
                                | logistics_target STRING,
                                | logistics_source STRING,
                                | logistics_time timestamp,
                                | PRIMARY KEY(order_id) NOT ENFORCED
                                |) WITH (
                                | 'connector' = 'values',
                                | 'sink-insert-only' = 'false'
                                |)""".stripMargin)

    thrown.expectMessage(
      "The column(s): logistics_time(generated by non-deterministic function: NOW ) can not satisfy the determinism requirement")
    thrown.expect(classOf[TableException])

    util.verifyExecPlanInsert(
      s"""
         |INSERT INTO t_join_sink
         |SELECT ord.order_id,
         |ord.order_name,
         |logistics.logistics_id,
         |logistics.logistics_target,
         |logistics.logistics_source,
         |now()
         |FROM t_order AS ord
         |LEFT JOIN t_logistics AS logistics ON ord.order_id=logistics.order_id; 
         |""".stripMargin)
  }

  // TODO add a case use proctime dedup with cdc source

  // TODO add a case use rowtime dedup with cdc source

  @Test
  def testWindowDedupOnCdcWithMetadata(): Unit = {

    util.tableEnv.executeSql("""
                               |create temporary table sink1 (
                               | a int,
                               | b bigint,
                               | c string,
                               | d boolean,
                               | ts timestamp(3),
                               | primary key (a,d) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'sink-insert-only' = 'false'
                               |)""".stripMargin)

    // TODO this should be updated after StreamPhysicalWindowDeduplicate supports consuming update
    thrown.expectMessage(
      "StreamPhysicalWindowDeduplicate doesn't support consuming update and delete changes")
    thrown.expect(classOf[TableException])

    util.verifyExecPlanInsert(
      """
        |insert into sink1
        |SELECT a, b, c, d, window_start
        |FROM (
        |SELECT *,
        |   ROW_NUMBER() OVER(PARTITION BY a, window_start, window_end
        |   ORDER BY op_ts DESC) as rownum
        |FROM TABLE(TUMBLE(TABLE cdc_with_meta_and_wm, DESCRIPTOR(op_ts), INTERVAL '1' MINUTE))
        |)
        |WHERE rownum <= 1""".stripMargin)

  }

  @Test
  def testNestedSourceWithMultiSink(): Unit = {
    val ddl =
      s"""
         |CREATE TABLE nested_src (
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
        |FROM nested_src
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
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testMultiSinkOnJoinedView(): Unit = {
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
    if (tryResolve) {
      thrown.expectMessage(
        "column(s): day(generated by non-deterministic function: CURRENT_TIMESTAMP ) can not satisfy the determinism")
      thrown.expect(classOf[TableException])
    }
    util.verifyExecPlan(stmtSet)
  }

  @Test
  def testUpdateRankOutputRowNumberSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                | create temporary view v1 as
                                |  select a, max(c) c, sum(b) filter (where b > 0) cnt
                                |  from src
                                |  group by a
                                | """.stripMargin)

    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_composite_pk
         |select a, cnt, c, rn from (
         | select
         |  a, cnt, c, row_number() over (partition by a order by cnt desc) rn
         | from v1
         | ) t where t.rn <= 100 
         |""".stripMargin)
  }

  @Test
  def testRetractRankOutputRowNumberSinkWithPk(): Unit = {
    util.tableEnv.executeSql(s"""
                                | create temporary view v1 as
                                |  select a, max(c) c, sum(b) cnt
                                |  from src
                                |  group by a
                                | """.stripMargin)

    util.verifyExecPlanInsert(
      s"""
         |insert into sink_with_composite_pk
         |select a, cnt, c, rn from (
         | select
         |  a, cnt, c, row_number() over (partition by a order by cnt desc) rn
         | from v1
         | ) t where t.rn <= 100 
         |""".stripMargin)
  }

  @SerialVersionUID(1L)
  object TestNonDeterministicUdf extends ScalarFunction {
    val random = new Random()

    def eval(id: JLong): JLong = {
      id + random.nextInt()
    }

    def eval(id: Int): Int = {
      id + random.nextInt()
    }

    def eval(id: String): String = {
      s"$id-${random.nextInt()}"
    }

    override def isDeterministic: Boolean = false
  }

  @SerialVersionUID(1L)
  object TestNonDeterministicUdtf extends TableFunction[String] {

    val random = new Random()

    def eval(id: Int): Unit = {
      collect(s"${id + random.nextInt()}")
    }

    def eval(id: String): Unit = {
      id.split(",").foreach(str => collect(s"$str#${random.nextInt()}"))
    }

    override def isDeterministic: Boolean = false
  }

  object TestTestNonDeterministicUdaf extends AggregateFunction[JLong, CountAccumulator] {

    val random = new Random()

    def accumulate(acc: CountAccumulator, in: JLong): Unit = {
      acc.f0 += (in + random.nextInt())
    }

    override def getValue(acc: CountAccumulator): JLong = acc.f0

    override def createAccumulator(): CountAccumulator = new CountAccumulator

    override def isDeterministic: Boolean = false
  }
}

object NonDeterministicDagTest {

  @Parameterized.Parameters(name = "tryResolve={0}")
  def parameters(): util.Collection[JBoolean] = {
    util.Arrays.asList(true, false)
  }
}

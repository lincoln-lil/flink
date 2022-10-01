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
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.source.{DynamicTableSource, ScanTableSource, SourceFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSourceFactory}
import org.apache.flink.table.planner.utils.{TableTestBase, TestingTableEnvironment}

import org.assertj.core.api.Assertions
import org.junit.Test

import java.util

class TableSinkTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  util.tableEnv.executeSql("""
                             |CREATE TABLE src (person String, votes BIGINT) WITH(
                             |  'connector' = 'values'
                             |)
                             |""".stripMargin)

  util.tableEnv.executeSql(
    """
      |CREATE TABLE award (votes BIGINT, prize DOUBLE, PRIMARY KEY(votes) NOT ENFORCED) WITH(
      |  'connector' = 'values'
      |)
      |""".stripMargin)

  util.tableEnv.executeSql(
    """
      |CREATE TABLE people (person STRING, age INT, PRIMARY KEY(person) NOT ENFORCED) WITH(
      |  'connector' = 'values'
      |)
      |""".stripMargin)

  @Test
  def testInsertWithTargetColumnsAndSqlHint(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE appendSink (
                     |  `a` BIGINT,
                     |  `b` STRING
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO appendSink /*+ OPTIONS('sink.parallelism' = '1') */(a, b) SELECT a + b, c FROM MyTable")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInsertMismatchTypeForEmptyChar(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE my_sink (
                     |  name STRING,
                     |  email STRING,
                     |  message_offset BIGINT
                     |) WITH (
                     |  'connector' = 'values'
                     |)
                     |""".stripMargin)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Query schema: [a: INT, EXPR$1: CHAR(0) NOT NULL, EXPR$2: CHAR(0) NOT NULL]\n" +
        "Sink schema:  [name: STRING, email: STRING, message_offset: BIGINT]")
    util.verifyExecPlanInsert("INSERT INTO my_sink SELECT a, '', '' FROM MyTable")
  }

  @Test
  def testExceptionForAppendSink(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE appendSink (
                     |  `a` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'true'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO appendSink SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")

    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "Table sink 'default_catalog.default_database.appendSink' doesn't " +
        "support consuming update changes which is produced by node " +
        "GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testExceptionForOverAggregate(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE retractSink1 (
                     |  `cnt` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    util.addTable(s"""
                     |CREATE TABLE retractSink2 (
                     |  `cnt` BIGINT,
                     |  `total` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val table = util.tableEnv.sqlQuery("SELECT COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.tableEnv.createTemporaryView("TempTable", table)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO retractSink1 SELECT * FROM TempTable")

    stmtSet.addInsertSql(
      "INSERT INTO retractSink2 SELECT cnt, SUM(cnt) OVER (ORDER BY PROCTIME()) FROM TempTable")

    thrown.expect(classOf[TableException])
    thrown.expectMessage(
      "OverAggregate doesn't support consuming update changes " +
        "which is produced by node GroupAggregate(groupBy=[a], select=[a, COUNT(*) AS cnt])")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAppendSink(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE appendSink (
                     |  `a` BIGINT,
                     |  `b` STRING
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'true'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO appendSink SELECT a + b, c FROM MyTable")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink1(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE retractSink (
                     |  `a` INT,
                     |  `cnt` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(
      "INSERT INTO retractSink SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractSink2(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE retractSink (
                     |  `cnt` BIGINT,
                     |  `a` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val dml =
      """
        |INSERT INTO retractSink
        |SELECT cnt, COUNT(a) AS a FROM (
        |    SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a) t
        |GROUP BY cnt
      """.stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(dml)
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSink(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE upsertSink (
                     |  `a` INT,
                     |  `cnt` BIGINT,
                     |  PRIMARY KEY (a) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO upsertSink SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testUpsertSinkWithFilter(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE upsertSink (
                     |  `a` INT,
                     |  `cnt` BIGINT,
                     |  PRIMARY KEY (a) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val sql =
      """
        |INSERT INTO upsertSink
        |SELECT *
        |FROM (SELECT a, COUNT(*) AS cnt FROM MyTable GROUP BY a)
        |WHERE cnt < 10
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)
    // a filter after aggregation, the Aggregation and Calc should produce UPDATE_BEFORE
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRetractAndUpsertSink(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE retractSink (
                     |  `b` BIGINT,
                     |  `cnt` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    util.addTable(s"""
                     |CREATE TABLE upsertSink (
                     |  `b` BIGINT,
                     |  `cnt` BIGINT,
                     |  PRIMARY KEY (b) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)

    val table = util.tableEnv.sqlQuery("SELECT b, COUNT(a) AS cnt FROM MyTable GROUP BY b")
    util.tableEnv.createTemporaryView("TempTable", table)

    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO retractSink SELECT b, cnt FROM TempTable WHERE b < 4")
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink SELECT b, cnt FROM TempTable WHERE b >= 4 AND b < 6")
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink " +
        "SELECT cnt, COUNT(b) AS frequency FROM TempTable WHERE b < 4 GROUP BY cnt")

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testAppendUpsertAndRetractSink(): Unit = {
    util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
    util.addDataStream[(Int, Long, String)]("MyTable3", 'i, 'j, 'k)
    util.addTable(s"""
                     |CREATE TABLE appendSink (
                     |  `a` INT,
                     |  `b` BIGINT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'true'
                     |)
                     |""".stripMargin)

    val table =
      util.tableEnv.sqlQuery("SELECT a, b FROM MyTable UNION ALL SELECT d, e FROM MyTable2")
    util.tableEnv.createTemporaryView("TempTable", table)
    val stmtSet = util.tableEnv.createStatementSet()

    stmtSet.addInsertSql("INSERT INTO appendSink SELECT * FROM TempTable")

    util.addTable(s"""
                     |CREATE TABLE retractSink (
                     |  `total_sum` INT
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    val table1 =
      util.tableEnv.sqlQuery("SELECT a, b FROM TempTable UNION ALL SELECT i, j FROM MyTable3")
    util.tableEnv.createTemporaryView("TempTable1", table1)
    stmtSet.addInsertSql("INSERT INTO retractSink SELECT SUM(a) AS total_sum FROM TempTable1")

    util.addTable(s"""
                     |CREATE TABLE upsertSink (
                     |  `a` INT,
                     |  `total_min` BIGINT,
                     |  PRIMARY KEY (a) NOT ENFORCED
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'false'
                     |)
                     |""".stripMargin)
    stmtSet.addInsertSql(
      "INSERT INTO upsertSink SELECT a, MIN(b) AS total_min FROM TempTable1 GROUP BY a")

    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testExceptionForWritingVirtualMetadataColumn(): Unit = {
    // test reordering, skipping, casting of (virtual) metadata columns
    util.addTable(
      s"""
         |CREATE TABLE MetadataTable (
         |  `a` INT,
         |  `m_3` INT METADATA FROM 'metadata_3' VIRTUAL,
         |  `m_2` INT METADATA FROM 'metadata_2',
         |  `b` BIGINT,
         |  `c` INT,
         |  `metadata_1` STRING METADATA
         |) WITH (
         |  'connector' = 'values',
         |  'readable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT, metadata_3:BIGINT',
         |  'writable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT'
         |)
       """.stripMargin)

    val sql =
      """
        |INSERT INTO MetadataTable
        |SELECT *
        |FROM MetadataTable
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)

    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Query schema: [a: INT, m_3: INT, m_2: INT, b: BIGINT, c: INT, metadata_1: STRING]\n" +
        "Sink schema:  [a: INT, m_2: INT, b: BIGINT, c: INT, metadata_1: STRING]")

    util.verifyRelPlan(stmtSet)
  }

  @Test
  def testExceptionForWritingInvalidMetadataColumn(): Unit = {
    // test casting of metadata columns
    util.addTable(s"""
                     |CREATE TABLE MetadataTable (
                     |  `a` INT,
                     |  `metadata_1` TIMESTAMP(3) METADATA
                     |) WITH (
                     |  'connector' = 'values',
                     |  'writable-metadata' = 'metadata_1:BOOLEAN'
                     |)
       """.stripMargin)

    val sql =
      """
        |INSERT INTO MetadataTable
        |SELECT TIMESTAMP '1990-10-14 06:00:00.000'
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)

    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Invalid data type for metadata column 'metadata_1' of table " +
        "'default_catalog.default_database.MetadataTable'. The column cannot be declared as " +
        "'TIMESTAMP(3)' because the type must be castable to metadata type 'BOOLEAN'.")

    util.verifyRelPlan(stmtSet)
  }

  @Test
  def testMetadataColumn(): Unit = {
    // test reordering, skipping, casting of (virtual) metadata columns
    util.addTable(
      s"""
         |CREATE TABLE MetadataTable (
         |  `a` INT,
         |  `m_3` INT METADATA FROM 'metadata_3' VIRTUAL,
         |  `m_2` INT METADATA FROM 'metadata_2',
         |  `b` BIGINT,
         |  `c` INT,
         |  `metadata_1` STRING METADATA
         |) WITH (
         |  'connector' = 'values',
         |  'readable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT, metadata_3:BIGINT',
         |  'writable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT'
         |)
       """.stripMargin)

    val sql =
      """
        |INSERT INTO MetadataTable
        |SELECT `a`, `m_2`, `b`, `c`, `metadata_1`
        |FROM MetadataTable
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)

    util.verifyRelPlan(stmtSet)
  }

  @Test
  def testMetadataColumnThatConflictsWithPhysicalColumn(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE MetadataTable (
                     |  `metadata_1` DOUBLE,
                     |  `m_1` STRING METADATA FROM 'metadata_1' VIRTUAL,
                     |  `m_2` BIGINT METADATA FROM 'metadata_2',
                     |  `metadata_2` DOUBLE,
                     |  `other` STRING
                     |) WITH (
                     |  'connector' = 'values',
                     |  'readable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT',
                     |  'writable-metadata' = 'metadata_1:STRING, metadata_2:BIGINT'
                     |)
       """.stripMargin)

    val sql =
      """
        |INSERT INTO MetadataTable
        |SELECT `metadata_1`, `m_2`, `metadata_2`, `other`
        |FROM MetadataTable
        |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)

    util.verifyRelPlan(stmtSet)
  }

  @Test
  def testSinkDisorderChangeLogWithJoin(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TABLE SinkJoinChangeLog (
                               |  person STRING, votes BIGINT, prize DOUBLE,
                               |  PRIMARY KEY(person) NOT ENFORCED) WITH(
                               |  'connector' = 'values',
                               |  'sink-insert-only' = 'false'
                               |)
                               |""".stripMargin)

    util.verifyExecPlanInsert(
      """
        |INSERT INTO SinkJoinChangeLog
        |SELECT T.person, T.sum_votes, award.prize FROM
        |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T, award
        |   WHERE T.sum_votes = award.votes
        |""".stripMargin)
  }

  @Test
  def testSinkDisorderChangeLogWithRank(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TABLE SinkRankChangeLog (
                               |  person STRING, votes BIGINT,
                               |  PRIMARY KEY(person) NOT ENFORCED) WITH(
                               |  'connector' = 'values',
                               |  'sink-insert-only' = 'false'
                               |)
                               |""".stripMargin)

    util.verifyExecPlanInsert(
      """
        |INSERT INTO SinkRankChangeLog
        |SELECT person, sum_votes FROM
        | (SELECT person, sum_votes,
        |   ROW_NUMBER() OVER (PARTITION BY vote_section ORDER BY sum_votes DESC) AS rank_number
        |   FROM (SELECT person, SUM(votes) AS sum_votes, SUM(votes) / 2 AS vote_section FROM src
        |      GROUP BY person))
        |   WHERE rank_number < 10
        |""".stripMargin)
  }

  @Test def testAppendStreamToSinkWithPkAutoKeyBy(): Unit = {
    val tEnv = util.tableEnv
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'values',
                      | 'changelog-mode' = 'I'
                      |)""".stripMargin)
    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | primary key (id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '9'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    // we set the sink parallelism to 9 which differs from the source, expect 'keyby' was added.
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testAppendStreamToSinkWithPkNoKeyBy(): Unit = {
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.NONE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'values',
                      | 'changelog-mode' = 'I'
                      |)""".stripMargin)
    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | primary key (id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '9'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    // we set the sink parallelism to 9 which differs from the source, but disable auto keyby
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testAppendStreamToSinkWithPkForceKeyBy(): Unit = {
    util.getStreamEnv.setParallelism(4)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'test_source'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | primary key (id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '4'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    // source and sink has same parallelism, but sink shuffle by pk is enforced
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testSingleParallelismAppendStreamToSinkWithPkForceKeyBy(): Unit = {
    util.getStreamEnv.setParallelism(1)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'test_source'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | primary key (id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '1'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    // source and sink has same parallelism, but sink shuffle by pk is enforced
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testAppendStreamToSinkWithoutPkForceKeyBy(): Unit = {
    util.getStreamEnv.setParallelism(4)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'test_source'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '4'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    // source and sink has same parallelism, but sink shuffle by pk is enforced
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testAppendStreamToSinkWithoutPkForceKeyBySingleParallelism(): Unit = {
    util.getStreamEnv.setParallelism(4)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'test_source'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '1'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test def testChangelogStreamToSinkWithPkDifferentParallelism(): Unit = {
    util.getStreamEnv.setParallelism(1)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.AUTO)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar,
                      | primary key(id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'changelog-mode' = 'I,UB,UA,D'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | primary key(id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '2'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql("insert into sink select * from source")
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testChangelogStreamToSinkWithPkSingleParallelism(): Unit = {
    util.getStreamEnv.setParallelism(4)
    val tEnv = util.tableEnv
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.FORCE)
    tEnv.executeSql("""
                      |create table source (
                      | id varchar,
                      | city_name varchar,
                      | ts bigint
                      |) with (
                      | 'connector' = 'test_source'
                      |)""".stripMargin)

    tEnv.executeSql("""
                      |create table sink (
                      | id varchar,
                      | city_name varchar,
                      | ts bigint,
                      | rn bigint,
                      | primary key(id) not enforced
                      |) with (
                      | 'connector' = 'values',
                      | 'sink-insert-only' = 'false',
                      | 'sink.parallelism' = '1'
                      |)""".stripMargin)
    val stmtSet = tEnv.asInstanceOf[TestingTableEnvironment].createStatementSet
    stmtSet.addInsertSql(s"""
                            |insert into sink
                            |select * from (
                            |  select *, row_number() over (partition by id order by ts desc) rn
                            |  from source
                            |) where rn=1""".stripMargin)
    util.verifyExplain(stmtSet, ExplainDetail.JSON_EXECUTION_PLAN)
  }

  @Test
  def testManagedTableSinkWithDisableCheckpointing(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE sink (
                     |  `a` INT,
                     |  `b` BIGINT,
                     |  `c` STRING
                     |) WITH(
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink SELECT * FROM MyTable")

    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      s"You should enable the checkpointing for sinking to managed table " +
        s"'default_catalog.default_database.sink', " +
        s"managed table relies on checkpoint to commit and " +
        s"the data is visible only after commit.")
    util.verifyAstPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testManagedTableSinkWithEnableCheckpointing(): Unit = {
    util.getStreamEnv.enableCheckpointing(10)
    util.addTable(s"""
                     |CREATE TABLE sink (
                     |  `a` INT,
                     |  `b` BIGINT,
                     |  `c` STRING
                     |) WITH(
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO sink SELECT * FROM MyTable")

    util.verifyAstPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInsertPartColumn(): Unit = {
    util.addTable(s"""
                     |CREATE TABLE zm_test (
                     |  `a` BIGINT,
                     |  `m1` MAP<STRING, BIGINT>,
                     |  `m2` MAP<STRING NOT NULL, BIGINT>,
                     |  `m3` MAP<STRING, BIGINT NOT NULL>,
                     |  `m4` MAP<STRING NOT NULL, BIGINT NOT NULL>
                     |) WITH (
                     |  'connector' = 'values',
                     |  'sink-insert-only' = 'true'
                     |)
                     |""".stripMargin)
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql("INSERT INTO zm_test(`a`) SELECT `a` FROM MyTable")
    util.verifyRelPlan(stmtSet, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testCreateTableAsSelect(): Unit = {
    // TODO: support explain CreateTableASOperation
    // Flink does not support explain CreateTableASOperation yet, we will fix it in FLINK-28770.
    Assertions
      .assertThatThrownBy(
        () => util.tableEnv.explainSql("CREATE TABLE zm_ctas_test AS SELECT * FROM MyTable"))
      .hasMessage(
        "Unsupported operation: org.apache.flink.table.operations.ddl.CreateTableASOperation")
  }

  @Test
  def debug(): Unit = {
    val tEnv = util.tableEnv
    // default config is auto
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE,
      ExecutionConfigOptions.SinkKeyedShuffle.AUTO)

    tEnv.executeSql(
      s"""
         |CREATE TEMPORARY TABLE `dbo_Book` (
         |  `_key_BookID` INT NOT NULL,
         |  `before` ROW<`BookID` INT NOT NULL, `ParentBookID` INT, `BookTypeID` INT NOT NULL, `Code` VARCHAR(2147483647) NOT NULL, `CodeDate` BIGINT NOT NULL, `Name` VARCHAR(2147483647) NOT NULL, `TodaysPandL` VARCHAR(2147483647) NOT NULL, `YestPandL` VARCHAR(2147483647) NOT NULL, `MTDPandL` VARCHAR(2147483647) NOT NULL, `YTDPandL` VARCHAR(2147483647) NOT NULL, `UnrealisedPandL` VARCHAR(2147483647) NOT NULL, `CCY` VARCHAR(2147483647) NOT NULL, `FinancialYear` BIGINT NOT NULL, `Show` INT NOT NULL, `CheckPositions` INT NOT NULL, `CheckUnRealisedYearEnd` INT NOT NULL, `SortOrder` INT NOT NULL, `Depth` INT NOT NULL, `NetBookSize` VARCHAR(2147483647) NOT NULL, `NetFundSize` VARCHAR(2147483647) NOT NULL, `LongPosition` VARCHAR(2147483647) NOT NULL, `LongExposure` VARCHAR(2147483647) NOT NULL, `LongBeta60` VARCHAR(2147483647) NOT NULL, `LongBeta90` VARCHAR(2147483647) NOT NULL, `LongBeta180` VARCHAR(2147483647) NOT NULL, `ShortPosition` VARCHAR(2147483647) NOT NULL, `ShortExposure` VARCHAR(2147483647) NOT NULL, `ShortBeta60` VARCHAR(2147483647) NOT NULL, `ShortBeta90` VARCHAR(2147483647) NOT NULL, `ShortBeta180` VARCHAR(2147483647) NOT NULL, `TodaysValueAdded` VARCHAR(2147483647) NOT NULL, `CodeBeta` VARCHAR(2147483647), `CodeOMS` VARCHAR(2147483647), `CodeOMSSub` VARCHAR(2147483647), `CodeFundSize` VARCHAR(2147483647), `MaxPositionPerCent` VARCHAR(2147483647), `CapitalAllocation` VARCHAR(2147483647), `OptionExposure` VARCHAR(2147483647), `DaysToClose` VARCHAR(2147483647), `ProcessOrder` INT NOT NULL, `BackgroundColour` VARCHAR(2147483647), `TradableUntil` BIGINT, `BookStatus` VARCHAR(2147483647) NOT NULL, `SectorCode` VARCHAR(2147483647), `CountryCode` VARCHAR(2147483647), `FuturePandL` VARCHAR(2147483647) NOT NULL, `LayoutGridIdView` VARCHAR(2147483647), `AsAtDate` BIGINT, `PreTodayMTDPct` VARCHAR(2147483647) NOT NULL, `CalcMethod` VARCHAR(2147483647) NOT NULL, `Dissag_FundId` INT, `PrimaryTradingLocation` VARCHAR(2147483647), `TOPSScaling` BOOLEAN NOT NULL, `BookShortName` VARCHAR(2147483647), `RiskBookSizeSourceId` INT, `SourceType` VARCHAR(2147483647) NOT NULL, `FCPBookSize` VARCHAR(2147483647) NOT NULL, `DealingDateBookSize` VARCHAR(2147483647), `DealingDateFundSize` VARCHAR(2147483647), `BaseCCY` VARCHAR(2147483647) NOT NULL, `CalcBaseCcy` BOOLEAN NOT NULL, `StrategyTemplateId` INT, `SubStrategyTemplateId` INT, `AssetAllocFundId` INT, `DisplayFlag` BIGINT, `IncludeInFuturesRoll` BOOLEAN NOT NULL, `NetExposure` VARCHAR(2147483647) NOT NULL, `GrossExposure` VARCHAR(2147483647) NOT NULL, `BaseNetExposure` VARCHAR(2147483647) NOT NULL, `BaseGrossExposure` VARCHAR(2147483647) NOT NULL, `CYTD` VARCHAR(2147483647) NOT NULL, `recTimeStamp` BIGINT, `IsGNME` BOOLEAN NOT NULL, `IsFX` BOOLEAN NOT NULL, `PFYTD` VARCHAR(2147483647) NOT NULL, `DealingDate` INT, `Tplus1BookSize` VARCHAR(2147483647) NOT NULL, `MTDReturn` VARCHAR(2147483647), `EmbeddedFXPandL` VARCHAR(2147483647) NOT NULL, `IsUnHedged` BOOLEAN NOT NULL, `RiskBookSize` VARCHAR(2147483647), `TodaysLongPandL` VARCHAR(2147483647) NOT NULL, `TodaysShortPandL` VARCHAR(2147483647) NOT NULL, `CovidExposure` VARCHAR(2147483647) NOT NULL, `ManualNameOverride` BOOLEAN NOT NULL>,
         |  `after` ROW<`BookID` INT NOT NULL, `ParentBookID` INT, `BookTypeID` INT NOT NULL, `Code` VARCHAR(2147483647) NOT NULL, `CodeDate` BIGINT NOT NULL, `Name` VARCHAR(2147483647) NOT NULL, `TodaysPandL` VARCHAR(2147483647) NOT NULL, `YestPandL` VARCHAR(2147483647) NOT NULL, `MTDPandL` VARCHAR(2147483647) NOT NULL, `YTDPandL` VARCHAR(2147483647) NOT NULL, `UnrealisedPandL` VARCHAR(2147483647) NOT NULL, `CCY` VARCHAR(2147483647) NOT NULL, `FinancialYear` BIGINT NOT NULL, `Show` INT NOT NULL, `CheckPositions` INT NOT NULL, `CheckUnRealisedYearEnd` INT NOT NULL, `SortOrder` INT NOT NULL, `Depth` INT NOT NULL, `NetBookSize` VARCHAR(2147483647) NOT NULL, `NetFundSize` VARCHAR(2147483647) NOT NULL, `LongPosition` VARCHAR(2147483647) NOT NULL, `LongExposure` VARCHAR(2147483647) NOT NULL, `LongBeta60` VARCHAR(2147483647) NOT NULL, `LongBeta90` VARCHAR(2147483647) NOT NULL, `LongBeta180` VARCHAR(2147483647) NOT NULL, `ShortPosition` VARCHAR(2147483647) NOT NULL, `ShortExposure` VARCHAR(2147483647) NOT NULL, `ShortBeta60` VARCHAR(2147483647) NOT NULL, `ShortBeta90` VARCHAR(2147483647) NOT NULL, `ShortBeta180` VARCHAR(2147483647) NOT NULL, `TodaysValueAdded` VARCHAR(2147483647) NOT NULL, `CodeBeta` VARCHAR(2147483647), `CodeOMS` VARCHAR(2147483647), `CodeOMSSub` VARCHAR(2147483647), `CodeFundSize` VARCHAR(2147483647), `MaxPositionPerCent` VARCHAR(2147483647), `CapitalAllocation` VARCHAR(2147483647), `OptionExposure` VARCHAR(2147483647), `DaysToClose` VARCHAR(2147483647), `ProcessOrder` INT NOT NULL, `BackgroundColour` VARCHAR(2147483647), `TradableUntil` BIGINT, `BookStatus` VARCHAR(2147483647) NOT NULL, `SectorCode` VARCHAR(2147483647), `CountryCode` VARCHAR(2147483647), `FuturePandL` VARCHAR(2147483647) NOT NULL, `LayoutGridIdView` VARCHAR(2147483647), `AsAtDate` BIGINT, `PreTodayMTDPct` VARCHAR(2147483647) NOT NULL, `CalcMethod` VARCHAR(2147483647) NOT NULL, `Dissag_FundId` INT, `PrimaryTradingLocation` VARCHAR(2147483647), `TOPSScaling` BOOLEAN NOT NULL, `BookShortName` VARCHAR(2147483647), `RiskBookSizeSourceId` INT, `SourceType` VARCHAR(2147483647) NOT NULL, `FCPBookSize` VARCHAR(2147483647) NOT NULL, `DealingDateBookSize` VARCHAR(2147483647), `DealingDateFundSize` VARCHAR(2147483647), `BaseCCY` VARCHAR(2147483647) NOT NULL, `CalcBaseCcy` BOOLEAN NOT NULL, `StrategyTemplateId` INT, `SubStrategyTemplateId` INT, `AssetAllocFundId` INT, `DisplayFlag` BIGINT, `IncludeInFuturesRoll` BOOLEAN NOT NULL, `NetExposure` VARCHAR(2147483647) NOT NULL, `GrossExposure` VARCHAR(2147483647) NOT NULL, `BaseNetExposure` VARCHAR(2147483647) NOT NULL, `BaseGrossExposure` VARCHAR(2147483647) NOT NULL, `CYTD` VARCHAR(2147483647) NOT NULL, `recTimeStamp` BIGINT, `IsGNME` BOOLEAN NOT NULL, `IsFX` BOOLEAN NOT NULL, `PFYTD` VARCHAR(2147483647) NOT NULL, `DealingDate` INT, `Tplus1BookSize` VARCHAR(2147483647) NOT NULL, `MTDReturn` VARCHAR(2147483647), `EmbeddedFXPandL` VARCHAR(2147483647) NOT NULL, `IsUnHedged` BOOLEAN NOT NULL, `RiskBookSize` VARCHAR(2147483647), `TodaysLongPandL` VARCHAR(2147483647) NOT NULL, `TodaysShortPandL` VARCHAR(2147483647) NOT NULL, `CovidExposure` VARCHAR(2147483647) NOT NULL, `ManualNameOverride` BOOLEAN NOT NULL>,
         |  `source` ROW<`version` VARCHAR(2147483647) NOT NULL, `connector` VARCHAR(2147483647) NOT NULL, `name` VARCHAR(2147483647) NOT NULL, `ts_ms` BIGINT NOT NULL, `snapshot` VARCHAR(2147483647), `db` VARCHAR(2147483647) NOT NULL, `sequence` VARCHAR(2147483647), `schema` VARCHAR(2147483647) NOT NULL, `table` VARCHAR(2147483647) NOT NULL, `commit_version` BIGINT, `transaction_finished` BOOLEAN NOT NULL> NOT NULL,
         |  `op` VARCHAR(2147483647) NOT NULL,
         |  `ts_ms` BIGINT,
         |  `transaction` ROW<`id` VARCHAR(2147483647) NOT NULL, `total_order` BIGINT NOT NULL, `data_collection_order` BIGINT NOT NULL>,
         |  PRIMARY KEY (`_key_BookID`) NOT ENFORCED
         |) with ('connector'='values', 'bounded'='false', 'changelog-mode'='UA,I,D')
         |""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE TEMPORARY TABLE `dbo_BookData` (
         |  `_key_BookID` INT NOT NULL,
         |  `_key_IsBaseCcy` BOOLEAN NOT NULL,
         |  `_key_IsDisplayCcy` BOOLEAN NOT NULL,
         |  `_key_Ccy` VARCHAR(2147483647) NOT NULL,
         |  `before` ROW<`BookID` INT NOT NULL, `Ccy` VARCHAR(2147483647) NOT NULL, `IsBaseCcy` BOOLEAN NOT NULL, `IsDisplayCcy` BOOLEAN NOT NULL, `TodaysPandL` VARCHAR(2147483647) NOT NULL, `YestPandL` VARCHAR(2147483647) NOT NULL, `MTDPandL` VARCHAR(2147483647) NOT NULL, `YTDPandL` VARCHAR(2147483647) NOT NULL, `TodaysFXPandL` VARCHAR(2147483647) NOT NULL, `YestFXPandL` VARCHAR(2147483647) NOT NULL, `MTDFXPandL` VARCHAR(2147483647) NOT NULL, `YTDFXPandL` VARCHAR(2147483647) NOT NULL, `FuturePandL` VARCHAR(2147483647) NOT NULL, `LongPosition` VARCHAR(2147483647) NOT NULL, `LongExposure` VARCHAR(2147483647) NOT NULL, `ShortPosition` VARCHAR(2147483647) NOT NULL, `ShortExposure` VARCHAR(2147483647) NOT NULL, `OptionExposure` VARCHAR(2147483647), `CapitalAllocation` VARCHAR(2147483647), `TodaysValueAdded` VARCHAR(2147483647) NOT NULL, `PreTodayMTDPct` VARCHAR(2147483647) NOT NULL, `NetBookSize` VARCHAR(2147483647) NOT NULL, `NetFundSize` VARCHAR(2147483647) NOT NULL, `FCPBookSize` VARCHAR(2147483647) NOT NULL, `DealingDateBookSize` VARCHAR(2147483647), `DealingDateFundSize` VARCHAR(2147483647), `NetExposure` VARCHAR(2147483647) NOT NULL, `GrossExposure` VARCHAR(2147483647) NOT NULL, `CYTD` VARCHAR(2147483647) NOT NULL, `GNME` VARCHAR(2147483647), `GNMEFX` VARCHAR(2147483647), `RiskGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `ConsolidatedGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `ConsolidatedNettedGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `GrossExposureFX` VARCHAR(2147483647) NOT NULL, `PFYTD` VARCHAR(2147483647) NOT NULL, `EmbeddedFXPandL` VARCHAR(2147483647) NOT NULL, `RiskBookSize` VARCHAR(2147483647), `OverrrideDisplayBookSize` VARCHAR(2147483647), `LongBetaExposure` VARCHAR(2147483647) NOT NULL, `ShortBetaExposure` VARCHAR(2147483647) NOT NULL, `TodaysLongPandL` VARCHAR(2147483647) NOT NULL, `TodaysShortPandL` VARCHAR(2147483647) NOT NULL, `CovidExposure` VARCHAR(2147483647) NOT NULL, `MTDReturn` VARCHAR(2147483647), `LongEquityBetaExposure` VARCHAR(2147483647) NOT NULL, `ShortEquityBetaExposure` VARCHAR(2147483647) NOT NULL>,
         |  `after` ROW<`BookID` INT NOT NULL, `Ccy` VARCHAR(2147483647) NOT NULL, `IsBaseCcy` BOOLEAN NOT NULL, `IsDisplayCcy` BOOLEAN NOT NULL, `TodaysPandL` VARCHAR(2147483647) NOT NULL, `YestPandL` VARCHAR(2147483647) NOT NULL, `MTDPandL` VARCHAR(2147483647) NOT NULL, `YTDPandL` VARCHAR(2147483647) NOT NULL, `TodaysFXPandL` VARCHAR(2147483647) NOT NULL, `YestFXPandL` VARCHAR(2147483647) NOT NULL, `MTDFXPandL` VARCHAR(2147483647) NOT NULL, `YTDFXPandL` VARCHAR(2147483647) NOT NULL, `FuturePandL` VARCHAR(2147483647) NOT NULL, `LongPosition` VARCHAR(2147483647) NOT NULL, `LongExposure` VARCHAR(2147483647) NOT NULL, `ShortPosition` VARCHAR(2147483647) NOT NULL, `ShortExposure` VARCHAR(2147483647) NOT NULL, `OptionExposure` VARCHAR(2147483647), `CapitalAllocation` VARCHAR(2147483647), `TodaysValueAdded` VARCHAR(2147483647) NOT NULL, `PreTodayMTDPct` VARCHAR(2147483647) NOT NULL, `NetBookSize` VARCHAR(2147483647) NOT NULL, `NetFundSize` VARCHAR(2147483647) NOT NULL, `FCPBookSize` VARCHAR(2147483647) NOT NULL, `DealingDateBookSize` VARCHAR(2147483647), `DealingDateFundSize` VARCHAR(2147483647), `NetExposure` VARCHAR(2147483647) NOT NULL, `GrossExposure` VARCHAR(2147483647) NOT NULL, `CYTD` VARCHAR(2147483647) NOT NULL, `GNME` VARCHAR(2147483647), `GNMEFX` VARCHAR(2147483647), `RiskGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `ConsolidatedGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `ConsolidatedNettedGrossMarketExposure` VARCHAR(2147483647) NOT NULL, `GrossExposureFX` VARCHAR(2147483647) NOT NULL, `PFYTD` VARCHAR(2147483647) NOT NULL, `EmbeddedFXPandL` VARCHAR(2147483647) NOT NULL, `RiskBookSize` VARCHAR(2147483647), `OverrrideDisplayBookSize` VARCHAR(2147483647), `LongBetaExposure` VARCHAR(2147483647) NOT NULL, `ShortBetaExposure` VARCHAR(2147483647) NOT NULL, `TodaysLongPandL` VARCHAR(2147483647) NOT NULL, `TodaysShortPandL` VARCHAR(2147483647) NOT NULL, `CovidExposure` VARCHAR(2147483647) NOT NULL, `MTDReturn` VARCHAR(2147483647), `LongEquityBetaExposure` VARCHAR(2147483647) NOT NULL, `ShortEquityBetaExposure` VARCHAR(2147483647) NOT NULL>,
         |  `source` ROW<`version` VARCHAR(2147483647) NOT NULL, `connector` VARCHAR(2147483647) NOT NULL, `name` VARCHAR(2147483647) NOT NULL, `ts_ms` BIGINT NOT NULL, `snapshot` VARCHAR(2147483647), `db` VARCHAR(2147483647) NOT NULL, `sequence` VARCHAR(2147483647), `schema` VARCHAR(2147483647) NOT NULL, `table` VARCHAR(2147483647) NOT NULL, `commit_version` BIGINT, `transaction_finished` BOOLEAN NOT NULL> NOT NULL,
         |  `op` VARCHAR(2147483647) NOT NULL,
         |  `ts_ms` BIGINT,
         |  `transaction` ROW<`id` VARCHAR(2147483647) NOT NULL, `total_order` BIGINT NOT NULL, `data_collection_order` BIGINT NOT NULL>,
         |  PRIMARY KEY (`_key_BookID`, `_key_IsBaseCcy`, `_key_IsDisplayCcy`, `_key_Ccy`) NOT ENFORCED
         |) with ('connector'='values', 'bounded'='false', 'changelog-mode'='UA,I,D')
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TEMPORARY TABLE `Static_Book` (
         |  `_key_BookId` INT NOT NULL,
         |  `before` ROW<`BookId` INT NOT NULL, `Name` VARCHAR(2147483647) NOT NULL, `Code` VARCHAR(2147483647) NOT NULL, `BookTypeId` INT NOT NULL, `CCY` VARCHAR(2147483647) NOT NULL, `BaseCCY` VARCHAR(2147483647) NOT NULL, `BenchmarkId` INT, `RiskModelId` INT, `CodeOMS` VARCHAR(2147483647), `CodeOMSSub` VARCHAR(2147483647), `TradableUntil` INT, `SectorCode` VARCHAR(2147483647), `CountryCode` VARCHAR(2147483647), `CalcMethod` VARCHAR(2147483647), `BookHierarchyBreadCrumb` VARCHAR(2147483647), `Dissag_FundId` INT, `AAFundId` INT, `AASubStrategyId` INT, `AAStrategyId` INT, `DisplayFlag` BIGINT, `IsGNME` BOOLEAN NOT NULL, `IsFX` BOOLEAN NOT NULL, `IsActive` BOOLEAN, `RecTimeStamp` BIGINT NOT NULL>,
         |  `after` ROW<`BookId` INT NOT NULL, `Name` VARCHAR(2147483647) NOT NULL, `Code` VARCHAR(2147483647) NOT NULL, `BookTypeId` INT NOT NULL, `CCY` VARCHAR(2147483647) NOT NULL, `BaseCCY` VARCHAR(2147483647) NOT NULL, `BenchmarkId` INT, `RiskModelId` INT, `CodeOMS` VARCHAR(2147483647), `CodeOMSSub` VARCHAR(2147483647), `TradableUntil` INT, `SectorCode` VARCHAR(2147483647), `CountryCode` VARCHAR(2147483647), `CalcMethod` VARCHAR(2147483647), `BookHierarchyBreadCrumb` VARCHAR(2147483647), `Dissag_FundId` INT, `AAFundId` INT, `AASubStrategyId` INT, `AAStrategyId` INT, `DisplayFlag` BIGINT, `IsGNME` BOOLEAN NOT NULL, `IsFX` BOOLEAN NOT NULL, `IsActive` BOOLEAN, `RecTimeStamp` BIGINT NOT NULL>,
         |  `source` ROW<`version` VARCHAR(2147483647) NOT NULL, `connector` VARCHAR(2147483647) NOT NULL, `name` VARCHAR(2147483647) NOT NULL, `ts_ms` BIGINT NOT NULL, `snapshot` VARCHAR(2147483647), `db` VARCHAR(2147483647) NOT NULL, `sequence` VARCHAR(2147483647), `schema` VARCHAR(2147483647) NOT NULL, `table` VARCHAR(2147483647) NOT NULL, `commit_version` BIGINT, `transaction_finished` BOOLEAN NOT NULL> NOT NULL,
         |  `op` VARCHAR(2147483647) NOT NULL,
         |  `ts_ms` BIGINT,
         |  `transaction` ROW<`id` VARCHAR(2147483647) NOT NULL, `total_order` BIGINT NOT NULL, `data_collection_order` BIGINT NOT NULL>,
         |  PRIMARY KEY (`_key_BookId`) NOT ENFORCED
         |)  with ('connector'='values', 'bounded'='false', 'changelog-mode'='UA,I,D')
         |""".stripMargin)
    tEnv.executeSql(
      s"""
         |CREATE TEMPORARY TABLE book_data_sink (
         |  `_key_BookID` INT NOT NULL,
         |  `_key_IsBaseCcy` BOOLEAN NOT NULL,
         |  `_key_IsDisplayCcy` BOOLEAN NOT NULL,
         |  `_key_Ccy` VARCHAR(2147483647) NOT NULL,
         |  `NetBookSize` DOUBLE NOT NULL,
         |  `RiskBookSize` DOUBLE NOT NULL,
         |  `PFYTD` DOUBLE NOT NULL,
         |  `PreTodayMTDPct` DOUBLE NOT NULL,
         |  `SectorCode` VARCHAR(2147483647),
         |  `CountryCode` VARCHAR(2147483647),
         |  `RegionCode` VARCHAR(2147483647),
         |  -- `BenchmarkIds` ARRAY<INT NOT NULL> NOT NULL,
         |  -- `RiskModels` ARRAY<ROW<`RiskModelId` INT NOT NULL, `ModelDescription` VARCHAR(2147483647) NOT NULL, `Provider` VARCHAR(2147483647), `ModelRegion` VARCHAR(2147483647), `RegionShortName` VARCHAR(2147483647)> NOT NULL> NOT NULL,
         |  `CalculationTimestamp` BIGINT,
         |  PRIMARY KEY (`_key_BookID`, `_key_IsBaseCcy`, `_key_IsDisplayCcy`, `_key_Ccy`) NOT ENFORCED
         |)
         |COMMENT 'topic: onebook.aggregate.book-data-test, key: string, value: v1, upsert'
         |WITH (
         |  'connector' = 'print'
         |);
         |""".stripMargin)

    util.verifyExecPlanInsert(
      s"""INSERT INTO book_data_sink
         |SELECT      _key_BookID,
         |            _key_IsBaseCcy,
         |            _key_IsDisplayCcy,
         |            _key_Ccy,
         |            0 NetBookSize,
         |            0 RiskBookSize,
         |            0 PFYTD,
         |            0 PreTodayMTDPct,
         |            '' SectorCode,
         |            '' CountryCode,
         |            '' RegionCode,
         |            -- MultiSetToArray(COLLECT(0)) BenchmarkIds,
         |            -- MultiSetToArray
         |            -- (
         |            --     COLLECT
         |            --     (
         |            --         CASE WHEN RiskModelId IS NULL
         |            --             THEN CAST(NULL AS ROW<RiskModelId INT, ModelDescription STRING, Provider STRING, ModelRegion STRING, RegionShortName STRING>)
         |            --             ELSE ROW(RiskModelId, ModelDescription, Provider, ModelRegion, RegionShortName)
         |            --         END
         |            --     )
         |            -- ) RiskModels,
         |            UNIX_TIMESTAMP() CalculationTimestamp
         |FROM        (
         |-- CREATE TEMPORARY VIEW book_data AS
         |    SELECT      b._key_BookID,
         |            COALESCE(bd._key_IsBaseCcy, false) _key_IsBaseCcy,
         |            COALESCE(bd._key_IsDisplayCcy, true) _key_IsDisplayCcy,
         |            COALESCE(bd._key_Ccy, 'USD') _key_Ccy,
         |            sb.after.RiskModelId,
         |            '' ModelDescription, '' Provider, '' ModelRegion, '' RegionShortName
         |    FROM        (
         |-- CREATE TEMPORARY VIEW dbo_book AS
         |        SELECT * FROM `dbo_Book`
         |        WHERE _key_BookID = 2600
         |
         |    ) b
         |    JOIN    (
         |-- CREATE TEMPORARY VIEW static_book AS
         |    SELECT  *
         |    FROM    `Static_Book`
         |    WHERE   after.IsActive = true
         |        AND _key_BookId = 2600
         |    ) sb
         |        ON      sb._key_BookId = b._key_BookID
         |    LEFT JOIN  (
         |
         |-- CREATE TEMPORARY VIEW dbo_book_data AS
         |    SELECT  *
         |    FROM    `dbo_BookData`
         |    WHERE   _key_IsDisplayCcy = true
         |        AND _key_IsBaseCcy = false
         |        AND _key_BookID = 2600
         |
         |) bd
         |  ON      bd._key_BookID = b._key_BookID
         |) b
         |GROUP BY    _key_BookID,
         |            _key_IsBaseCcy,
         |            _key_IsDisplayCcy,
         |            _key_Ccy
         |""".stripMargin)
  }

}

/** tests table factory use ParallelSourceFunction which support parallelism by env */
class TestTableFactory extends DynamicTableSourceFactory {
  override def createDynamicTableSource(
      context: DynamicTableFactory.Context): DynamicTableSource = {
    new TestParallelSource()
  }

  override def factoryIdentifier = "test_source"

  override def requiredOptions = new util.HashSet[ConfigOption[_]]

  override def optionalOptions = {
    new util.HashSet[ConfigOption[_]]()
  }

}

/** tests table source provide a {@link ParallelSourceFunction}. */
class TestParallelSource() extends ScanTableSource {
  override def copy = throw new TableException("Not supported")

  override def asSummaryString = "test source"

  override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

  override def getScanRuntimeProvider(
      runtimeProviderContext: ScanTableSource.ScanContext): ScanTableSource.ScanRuntimeProvider = {
    SourceFunctionProvider.of(
      new ParallelSourceFunction[RowData] {
        override def run(ctx: SourceFunction.SourceContext[RowData]): Unit = ???
        override def cancel(): Unit = ???
      },
      false)
  }
}

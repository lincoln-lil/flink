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

package org.apache.flink.table.planner.plan.utils.debug

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.catalog.CatalogTableImpl
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation, StatementSetOperation}
import org.apache.flink.table.operations.ddl.{CreateCatalogFunctionOperation, CreateTableOperation, CreateViewOperation}
import org.apache.flink.table.planner.delegation.{BatchPlanner, PlannerBase, PlannerContext, StreamPlanner}
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, StreamTableTestUtil, TableTestBase}
import org.apache.flink.util.FileUtils

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.Collections

import scala.collection.JavaConversions._
import org.junit._

class DebugTest extends TableTestBase {

  def prepareBatch(): BatchTableTestUtil = {
    val util = batchTestUtil()
    util.tableEnv.getConfig.getConfiguration.set(
      OptimizerConfigOptions.TABLE_IMPLICIT_TYPE_COERCION_ENABLED,
      java.lang.Boolean.valueOf(true))
//    util.tableEnv.getConfig.getConfiguration.setBoolean(
//      PlannerContext.TABLE_OPTIMIZER_SQL_TO_REL_PROJECT_MERGE_ENABLED,
//      java.lang.Boolean.valueOf(false))
//    util.tableEnv.getConfig.getConfiguration.setBoolean(
//      PlannerContext.TABLE_OPTIMIZER_SQL_TO_REL_PUSH_DOWN_JOIN_CONDITION,
//      java.lang.Boolean.valueOf(false))
    util.tableEnv.getConfig.getConfiguration.setString("table.exec.default-partitioner", "rescale")
    util.tableEnv.getConfig.getConfiguration
      .setString("table.optimizer.agg-phase-strategy", "TWO_PHASE")
    util.tableEnv.getConfig.getConfiguration
      .setString("table.optimizer.temporal-join.lookup-enabled", "false")
    util.tableEnv.getConfig.getConfiguration
      .setString("table.exec.skew-join.replicate-num", "32")
    util.tableEnv.getConfig.getConfiguration
      .setString("table.exec.source.force-spilling", "true")
    util.tableEnv.getConfig.getConfiguration
      .setString("table.optimizer.agg-phase-strategy", "TWO_PHASE")
    util
  }

  def prepareStream(): StreamTableTestUtil = {
    val util = streamTestUtil()
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      "table.optimizer.union-all-as-breakpoint-enabled",
      java.lang.Boolean.valueOf(false))
    util.tableEnv.getConfig.getConfiguration
      .setBoolean("table.optimizer.distinct-agg.split.enabled", java.lang.Boolean.valueOf(true))
    util.tableEnv.getConfig.getConfiguration
      .setBoolean("table.exec.mini-batch.enabled", java.lang.Boolean.valueOf(true))
    util.tableEnv.getConfig.getConfiguration.setString("table.exec.mini-batch.allow-latency", "10s")
    util
  }


  @Test
  def debugPlan(): Unit = {
    val isBatch = false
    val util = if (isBatch) {
      prepareBatch()
    } else {
      prepareStream()
    }
//    loadUserJar()
    val st = System.currentTimeMillis()
    val sql = FileUtils.readFileUtf8(new File("/Users/lilin/work/git/tmp/compile/uk-err.sql"))
    val tableEnv = util.tableEnv.asInstanceOf[TableEnvironmentImpl]
    val operations: Iterator[Operation] = tableEnv.getParser().parseStatements(sql)
    val execPlan = getExecPlanFromOperations(
      operations,
      mockDdl = true,
      tableEnv,
      printPhysicalPlan = true,
      batchMode = true)
    println(s"physical plan cost: ${System.currentTimeMillis() - st}ms")
    println("\n=== exec plan  ===")
//    if (isBatch) {
//      println(
//        tableEnv.getPlanner.asInstanceOf[BatchPlanner]
//              .explainExecNodeGraph(execPlan, ExplainDetail.CHANGELOG_MODE))
//    } else {
//      println(
//        tableEnv.getPlanner.asInstanceOf[StreamPlanner]
//            .explainPlan(execPlan, ExplainDetail.CHANGELOG_MODE))
//    }
    println(s"total cost: ${System.currentTimeMillis() - st}ms")
  }

  def loadUserJar(): Unit = {
    val jars: Array[URL] = Array(new File("/Users/lilin/work/git/tmp/compile/dt.jar").toURL)
    val c: URLClassLoader = new URLClassLoader(jars, Thread.currentThread().getContextClassLoader)
    Class.forName("com.alibaba.dt.streaming.blinksql.common.udf.DateAddOrSub", true, c)
  }

  def getExecPlanFromOperations(
      operations: Iterator[Operation],
      mockDdl: Boolean,
      tableEnv: TableEnvironmentImpl,
      printPhysicalPlan: Boolean = false,
      batchMode: Boolean = false): ExecNodeGraph = {
    var execNodeGraph: ExecNodeGraph = null
    while (operations.hasNext) {
      val op = operations.next()
      try {
        op match {
          case _: QueryOperation =>
            throw new UnsupportedOperationException("unsupported query")
          case insert: ModifyOperation =>
            if (printPhysicalPlan) {
              println(
                tableEnv.getPlanner.asInstanceOf[PlannerBase]
                  .explain(Collections.singletonList(insert), ExplainDetail.CHANGELOG_MODE))
            }
            execNodeGraph = tableEnv.getPlanner.asInstanceOf[PlannerBase]
              .getExecNodeGraph(Collections.singletonList(insert))
          case stmtSet: StatementSetOperation =>
            if (printPhysicalPlan) {
              println(
                tableEnv.getPlanner.asInstanceOf[PlannerBase]
                  .explain(
                    stmtSet.getOperations.asInstanceOf[java.util.List[Operation]],
                    ExplainDetail.CHANGELOG_MODE))
            }
            execNodeGraph = tableEnv.getPlanner.asInstanceOf[PlannerBase]
              .getExecNodeGraph(stmtSet.getOperations)
          case ct: CreateTableOperation =>
            if (mockDdl) {
              tableEnv.executeInternal(replaceMockTable(ct, batchMode))
            } else {
              tableEnv.executeInternal(ct)
            }
          case cv: CreateViewOperation =>
            tableEnv.executeInternal(cv)
          case cf: CreateCatalogFunctionOperation =>
            tableEnv.executeInternal(cf)
          case e =>
            println(e.getClass + "@" + e.asSummaryString())
            throw new UnsupportedOperationException("unsupported query!")
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
    execNodeGraph
  }

  def replaceMockTable(ddl: CreateTableOperation, batchMode: Boolean): CreateTableOperation = {
    val srcTable = ddl.getCatalogTable.asInstanceOf[CatalogTableImpl]
    srcTable.getOptions.clear()
    srcTable.getOptions.put("connector", "values")
    srcTable.getOptions.put("sink-insert-only", "false")
    if (batchMode) {
      srcTable.getOptions.put("bounded", "true")
    }
    ddl
   }
}

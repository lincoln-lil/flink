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
package org.apache.flink.table.planner.plan.validate

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata
import org.apache.flink.table.planner.{JHashMap, JList}
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.connectors.DynamicSourceUtils
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalcBase, StreamPhysicalChangelogNormalize, StreamPhysicalCorrelateBase, StreamPhysicalDataStreamScan, StreamPhysicalDeduplicate, StreamPhysicalDropUpdateBefore, StreamPhysicalExchange, StreamPhysicalExpand, StreamPhysicalGroupAggregateBase, StreamPhysicalLegacyTableSourceScan, StreamPhysicalLookupJoin, StreamPhysicalMatch, StreamPhysicalMiniBatchAssigner, StreamPhysicalOverAggregateBase, StreamPhysicalRank, StreamPhysicalRel, StreamPhysicalSink, StreamPhysicalSort, StreamPhysicalSortLimit, StreamPhysicalTableSourceScan, StreamPhysicalTemporalSort, StreamPhysicalUnion, StreamPhysicalWatermarkAssigner, StreamPhysicalWindowAggregateBase, StreamPhysicalWindowDeduplicate, StreamPhysicalWindowRank, StreamPhysicalWindowTableFunction}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, FlinkRelOptUtil, JoinUtil, OverAggregateUtil, RankProcessStrategy}
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy.{RetractStrategy, UpdateFastStrategy}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapClassLoader
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.types.RowKind

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral, RexNode, RexUtil, RexVisitorImpl}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, Util}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * The {@link StreamNonDeterministicPlanResolver} tries to resolve the correctness issue caused by
 * 'Non-Deterministic Updates' (NDU) in a changelog pipeline. Changelog may contains kinds of
 * messages: Insert (I), Delete (D), Update_before (UB), Update_after (UA).
 *
 * There's no NDU problem in an insert only pipeline.
 *
 * For the updates, there are two cases, with and without upsertKey(a metadata from {@link
 * FlinkRelMdUpsertKeys}, consider it as the primary key of the changelog). The upsertKey can be
 * always treated as deterministic, so if all of the pipeline operators can transmit upsertKey
 * normally (include working with sink's primary key), everything goes well.
 *
 * The key problem is upsertKey can be easily lost in a pipeline or does not exist from the source
 * or at the sink. All stateful operators can only process an update (D/UB/UA) message by comparing
 * the complete row (retract by row) if without a key identifier, also include a sink without
 * primary key that works as retractSink. So under the retract by row mode, an stateful operator
 * requires no non-deterministic column disturb the original changelog row. There're three killers:
 *
 * <p> 1. Non-deterministic functions(include scalar, table, aggregate functions, builtin or custom
 * ones) <p> 2. LookupJoin on an evolving source <p> 3. Cdc-source carries metadata field(system
 * columns, not belongs to the entity data itself)
 *
 * For the first step, this resolver automatically enables the materialization for No.2(LookupJoin)
 * if needed, and gives the detailed error message for No.1 (Non-deterministic functions) and
 * No.3(Cdc-source with metadata) which we think it is relatively easy to change the SQL(add
 * materialization is not a good idea for now, it has very high cost and will bring too much
 * complexity to the operators)
 *
 * Why not do this validation and rewrite in physical-rewrite phase? like {@link
 * FlinkChangelogModeInferenceProgram} does.
 *   - because the physical plan may be changed a lot after physical rewrite being done, we should
 *     check the 'final' plan.
 *
 * Some specific plan patterns:
 *
 * <p> 1. Non-deterministic scalar function calls
 * {{{
 *  Sink
 *   |
 * Project1{select col1,col2,now(),...}
 *    |
 *  Scan1
 * }}}
 *
 * <p> 2. Non-deterministic table function calls
 * {{{
 *       Sink
 *        |
 *     Correlate
 *     /      \
 * Project1  TableFunctionScan1
 *    |
 *  Scan1
 * }}}
 *
 * <p> 3. lookup join: lookup a source which data may change over time
 * {{{
 *       Sink
 *        |
 *     LookupJoin
 *     /      \
 * Filter1  Source2
 *    |
 * Project1
 *    |
 *  Scan1
 * }}}
 *
 * <p> 3.1 lookup join: a inner project with non-deterministic function calls or remaining join
 * condition is non-deterministic
 * {{{
 *       Sink
 *        |
 *     LookupJoin
 *     /      \
 * Filter1  Project2
 *    |        |
 * Project1   Source2
 *    |
 *  Scan1
 * }}}
 *
 * <p> 4. cdc source with metadata
 * {{{
 *      Sink
 *        | no upsertKey can be inferred
 *    Correlate
 *      /      \
 *    /       TableFunctionScan1(deterministic)
 *  Project1 {select id,name,attr1,op_time}
 *    |
 *  Scan {cdc source <id,name,attr1,op_type,op_time> }
 * }}}
 *
 * <p> 4.1 cdc source with metadata
 * {{{
 *      Sink
 *        | no upsertKey can be inferred
 *    LookupJoin {lookup key not contains the dim's pk}
 *      /      \
 *    /       Source2
 *  Project1 {select id,name,attr1,op_time}
 *    |
 *  Scan {cdc source <id,name,attr1,op_type,op_time> }
 * }}}
 *
 * CDC source with metadata is another form of non-deterministic update.
 *
 * <p> 5. grouping keys with non-deterministic column
 * {{{
 *  Sink{pk=(c3,day)}
 *   | upsertKey=(c3,day)
 *  GroupAgg{group by c3, day}
 *   |
 * Project{select c1,c2,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') day,...}
 *   |
 * Deduplicate{keep last row, dedup on c1,c2}
 *   |
 *  Scan
 * }}}
 */
object StreamNonDeterministicPlanResolver {

  val NO_REQUIRED_DETERMINISM = ImmutableBitSet.of();

  /**
   * Try to resolve the NDU problem if configured {@link
   * OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_HANDLING}. is in `TRY_RESOLVE`
   * mode. Will raise an error if the NDU problems in the given plan can not be completely solved.
   */
  def resolvePhysicalPlan(
      physicalRelNodes: Seq[FlinkPhysicalRel],
      tableConfig: TableConfig): Seq[FlinkPhysicalRel] = {
    tableConfig.getConfiguration.get(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_HANDLING) match {
      case handling: OptimizerConfigOptions.NonDeterministicUpdateHandling
          if handling == OptimizerConfigOptions.NonDeterministicUpdateHandling.TRY_RESOLVE =>
        val planResolver = new NonDeterministicUpdatePlanVisitor
        physicalRelNodes.map {
          root =>
            root match {
              case rel: StreamPhysicalRel =>
                // set initial requirement to NO_REQUIRED_DETERMINISM
                planResolver.visit(rel, NO_REQUIRED_DETERMINISM)

              case _ => throw new TableException("")
            }
        }
      case _ =>
        // do nothing, return original relNodes
        physicalRelNodes
    }
  }

  def inputInsertOnly(rel: StreamPhysicalRel): Boolean = {
    val existUpdate = rel.getInputs.exists {
      input =>
        val inputChangelogMode = ChangelogPlanUtils
          .getChangelogMode(input.asInstanceOf[StreamPhysicalRel])
          .get
        !inputChangelogMode.containsOnly(RowKind.INSERT)
    }
    !existUpdate
  }

  /**
   * An inner visitor to validate if there's any NDU problems which may cause wrong result and try
   * to rewrite lookup join node with materialization (to eliminate the non determinism generated by
   * lookup join node only).
   *
   * The visitor will try to satisfy the required determinism(represent by ImmutableBitSet) from
   * root. The transmission rule of required determinism:
   *
   * <p> 0. all required determinism is under the precondition: input has updates, that is say no
   * update determinism will be passed to an insert only stream
   *
   * <p> 1. the initial required determinism to the root node(e.g., sink node) was none
   *
   * <p> 2. for a relNode, it will process on two aspects:
   *   - can satisfy non-empty required determinism
   *   - actively requires determinism from input by self requirements(e.g., stateful node works on
   *     retract by row mode)
   *
   * <p>
   * {{{
   *  Rel3
   *   | require input
   *   v
   *  Rel2 {1. satisfy Rel3's requirement 2. append new requirement to input Rel1}
   *   | require input
   *   v
   *  Rel1
   * }}}
   *
   * the requiredDeterminism passed to input will exclude columns which were in upsertKey e.g.,
   * {{{
   *  Sink {pk=(c3)} requiredDeterminism=(c3)
   *   | passed requiredDeterminism={}
   *  GroupAgg{group by c3, day} append requiredDeterminism=(c3, day)
   *   | passed requiredDeterminism=(c3, day)
   * Project{select c1,c2,DATE_FORMAT(CURRENT_TIMESTAMP, 'yyMMdd') day,...} [x] can not satisfy
   *   |
   * Deduplicate{keep last row, dedup on c1,c2}
   *   |
   *  Scan
   * }}}
   *
   * <p> 3. for a sink node, it will require key columns' determinism when primary key is defined or
   * require all columns' determinism when no primary key is defined
   *
   * <p> 4. for a cdc source node(which will generate updates), the metadata columns are treated as
   * non-deterministic.
   */
  private class NonDeterministicUpdatePlanVisitor {

    val NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE =
      "There exists non deterministic function: '%s' in condition: '%s' which may cause wrong result in update pipeline."

    /**
     * Visit the given rel node to check if it satisfies the required determinism of the specified
     * columns. Note all operators which append new columns to output should exclude them from input
     * requireDeterminism.
     *
     * @param rel
     *   relNode to be validated
     * @param requireDeterminism
     *   downstream operator requires that the specified column represented by the ImmutableBitSet
     *   is deterministic
     */
    def visit(rel: StreamPhysicalRel, requireDeterminism: ImmutableBitSet): StreamPhysicalRel =
      rel match {
        case sink: StreamPhysicalSink =>
          if (inputInsertOnly(sink)) {
            // for append stream, not care about NDU
            transmitDeterminismRequirement(sink, NO_REQUIRED_DETERMINISM)
          } else {
            // for update streaming, when
            // 1. sink with pk:
            // upsert sink, update by pk, ideally pk == input.upsertKey,
            // (otherwise upsertMaterialize will handle it)

            // 1.1 input.upsertKey nonEmpty -> not care about NDU
            // 1.2 input.upsertKey isEmpty -> retract by complete row, must not contain NDU

            // once sink's requirement on pk was satisfied, no further request will be transited
            // only when new requirement generated at stateful node which input has update
            // (e.g., grouping keys)

            // 2. sink without pk:
            // retract sink, retract by complete row (all input columns should be deterministic)
            // whether input.upsertKey is empty or not, must not contain NDU

            val primaryKey = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes
            val requireInputDeterminism = if (sink.upsertMaterialize || primaryKey.isEmpty) {
              // SinkUpsertMaterializer only support no upsertKey mode, it says all input columns
              // should be deterministic (same as no primary key defined on sink)
              // TODO should optimize it after SinkUpsertMaterializer support upsertKey FLINK-28569.
              ImmutableBitSet.range(sink.getInput.getRowType.getFieldCount)
            } else {
              ImmutableBitSet.of(primaryKey: _*)
            }
            transmitDeterminismRequirement(sink, requireInputDeterminism)
          }

        case calc: StreamPhysicalCalcBase =>
          if (inputInsertOnly(calc) || requireDeterminism.isEmpty) {
            transmitDeterminismRequirement(calc, NO_REQUIRED_DETERMINISM)
          } else {
            // if input has updates, any non-deterministic conditions are not acceptable, also
            // requireDeterminism should be satisfied.
            if (null != calc.getProgram.getCondition) {
              // firstly check if exists non-deterministic condition
              val rexNode = calc.getProgram.expandLocalRef(calc.getProgram.getCondition)
              val ndCall = getNonDeterministicCallName(rexNode)
              if (ndCall.isDefined) {
                throw new TableException(
                  generateNonDeterministicConditionErrorMessage(ndCall.get, rexNode, calc))
              }
            }
            // extract all non deterministic output columns first and check if any of them were
            // required be deterministic.
            val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)
            val nonDeterministicCols = new JHashMap[Int, String]
            projects.zipWithIndex.foreach {
              case (projExpr, i) =>
                projExpr match {
                  case rexNode: RexNode =>
                    val ndCall = getNonDeterministicCallName(rexNode)
                    if (ndCall.isDefined) {
                      nonDeterministicCols.put(i, ndCall.get)
                    }
                  case _ => // ignore
                }
            }
            val unsatisfiedCols =
              requireDeterminism.toList.filter(nonDeterministicCols.contains(_))
            if (unsatisfiedCols.nonEmpty) {
              val errorMsg = generateNonDeterministicColumnsErrorMessage(
                unsatisfiedCols.toArray,
                calc.getRowType,
                calc,
                nonDeterministicCols)
              throw new TableException(errorMsg)
            }
            val outFromSourcePos = extractSourceMapping(projects)
            val conv2inputs = requireDeterminism.toList
              .map(
                out =>
                  outFromSourcePos.getOrElse(
                    out,
                    throw new TableException(
                      s"Invalid pos:$out over projection:${calc.getProgram.toString}")))
              // we set RexLiteral's source index to -1
              .filter(index => index != -1)

            val requireInputDeterminism = ImmutableBitSet.of(conv2inputs: _*)
            transmitDeterminismRequirement(calc, requireInputDeterminism)
          }

        case correlate: StreamPhysicalCorrelateBase =>
          if (inputInsertOnly(correlate) || requireDeterminism.isEmpty) {
            transmitDeterminismRequirement(correlate, NO_REQUIRED_DETERMINISM)
          } else {
            // check if exists non-deterministic condition
            if (correlate.condition.isDefined) {
              val rexNode = correlate.condition.get
              val ndCall = getNonDeterministicCallName(rexNode)
              if (ndCall.isDefined) {
                throw new TableException(
                  generateNonDeterministicConditionErrorMessage(ndCall.get, rexNode, correlate))
              }
            }
            val leftFieldCnt = correlate.inputRel.getRowType.getFieldCount
            val ndCall = getNonDeterministicCallName(correlate.scan.getCall)
            if (ndCall.isDefined) {
              // all columns from table function scan cannot satisfy the required determinism
              val unsatisfiedCols = requireDeterminism.toList.filter(index => index >= leftFieldCnt)
              if (unsatisfiedCols.nonEmpty) {
                val errorMsg = generateNonDeterministicColumnsErrorMessage(
                  unsatisfiedCols.toArray,
                  correlate.getRowType,
                  correlate,
                  null,
                  ndCall)
                throw new TableException(errorMsg)
              }
            }
            val fromLeft = requireDeterminism.toList.filter(index => index < leftFieldCnt)
            // only check left input if call is deterministic
            val requireInputDeterminism = if (fromLeft.nonEmpty) {
              ImmutableBitSet.of(fromLeft.toList)
            } else {
              NO_REQUIRED_DETERMINISM
            }
            transmitDeterminismRequirement(correlate, requireInputDeterminism)
          }

        case lookupJoin: StreamPhysicalLookupJoin =>
          if (inputInsertOnly(lookupJoin) || requireDeterminism.isEmpty) {
            transmitDeterminismRequirement(lookupJoin, NO_REQUIRED_DETERMINISM)
          } else {
            // if input has updates, the lookup join may produce non-deterministic result itself due
            // to backed lookup source which data may change over time, we can try to eliminate this
            // non-determinism by adding materialization to the join operator, but still exists two
            // non determinism we cannot solve: 1. join condition  2. the inner calc in lookJoin
            val leftFieldCnt = lookupJoin.getInput.getRowType.getFieldCount
            val requireRight = requireDeterminism.toList.filter(index => index >= leftFieldCnt)

            // required determinism cannot be satisfied even upsert materialize was enabled if:
            // 1. remaining join condition contains non deterministic call
            if (lookupJoin.remainingCondition.isDefined) {
              val remainingCondition = lookupJoin.remainingCondition.get
              val ndCall = getNonDeterministicCallName(remainingCondition)
              if (ndCall.isDefined) {
                throw new TableException(
                  generateNonDeterministicConditionErrorMessage(
                    ndCall.get,
                    remainingCondition,
                    lookupJoin))
              }
            }
            // 2. the inner calc in lookJoin contains either non deterministic condition or calls
            if (lookupJoin.calcOnTemporalTable.isDefined) {
              val calc = lookupJoin.calcOnTemporalTable.get
              if (calc.getCondition != null) {
                val rexCondition = calc.expandLocalRef(calc.getCondition)
                val ndCall = getNonDeterministicCallName(rexCondition)
                if (ndCall.isDefined) {
                  throw new TableException(
                    generateNonDeterministicConditionErrorMessage(
                      ndCall.get,
                      rexCondition,
                      lookupJoin))
                }
              }
              if (requireRight.nonEmpty) {
                // extract all non deterministic output columns first
                val projects = calc.getProjectList.map(calc.expandLocalRef)
                val nonDeterministicOutput = new JHashMap[Int, String]
                projects.zipWithIndex.foreach {
                  case (projExpr, i) =>
                    projExpr match {
                      case rexNode: RexNode =>
                        val ndCall = getNonDeterministicCallName(rexNode)
                        if (ndCall.isDefined) {
                          nonDeterministicOutput.put(i, ndCall.get)
                        }
                      case _ => // ignore
                    }
                }
                // for better exception message
                val unsatisfiedCols = requireRight.filter(nonDeterministicOutput.contains(_))
                if (unsatisfiedCols.nonEmpty && !lookupJoin.upsertMaterialize) {
                  // nonDeterminism will be eliminated when enable upsertMaterialize
                  val errorMsg = generateNonDeterministicColumnsErrorMessage(
                    unsatisfiedCols.toArray,
                    calc.getOutputRowType,
                    lookupJoin,
                    nonDeterministicOutput)
                  throw new TableException(errorMsg)
                }
              }
            }

            // Resolve non-determinism by adding materialization: we can enable upsertMaterialize to
            // eliminate non-determinism produced by lookup join via an evolving source.
            lazy val outputPkIdx = lookupJoin.getOutputPrimaryKeyIndexes
            lazy val outputPkBitSet = ImmutableBitSet.of(outputPkIdx: _*)
            lazy val lookupKeysContainsPk = outputPkIdx.nonEmpty && outputPkIdx.forall(
              index => lookupJoin.allLookupKeys.contains(index))

            // optimization: if lookup key contains primary key and no requirement on other fields
            // we can omit materialization, otherwise upsert materialize can not be omitted.
            val omitUpsertMaterialize =
              requireRight.isEmpty || (lookupKeysContainsPk && requireRight.forall(
                outputPkBitSet.get(_)))

            val newLookupJoin = if (!omitUpsertMaterialize) {
              lookupJoin.copy(true)
            } else {
              lookupJoin
            }
            val requireLeft = requireDeterminism.toList.filter(index => index < leftFieldCnt)
            val finalRequiredLeftDeterminism = if (requireLeft.nonEmpty) {
              ImmutableBitSet.of(requireLeft.toList)
            } else {
              NO_REQUIRED_DETERMINISM
            }
            transmitDeterminismRequirement(newLookupJoin, finalRequiredLeftDeterminism)
          }

        case tableScan: StreamPhysicalTableSourceScan =>
          // tableScan has no input, so only check meta data from cdc source
          if (requireDeterminism.nonEmpty) {
            val insertOnly = tableScan.tableSource.getChangelogMode.containsOnly(RowKind.INSERT)
            val supportsReadingMetadata = tableScan.tableSource
              .isInstanceOf[SupportsReadingMetadata]
            if (!insertOnly && supportsReadingMetadata) {
              val sourceTable = tableScan.getTable.unwrap(classOf[TableSourceTable])
              // check if requireDeterminism contains metadata column
              val metadataColumns = DynamicSourceUtils.extractMetadataColumns(
                sourceTable.contextResolvedTable.getResolvedSchema)
              val metaColumnSet = metadataColumns.map(col => col.getName).toSet
              val metadataCauseErr = tableScan.getRowType.getFieldNames.zipWithIndex.filter {
                case (name: String, index: Int) =>
                  metaColumnSet.contains(name) && requireDeterminism.get(index)
              }
              if (metadataCauseErr.nonEmpty) {
                val errorMsg = mutable.StringBuilder.newBuilder
                errorMsg
                  .append("The metadata column(s): '")
                  .append(
                    metadataCauseErr.map { case (name: String, _: Int) => name }.mkString(", "))
                  .append("' in cdc source may cause wrong result or error on downstream operators,"
                    + " please consider removing these columns or use a non-cdc source that only has insert messages.")
                  .append("\nsource node:\n")
                  .append(FlinkRelOptUtil.toString(tableScan))
                throw new TableException(errorMsg.toString())
              }
            }
          }
          tableScan

        case _: StreamPhysicalLegacyTableSourceScan | _: StreamPhysicalDataStreamScan =>
          // not cdc source, end visit
          rel

        // output row type = grouping keys + aggCalls
        case groupAgg: StreamPhysicalGroupAggregateBase =>
          if (inputInsertOnly(groupAgg)) {
            // no further requirement to input, only check if can satisfy the requiredDeterminism
            if (requireDeterminism.nonEmpty) {
              checkUnsatisfiedDeterminism(
                requireDeterminism,
                groupAgg.grouping,
                groupAgg.aggCalls,
                groupAgg.getRowType,
                groupAgg)
            }
            transmitDeterminismRequirement(groupAgg, NO_REQUIRED_DETERMINISM)
          } else {
            // agg works under retract mode if input is not insert only, and requires all input
            // columns be deterministic
            transmitDeterminismRequirement(
              groupAgg,
              ImmutableBitSet.range(groupAgg.getInput.getRowType.getFieldCount))
          }

        // output row type = grouping keys + aggCalls + windowProperties
        case windowAgg: StreamPhysicalWindowAggregateBase =>
          // same logic with groupAgg but they have no common parent
          if (inputInsertOnly(windowAgg)) {
            // no further requirement to input, only check if can satisfy the requiredDeterminism
            if (requireDeterminism.nonEmpty) {
              checkUnsatisfiedDeterminism(
                requireDeterminism,
                windowAgg.grouping,
                windowAgg.aggCalls,
                windowAgg.getRowType,
                windowAgg)
            }
            transmitDeterminismRequirement(windowAgg, NO_REQUIRED_DETERMINISM)
          } else {
            // agg works under retract mode if input is not insert only, and requires all input
            // columns be deterministic
            transmitDeterminismRequirement(
              windowAgg,
              ImmutableBitSet.range(windowAgg.getInput.getRowType.getFieldCount))
          }

        case expand: StreamPhysicalExpand =>
          // Currently expand is an internal operator only for plan rewriting, so only remove the
          // expandIdIndex from requireDeterminism. Also we skip checking if input has updates due
          // to this is a non-stateful node which never changes the changelog mode.
          val requireInputDeterminism = if (requireDeterminism.isEmpty) {
            NO_REQUIRED_DETERMINISM
          } else {
            requireDeterminism.except(ImmutableBitSet.of(expand.expandIdIndex))
          }
          transmitDeterminismRequirement(expand, requireInputDeterminism)

        // output row type = left row type + right row type
        case join: CommonPhysicalJoin =>
          val leftRel = join.getLeft.asInstanceOf[StreamPhysicalRel]
          val rightRel = join.getRight.asInstanceOf[StreamPhysicalRel]
          lazy val leftInputHasUpdate = !inputInsertOnly(leftRel)
          lazy val rightInputHasUpdate = !inputInsertOnly(rightRel)
          lazy val innerOrSemi = join.joinSpec.getJoinType == FlinkJoinType.INNER ||
            join.joinSpec.getJoinType == FlinkJoinType.SEMI

          /**
           * we do not distinguish the time attribute condition in interval/temporal join from
           * regular/window join here because: rowtime field always from source, proctime is not
           * limited (from source), when proctime appended to an update row without upsertKey then
           * result may goes wrong, in such a case proctime( was materialized as
           * PROCTIME_MATERIALIZE(PROCTIME())) is equal to a normal dynamic temporal function and
           * will be validated in calc node.
           */

          lazy val ndCall = getNonDeterministicCallName(join.getCondition)
          // check non-deterministic condition first because theoretically there may exist functions
          // in join condition though function calls were pushed down under normal conditions.
          if ((leftInputHasUpdate || rightInputHasUpdate || !innerOrSemi) && ndCall.isDefined) {
            // when output has update, the join condition cannot be non-deterministic:
            // 1. input has update -> output has update
            // 2. input insert only and is not innerOrSemi join -> output has update
            throw new TableException(
              generateNonDeterministicConditionErrorMessage(ndCall.get, join.getCondition, join))
          }
          val leftFieldCnt = leftRel.getRowType.getFieldCount
          val newLeft =
            visitJoinChild(
              requireDeterminism,
              leftRel,
              leftInputHasUpdate,
              leftFieldCnt,
              true,
              join.joinSpec.getLeftKeys,
              join.getUniqueKeys(leftRel, join.joinSpec.getLeftKeys))
          val newRight =
            visitJoinChild(
              requireDeterminism,
              rightRel,
              rightInputHasUpdate,
              leftFieldCnt,
              false,
              join.joinSpec.getRightKeys,
              join.getUniqueKeys(rightRel, join.joinSpec.getRightKeys))

          join
            .copy(
              join.getTraitSet,
              join.getCondition,
              newLeft,
              newRight,
              join.getJoinType,
              join.isSemiJoin)
            .asInstanceOf[StreamPhysicalRel]

        case _: StreamPhysicalMatch =>
          // TODO to be supported in FLINK-28743
          throw new TableException(
            "Unsupported to resolve non-deterministic issue in match-recognize.")

        case _: StreamPhysicalChangelogNormalize | _: StreamPhysicalDropUpdateBefore |
            _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalUnion | _: StreamPhysicalSort |
            _: StreamPhysicalSortLimit | _: StreamPhysicalTemporalSort |
            _: StreamPhysicalWatermarkAssigner | _: StreamPhysicalExchange =>
          // transit requireDeterminism transparently
          transmitDeterminismRequirement(rel, requireDeterminism)

        // output row type = input row type + overAgg outputs
        case overAgg: StreamPhysicalOverAggregateBase =>
          if (inputInsertOnly(overAgg)) {
            // no further requirement to input, only check if the agg outputs can satisfy the
            // requiredDeterminism
            if (requireDeterminism.nonEmpty) {
              // skip checking non-deterministic columns in filter args in agg call because they
              // were pushed down to input project which processes input only message
              val inputFieldCnt = overAgg.getInput.getRowType.getFieldCount
              val nonDeterministicOutput = new JHashMap[Int, String]
              val overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow)
              overSpec.getGroups.foreach {
                group =>
                  group.getAggCalls.zipWithIndex.foreach {
                    case (aggCall: AggregateCall, absPos: Int) =>
                      if (aggCall.getAggregation.isDeterministic) {
                        nonDeterministicOutput.put(
                          inputFieldCnt + absPos,
                          aggCall.getAggregation.getName)
                      }
                  }
              }
              // check if exist non-deterministic aggCalls which were in requireDeterminism
              val unsatisfiedColumn =
                requireDeterminism.toList.filter(index => nonDeterministicOutput.contains(index))
              if (unsatisfiedColumn.nonEmpty) {
                val errorMsg = generateNonDeterministicColumnsErrorMessage(
                  unsatisfiedColumn.toArray,
                  overAgg.getRowType,
                  overAgg,
                  nonDeterministicOutput)
                throw new TableException(errorMsg)
              }
            }
            transmitDeterminismRequirement(overAgg, NO_REQUIRED_DETERMINISM)
          } else {
            // Currently overAgg does not support input with updates, so this branch will not be
            // reached for now.

            // We should append partition keys and order key to requireDeterminism
            transmitDeterminismRequirement(
              overAgg,
              mappingRequireDeterminismToInput(requireDeterminism, overAgg))
          }

        // if outputRankNumber:  output row type = input row type + rank number type
        // else keeps the same as input
        case rank: StreamPhysicalRank =>
          if (inputInsertOnly(rank)) {
            // rank output is deterministic when input is insert only, so required determinism
            // always be satisfied here.
            transmitDeterminismRequirement(rank, NO_REQUIRED_DETERMINISM)
          } else {
            // if input has updates, we evaluate the required determinism by rankStrategy
            val inputFieldCnt = rank.getInput.getRowType.getFieldCount
            rank.rankStrategy match {
              case _: UpdateFastStrategy =>
                // in update fast mode, pass required determinism excludes partition keys and order key
                val bitSetBuilder = ImmutableBitSet.builder()
                rank.partitionKey.toArray.foreach(bitSetBuilder.set(_))
                rank.orderKey.getKeys.toIntArray.foreach(bitSetBuilder.set(_))
                if (rank.outputRankNumber) {
                  // exclude last column
                  bitSetBuilder.set(inputFieldCnt)
                }
                val requireInputDeterminism = requireDeterminism.except(bitSetBuilder.build())
                transmitDeterminismRequirement(rank, requireInputDeterminism)

              case _: RetractStrategy =>
                // in retract mode then require all input columns be deterministic
                transmitDeterminismRequirement(rank, ImmutableBitSet.range(inputFieldCnt))

              case unknown: RankProcessStrategy =>
                // AppendFastStrategy only applicable for insert only input, so the undefined
                // strategy is not as expected here
                throw new TableException(
                  s"Can not infer the determinism for unsupported rank strategy: $unknown, this is a bug, please file an issue.")
            }
          }

        // output row type same as input and does not change output columns' order
        case dedup: StreamPhysicalDeduplicate =>
          if (inputInsertOnly(dedup)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            transmitDeterminismRequirement(dedup, NO_REQUIRED_DETERMINISM)
          } else {
            // Currently deduplicate always has uniqueKeys(exec node has null check and inner state
            // only support data with keys), so only pass the left columns of required determinism
            // to input
            val requireInputDeterminism = if (requireDeterminism.nonEmpty) {
              requireDeterminism.except(ImmutableBitSet.of(dedup.getUniqueKeys: _*))
            } else {
              NO_REQUIRED_DETERMINISM
            }
            transmitDeterminismRequirement(dedup, requireInputDeterminism)
          }

        // output row type same as input and does not change output columns' order
        case winDedup: StreamPhysicalWindowDeduplicate =>
          if (inputInsertOnly(winDedup)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            transmitDeterminismRequirement(winDedup, NO_REQUIRED_DETERMINISM)
          } else {
            // Currently WindowDeduplicate does not support input with updates, so this branch will
            // not be reached for now.

            // only append partition keys, order key always come from window(no need to process it)
            val requireInputDeterminism = requireDeterminism
              .clear(winDedup.orderKey)
              .union(ImmutableBitSet.of(winDedup.partitionKeys: _*))
            transmitDeterminismRequirement(winDedup, requireInputDeterminism)
          }

        case winRank: StreamPhysicalWindowRank =>
          if (inputInsertOnly(winRank)) {
            // similar to rank, output is deterministic when input is insert only, so required
            // determinism always be satisfied here.
            transmitDeterminismRequirement(winRank, NO_REQUIRED_DETERMINISM)
          } else {
            // Currently WindowDeduplicate does not support input with updates, so this branch will
            // not be reached for now.

            // only append partition keys, order key always come from window(no need to process it)
            val inputFieldCnt = winRank.getInput.getRowType.getFieldCount
            val requireInputDeterminism = requireDeterminism
              .intersect(ImmutableBitSet.range(inputFieldCnt))
              .union(ImmutableBitSet.of(winRank.partitionKey.toArray: _*))
            transmitDeterminismRequirement(winRank, requireInputDeterminism)
          }

        // output row type = input row type + window attributes
        case winTVF: StreamPhysicalWindowTableFunction =>
          if (inputInsertOnly(winTVF)) {
            transmitDeterminismRequirement(winTVF, NO_REQUIRED_DETERMINISM)
          } else {
            // pass the left columns of required determinism to input exclude window attributes
            val requireInputDeterminism = requireDeterminism.intersect(
              ImmutableBitSet.range(winTVF.getInput.getRowType.getFieldCount))
            transmitDeterminismRequirement(winTVF, requireInputDeterminism)
          }

        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported to visit node ${rel.getClass.getSimpleName}, please add the visit "
              + s"implementation if it is a newly added stream physical node.")
      }

    private def transmitDeterminismRequirement(
        parent: StreamPhysicalRel,
        requireDeterminism: ImmutableBitSet): StreamPhysicalRel = {
      val newChildren = visitInputs(parent, requireDeterminism)
      parent.copy(parent.getTraitSet, newChildren).asInstanceOf[StreamPhysicalRel]
    }

    private def visitInputs(
        parent: StreamPhysicalRel,
        requireDeterminism: ImmutableBitSet): List[StreamPhysicalRel] = {
      val newChildren = for (i <- 0 until parent.getInputs.size()) yield {
        val input = parent.getInput(i).asInstanceOf[StreamPhysicalRel]
        // unified processing on input upsertKey
        visit(input, requireDeterminismExcludeUpsertKey(input, requireDeterminism))
      }
      newChildren.toList
    }

    private def visitJoinChild(
        requireDeterminism: ImmutableBitSet,
        rel: StreamPhysicalRel,
        inputHasUpdate: Boolean,
        leftFieldCnt: Int,
        isLeft: Boolean,
        joinKeys: Array[Int],
        inputUniqueKeys: List[Array[Int]]
    ): StreamPhysicalRel = {

      val joinInputSideSpec = JoinUtil.analyzeJoinInput(
        unwrapClassLoader(rel),
        InternalTypeInfo.of(FlinkTypeFactory.toLogicalRowType(rel.getRowType)),
        joinKeys,
        inputUniqueKeys)
      val inputRequireDeterminism =
        if (inputHasUpdate) {
          if (joinInputSideSpec.hasUniqueKey || joinInputSideSpec.joinKeyContainsUniqueKey()) {
            // join hasUniqueKey or joinKeyContainsUniqueKey, then transmit corresponding
            // requirement to input
            if (isLeft) {
              ImmutableBitSet.of(
                requireDeterminism.toArray.filter(index => index < leftFieldCnt): _*)
            } else {
              ImmutableBitSet.of(
                requireDeterminism.toArray
                  .filter(index => index >= leftFieldCnt)
                  .map(index => index - leftFieldCnt): _*)
            }
          } else {
            // join need to retract by whole input row
            ImmutableBitSet.range(rel.getRowType.getFieldCount)
          }
        } else {
          NO_REQUIRED_DETERMINISM
        }
      transmitDeterminismRequirement(rel, inputRequireDeterminism)
    }

    /** Extracts the out from source field index mapping of the given projects. */
    private def extractSourceMapping(projects: JList[RexNode]): JHashMap[Int, Int] = {
      val mapOutFromInPos = new JHashMap[Int, Int]()

      // Build an input to output position map.
      projects.zipWithIndex.foreach {
        case (projExpr, i) =>
          projExpr match {
            case ref: RexInputRef => mapOutFromInPos.put(i, ref.getIndex)
            // rename or cast
            case a: RexCall
                if (a.getKind.equals(SqlKind.AS) || a.getKind.equals(SqlKind.CAST)) &&
                  a.getOperands.get(0).isInstanceOf[RexInputRef] =>
              mapOutFromInPos.put(i, a.getOperands.get(0).asInstanceOf[RexInputRef].getIndex)
            case literal: RexLiteral => mapOutFromInPos.put(i, -1)
            case _ => // ignore
          }
      }
      mapOutFromInPos
    }

    private def generateNonDeterministicConditionErrorMessage(
        ndCall: String,
        condition: RexNode,
        relatedRel: StreamPhysicalRel
    ): String = {
      val errorMsg = mutable.StringBuilder.newBuilder
      errorMsg.append(
        String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, ndCall, condition))
      errorMsg
        .append("\nrelated rel plan:\n")
        .append(
          FlinkRelOptUtil.toString(relatedRel, withChangelogTraits = true, withUpsertKey = true))

      errorMsg.toString
    }

    private def generateNonDeterministicColumnsErrorMessage(
        indexes: Array[Integer],
        rowType: RelDataType,
        relatedRel: StreamPhysicalRel,
        ndCallMap: JHashMap[Int, String],
        ndCallName: Option[String] = Option.empty): String = {
      val errorMsg = mutable.StringBuilder.newBuilder
      errorMsg.append("The column(s): ")
      rowType.getFieldNames.zipWithIndex
        .foreach {
          case (column: String, index: Int) =>
            if (indexes.contains(index)) {
              errorMsg
                .append(column)
                .append("(generated by non-deterministic function: ")
              if (ndCallName.isDefined) {
                errorMsg.append(ndCallName.get)
              } else {
                errorMsg.append(ndCallMap.get(index))
              }
              errorMsg.append(" ) ")
            }
        }
      errorMsg.append(
        "can not satisfy the determinism requirement for correctly processing update message("
          + "'UB'/'UA'/'D' in changelogMode, not 'I' only), this usually happens when input node has"
          + " no upsertKey(upsertKeys=[{}]) or current node outputs non-deterministic update "
          + "messages. Please consider removing these non-deterministic columns or making them "
          + "deterministic by using deterministic functions.\n")
      errorMsg
        .append("\nrelated rel plan:\n")
        .append(
          FlinkRelOptUtil.toString(relatedRel, withChangelogTraits = true, withUpsertKey = true))

      errorMsg.toString
    }

    private def checkUnsatisfiedDeterminism(
        requireDeterminism: ImmutableBitSet,
        grouping: Array[Int],
        aggCalls: Seq[AggregateCall],
        rowType: RelDataType,
        relatedRel: StreamPhysicalRel): Unit = {
      val nonDeterministicOutput = new JHashMap[Int, String]
      val groupKeyLen = grouping.length
      // skip checking non-deterministic columns in grouping keys or filter args in agg call
      // because they were pushed down to input project which processes input only message
      aggCalls.zipWithIndex.foreach {
        case (aggCall: AggregateCall, absPos: Int) =>
          if (!aggCall.getAggregation.isDeterministic) {
            nonDeterministicOutput.put(groupKeyLen + absPos, aggCall.getAggregation.getName)
          }
      }
      // check if exist non-deterministic aggCalls which were in requireDeterminism
      val unsatisfiedColumn =
        requireDeterminism.toList.filter(index => nonDeterministicOutput.contains(index))
      if (unsatisfiedColumn.nonEmpty) {
        val errorMsg = generateNonDeterministicColumnsErrorMessage(
          unsatisfiedColumn.toArray,
          rowType,
          relatedRel,
          nonDeterministicOutput)
        throw new TableException(errorMsg)
      }
    }

    /**
     * Returns the non-deterministic call name from the given expression, differs from calcite's
     * [[RexUtil]], it considers both non-deterministic and dynamic functions.
     */
    private def getNonDeterministicCallName(e: RexNode): Option[String] = try {
      val visitor = new RexVisitorImpl[Void](true) {
        override def visitCall(call: RexCall): Void = {
          // dynamic function call is also non-deterministic to streaming
          if (!call.getOperator.isDeterministic || call.getOperator.isDynamicFunction)
            throw new Util.FoundOne(call.getOperator.getName)
          super.visitCall(call)
        }
      }
      e.accept(visitor)
      Option.empty
    } catch {
      case ex: Util.FoundOne =>
        Util.swallow(ex, null)
        Option(ex.getNode.toString)
    }

    private def mappingRequireDeterminismToInput(
        requireDeterminism: ImmutableBitSet,
        overAgg: StreamPhysicalOverAggregateBase): ImmutableBitSet = {
      val inputFieldCnt = overAgg.getInput.getRowType.getFieldCount
      val requireInput = requireDeterminism.toList.filter(index => index < inputFieldCnt)
      if (requireInput.size == inputFieldCnt) {
        // already requires all input columns, no need to check aggCalls
        ImmutableBitSet.range(inputFieldCnt)
      } else {
        val allRequiredInputSet = new mutable.HashSet[Int]()
        requireInput.foreach(allRequiredInputSet.apply(_))

        val overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow)
        // add partitionKeys
        overSpec.getPartition.getFieldIndices.foreach(allRequiredInputSet.apply(_))
        // add aggCall's input
        overSpec.getGroups.foreach {
          group =>
            // add sortKeys
            group.getSort.getFieldIndices.foreach(allRequiredInputSet.apply(_))
            group.getAggCalls.zipWithIndex.foreach {
              case (aggCall: AggregateCall, absPos: Int) =>
                if (requireDeterminism.get(absPos + inputFieldCnt)) {
                  requiredSourceInput(aggCall, allRequiredInputSet)
                }
            }
        }
        assert(allRequiredInputSet.size <= inputFieldCnt)
        ImmutableBitSet.of(allRequiredInputSet.toArray: _*)
      }
    }

    private def requiredSourceInput(
        aggCall: AggregateCall,
        requiredInputSet: mutable.HashSet[Int]): Unit = {
      aggCall.getArgList.foreach(requiredInputSet.apply(_))
      if (aggCall.filterArg > -1) {
        requiredInputSet.apply(_)
      }
    }

    private def requireDeterminismExcludeUpsertKey(
        inputRel: StreamPhysicalRel,
        requireDeterminism: ImmutableBitSet): ImmutableBitSet = {
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(inputRel.getCluster.getMetadataQuery)
      val inputUpsertKeys = fmq.getUpsertKeys(inputRel)
      val finalRequireDeterminism = if (null == inputUpsertKeys || inputUpsertKeys.isEmpty) {
        requireDeterminism
      } else {
        if (inputUpsertKeys.exists(uk => uk.contains(requireDeterminism))) {
          // upsert keys can satisfy the requireDeterminism because they are always deterministic
          NO_REQUIRED_DETERMINISM
        } else {
          // otherwise we should check the column(s) that not in upsert keys
          val leftKeys =
            inputUpsertKeys.map(requireDeterminism.except(_)).toList.sortBy(f => f.cardinality())
          assert(leftKeys.size > 0)
          val leastRequireDeterminism = leftKeys.head
          leastRequireDeterminism
        }
      }
      finalRequireDeterminism
    }
  }
}

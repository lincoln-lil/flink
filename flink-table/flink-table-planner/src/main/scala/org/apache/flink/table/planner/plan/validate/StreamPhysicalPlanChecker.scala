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
import org.apache.flink.table.api.config.OptimizerConfigOptions.NonDeterministicUpdateHandling
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata
import org.apache.flink.table.planner.{JArrayList, JHashMap, JHashSet, JList}
import org.apache.flink.table.planner.connectors.DynamicSourceUtils
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalcBase, StreamPhysicalChangelogNormalize, StreamPhysicalCorrelateBase, StreamPhysicalDataStreamScan, StreamPhysicalDeduplicate, StreamPhysicalDropUpdateBefore, StreamPhysicalExpand, StreamPhysicalGroupAggregateBase, StreamPhysicalLegacyTableSourceScan, StreamPhysicalLookupJoin, StreamPhysicalMatch, StreamPhysicalMiniBatchAssigner, StreamPhysicalOverAggregateBase, StreamPhysicalRank, StreamPhysicalRel, StreamPhysicalSink, StreamPhysicalSort, StreamPhysicalSortLimit, StreamPhysicalTableSourceScan, StreamPhysicalTemporalSort, StreamPhysicalUnion, StreamPhysicalWatermarkAssigner, StreamPhysicalWindowAggregateBase, StreamPhysicalWindowDeduplicate}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, OverAggregateUtil}
import org.apache.flink.types.RowKind

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexInputRef, RexNode, RexUtil, RexVisitorImpl}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.{ImmutableBitSet, Util}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Validate the correctness of the given physical plan. The main reason is: 'Non-Deterministic
 * Update' (NDU) Mainly includes: <p> 1. Non-deterministic updates <p> 2. CDC source with metadata:
 * <p> 2.1 Upsert key lost in pipeline which source has pk <p> 2.2 No upsert key in pipeline which
 * source has no pk (cdc source without pk, but with metadata) TODO consider CDC source with
 * metadata is another form of non-deterministic update, this can be an unification
 *
 * Why not do this validation in physical rewrite phase? like FlinkChangelogModeInferenceProgram
 * does.
 *   - because the physical plan may be changed a lot after physical rewrite being done, we should
 *     check the 'final' plan.
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
 * <p> 3.1 lookup join: a projection
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
 *  Sink
 *   |
 * Project1{select col1,col2,now(),...}
 *    |
 *  Scan {cdc source}
 * }}}
 *
 * CDC source with metadata is another form of non-deterministic update.
 */
object StreamPhysicalPlanChecker {

  val NON_DETERMINISTIC_UPDATE_ERROR_MSG = ""
  val NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE =
    "There exists non deterministic condition which may cause wrong result in update pipeline: %s"

  def validatePhysicalPlan(
      physicalRelNodes: Seq[FlinkPhysicalRel],
      tableConfig: TableConfig): Seq[FlinkPhysicalRel] = {
    val nonDeterministicUpdateHandling = tableConfig.getConfiguration.get(
      OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_HANDLING)

    val planChecker = new NonDeterministicUpdatePlanVisitor(nonDeterministicUpdateHandling)

    physicalRelNodes.map {
      root =>
        root match {
          case sink: StreamPhysicalSink =>
            val inputChangelogMode =
              ChangelogPlanUtils.getChangelogMode(sink.getInput.asInstanceOf[StreamPhysicalRel]).get

            if (inputInsertOnly(sink)) {
              // no request on upsert key, and not care about NDU
              planChecker.visit(sink.getInput.asInstanceOf[StreamPhysicalRel], ImmutableBitSet.of())
            } else {
              // update streaming
              // 1. sink with pk: upsert sink, update by pk, ideally pk == input.upsertKey,
              // (otherwise upsertMaterialize will handle it)
              // 1.1 input.upsertKey nonEmpty -> not care about NDU

              // 1.2 input.upsertKey isEmpty -> must not contain NDU

              // once sink's requirement on pk was satisfied, no further request will be transited

              // 2. sink without pk: retract sink, retract by complete row
              // whether input.upsertKey is empty or not, must not contain NDU

              val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
              val inputUpsertKeys = fmq.getUpsertKeys(sink.getInput)
              val primaryKey = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes

              // sink has no pk, then it does not require upsert key, otherwise requires
              val requireUpsertKey = primaryKey.nonEmpty

              val requireDeterminism = if (inputUpsertKeys.isEmpty || primaryKey.isEmpty) {
                // if input.upsertKey isEmpty, then input must not contain NDU
                // keep require upsert key due to the update stream
                // all input columns should be deterministic
                ImmutableBitSet.range(sink.getInput.getRowType.getFieldCount)
              } else {
                // input.upsertKey nonEmpty then not care about NDU if sink has pk and pk value
                // is deterministic.
                val pk = ImmutableBitSet.of(primaryKey: _*)
                if (inputUpsertKeys.exists(pk.equals)) {
                  // input upsert key exactly match sink's pk, not care about NDU
                  pk
                } else {
                  // when sink's pk(s) contains input upsertKeys, we should check the rest of
                  // column(s)'s determinism
                  val leftKeys =
                    inputUpsertKeys.map(pk.except(_)).toList.sortBy(f => f.cardinality())
                  assert(leftKeys.size > 0)
                  val leastRequireDeterminism = leftKeys.head
                  leastRequireDeterminism
                }
              }

              planChecker.visit(sink.getInput.asInstanceOf[StreamPhysicalRel], requireDeterminism)
            }

          case rel: StreamPhysicalRel => rel
          // TODO plan has been expanded, so dead path here?
        }
    }
    physicalRelNodes
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
   *   1. append only: no problem 2. has update:
   *      - Sink with primary key:
   *      - Sink without primary key:
   */

  private class NonDeterministicUpdatePlanVisitor(
      nonDeterministicUpdateHandling: NonDeterministicUpdateHandling) {

    /** Extracts the out from source field index mapping of the given projects. */
    def extractSourceMapping(projects: JList[RexNode]): JHashMap[Int, Int] = {
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
            case _ => // ignore
          }
      }
      mapOutFromInPos
    }

    /** Extracts the in to out field index mapping of the given projects. */
    def extractInToOutMapping(projects: JList[RexNode]): JHashMap[Int, JArrayList[Int]] = {
      val mapInToOutPos = new JHashMap[Int, JArrayList[Int]]()

      def appendMapInToOutPos(inIndex: Int, outIndex: Int): Unit = {
        if (mapInToOutPos.contains(inIndex)) {
          mapInToOutPos(inIndex).add(outIndex)
        } else {
          val arrayBuffer = new JArrayList[Int]()
          arrayBuffer.add(outIndex)
          mapInToOutPos.put(inIndex, arrayBuffer)
        }
      }
      // Build an input to output position map.
      projects.zipWithIndex.foreach {
        case (projExpr, i) =>
          projExpr match {
            case ref: RexInputRef => appendMapInToOutPos(ref.getIndex, i)
            // rename or cast
            case a: RexCall
                if (a.getKind.equals(SqlKind.AS) || a.getKind.equals(SqlKind.CAST)) &&
                  a.getOperands.get(0).isInstanceOf[RexInputRef] =>
              appendMapInToOutPos(a.getOperands.get(0).asInstanceOf[RexInputRef].getIndex, i)
            case _ => // ignore
          }
      }
      mapInToOutPos
    }

    def generateErrorMessage(indexes: Array[Integer], rowType: RelDataType): String = {
      val errorMsg = mutable.StringBuilder.newBuilder
      errorMsg.append("The output column(s): ")
      rowType.getFieldNames.zipWithIndex
        .foreach {
          case (column: String, index: Int) if indexes.contains(index) =>
            errorMsg.append(column).append(", ")
        }
      errorMsg.append(
        "not satisfy the determinism requirements for correctly processing updates. Please " +
          "consider removing these non-deterministic columns or making them deterministic.")

      errorMsg.toString
    }

    def errorHandling(
        nonDeterministicUpdateHandling: NonDeterministicUpdateHandling,
        errorMsg: String): Boolean = {
      nonDeterministicUpdateHandling match {
        case NonDeterministicUpdateHandling.ERROR =>
          throw new TableException(errorMsg)

        case NonDeterministicUpdateHandling.IGNORE => false // ignore
//        case NonDeterministicUpdateHandling.RESOLVE_ALL || NonDeterministicUpdateHandling.RESOLVE_FUNCTION_CALL_ONLY =>
        // won't support for now
//          throw new UnsupportedOperationException("")

      }
    }

    /**
     * Returns whether a given expression is deterministic, differs from calcite's [[RexUtil]], it
     * considers both non-deterministic and dynamic functions.
     */
    def isDeterministic(e: RexNode): Boolean = try {
      val visitor = new RexVisitorImpl[Void](true) {
        override def visitCall(call: RexCall): Void = {
          // dynamic function call is also non-deterministic to streaming
          if (!call.getOperator.isDeterministic || call.getOperator.isDynamicFunction)
            throw Util.FoundOne.NULL
          super.visitCall(call)
        }
      }
      e.accept(visitor)
      true
    } catch {
      case ex: Util.FoundOne =>
        Util.swallow(ex, null)
        false
    }

    /**
     * Visit the given rel node to check if it satisfies the requirement of the upsert key and the
     * determinism of the specified column.
     * @param rel
     *   relNode to be validated
     * @param requireDeterminism
     *   downstream operator requires that the specified column represented by the ImmutableBitSet
     *   is deterministic
     */
    def visit(rel: StreamPhysicalRel, requireDeterminism: ImmutableBitSet): Unit =
      rel match {
        case calc: StreamPhysicalCalcBase =>
          // check if exists non-deterministic condition
          if (!inputInsertOnly(calc) && null != calc.getProgram.getCondition) {
            val rexNode = calc.getProgram.expandLocalRef(calc.getProgram.getCondition)
            if (!isDeterministic(rexNode)) {
              throw new TableException(
                String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, rexNode.toString))
            }
          }

          if (requireDeterminism.nonEmpty) {
            // we need to evaluate the column mapping of the required determinism from input
            val projects = calc.getProgram.getProjectList.map(calc.getProgram.expandLocalRef)

            // extract all non deterministic output columns first
            val nonDeterministicOutput = new JHashSet[Int]
            projects.zipWithIndex.foreach {
              case (projExpr, i) =>
                projExpr match {
                  case rex: RexNode if !isDeterministic(rex) =>
                    nonDeterministicOutput.add(i)
                  case _ => // ignore
                }
            }

            // for better exception message
            val unsatisfiedCols = requireDeterminism.toList.filter(nonDeterministicOutput.contains)
            if (unsatisfiedCols.nonEmpty) {
              val errorMsg = generateErrorMessage(unsatisfiedCols.toArray, calc.getRowType)
              errorHandling(nonDeterministicUpdateHandling, errorMsg)
            }

            val outFromSourcePos = extractSourceMapping(projects)
            val conv2inputs = requireDeterminism.toList.map(
              out =>
                outFromSourcePos.getOrElse(
                  out,
                  throw new TableException(
                    s"Invalid pos:$out over projection:${calc.getProgram.toString}")))
            val requireInputDeterminism = ImmutableBitSet.of(conv2inputs: _*)

            visitChildren(calc, requireInputDeterminism)
          } else {
            visitChildren(calc, ImmutableBitSet.of())
          }

        case correlate: StreamPhysicalCorrelateBase =>
          // check if exists non-deterministic condition
          if (!inputInsertOnly(correlate) && correlate.condition.isDefined) {
            val rexNode = correlate.condition.get
            if (!isDeterministic(rexNode)) {
              throw new TableException(
                String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, rexNode.toString))
            }
          }

          if (requireDeterminism.nonEmpty) {
            lazy val leftFieldCnt = correlate.inputRel.getRowType.getFieldCount
            if (isDeterministic(correlate.scan.getCall)) {
              // only check left input if call is deterministic
              val fromLeft = requireDeterminism.toList.filter(index => index < leftFieldCnt)
              if (fromLeft.nonEmpty) {

                val requireInputDeterminism = ImmutableBitSet.of(fromLeft.toList)
                visitChildren(correlate, requireInputDeterminism)
              } else {
                visitChildren(correlate, ImmutableBitSet.of())
              }

            } else {
              val unsatisfiedCols = requireDeterminism.toList.filter(index => index >= leftFieldCnt)
              if (unsatisfiedCols.nonEmpty) {
                val errorMsg = generateErrorMessage(unsatisfiedCols.toArray, correlate.getRowType)
                errorHandling(nonDeterministicUpdateHandling, errorMsg)
              }

            }
          }

        case lookupJoin: StreamPhysicalLookupJoin =>
          if (!inputInsertOnly(lookupJoin)) {
            lazy val leftFieldCnt = lookupJoin.getInput.getRowType.getFieldCount
            val requireRight = requireDeterminism.toList.filter(index => index >= leftFieldCnt)

            // check require determinism on right can be satisfied only if lookup key is selected
            // and lookup key is primary key, otherwise can not be satisfied, because we think only
            // the primary key of lookup source is always deterministic.

            if (lookupJoin.calcOnTemporalTable.isDefined) {
              val calc = lookupJoin.calcOnTemporalTable.get
              if (requireRight.nonEmpty && !isDeterministic(calc.getCondition)) {
                throw new TableException(
                  String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, calc.toString))
              }

              // extract all non deterministic output columns first
              val projects = calc.getProjectList.map(calc.expandLocalRef)
              val nonDeterministicOutput = new JHashSet[Int]
              projects.zipWithIndex.foreach {
                case (projExpr, i) =>
                  projExpr match {
                    case rex: RexNode if !isDeterministic(rex) =>
                      nonDeterministicOutput.add(leftFieldCnt + i)
                    case _ => // ignore
                  }
              }
              // for better exception message
              val unsatisfiedCols = requireRight.filter(nonDeterministicOutput.contains)
              if (unsatisfiedCols.nonEmpty && !lookupJoin.upsertMaterialize) {
                // nonDeterminism will be eliminated when enable upsertMaterialize
                val errorMsg = generateErrorMessage(unsatisfiedCols.toArray, calc.getOutputRowType)
                errorHandling(nonDeterministicUpdateHandling, errorMsg)
              }
            }
            val requireLeft = requireDeterminism.toList.filter(index => index < leftFieldCnt)
            if (requireLeft.nonEmpty) {
              visitChildren(lookupJoin, ImmutableBitSet.of(requireLeft.toList))
            }
          }
          visitChildren(lookupJoin, ImmutableBitSet.of())

        case tableScan: StreamPhysicalTableSourceScan =>
          // check meta data from cdc source
          if (requireDeterminism.nonEmpty) {
            val insertOnly = tableScan.tableSource.getChangelogMode.containsOnly(RowKind.INSERT)
            val supportsReadingMetadata = tableScan.tableSource
              .isInstanceOf[SupportsReadingMetadata]
            if (!insertOnly && supportsReadingMetadata) {
              val sourceTable = tableScan.getTable.unwrap(classOf[TableSourceTable])
              // check if requireDeterminism contains metadata column
              val metadataColumns = DynamicSourceUtils.extractMetadataColumns(
                sourceTable.contextResolvedTable.getResolvedSchema)
              val metaColumnSet =
                metadataColumns.map(col => col.getName).toSet
              val metadataCauseErr = tableScan.getRowType.getFieldNames.zipWithIndex.filter {
                case (name: String, index: Int) =>
                  metaColumnSet.contains(name) && requireDeterminism.get(index)
              }
              if (metadataCauseErr.nonEmpty) {
                // TODO detailed error message, includes column name
                throw new TableException(
                  "CDC source with meta columns may cause error result on downstream stateful operators")
              }
            }
          }

        case _: StreamPhysicalLegacyTableSourceScan | _: StreamPhysicalDataStreamScan =>
        // not CDC source, end visit

        case groupAgg: StreamPhysicalGroupAggregateBase =>
          // only check final grouping for StreamPhysicalIncrementalGroupAggregate
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || groupAgg.grouping.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(groupAgg.grouping: _*))
            }
          visitChildren(groupAgg, requireChildDeterminism)

        case normalize: StreamPhysicalChangelogNormalize =>
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || normalize.uniqueKeys.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(normalize.uniqueKeys: _*))
            }
          visitChildren(normalize, requireChildDeterminism)

        case dedup: StreamPhysicalDeduplicate =>
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || dedup.getUniqueKeys.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(dedup.getUniqueKeys: _*))
            }
          visitChildren(dedup, requireChildDeterminism)

        case dropUB: StreamPhysicalDropUpdateBefore =>
          visitChildren(dropUB, requireDeterminism)

        case expand: StreamPhysicalExpand =>
          // Currently expand is an internal operator only for plan rewriting, so only remove the
          // expandIdIndex from requireDeterminism
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(expand.expandIdIndex))
            }
          visitChildren(expand, requireDeterminism)

        case windowAgg: StreamPhysicalWindowAggregateBase =>
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || windowAgg.grouping.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(windowAgg.grouping: _*))
            }
          visitChildren(windowAgg, requireChildDeterminism)

        case join: CommonPhysicalJoin =>
          val leftRel = join.getLeft.asInstanceOf[StreamPhysicalRel]
          val rightRel = join.getRight.asInstanceOf[StreamPhysicalRel]

          val condition = join.getCondition

          if (
            (!inputInsertOnly(leftRel) || !inputInsertOnly(rightRel)) && !isDeterministic(condition)
          ) {
            throw new TableException(
              String.format(NON_DETERMINISTIC_CONDITION_ERROR_MSG_TEMPLATE, condition.toString))
          }

          val leftFieldCnt = leftRel.getRowType.getFieldCount
          val leftRequireDeterminism = Array[Int]()
          val rightRequireDeterminism = Array[Int]()
          if (requireDeterminism.nonEmpty) {
            requireDeterminism.toList.foreach {
              index =>
                if (index < leftFieldCnt) { leftRequireDeterminism :+ index }
                else { rightRequireDeterminism :+ index }
            }
          }
          // visit left
          visitChildren(leftRel, ImmutableBitSet.of(leftRequireDeterminism: _*))
          // visit right
          visitChildren(rightRel, ImmutableBitSet.of(rightRequireDeterminism: _*))

        case streamMatch: StreamPhysicalMatch =>
          val partitionKeys = streamMatch.getLogicalMatch.partitionKeys
          // only check final grouping for StreamPhysicalIncrementalGroupAggregate
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || partitionKeys.cardinality() == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(partitionKeys)
            }
          visitChildren(streamMatch, requireChildDeterminism)

        case _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalUnion | _: StreamPhysicalSort |
            _: StreamPhysicalSortLimit | _: StreamPhysicalTemporalSort |
            _: StreamPhysicalWatermarkAssigner =>
          // transit requireDeterminism transparently
          visitChildren(rel, requireDeterminism)

        case overAgg: StreamPhysicalOverAggregateBase =>
          val overSpec = OverAggregateUtil.createOverSpec(overAgg.logicWindow)
          val partitionKeys = overSpec.getPartition.getFieldIndices
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || partitionKeys.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(partitionKeys: _*))
            }
          visitChildren(overAgg, requireChildDeterminism)

        case rank: StreamPhysicalRank =>
          val partitionKeys = rank.partitionKey
          // only check final grouping for StreamPhysicalIncrementalGroupAggregate
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || partitionKeys.cardinality() == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(partitionKeys)
            }
          visitChildren(rank, requireChildDeterminism)

        case winDedup: StreamPhysicalWindowDeduplicate =>
          val partitionKeys = winDedup.partitionKeys
          val requireChildDeterminism =
            if (requireDeterminism.isEmpty || partitionKeys.length == 0) {
              ImmutableBitSet.of()
            } else {
              requireDeterminism.intersect(ImmutableBitSet.of(partitionKeys: _*))
            }
          visitChildren(winDedup, requireChildDeterminism)

        // TODO check all operators which append new columns to output, should exclude them from input requireDeterminism

        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported visit for node ${rel.getClass.getSimpleName}, please add the visit implementation if it is a newly added physical node")
      }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requireDeterminism: ImmutableBitSet): Unit = {
      for (i <- 0 until parent.getInputs.size()) yield {
        val child = parent.getInput(i).asInstanceOf[StreamPhysicalRel]
        visit(child, requireDeterminism)
      }
    }
  }

  /**
   * Visit dag to check <p>1. Whether should add materialization for non-deterministic calculation,
   * e.g., temporal lookup join and non-deterministic function (scalar function or table function).
   * If need materialization, copy the physical node with needMaterialize flag (and will add
   * materialization for runtime).
   *
   * <p>2. Also it will check the input changelog stream's upsertKey info, will try to add back from
   * children (the nearest calc node or the projectable table source scan) if the upsertKey lost.
   *
   * A simple example: SQL: select `day`, count(*) cnt, sum(b) qmt from ( select *, concat(c,
   * DATE_FORMAT(CURRENT_TIMESTAMP, '-', 'yyMMdd')) `day` from cdc_source ) t group by `day`
   *
   * wrong plan before correction:
   * {{{
   * GroupAggregate(groupBy=[day], select=[day, COUNT_RETRACT(*) AS cnt, SUM_RETRACT(b) AS qmt],
   * /changelogMode=[I,UA,D])
   * +- Exchange(distribution=[hash[day]], changelogMode=[I,UB,UA,D])
   *    +- Calc(select=[CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd')) AS day, b],
   *    /changelogMode=[I,UB,UA,D])
   *        +- TableSourceScan(table=[[default_catalog, default_database, cdc, project=[c, b],
   *        /metadata=[]]], fields=[c, b], changelogMode=[I,UB,UA,D])
   * }}}
   *
   * right plan after correction:
   * {{{
   * GroupAggregate(groupBy=[day], select=[day, COUNT_RETRACT(*) AS cnt, SUM_RETRACT(b) AS qmt],
   * /changelogMode=[I,UA,D])
   * +- Exchange(distribution=[hash[day]], changelogMode=[I,UB,UA,D])
   *    +- Calc(select=[CONCAT(c, DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyMMdd')) AS day, b],
   *    /changelogMode=[I,UB,UA,D])
   *        +- TableSourceScan(table=[[default_catalog, default_database, cdc, project=[c, b],
   *        /metadata=[]]], fields=[c, b], changelogMode=[I,UB,UA,D])
   * }}}
   */
}

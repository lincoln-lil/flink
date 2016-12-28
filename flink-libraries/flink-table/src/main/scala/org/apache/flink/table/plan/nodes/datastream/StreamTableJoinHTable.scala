package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.table.sources.HBaseTableSource
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
import org.apache.flink.table.functions.utils.hbase.{AsyncHBaseTableFetchFunction, HTableRowGetFunction}

import scala.collection.JavaConverters._
import scala.concurrent.duration
import java.util.List

import org.apache.flink.types.Row


class StreamTableJoinHTable(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftNode: RelNode,
    hTableSource: HBaseTableSource,
    rowRelDataType: RelDataType,
    joinCondition: RexNode,
    joinRowType: RelDataType,
    joinInfo: JoinInfo,
    keyPairs: List[IntPair],
    joinType: JoinRelType,
    joinHint: JoinHint,
    ruleDescription: String)
  extends SingleRel(cluster, traitSet, leftNode)
          with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableJoinHTable(
      cluster,
      traitSet,
      inputs.get(0),
      hTableSource,
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription
    )
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    .item("where", joinConditionToString)
    .item("join", joinSelectionToString)
    .item("joinType", joinTypeToString)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 1.0d //fetch one row from a HBase table each request, low cost.
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }


  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return DataStream of type [[Row]]
    */
  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[Row] = {
    val config = tableEnv.getConfig
    val returnType = FlinkTypeFactory.toInternalRowTypeInfo(rowType)

    if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
      throw TableException("Only support inner or left join with a HBaseTable.")
    }

    if (keyPairs.size != 1) {
      // invalid join condition,  support single only, e.g, 'id === tid'
      throw TableException(
        "Join a HBaseTable should have exactly one equality condition.\n" +
          s"\tLeft: ${leftNode.toString},\n" +
          s"\tRight: ${hTableSource.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
      )
    } else {
      val pair = keyPairs.get(0)
      // HBaseTable's rowKey index always be zero
      val leftKeyIdx = if (pair.source == hTableSource.getRowKeyIndex) {
        pair.target
      } else {
        pair.source
      }

      val inputDataStream = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
                            .asInstanceOf[DataStream[Row]]

      // for test env, translate to test HBaseGetFunction, otherwise production Function
      /*  val flatMapFunction: FlatMapFunction[Row, Row] = new HTableRowGetFunction(
          returnType.asInstanceOf[CompositeType[Row]],
          hTableSource,
          joinType,
          leftKeyIdx)

        inputDataStream.flatMap(flatMapFunction)
        .name(s"${joinTypeToString}HTable#${hTableSource.getTableName}")
        .asInstanceOf[DataStream[Row]]*/

        val asyncFunction = new AsyncHBaseTableFetchFunction[Row, Row](
          returnType.asInstanceOf[CompositeType[Row]],
          hTableSource,
          joinType,
          leftKeyIdx).asInstanceOf[AsyncFunction[Row, Row]]

        AsyncDataStream
        .orderedWait(
          inputDataStream,
          asyncFunction,
          hTableSource.ASYNC_GET_DEFAULT_TIMEOUT_SECOND,
          duration.SECONDS,
          hTableSource.ASYNC_BUFFER_DEFAULT_CAPACITY)

    }

  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {
    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT => "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }
}

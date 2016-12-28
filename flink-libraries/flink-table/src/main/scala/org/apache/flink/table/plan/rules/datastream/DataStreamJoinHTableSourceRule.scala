package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject, LogicalTableScan}
import org.apache.flink.table.plan.nodes.datastream.{DataStreamConvention, StreamTableJoinHTable, StreamTableSourceScan}
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.HBaseTableSource

class DataStreamJoinHTableSourceRule  extends RelOptRule (
  operand(classOf[LogicalJoin], operand(classOf[RelNode], any), operand(classOf[RelNode], any)),
  "DataStreamJoinHTableSourceRule"
){

  override def matches(call: RelOptRuleCall): Boolean = {
    val join = call.rel(0).asInstanceOf[LogicalJoin]

    def findAimNode(rel: RelNode): Boolean = {
      rel match {
          case rel: RelSubset =>
            findAimNode(rel.getRelList.get(0))

          case rel: LogicalJoin =>
            findAimNode(rel.getRight)

          case rel: LogicalTableScan => {
            if(rel.getTable.isInstanceOf[RelOptTableImpl]){
              rel.getTable.unwrap(classOf[TableSourceTable]).tableSource
              .isInstanceOf[HBaseTableSource]
            }else {
              false
            }
          }

          case _ =>
            false
      }
    }
    findAimNode(join.getRight)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join = call.rel(0).asInstanceOf[LogicalJoin]
    val traitSet: RelTraitSet = join.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataStreamConvention.INSTANCE)
    val joinInfo = join.analyzeCondition

    def extractTableSource(rel: RelNode): HBaseTableSource = {
      rel match {
        case s: RelSubset => extractTableSource(s.getRelList.get(0))

        case l: LogicalTableScan => {
          l.getTable.asInstanceOf[RelOptTableImpl].unwrap(classOf[TableSourceTable]).tableSource
          .asInstanceOf[HBaseTableSource]
        }
      }
    }

    val hTableSource = extractTableSource(join.getRight)

    def convertToJoinHTable(relNode: RelNode): StreamTableJoinHTable = {
      relNode match {
        case join: LogicalJoin =>
          convertToJoinHTable(join.getLeft)

        case rel: RelSubset =>
          convertToJoinHTable(rel.getRelList.get(0))

        //         case filter: LogicalFilter =>
        //           convertToJoinHTable(filter.getInput.asInstanceOf[RelSubset].getOriginal)

        case project: LogicalProject =>
          new StreamTableJoinHTable(
            join.getCluster,
            traitSet,
            convLeft,
            hTableSource,
            join.getRowType,
            join.getCondition,
            join.getRowType,
            joinInfo,
            joinInfo.pairs,
            join.getJoinType,
            null,
            description)

        case lscan: LogicalTableScan => {
          new StreamTableJoinHTable(
            join.getCluster,
            traitSet,
            convLeft,
            hTableSource,
            join.getRowType,
            join.getCondition,
            join.getRowType,
            joinInfo,
            joinInfo.pairs,
            join.getJoinType,
            null,
            description)
        }

        case scan: StreamTableSourceScan => {
          new StreamTableJoinHTable(
            join.getCluster,
            traitSet,
            convLeft,
            hTableSource,
            join.getRowType,
            join.getCondition,
            join.getRowType,
            joinInfo,
            joinInfo.pairs,
            join.getJoinType,
            null,
            description)
        }

      }
    }

    val newJoin =  convertToJoinHTable(join.getLeft)
    call.transformTo(newJoin)
  }

}

object DataStreamJoinHTableSourceRule {
  val INSTANCE: RelOptRule = new DataStreamJoinHTableSourceRule
}
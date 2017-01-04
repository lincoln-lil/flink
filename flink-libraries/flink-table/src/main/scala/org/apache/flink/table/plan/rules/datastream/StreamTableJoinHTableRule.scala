package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.logical.{LogicalFilter, LogicalJoin, LogicalProject, LogicalTableScan}
import org.apache.flink.table.plan.logical.CatalogNode
import org.apache.flink.table.plan.nodes.datastream._
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.HBaseTableSource

/**
  * Created by lincoln.lil on 2016/12/12.
  */
class StreamTableJoinHTableRule extends ConverterRule(
  classOf[DataStreamJoin],
  Convention.NONE,
  DataStreamConvention.INSTANCE,
  "StreamTableJoinHTableRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: DataStreamJoin = call.rel(0).asInstanceOf[DataStreamJoin]

    // joins require an equi-condition or a conjunctive predicate with at least one equi-condition
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

//        case rel: StreamTableSourceScan

        case _ =>
          false
      }
    }
    findAimNode(join.getRight)

  }


  override def convert(rel: RelNode): RelNode = {
    val join: DataStreamJoin = rel.asInstanceOf[DataStreamJoin]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convLeft: RelNode = RelOptRule.convert(join.getInput(0), DataStreamConvention.INSTANCE)
//    val convRight: RelNode = RelOptRule.convert(join.getInput(1), DataStreamConvention.INSTANCE)
    //    val joinInfo = join.analyzeCondition()

    def convertToJoinHTable(relNode: RelNode): StreamTableJoinHTable = {
      relNode match {
        case join: LogicalJoin =>
          convertToJoinHTable(join.getLeft)

        case rel: RelSubset =>
          convertToJoinHTable(rel.getRelList.get(0))

        // case filter: LogicalFilter =>
        //   convertToJoinHTable(
        //   filter.getInput.asInstanceOf[RelSubset].getOriginal)
        //
        // case project: LogicalProject =>
        //   new StreamTableJoinHTable(
        //     rel.getCluster, traitSet, convLeft, hTableSource,
        //     rel.getRowType, null, description)

        case scan: StreamTableSourceScan => {
          new StreamTableJoinHTable(
          rel.getCluster,
          traitSet,
          convLeft,
          scan.getTable.unwrap(classOf[TableSourceTable]).tableSource
          .asInstanceOf[HBaseTableSource],
          join.getRowType,
          join.joinCondition,
          join.getRowType,
          join.joinInfo,
          join.joinInfo.pairs,
          join.joinType,
          null,
          description
          )
        }

      }
    }

    convertToJoinHTable(join.getLeft)
  }
}

object StreamTableJoinHTableRule {
  val INSTANCE: RelOptRule = new StreamTableJoinHTableRule
}

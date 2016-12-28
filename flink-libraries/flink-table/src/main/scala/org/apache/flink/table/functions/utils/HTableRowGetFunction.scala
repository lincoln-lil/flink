package org.apache.flink.table.functions.utils

import org.apache.calcite.rel.core.JoinRelType
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.table.sources.HBaseTableSource
import org.apache.flink.types.Row
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.{Configuration => HConfiguration}

class HTableRowGetFunction[IN, OUT](
    resultType: TypeInformation[OUT],
    hTableSource: HBaseTableSource,
    joinType: JoinRelType,
    sourceKeyIndex: Int) extends RichFlatMapFunction[IN, OUT] with ResultTypeQueryable[OUT] {

  val tableName: String = hTableSource.getTableName

  val serializedConfig: Array[Byte] = {
    val configuration = HBaseConfiguration.create();
    hTableSource.configMap.map.foreach((e: (String, String)) => configuration.set(e._1, e._2))
    WritableSerializer.serializeWritable(configuration)
  }

  val fieldLength: Int = hTableSource.getNumberOfFields

  val fieldNames: Array[String] = hTableSource.getFieldsNames

  val fieldTypes: Array[TypeInformation[_]] = hTableSource.getFieldTypes

  val hTableStore: Map[Int, Tuple3[String, Int, String]] = Map(
    1 -> Tuple3("a", 1, "No.1"),
    2 -> Tuple3("b", 2, "No.2"),
    3 -> Tuple3("c", 3, "No.3"),
    4 -> Tuple3("d", 4, "No.4"),
    8 -> Tuple3("e", 8, "No.8"),
    16 -> Tuple3("f", 16, "No.16")
  )
  //  val returnType: TypeInformation[OUT] = hTableSource.getReturnType

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val hbaseConfiguration: HConfiguration = HBaseConfiguration.create()
    WritableSerializer.deserializeWritable(hbaseConfiguration, serializedConfig)
    val zkQuarum: String = hbaseConfiguration.get("hbase.zookeeper.quorum")

    println(
      s"${getRuntimeContext.getTaskName} task-${
        getRuntimeContext
        .getIndexOfThisSubtask
      }/apptempt${getRuntimeContext.getAttemptNumber}" +
        s" connect to" +
        s" hbase " +
        s"table:${tableName}" +
        s"  by zkQuarum:${zkQuarum}")

  }

  override def flatMap(value: IN, out: Collector[OUT]): Unit = {
    val row = value.asInstanceOf[Row]
    val keyType: TypeInformation[_] = fieldTypes(sourceKeyIndex)
    val keyValue: Int = row.getField(sourceKeyIndex).asInstanceOf[Int]
    val resRowLen = row.getArity + fieldLength
    val result: Row = new Row(resRowLen)
    val getRes:Option[Tuple3[String, Int, String]] = hTableStore.get(keyValue)

    def cloneRow(src: Row, target: Row): Unit = {
      for (i <- 0 until src.getArity) {
        target.setField(i, src.getField(i))
      }
    }

    if(getRes != None){
      val rightRow = getRes.get
      cloneRow(row, result)
      result.setField(row.getArity, keyValue)
      result.setField(row.getArity + 1, rightRow._1)
      result.setField(row.getArity + 2, rightRow._2)
      result.setField(row.getArity + 3, rightRow._3)

      out.collect(result.asInstanceOf[OUT])
    }else if (joinType == JoinRelType.LEFT){
      cloneRow(row, result)
      result.setField(row.getArity, null)
      result.setField(row.getArity + 1, null)
      result.setField(row.getArity + 2, null)
      result.setField(row.getArity + 3, null)

      out.collect(result.asInstanceOf[OUT])
    }// else filtered
  }

  override def getProducedType: TypeInformation[OUT] = resultType


}
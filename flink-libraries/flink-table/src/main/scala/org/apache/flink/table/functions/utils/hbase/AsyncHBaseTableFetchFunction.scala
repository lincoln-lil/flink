package org.apache.flink.table.functions.utils.hbase

import org.apache.calcite.rel.core.JoinRelType
import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector
import org.apache.flink.table.functions.utils.WritableSerializer
import org.apache.flink.table.sources.HBaseTableSource
import org.apache.flink.types.Row
import org.apache.hadoop.conf.{Configuration => HConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

class AsyncHBaseTableFetchFunction[IN, OUT](
    resultType: TypeInformation[OUT],
    hTableSource: HBaseTableSource,
    joinType: JoinRelType,
    sourceKeyIndex: Int) extends RichAsyncFunction[IN, OUT] with Serializable with
                                 ResultTypeQueryable[OUT]{
  val tableName: String = hTableSource.getTableName

  @transient var hTableConnection: Connection = null

  @transient var hTable: AsyncableHTableInterface = null

  @transient var resultParser: ResultParser = null

  val hbaseQuorumName = "hbase.zookeeper.quorum"

  val serializedConfig: Array[Byte] = {
    val configuration = HBaseConfiguration.create()
    hTableSource.configMap.map.foreach((e: (String, String)) => configuration.set(e._1, e._2))
    WritableSerializer.serializeWritable(configuration)
  }

  val fieldLength: Int = hTableSource.getNumberOfFields

  val fieldNames: Array[String] = hTableSource.getFieldsNames

  val fieldTypes: Array[TypeInformation[_]] = hTableSource.getFieldTypes

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    resultParser = hTableSource.parserClass

    // connect hbase
    val hbaseConfiguration: HConfiguration = HBaseConfiguration.create()
    WritableSerializer.deserializeWritable(hbaseConfiguration, serializedConfig)
    hbaseConfiguration.set("hbase.rpc.client.impl", "org.apache.hadoop.hbase.ipc.AsyncRpcClient")

    val zkQuorum: String = hbaseConfiguration.get(hbaseQuorumName)
    if (StringUtils.isEmpty(zkQuorum)) {
      throw new Exception(s"can not connect to hbase without '${hbaseQuorumName}' configuration")
    }

    hTableConnection = ConnectionFactory.createConnection(hbaseConfiguration)
    hTable = hTableConnection.getTable(TableName.valueOf(tableName))
             .asInstanceOf[AsyncableHTableInterface]

    // logger
    println(
      s"${getRuntimeContext.getTaskName} task-${
        getRuntimeContext
        .getIndexOfThisSubtask
      }/apptempt${
        getRuntimeContext
        .getAttemptNumber
      } connect to hbase table:${tableName} via ${hbaseQuorumName}:${zkQuorum}")
  }

  override def close(): Unit = {
    super.close()
    if (hTable != null) {
      try {
        hTable.close()
      } catch {
        // ignore exception when close.
        case _ => ???
      }
    }

    if (hTableConnection != null) {
      try {
        hTableConnection.close()
      } catch {
        // ignore exception when close.
        case _ => ???
      }
    }

  }

  override def asyncInvoke(input: IN, collector: AsyncCollector[OUT]): Unit = {
    //    val keyType =  fieldTypes(sourceKeyIndex)
    val inputRow = input.asInstanceOf[Row]
    val keyValue = inputRow.getField(sourceKeyIndex)
    val rowKeyBytes = Bytes.toBytes(String.valueOf(keyValue))

    val get = new Get(rowKeyBytes)
    for(idx <- 1 until hTableSource.getNumberOfFields){
      val kv: Array[String] = hTableSource.getFieldsNames(idx).split("$", 2)
      get.addColumn(Bytes.toBytes(kv(0)), Bytes.toBytes(kv(1)))
    }

    hTable
    .asyncGet(
      get,
      new JoinHTableAsyncGetCallback(
        collector,
        rowKeyBytes,
        hTableSource,
        joinType,
        sourceKeyIndex,
        inputRow,
        resultParser))
  }

  override def getProducedType: TypeInformation[OUT] = resultType
}
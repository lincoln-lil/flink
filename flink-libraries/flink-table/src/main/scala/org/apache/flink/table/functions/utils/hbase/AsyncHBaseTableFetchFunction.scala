package org.apache.flink.table.functions.utils.hbase

import java.util

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

  @transient var cache: LRUCache[String, FieldMap] = null

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

    if(hTableSource.cachePolicy!=CachePolicy.Strategy.NONE){
      cache = CacheFactory.getCache(tableName, hTableSource.cachePolicy)
    }

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
    val inputRow = input.asInstanceOf[Row]
    //    val keyType =  fieldTypes(sourceKeyIndex)
    val key = String.valueOf(inputRow.getField(sourceKeyIndex))
    val rowKeyBytes = Bytes.toBytes(key)

    val get = new Get(rowKeyBytes)
    for(idx <- 1 until hTableSource.getNumberOfFields){
      val kv: Array[String] = hTableSource.getFieldsNames(idx).split("$", 2)
      get.addColumn(Bytes.toBytes(kv(0)), Bytes.toBytes(kv(1)))
    }

    val cached = if(cache!=null) cache.get(key) else null
    if(cached!=null){
      // cache hit, do join and collect
      val row = new Row(inputRow.getArity+cached.size())
      for (i <- 0 until inputRow.getArity) {
        row.setField(i, inputRow.getField(i))
      }
      for (j <- 0 until fieldNames.length){
        row.setField(inputRow.getArity-1+j, cached.get(fieldNames(j)))
      }
      val resList = new util.ArrayList[OUT](1)
      resList.add(row.asInstanceOf[OUT])
      collector.collect(resList)

    }else{
      hTable.asyncGet(
        get,
        new JoinHTableAsyncGetCallback(
          collector,
          rowKeyBytes,
          hTableSource,
          joinType,
          sourceKeyIndex,
          inputRow,
          resultParser,
          cache))
    }
  }

  override def getProducedType: TypeInformation[OUT] = resultType
}
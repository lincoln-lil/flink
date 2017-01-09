package org.apache.flink.table.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.functions.utils.UserParamMap
import org.apache.flink.table.functions.utils.hbase.{CachePolicy, DefalultResultParser, ResultParser}
import org.apache.flink.types.Row

class HBaseTableSource(
    val name: String,
    val rowKey: (String,TypeInformation[_]),
    val fieldsInfo: Array[(String, TypeInformation[_])],
    val configMap: UserParamMap,
    var parserClass: ResultParser = new DefalultResultParser,
    var cachePolicy: CachePolicy = CachePolicy.none
) extends BatchTableSource[Row] with StreamTableSource[Row] with Serializable{

  private val returnType = new RowTypeInfo(rowKey._2 +: fieldsInfo.map(_._2): _*)

  val ASYNC_GET_DEFAULT_TIMEOUT_SECOND = 100

  val ASYNC_BUFFER_DEFAULT_CAPACITY = 100

  def withParserClass(newParser: ResultParser): HBaseTableSource = {
    parserClass = newParser
    this
  }

  def withCachePolicy(newPolicy: CachePolicy): HBaseTableSource = {
    cachePolicy = newPolicy
    this
  }

  def getParserClass: ResultParser = parserClass

  /** HBaseTable's rowKey always be the first field, so returns zero. */
  def getRowKeyIndex: Int = 0

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = fieldsInfo.length + 1

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = rowKey._1 +: fieldsInfo.map(_._1)

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = rowKey._2 +: fieldsInfo.map(_._2)

  /** Returns the [[TypeInformation]] for the return type of the [[TableSource]]. */
  override def getReturnType: TypeInformation[Row] = returnType

  def getConfiguration: UserParamMap = configMap

  def getTableName: String = name

  /**
    * Returns the data of the table as a [[org.apache.flink.api.scala.DataSet]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    throw new RuntimeException("unsupported operation.")
  }

  /**
    * Returns the data of the table as a [[org.apache.flink.streaming.api.scala.DataStream]].
    *
    * NOTE: This method is for internal use only for defining a [[TableSource]].
    * Do not use it in Table API programs.
    */
  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    throw new RuntimeException("unsupported operation.")
  }

}

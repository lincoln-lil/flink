package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.functions.utils.UserParamMap
import org.apache.flink.table.sources.HBaseTableSource
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
  * Created by lincoln.lil on 2016/12/19.
  */
class StreamTableJoinHBaseTableTest extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1, 2, "Hi"),
    (2, 5, "Hello"),
    (4, 5, "Hello "),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  @Test
  def testJoinMultiHBaseTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content)

    // specify two physical table name in HBase
    val tableName = "table_abc"


    val configMap: UserParamMap = UserParamMap().add("hbase.zookeeper.quorum", "localhost");

    // create a HBaseTableSource with schema
    val tableSource = new HBaseTableSource(
      tableName,
      "rk_1"->Types.STRING,
      Array("a"->Types.STRING, "b"->Types.INT, "c"->Types.STRING),
      configMap)


    // register table source, this 'tableName' can be an alias of the physical one
    tEnv.registerTableSource(tableName, tableSource)

    // ingest() will return a Table adapt to table api operation
    val hbaseTable: Table = tEnv.ingest(tableName)

    // here we define another HBaseTable named 'table_edf'
    val tableName2 = "table_def"
    val tableSource2 = new HBaseTableSource(
      tableName2,
      "rk_2"->Types.INT,
      Array("d"->Types.STRING, "e"->Types.INT, "f"->Types.STRING),
      configMap)

    tEnv.registerTableSource(tableName2, tableSource2)
    val hbaseTable2: Table = tEnv.ingest(tableName2)

    // we can not apply a filter or other operation on a HBaseTable since it's a virtual table
    // without DatSet or DataStream characteristic, so above code will cause compile error
    // val filteredHTable = hbaseTable.filter('a === "star") // invalid

    //probably we defined a custom UDF for the join key's calculation, this is useful when join
    // a HBase table using transformed RowKey
    // register UDF
    val rowkeyGenerator = KeyGenUsingHash
    tEnv.registerFunction("rowkeyGenerator", rowkeyGenerator)


    val rowkeyGenerator2 = KeyGenUsingNumber
    tEnv.registerFunction("rowkeyGenerator2", rowkeyGenerator2)

    // streamTable join with the
    val resultTable = streamTable
                      .select('id, 'len, 'content, rowkeyGenerator(""+'id, 8, 2).as('rowkey))
                      // only rowkey of a HBase table can be the join key
                      .join(hbaseTable, 'rowkey === 'rk_1) // specify the join key of left table
                      .join(hbaseTable2, "id === rk_2")
                      .select('id, 'len, 'content, 'a, 'c, 'd, 'f)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "1,1,Hi,a,No.1", "2,2,Hello,b,No.2", "4,5,Hello ,d,4,No.4", "8,11,Hello world,e,8,No.8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

  }

  @Test
  def testJoinSingleHBaseTable(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream: DataStream[(Int, Int, String)] = env.fromCollection(data)
    val streamTable = stream.toTable(tEnv, 'id, 'len, 'content)

    // specify physical table name in HBase
    val tableName = "table_abc"
    // create a custom configuration map
    val configMap: UserParamMap = UserParamMap().add("hbase.zookeeper.quorum", "localhost");

    // create a HBaseTableSource with schema
    val tableSource = new HBaseTableSource(
      tableName,
      "rk_1"->Types.INT,
      Array("a"->Types.STRING, "b"->Types.INT, "c"->Types.STRING),
      configMap)

    // register table source, this 'tableName' can be an alias of the physical one
    tEnv.registerTableSource(tableName, tableSource)

    // ingest() will return a Table adapt to table api operation
    val hbaseTable: Table = tEnv.ingest(tableName)

    // streamTable join with the
    val resultTable = streamTable
                      .join(hbaseTable, 'id === 'rk_1) // specify the join key of left table
                      .select('id, 'len, 'content, 'a, 'b, 'c)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "1,2,Hi,a,No.1",
      "2,5,Hello,b,No.2",
      "4,5,Hello ,d,4,No.4",
      "8,11,Hello world,e,8,No.8"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)

  }


}

object KeyGenUsingNumber extends ScalarFunction {
  def eval(num: Int): Int = num % 8

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.INT
  }
}

object KeyGenUsingHash extends ScalarFunction {
  def eval(s: String, mod: Int, prefixLen: Int): String = {
    s"%0${prefixLen}d:${s}".format(Math.abs(s.hashCode) % mod)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.STRING
  }
}

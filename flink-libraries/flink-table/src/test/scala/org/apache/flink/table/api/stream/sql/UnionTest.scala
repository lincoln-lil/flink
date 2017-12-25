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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.junit.{Ignore, Test}

class UnionTest extends TableTestBase {

  @Test
  def testUnionAllNullableCompositeType() = {
    val streamUtil = streamTestUtil()
    streamUtil.addTable[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "CASE(>(c, 0), b, null) AS EXPR$0")
      ),
      term("union all", "a")
    )

    streamUtil.verifySql(
      "SELECT a FROM A UNION ALL SELECT CASE WHEN c > 0 THEN b ELSE NULL END FROM A",
      expected
    )
  }

  @Ignore
  @Test
  def testUnionAll() = {
    val streamUtil = streamTestUtil()
    streamUtil.addTable[(Int, Int, String)]("A", 'a, 'b, 'c)
    streamUtil.tableEnv.registerFunction("udf1", MyUDF)

    val sql =
      s"""
         |select a1, c1, d1
         |from
         |(select (a + b) as a1, udf1(c) as c1, (d + 2) as d1
         |  from
         |  (select a, b, c, 0 as d from A
         |    union all
         |   select a, b, c, 1 as d from A) t
         |  ) t1
         |where  d1 > 2
       """.stripMargin

//    val expected = binaryNode("", "", "")

    streamUtil.verifySql(sql, "")
  }

  @Test
  def testGroupHaving() = {
    val streamUtil = streamTestUtil()
    streamUtil.addTable[(Int, Int, String)]("A", 'a, 'b, 'c)
    streamUtil.tableEnv.registerFunction("udf1", MyUDF)

    val sql =
      s"""
         |select a, c, sum(b)
         |from A
         |group by a, c
         |having sum(b) + 1 > 100
       """.stripMargin

//    val expected = binaryNode("", "", "")

    streamUtil.verifySql(sql, "")
  }

  @Test
  def testUnionAnyType(): Unit = {
    val streamUtil = streamTestUtil()
    val typeInfo = Types.ROW(
      new GenericTypeInfo(classOf[NonPojo]),
      new GenericTypeInfo(classOf[NonPojo]))
    streamUtil.addJavaTable(typeInfo, "A", "a, b")

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "b")
      ),
      term("union all", "a")
    )

    streamUtil.verifyJavaSql("SELECT a FROM A UNION ALL SELECT b FROM A", expected)
  }
}

object MyUDF extends ScalarFunction {
  def eval(src: String): String = {
    s"${if(null == src) 0 else src.length}:$src"
  }
}

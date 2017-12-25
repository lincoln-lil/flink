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

package org.apache.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.table.utils.TableTestBase
import org.junit._

@Ignore
class TestCompile extends TableTestBase {
  val data = List(
    (1, 2, "Hi"),
    (2, 5, "Hello"),
    (4, 5, "Hello "),
    (8, 11, "Hello world"),
    (9, 12, "Hello world!"))

  @Test
  def validate(): Unit = {
    val util = streamTestUtil()
    val tEnv = util.tableEnv
    val t1 = util.addTable[(String, String, String)](
      'nid, 'user_id, 'tableName, 'proctime.proctime)
    val t2 = util.addTable[(String, String, String, String, String)](
      'nid, 'user_id, 'cmd, 'mainse_filter, 'isValid, 'proctime.proctime)

    val transitAggFunction = new TransitAggFunction
    val isValidSets = IsValidSets
    val setsSize = SetsSize
    val collectionSize = SetSize
//    val mainTable = t1
//      .select(
//      "nid, user_id, 'add' as cmd, '0' as mainse_filter, tableName, 'mainFlow' as flow, '0' as " +
//          "flag", 'proctime)

    val table = t2
      //.leftOuterJoin(productFieldsUDTF('contents,'nid) as ('user_id,'cmd,'mainse_filter,'isValid))
      .where("(isValid = '1' && user_id != '2832360567') && (cmd = 'add' || cmd = 'delete')")
      .select(
        'nid, 'user_id, 'cmd, 'mainse_filter, 'isValid, "updateStateFlow" as 'flow,
        "update" as 'tableName, 'proctime)
      .unionAll(
        t1.select(
        "nid, user_id, 'add' as cmd, '0' as mainse_filter, '0' as isValid, 'mainFlow' as flow, " +
          "tableName, proctime"
        )
      )
      .window(Over partitionBy 'user_id orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
      .select('user_id,
              transitAggFunction('nid, 'user_id, 'cmd, 'mainse_filter, 'tableName, 'flow) over 'w as
                'nidSets)
      .select('user_id, 'nidSets, isValidSets('nidSets) as 'setValid)
//      .select('user_id, 'nidSets, isValidSets('nidSets) as 'setValid)
//      .where(" setValid = '1' ") // Alias issue cause NPE
//      .select('user_id, 'nidSets, isValidSets('nidSets) as 'filter_flag)
//      .where('filter_flag) // also a NPE
//      .where(isValidSets('nidSets))
        .where('setValid === "1")
      .select('user_id, collectionSize('nidSets))

    util.verifyTable(table, "")
//    tEnv.compile()

  }
}

object CollectionSize extends ScalarFunction {
  def eval(collection: _root_.java.util.Collection[_]): Int = {
    if (null == collection || collection.isEmpty) {
      0
    } else {
      collection.size()
    }
  }
}

object SetSize extends ScalarFunction {
  def eval(collection: _root_.java.util.HashSet[_]): Int = {
    if (null == collection || collection.isEmpty) {
      0
    } else {
      collection.size()
    }
  }
}

object SetsSize extends ScalarFunction {
  def eval(sets: _root_.java.util.HashSet[_]): String = {
    if (null == sets || sets.isEmpty) {
      return "0"
    }
    return s"${sets.size}";
  }
}

object IsValidSets extends ScalarFunction {
  def eval(sets: _root_.java.util.HashSet[String]): String = {
    if (null == sets || sets.isEmpty) {
      return "0"
    }
    return "1";
  }

/*  def eval(sets: util.HashSet[String]): Boolean = {
    null != sets && sets.isEmpty
  }*/

//  override def isDeterministic = false
}

class TransitAggFunction extends AggregateFunction[_root_.java.util.HashSet[String], TransitAcc] {

  override def createAccumulator() = {
   null
  }

  def accumulate(acc: TransitAcc, nid: String, userId: String, cmd: String, mainseFilter: String,
      tableName: String, flow: String): Unit = {

  }

  override def getValue(accumulator: TransitAcc): _root_.java.util.HashSet[String] = {
    null
  }
}

class TransitAcc{
  var nids: _root_.java.util.HashSet[String] = null
  var tableCount: _root_.java.util.HashSet[String] = null
}

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

package org.apache.flink.table.planner.plan.stream.sql.join.hint;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.stream.sql.join.TestTemporalTable;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/** LookupJoinHintTest tests for lookup join with hints. */
@RunWith(Parameterized.class)
public class LookupJoinHintTest extends TableTestBase {

    private StreamTableTestUtil util;

    private final boolean legacyTableSource;

    public LookupJoinHintTest(boolean legacyTableSource) {
        this.legacyTableSource = legacyTableSource;
    }

    @Parameterized.Parameters(name = "{legacyTableSource}")
    public static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    @Before
    public void before() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1 (\n"
                                + "  a INT,\n"
                                + "  b VARCHAR,\n"
                                + "  c BIGINT,\n"
                                + "  proctime AS PROCTIME()\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'"
                                + ")");

        util.tableEnv().executeSql("CREATE View V1 as select * from T1 where c > 100");

        if (legacyTableSource) {
            TestTemporalTable.createTemporaryTable(util.tableEnv(), "LookupTable", false);
        } else {
            util.addTable(
                    "CREATE TABLE SyncLookupTable (\n"
                            + "  `id` INT,\n"
                            + "  `name` STRING,\n"
                            + "  `age` INT\n"
                            + ") WITH (\n"
                            + "  'connector' = 'values'\n"
                            + ")");
            util.addTable(
                    "CREATE TABLE AsyncLookupTable (\n"
                            + "  `id` INT,\n"
                            + "  `name` STRING,\n"
                            + "  `age` INT\n"
                            + ") WITH (\n"
                            + "  'connector' = 'values',\n"
                            + "  'async' = 'true'\n"
                            + ")");
        }
    }

    @Test
    public void testInvalidJoinHints() {}

    // TODO test  lookup

    @Test
    public void testJoinHintWithTableNameOnly() {

        util.verifyRelPlan(
                "SELECT /*+ LOOKUP('table'='D') */ * FROM T1 AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id");
    }

    @Test
    public void testSimpleJoinHintWithRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstSideAsBuildSide1() {
        // the T1 will be the build side in first join
        String sql =
                "select /*+ %s(T1, T2) */* from T1, T2, T3 where T1.a1 = T2.a2 and T1.b1 = T3.b3";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstSideAsBuildSide2() {
        String sql =
                "select /*+ %s(T1, T2) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiJoinAndSecondThirdSideAsBuildSides1() {
        String sql =
                "select /*+ %s(T2, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T1.b1 = T3.b3";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiJoinAndSecondThirdSideAsBuildSides2() {
        String sql =
                "select /*+ %s(T2, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstThirdSideAsBuildSides() {
        String sql =
                "select /*+ %s(T1, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithUnknownTable() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        "The options of following hints cannot match the name of input tables or views:");
        String sql = "select /*+ %s(T99) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithView() {
        String sql = "select /*+ %s(V4) */* from T1 join V4 on T1.a1 = V4.a4";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithEquiPred() {
        String sql = "select /*+ %s(T1) */* from T1, T2 where T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithEquiPredAndFilter() {
        String sql = "select /*+ %s(T1) */* from T1, T2 where T1.a1 = T2.a2 and T1.a1 > 1";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithEquiAndLocalPred() {
        String sql = "select /*+ %s(T1) */* from T1 inner join T2 on T1.a1 = T2.a2 and T1.a1 < 1";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithEquiAndNonEquiPred() {
        String sql =
                "select /*+ %s(T1) */* from T1 inner join T2 on T1.b1 = T2.b2 and T1.a1 < 1 and T1.a1 < T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithoutJoinPred() {
        String sql = "select /*+ %s(T1) */* from T1, T2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithNonEquiPred() {
        String sql = "select /*+ %s(T1) */* from T1 inner join T2 on T1.a1 > T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithLeftJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 left join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithLeftJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 left join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithRightJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithRightJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithFullJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 full join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithFullJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 full join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiArgsAndLeftSideFirst() {
        // the first arg will be chosen as the build side
        String sql = "select /*+ %s(T1, T2) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithMultiArgsAndRightSideFirst() {
        // the first arg will be chosen as the build side
        String sql = "select /*+ %s(T2, T1) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testMultiJoinHintsWithTheFirstOneIsInvalid() {
        // the first join hint is invalid because it is not equi join except NEST_LOOP
        String sql = "select /*+ %s(T1), NEST_LOOP(T1) */* from T1 join T2 on T1.a1 > T2.a2";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithoutAffectingJoinInView() {
        // the join in V2 will use the planner's default join strategy,
        // and the join between T1 and V2 will use BROADCAST
        util.tableEnv()
                .executeSql("create view V2 as select T1.* from T1 join T2 on T1.a1 = T2.a2");

        String sql = "select /*+ %s(T1)*/T1.* from T1 join V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithoutAffectingJoinInSubQuery() {
        // the join in sub-query will use the planner's default join strategy,
        // and the join outside will use BROADCAST
        String sql =
                "select /*+ %s(T1)*/T1.* from T1 join (select T1.* from T1 join T2 on T1.a1 = T2.a2) V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(sql);
    }

    @Test
    public void testJoinHintWithTableAlias() {
        // the join in sub-query will use the planner's default join strategy,
        // and the join between T1 and alias V2 will use BROADCAST
        String sql =
                "select /*+ %s(V2)*/T1.* from T1 join (select T1.* from T1 join T2 on T1.a1 = T2.a2) V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(sql);
    }
}

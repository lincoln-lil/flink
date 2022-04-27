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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;

import java.util.Collection;

/** TemporalTableCalcResultFuture for async lookup join runner. */
@Internal
public class TemporalTableCalcResultFuture extends TableFunctionResultFuture<RowData> {

    private static final long serialVersionUID = 1L;

    private final FlatMapFunction<RowData, RowData> calc;
    private final TableFunctionResultFuture<RowData> joinConditionResultFuture;
    private final CalcCollectionCollector calcCollector;

    public TemporalTableCalcResultFuture(
            FlatMapFunction<RowData, RowData> calc,
            TableFunctionResultFuture<RowData> joinConditionResultFuture,
            CalcCollectionCollector calcCollector) {
        this.calc = calc;
        this.joinConditionResultFuture = joinConditionResultFuture;
        this.calcCollector = calcCollector;
    }

    @Override
    public void setInput(Object input) {
        joinConditionResultFuture.setInput(input);
        calcCollector.reset();
    }

    @Override
    public void setResultFuture(ResultFuture<?> resultFuture) {
        joinConditionResultFuture.setResultFuture(resultFuture);
    }

    @Override
    public void complete(Collection<RowData> result) {
        if (result == null || result.size() == 0) {
            joinConditionResultFuture.complete(result);
        } else {
            for (RowData row : result) {
                try {
                    calc.flatMap(row, calcCollector);
                } catch (Exception e) {
                    joinConditionResultFuture.completeExceptionally(e);
                }
            }
            joinConditionResultFuture.complete(calcCollector.collection);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        joinConditionResultFuture.close();
        FunctionUtils.closeFunction(calc);
    }
}

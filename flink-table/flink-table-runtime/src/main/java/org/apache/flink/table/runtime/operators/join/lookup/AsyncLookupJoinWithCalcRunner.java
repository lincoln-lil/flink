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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

/** The async join runner with an additional calculate function on the dimension table. */
public class AsyncLookupJoinWithCalcRunner extends AsyncLookupJoinRunner {

    private static final long serialVersionUID = 8758670006385551407L;

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;

    public AsyncLookupJoinWithCalcRunner(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity) {
        super(
                generatedFetcher,
                fetcherConverter,
                generatedResultFuture,
                rightRowSerializer,
                isLeftOuterJoin,
                asyncBufferCapacity);
        this.generatedCalc = generatedCalc;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // try to compile the generated ResultFuture, fail fast if the code is corrupt.
        generatedCalc.compile(getRuntimeContext().getUserCodeClassLoader());
    }

    @Override
    public TableFunctionResultFuture<RowData> createFetcherResultFuture(Configuration parameters)
            throws Exception {
        TableFunctionResultFuture<RowData> joinConditionCollector =
                super.createFetcherResultFuture(parameters);
        FlatMapFunction<RowData, RowData> calc =
                generatedCalc.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(calc, getRuntimeContext());
        FunctionUtils.openFunction(calc, parameters);
        return new TemporalTableCalcResultFuture(
                calc, joinConditionCollector, new CalcCollectionCollector(rightRowSerializer));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}

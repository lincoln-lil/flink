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

package org.apache.flink.table.runtime.operators.join.lookup.retry;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nonnull;

/** Retryable AsyncWaitOperator. */
@Internal
public class RetryableAsyncWaitOperator<IN, OUT> extends AsyncWaitOperator<IN, OUT> {

    private final RetryableAsyncFunction function;

    public RetryableAsyncWaitOperator(
            @Nonnull RetryableAsyncFunction<IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull MailboxExecutor mailboxExecutor) {
        super(
                asyncFunction,
                timeout,
                capacity,
                AsyncDataStream.OutputMode.UNORDERED,
                processingTimeService,
                mailboxExecutor);
        this.function = asyncFunction;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        // retry first
        function.doRetry();
        // then new input
        super.processElement(record);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // retry first
        function.doRetry();
        super.processWatermark(mark);
    }

    @Override
    public void endInput() throws Exception {
        function.prepareFinish();
        super.endInput();
    }

    @Override
    public void finish() throws Exception {
        // do not drain queue in finish because of the stopMode's semantic:
        // DRAIN,   -- stop-with-savepoint --drain or source finish
        // NO_DRAIN -- others: cancel, failover ..
        //        function.prepareFinish();
        super.finish();
    }
}

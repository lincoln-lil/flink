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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * The async join runner with retry to lookup the data in dimension table. The retry info is
 * ephemeral, not persist when snapshot state, so when failover happens, the retry operation will be
 * re-evaluated same as the new input records.
 */
public class RetryableAsyncLookupJoinRunner extends RichRetryableAsyncFunction<RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RetryableAsyncLookupJoinRunner.class);
    private final GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher;
    private final DataStructureConverter<RowData, Object> fetcherConverter;
    private final GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture;
    private final boolean isLeftOuterJoin;
    private final int asyncBufferCapacity;
    private final RetryStrategy retryStrategy;
    private final int delayQueueCapacity;
    private transient AsyncFunction<RowData, Object> fetcher;
    private transient DelayQueue<RetryableJoinedRowResultFuture> delayQueue;
    private transient List<RetryableJoinedRowResultFuture> reusedExpiredList;

    protected final RowDataSerializer rightRowSerializer;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<RetryableJoinedRowResultFuture> resultFutureBuffer;
    /**
     * A Collection contains all ResultFutures in the runner which is used to invoke {@code close()}
     * on every ResultFuture. {@link #resultFutureBuffer} may not contain all the ResultFutures
     * because ResultFutures will be polled from the buffer when processing.
     */
    private transient List<RetryableJoinedRowResultFuture> allResultFutures;

    public RetryableAsyncLookupJoinRunner(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity,
            int delayQueueCapacity,
            RetryStrategy retryStrategy) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
        this.generatedResultFuture = generatedResultFuture;
        this.rightRowSerializer = rightRowSerializer;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.delayQueueCapacity = delayQueueCapacity;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, parameters);

        // try to compile the generated ResultFuture, fail fast if the code is corrupt.
        generatedResultFuture.compile(getRuntimeContext().getUserCodeClassLoader());

        fetcherConverter.open(getRuntimeContext().getUserCodeClassLoader());

        delayQueue = new DelayQueue<>();
        reusedExpiredList = new ArrayList<>();

        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        this.allResultFutures = new ArrayList<>();
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            RetryableJoinedRowResultFuture rf =
                    new RetryableJoinedRowResultFuture(
                            resultFutureBuffer,
                            createFetcherResultFuture(parameters),
                            fetcherConverter,
                            isLeftOuterJoin,
                            rightRowSerializer.getArity(),
                            delayQueue,
                            delayQueueCapacity,
                            retryStrategy);
            // add will throw exception immediately if the queue is full which should never happen
            resultFutureBuffer.add(rf);
            allResultFutures.add(rf);
        }
    }

    public TableFunctionResultFuture<RowData> createFetcherResultFuture(Configuration parameters)
            throws Exception {
        TableFunctionResultFuture<RowData> resultFuture =
                generatedResultFuture.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(resultFuture, getRuntimeContext());
        FunctionUtils.openFunction(resultFuture, parameters);
        return resultFuture;
    }

    @Override
    public void doRetry() throws Exception {
        // See if any delayed task expires, then process expires directly, an explicit downside is
        // this way depends on new coming record or watermark's triggering, otherwise the expires
        // will not be processed timely(like a time window that never ends because no new record
        // arrives to push the watermark to trigger the end of the window), but it's simple indeed.
        // A viable way to save this, maybe bundled execution which will triggered by period
        // watermark based on processing time.
        delayQueue.drainTo(reusedExpiredList);
        if (reusedExpiredList.size() > 0) {
            for (RetryableJoinedRowResultFuture expired : reusedExpiredList) {
                expired.incrementAttempt();
                fetcher.asyncInvoke(expired.getOriginalInput(), expired);
            }
            reusedExpiredList.clear();
        }
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        // then process the new input
        RetryableJoinedRowResultFuture outResultFuture = resultFutureBuffer.take();
        // the input row is copied when object reuse in AsyncWaitOperator
        outResultFuture.reset(input, resultFuture);
        outResultFuture.incrementAttempt();
        // fetcher has copied the input field when object reuse is enabled
        fetcher.asyncInvoke(input, outResultFuture);
    }

    @Override
    public void prepareFinish() throws Exception {
        // force cleanup delay queue, no delayed execution anymore.
        int remainingItems = delayQueue.size();
        if (remainingItems > 0) {
            LOG.info("Still remains {} delayed items to finish.", remainingItems);
            RetryableJoinedRowResultFuture[] remaining =
                    delayQueue.toArray(new RetryableJoinedRowResultFuture[0]);
            for (RetryableJoinedRowResultFuture entry : remaining) {
                // set no-retry anymore
                entry.setForceStopRetry(true);
                fetcher.asyncInvoke(entry.getOriginalInput(), entry);
            }
            LOG.info("All remaining delayed items finish.");
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (allResultFutures != null) {
            for (RetryableJoinedRowResultFuture rf : allResultFutures) {
                rf.close();
            }
        }
    }

    @VisibleForTesting
    public List<RetryableJoinedRowResultFuture> getAllResultFutures() {
        return allResultFutures;
    }

    private static class RetryableJoinedRowResultFuture implements ResultFuture<Object>, Delayed {

        private final BlockingQueue<RetryableJoinedRowResultFuture> resultFutureBuffer;
        private final TableFunctionResultFuture<RowData> joinConditionResultFuture;
        private final DataStructureConverter<RowData, Object> resultConverter;
        private final boolean isLeftOuterJoin;

        private final RetryableJoinedRowResultFuture.DelegateResultFuture delegate;
        private final GenericRowData nullRow;
        private final DelayQueue<RetryableJoinedRowResultFuture> delayQueue;
        private final RetryStrategy retryStrategy;
        private final int delayQueueCapacity;
        private boolean forceStopRetry;

        private long startTimeInMillis;

        private int currentAttempts = 0;

        // not needed by default.
        private long backoffTimeMillis = -1L;
        private RowData leftRow;
        private ResultFuture<RowData> realOutput;

        public RetryableJoinedRowResultFuture(
                BlockingQueue<RetryableJoinedRowResultFuture> resultFutureBuffer,
                TableFunctionResultFuture<RowData> joinConditionResultFuture,
                DataStructureConverter<RowData, Object> resultConverter,
                boolean isLeftOuterJoin,
                int rightArity,
                DelayQueue<RetryableJoinedRowResultFuture> delayQueue,
                int delayQueueCapacity,
                RetryStrategy retryStrategy) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.joinConditionResultFuture = joinConditionResultFuture;
            this.resultConverter = resultConverter;
            this.isLeftOuterJoin = isLeftOuterJoin;
            this.delegate = new RetryableJoinedRowResultFuture.DelegateResultFuture();
            this.nullRow = new GenericRowData(rightArity);
            this.delayQueue = delayQueue;
            this.delayQueueCapacity = delayQueueCapacity;
            this.retryStrategy = retryStrategy;
        }

        public void reset(RowData row, ResultFuture<RowData> realOutput) {
            this.realOutput = realOutput;
            this.leftRow = row;
            this.startTimeInMillis = System.currentTimeMillis();
            joinConditionResultFuture.setInput(row);
            joinConditionResultFuture.setResultFuture(delegate);
            delegate.reset();
        }

        public RowData getOriginalInput() {
            return leftRow;
        }

        @Override
        public long getDelay(@Nonnull TimeUnit unit) {
            return unit.convert(backoffTimeMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(@Nonnull Delayed o) {
            if (o instanceof RetryableJoinedRowResultFuture) {
                RetryableJoinedRowResultFuture oth = (RetryableJoinedRowResultFuture) o;
                if (this.backoffTimeMillis > oth.backoffTimeMillis) {
                    return -1;
                } else if (this.backoffTimeMillis == oth.backoffTimeMillis) {
                    return 0;
                } else {
                    return 1;
                }
            }
            return -1;
        }

        public void setBackoffTimeMillis(long backoffTimeMillis) {
            this.backoffTimeMillis = backoffTimeMillis;
        }

        public int incrementAttempt() {
            return ++currentAttempts;
        }

        public void setForceStopRetry(boolean forceStopRetry) {
            this.forceStopRetry = forceStopRetry;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void complete(Collection<Object> result) {
            // retry triggered by lookup miss
            if (result.isEmpty()) {
                if (!forceStopRetry && retryStrategy.canRetry(currentAttempts)) {
                    long nextBackoffTimeMillis = retryStrategy.getBackoffTimeMillis();
                    if (nextBackoffTimeMillis > -1) {
                        this.setBackoffTimeMillis(nextBackoffTimeMillis);
                        // blocks here avoid delayQueue grows unlimited
                        while (delayQueue.size() >= delayQueueCapacity) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                completeExceptionally(e);
                                return;
                            }
                        }
                        delayQueue.put(this);
                        return;
                    }
                    // else normal process going on.
                }
            }
            Collection<RowData> rowDataCollection;
            if (resultConverter.isIdentityConversion()) {
                rowDataCollection = (Collection) result;
            } else {
                rowDataCollection = new ArrayList<>(result.size());
                for (Object element : result) {
                    rowDataCollection.add(resultConverter.toInternal(element));
                }
            }

            // call condition collector first,
            // the filtered result will be routed to the delegateCollector
            try {
                joinConditionResultFuture.complete(rowDataCollection);
            } catch (Throwable t) {
                // we should catch the exception here to let the framework know
                completeExceptionally(t);
                return;
            }

            Collection<RowData> rightRows = delegate.collection;
            if (rightRows == null || rightRows.isEmpty()) {
                if (isLeftOuterJoin) {
                    RowData outRow = new JoinedRowData(leftRow.getRowKind(), leftRow, nullRow);
                    realOutput.complete(Collections.singleton(outRow));
                } else {
                    realOutput.complete(Collections.emptyList());
                }
            } else {
                List<RowData> outRows = new ArrayList<>();
                for (RowData rightRow : rightRows) {
                    RowData outRow = new JoinedRowData(leftRow.getRowKind(), leftRow, rightRow);
                    outRows.add(outRow);
                }
                realOutput.complete(outRows);
            }
            try {
                // put this collector to the queue to avoid this collector is used
                // again before outRows in the collector is not consumed.
                resultFutureBuffer.put(this);
            } catch (InterruptedException e) {
                completeExceptionally(e);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            realOutput.completeExceptionally(error);
        }

        public void close() throws Exception {
            joinConditionResultFuture.close();
        }

        private final class DelegateResultFuture implements ResultFuture<RowData> {

            private Collection<RowData> collection;

            public void reset() {
                this.collection = null;
            }

            @Override
            public void complete(Collection<RowData> result) {
                this.collection = result;
            }

            @Override
            public void completeExceptionally(Throwable error) {
                RetryableJoinedRowResultFuture.this.completeExceptionally(error);
            }
        }
    }
}

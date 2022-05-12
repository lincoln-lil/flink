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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.apache.flink.streaming.util.retryable.AsyncRetryStrategies.NO_RETRY_STRATEGY;

/**
 * The {@link AsyncWaitOperator} allows to asynchronously process incoming stream records. For that
 * the operator creates an {@link ResultFuture} which is passed to an {@link AsyncFunction}. Within
 * the async function, the user can complete the async collector arbitrarily. Once the async
 * collector has been completed, the result is emitted by the operator's emitter to downstream
 * operators.
 *
 * <p>The operator offers different output modes depending on the chosen {@link OutputMode}. In
 * order to give exactly once processing guarantees, the operator stores all currently in-flight
 * {@link StreamElement} in it's operator state. Upon recovery the recorded set of stream elements
 * is replayed.
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain
 * are opened tail to head. The reason for this is that an opened {@link AsyncWaitOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncWaitOperator<IN, OUT>
        extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
    private static final long serialVersionUID = 1L;

    private static final String DATA_STATE_NAME = "_async_wait_operator_state_";
    private static final String ATTEMPT_STATE_NAME = "_async_wait_operator_attempt_state_";

    /** Capacity of the stream element queue. */
    private final int capacity;

    /** Output mode for this operator. */
    private final AsyncDataStream.OutputMode outputMode;

    /** Timeout for the async collectors. */
    private final long timeout;

    /** AsyncRetryStrategy for the async function. */
    private final AsyncRetryStrategy<OUT> asyncRetryStrategy;

    /** If the retry strategy is not no_retry. */
    private final boolean retryEnabled;

    /** {@link TypeSerializer} for inputs while making snapshots. */
    private transient StreamElementSerializer<IN> inStreamElementSerializer;

    private transient PojoSerializer<AsyncAttemptStatus> attemptSerializer;

    /** Recovered input stream elements. */
    private transient ListState<StreamElement> recoveredStreamElements;

    /** Recovered async attempts. */
    private transient ListState<AsyncAttemptStatus> recoveredAttempts;
    // TODO combine the two list state into one using an optimized serializer to reduce empty
    // attempt info.

    /** Queue, into which to store the currently in-flight stream elements. */
    private transient StreamElementQueue<OUT> queue;

    /**
     * DelayQueue only keep the reference(s) of to-retry items, they will be removed when a retry
     * happens. The real data still stores in the workerQueue. And the max size of the queue depends
     * on the capacity of the work queue.
     */
    private transient DelayQueue<StreamRecordQueueEntry<OUT>> delayQueue;

    /** Mailbox executor used to yield while waiting for buffers to empty. */
    private final transient MailboxExecutor mailboxExecutor;

    private transient TimestampedCollector<OUT> timestampedCollector;

    /** Whether object reuse has been enabled or disabled. */
    private transient boolean isObjectReuseEnabled;

    private transient Optional<Predicate<Collection<OUT>>> retryResultPredicate;

    private transient Optional<Predicate<Throwable>> retryExceptionPredicate;

    private transient List<StreamRecordQueueEntry<OUT>> reuseExpiredList;

    private transient AtomicBoolean delayQueueAvailable;

    public AsyncWaitOperator(
            @Nonnull AsyncFunction<IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            @Nonnull AsyncDataStream.OutputMode outputMode,
            @Nonnull AsyncRetryStrategy<OUT> asyncRetryStrategy,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull MailboxExecutor mailboxExecutor) {
        super(asyncFunction);

        setChainingStrategy(ChainingStrategy.ALWAYS);

        Preconditions.checkArgument(
                capacity > 0, "The number of concurrent async operation should be greater than 0.");
        this.capacity = capacity;

        this.outputMode = Preconditions.checkNotNull(outputMode, "outputMode");

        this.timeout = timeout;

        this.asyncRetryStrategy = asyncRetryStrategy;

        this.retryEnabled = asyncRetryStrategy != NO_RETRY_STRATEGY;

        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);

        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        this.inStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));
        this.attemptSerializer =
                ((PojoTypeInfo) Types.POJO(AsyncAttemptStatus.class))
                        .createPojoSerializer(getExecutionConfig());

        switch (outputMode) {
            case ORDERED:
                queue = new OrderedStreamElementQueue<>(capacity, retryEnabled);
                break;
            case UNORDERED:
                queue = new UnorderedStreamElementQueue<>(capacity, retryEnabled);
                break;
            default:
                throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
        }
        this.retryResultPredicate = asyncRetryStrategy.getRetryPredicate().resultPredicate();
        this.retryExceptionPredicate = asyncRetryStrategy.getRetryPredicate().exceptionPredicate();

        this.timestampedCollector = new TimestampedCollector<>(super.output);
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.isObjectReuseEnabled = getExecutionConfig().isObjectReuseEnabled();
        if (retryEnabled) {
            this.delayQueue = new DelayQueue<>();
            this.delayQueueAvailable = new AtomicBoolean(true);
            this.reuseExpiredList = new ArrayList<>();
        }

        // if exists recoveredAttempts then do retry as needed and check state consistency with
        // recoveredStreamElements.
        if (recoveredStreamElements != null) {
            Iterator<AsyncAttemptStatus> attemptsIterator = null;
            boolean recoverRetry = false;
            if (retryEnabled && recoveredAttempts != null) {
                attemptsIterator = recoveredAttempts.get().iterator();
                recoverRetry = true;
            }
            for (StreamElement element : recoveredStreamElements.get()) {
                if (recoverRetry) {
                    if (null == attemptsIterator || !attemptsIterator.hasNext()) {
                        throw new RuntimeException(
                                "Inconsistent state: stream elements more than attempts state. This should not happen!");
                    }
                    AsyncAttemptStatus asyncAttemptStatus = attemptsIterator.next();
                    if (!asyncAttemptStatus.isUntried()) {
                        processRestoredRetryEntry(element, asyncAttemptStatus);
                    } else {
                        processRestoredElements(element);
                    }
                } else {
                    processRestoredElements(element);
                }
            }

            // check state consistency.
            if (attemptsIterator != null && attemptsIterator.hasNext()) {
                throw new RuntimeException(
                        "Inconsistent state: stream elements less than attempts state. This should not happen!");
            }
            recoveredStreamElements = null;
            recoveredAttempts = null;
        }
    }

    private void processRestoredElements(StreamElement element) throws Exception {
        if (element.isRecord()) {
            processNewInput(element.<IN>asRecord());
        } else if (element.isWatermark()) {
            processWatermark(element.asWatermark());
        } else if (element.isLatencyMarker()) {
            processLatencyMarker(element.asLatencyMarker());
        } else {
            throw new IllegalStateException(
                    "Unknown record type "
                            + element.getClass()
                            + " encountered while opening the operator.");
        }
    }

    private void processRestoredRetryEntry(
            StreamElement restoredElement, AsyncAttemptStatus restoredAttempt) throws Exception {
        // unnecessary to copy the element since recovered from the state
        StreamRecord<IN> element = (StreamRecord<IN>) restoredElement.asRecord();

        final StreamRecordQueueEntry<OUT> entry = (StreamRecordQueueEntry) addToWorkQueue(element);
        entry.setCurrentAttempts(restoredAttempt.getCurrentAttempts());
        entry.setBackoffTimeMillis(restoredAttempt.getBackoffTimeMillis());
        entry.setStartTimeMillis(restoredAttempt.getStartTimeMillis());

        tryOnce(entry);
    }

    private void processNewInput(StreamRecord<IN> record) throws Exception {
        StreamRecord<IN> element;
        // copy the element avoid the element is reused
        if (isObjectReuseEnabled) {
            //noinspection unchecked
            element = (StreamRecord<IN>) inStreamElementSerializer.copy(record);
        } else {
            element = record;
        }

        // add element first to the queue
        final ResultFuture<OUT> entry = addToWorkQueue(element);

        final RetryableResultHandlerDelegator resultHandler =
                new RetryableResultHandlerDelegator(element, entry);

        // register a timeout for the entry if timeout is configured
        if (timeout > 0L) {
            resultHandler.registerTimeout(getProcessingTimeService(), timeout);
        }

        userFunction.asyncInvoke(element.getValue(), resultHandler);
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        // check delay queue if any entry expires, then process retry first.
        checkAndRetryAll();

        // then process new input.
        processNewInput(record);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // check delay queue if any entry expires, then process retry first.
        checkAndRetryAll();

        addToWorkQueue(mark);

        // watermarks are always completed
        // if there is no prior element, we can directly emit them
        // this also avoids watermarks being held back until the next element has been processed
        outputCompletedElement();
    }

    private int checkAndRetryAll() throws Exception {
        if (retryEnabled) {
            // drain delayed queue items
            int expires = delayQueue.drainTo(reuseExpiredList);
            if (expires > 0) {
                assert expires == reuseExpiredList.size();
                for (StreamRecordQueueEntry expired : reuseExpiredList) {
                    tryOnce(expired);
                }
                reuseExpiredList.clear();
            }
            return expires;
        }
        return 0;
    }

    private void tryOnce(StreamRecordQueueEntry expired) throws Exception {
        StreamRecord<IN> element = expired.getInputElement();
        expired.incrementAttempts();

        final RetryableResultHandlerDelegator resultHandler =
                new RetryableResultHandlerDelegator(element, expired);
        if (timeout > 0) {
            long leftTime = calcLeftTimeout(expired);
            resultHandler.registerTimeout(getProcessingTimeService(), leftTime);
        }
        // do not reset timeout
        userFunction.asyncInvoke(element.getValue(), resultHandler);
    }

    private long calcLeftTimeout(StreamRecordQueueEntry entry) {
        long leftTimeout = timeout - (System.currentTimeMillis() - entry.getStartTimeMillis());
        if (leftTimeout > 0) {
            return leftTimeout;
        }
        // no time left, use the backoff time instead
        return entry.getBackoffTimeMillis();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        ListState<StreamElement> partitionedElementsState =
                getOperatorStateBackend()
                        .getListState(
                                new ListStateDescriptor<>(
                                        DATA_STATE_NAME, inStreamElementSerializer));
        partitionedElementsState.clear();

        ListState<AsyncAttemptStatus> partitionedAttemptsState =
                getOperatorStateBackend()
                        .getListState(
                                new ListStateDescriptor<>(ATTEMPT_STATE_NAME, attemptSerializer));
        partitionedAttemptsState.clear();

        Tuple2<List<StreamElement>, List<AsyncAttemptStatus>> values = queue.retryableValues();
        try {
            partitionedElementsState.addAll(values.f0);
            partitionedAttemptsState.addAll(values.f1);
        } catch (Exception e) {
            partitionedElementsState.clear();
            partitionedAttemptsState.clear();

            throw new Exception(
                    "Could not add stream element queue entries to operator state "
                            + "backend of operator "
                            + getOperatorName()
                            + '.',
                    e);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        recoveredStreamElements =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        DATA_STATE_NAME, inStreamElementSerializer));
        recoveredAttempts =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(ATTEMPT_STATE_NAME, attemptSerializer));

        if (recoveredAttempts != null) {
            if (recoveredStreamElements == null) {
                throw new RuntimeException(
                        "Inconsistent empty stream elements state with non empty"
                                + " attempts state. This should not happen!");
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        // we should finish all delayed retry data in fight to be finished.
        finishInFlightDelayedInputs();

        // we should wait here for the data in flight to be finished. the reason is that the
        // timer not in running will be forbidden to fire after this, so that when the async
        // operation is stuck, it results in deadlock due to what the timeout timer is not fired
        waitInFlightInputsFinished();
    }

    /**
     * Add the given stream element to the operator's stream element queue. This operation blocks
     * until the element has been added.
     *
     * <p>Between two insertion attempts, this method yields the execution to the mailbox, such that
     * events as well as asynchronous results can be processed.
     *
     * @param streamElement to add to the operator's queue
     * @throws InterruptedException if the current thread has been interrupted while yielding to
     *     mailbox
     * @return a handle that allows to set the result of the async computation for the given
     *     element.
     */
    private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement) throws Exception {

        Optional<ResultFuture<OUT>> queueEntry;
        while (!(queueEntry = queue.tryPut(streamElement)).isPresent()) {
            if (retryEnabled) {
                if (delayQueue.size() > 0) {

                    // if worker queue full and delay queue not empty, try to check expires and do
                    // retry
                    int expires = checkAndRetryAll();
                    if (expires == 0) {
                        // not ready, wait for a while
                        StreamRecordQueueEntry expired = delayQueue.poll(10, TimeUnit.MILLISECONDS);
                        if (null != expired) {
                            tryOnce(expired);
                        }
                    }
                } else {
                    // we can't yield here because there maybe come new delayed element which is not
                    // completed to collect
                    mailboxExecutor.tryYield();
                }
            } else {
                // here means there must come at least one complete element in some time.
                mailboxExecutor.yield();
            }
        }

        return queueEntry.get();
    }

    private void addToDelayQueue(StreamRecordQueueEntry<OUT> retryEntry) {
        // the capacity of delayQueue is actually bounded by workerQueue
        delayQueue.put(retryEntry);
    }

    private void finishInFlightDelayedInputs() throws Exception {
        if (retryEnabled) {
            // disable new entries add to delay queue
            this.delayQueueAvailable.set(false);
            if (delayQueue.size() > 0) {
                StreamRecordQueueEntry<OUT>[] remaining =
                        delayQueue.toArray(new StreamRecordQueueEntry[0]);
                for (StreamRecordQueueEntry expired : remaining) {
                    tryOnce(expired);
                }
                delayQueue.clear();
            }
        }
    }

    private void waitInFlightInputsFinished() throws InterruptedException {

        while (!queue.isEmpty()) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Outputs one completed element. Watermarks are always completed if it's their turn to be
     * processed.
     *
     * <p>This method will be called from {@link #processWatermark(Watermark)} and from a mail
     * processing the result of an async function call.
     */
    private void outputCompletedElement() {
        if (queue.hasCompletedElements()) {
            // emit only one element to not block the mailbox thread unnecessarily
            queue.emitCompletedElement(timestampedCollector);
            // if there are more completed elements, emit them with subsequent mails
            if (queue.hasCompletedElements()) {
                try {
                    mailboxExecutor.execute(
                            this::outputCompletedElement,
                            "AsyncWaitOperator#outputCompletedElement");
                } catch (RejectedExecutionException mailboxClosedException) {
                    // This exception can only happen if the operator is cancelled which means all
                    // pending records can be safely ignored since they will be processed one more
                    // time after recovery.
                    LOG.debug(
                            "Attempt to complete element is ignored since the mailbox rejected the execution.",
                            mailboxClosedException);
                }
            }
        }
    }

    private class RetryableResultHandlerDelegator implements ResultFuture<OUT> {

        private final ResultHandler resultHandler;

        public RetryableResultHandlerDelegator(
                StreamRecord<IN> inputRecord, ResultFuture<OUT> resultFuture) {
            this.resultHandler = new ResultHandler(inputRecord, resultFuture);
        }

        public void registerTimeout(ProcessingTimeService processingTimeService, long timeout) {
            resultHandler.registerTimeout(processingTimeService, timeout);
        }

        @Override
        public void complete(Collection<OUT> results) {
            if (retryEnabled) {
                // if add to retry queue success, do not complete this task.
                if (!resultHandler.completed.get() && tryAddToRetry(results, null)) {
                    return;
                }
            }
            resultHandler.complete(results);
        }

        private boolean tryAddToRetry(Collection<OUT> results, Throwable error) {
            if (delayQueueAvailable.get() && resultHandler.inputRecord.isRecord()) {
                boolean satisfy = false;
                StreamRecordQueueEntry retryEntry =
                        (StreamRecordQueueEntry<OUT>) resultHandler.resultFuture;
                if (System.currentTimeMillis() - retryEntry.getStartTimeMillis() >= timeout) {
                    // total cost time beyond timeout, give up retry.
                    return false;
                }
                if (null != results && retryResultPredicate.isPresent()) {
                    satisfy = (satisfy || retryResultPredicate.get().test(results));
                }
                if (null != error && retryExceptionPredicate.isPresent()) {
                    satisfy = (satisfy || retryExceptionPredicate.get().test(error));
                }

                if (satisfy) {
                    if (asyncRetryStrategy.canRetry(retryEntry.getCurrentAttempts())) {
                        if (resultHandler.timeoutTimer != null) {
                            // TODO cancel is not necessary here, can be reused
                            // cancel this timer, will register for next retry
                            resultHandler.timeoutTimer.cancel(true);
                        }
                        long nextBackoffTimeMillis = asyncRetryStrategy.getBackoffTimeMillis();
                        // add to delay queue
                        retryEntry.setBackoffTimeMillis(nextBackoffTimeMillis);
                        if (delayQueueAvailable.get()) {
                            mailboxExecutor.submit(
                                    () -> trySubmitRetryInMailbox(results, error, retryEntry),
                                    "try add to delay queue or give up retry");
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private void trySubmitRetryInMailbox(
                Collection<OUT> results, Throwable error, StreamRecordQueueEntry retryEntry) {
            if (delayQueueAvailable.get()) {
                addToDelayQueue(retryEntry);
            } else {
                if (null != results) {
                    resultHandler.complete(results);
                } else {
                    resultHandler.completeExceptionally(error);
                }
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (retryEnabled) {
                // if add to retry queue success, do not fail task.
                if (tryAddToRetry(null, error)) {
                    return;
                }
            }
            resultHandler.completeExceptionally(error);
        }
    }

    /** A handler for the results of a specific input record. */
    private class ResultHandler implements ResultFuture<OUT> {
        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        private ScheduledFuture<?> timeoutTimer;
        /** Record for which this result handler exists. Used only to report errors. */
        private final StreamRecord<IN> inputRecord;
        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        private final ResultFuture<OUT> resultFuture;
        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invokations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        ResultHandler(StreamRecord<IN> inputRecord, ResultFuture<OUT> resultFuture) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<OUT> results) {
            Preconditions.checkNotNull(
                    results, "Results must not be null, use empty collection to emit nothing");

            // already completed (exceptionally or with previous complete call from ill-written
            // AsyncFunction), so
            // ignore additional result
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            processInMailbox(results);
        }

        private void processInMailbox(Collection<OUT> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
        }

        private void processResults(Collection<OUT> results) {
            // Cancel the timer once we've completed the stream record buffer entry. This will
            // remove the registered
            // timer task
            if (timeoutTimer != null) {
                // canceling in mailbox thread avoids
                // https://issues.apache.org/jira/browse/FLINK-13635
                timeoutTimer.cancel(true);
            }

            // update the queue entry with the result
            resultFuture.complete(results);
            // now output all elements from the queue that have been completed (in the correct
            // order)
            outputCompletedElement();
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // already completed, so ignore exception
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // signal failure through task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(
                            new Exception(
                                    "Could not complete the stream element: " + inputRecord + '.',
                                    error));

            // complete with empty result, so that we remove timer and move ahead processing (to
            // leave potentially
            // blocking section in #addToWorkQueue or #waitInFlightInputsFinished)
            processInMailbox(Collections.emptyList());
        }

        private void registerTimeout(ProcessingTimeService processingTimeService, long timeout) {
            final long timeoutTimestamp =
                    timeout + processingTimeService.getCurrentProcessingTime();

            timeoutTimer =
                    processingTimeService.registerTimer(
                            timeoutTimestamp, timestamp -> timerTriggered());
        }

        private void timerTriggered() throws Exception {
            if (!completed.get()) {
                userFunction.timeout(inputRecord.getValue(), this);
            }
        }
    }
}

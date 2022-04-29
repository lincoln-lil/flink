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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * {@link StreamElementQueueEntry} implementation for {@link StreamRecord}. This class also acts as
 * the {@link ResultFuture} implementation which is given to the {@link AsyncFunction}. The async
 * function completes this class with a collection of results.
 *
 * @param <OUT> Type of the asynchronous collection result.
 */
@Internal
public class StreamRecordQueueEntry<OUT> implements StreamElementQueueEntry<OUT>, Delayed {
    @Nonnull private final StreamRecord<?> inputRecord;

    // start from 1, when this entry created, the first attempt 'will' happen (if task failure
    // before function invoked, it will happen after recovery).
    private int currentAttempts = 1;
    // record initial start timestamp which can be used for total cost
    private long startTimeMillis = 0L;

    private long overdueTimeMillis;
    private long backoffTimeMillis = 0L;

    private Collection<OUT> completedElements;

    StreamRecordQueueEntry(StreamRecord<?> inputRecord) {
        this.inputRecord = Preconditions.checkNotNull(inputRecord);
    }

    StreamRecordQueueEntry(StreamRecord<?> inputRecord, long startTimeMillis) {
        this.inputRecord = Preconditions.checkNotNull(inputRecord);
        this.startTimeMillis = startTimeMillis;
    }

    @Override
    public boolean isDone() {
        return completedElements != null;
    }

    @Nonnull
    @Override
    public StreamRecord<?> getInputElement() {
        return inputRecord;
    }

    @Override
    public void emitResult(TimestampedCollector<OUT> output) {
        output.setTimestamp(inputRecord);
        for (OUT r : completedElements) {
            output.collect(r);
        }
    }

    @Override
    public void complete(Collection<OUT> result) {
        this.completedElements = Preconditions.checkNotNull(result);
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public long getBackoffTimeMillis() {
        return backoffTimeMillis;
    }

    public void setBackoffTimeMillis(@Nonnull long backoffTimeMillis) {
        this.backoffTimeMillis = backoffTimeMillis;
        this.overdueTimeMillis = System.currentTimeMillis() + backoffTimeMillis;
    }

    @Override
    public long getDelay(@Nonnull TimeUnit unit) {
        // must calc for current point.
        long diff = overdueTimeMillis - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(@Nonnull Delayed o) {
        if (o instanceof StreamRecordQueueEntry) {
            StreamRecordQueueEntry oth = (StreamRecordQueueEntry) o;
            if (this.backoffTimeMillis > oth.backoffTimeMillis) {
                return 1;
            } else if (this.backoffTimeMillis == oth.backoffTimeMillis) {
                return 0;
            } else {
                return -1;
            }
        }
        // new items are bigger by default.
        return 1;
    }

    public int getCurrentAttempts() {
        return currentAttempts;
    }

    public void setCurrentAttempts(int currentAttempts) {
        this.currentAttempts = currentAttempts;
    }

    public void incrementAttempts() {
        currentAttempts++;
    }

    public static boolean isUntried(int currentAttempts, long backoffTimeMillis) {
        return currentAttempts == 1 && backoffTimeMillis == 0;
    }
}

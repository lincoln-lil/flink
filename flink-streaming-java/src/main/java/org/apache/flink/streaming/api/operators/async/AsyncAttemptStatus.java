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
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;

import java.io.Serializable;

/** AsyncAttemptStatus. */
@Internal
public class AsyncAttemptStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final AsyncAttemptStatus EMPTY = new AsyncAttemptStatus();

    // start from 1, when this entry created, the first attempt 'will' happen (if task failure
    // before function invoked, it will happen after recovery).
    private int currentAttempts = 1;

    // record initial start timestamp which can be used for total cost.
    private long startTimeMillis = 0L;

    private long backoffTimeMillis = 0L;

    public AsyncAttemptStatus() {}

    public AsyncAttemptStatus(int currentAttempts, long startTimeMillis, long backoffTimeMillis) {
        this.currentAttempts = currentAttempts;
        this.startTimeMillis = startTimeMillis;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    public int getCurrentAttempts() {
        return currentAttempts;
    }

    public void setCurrentAttempts(int currentAttempts) {
        this.currentAttempts = currentAttempts;
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

    public void setBackoffTimeMillis(long backoffTimeMillis) {
        this.backoffTimeMillis = backoffTimeMillis;
    }

    public boolean isUntried() {
        return currentAttempts == 1 && backoffTimeMillis == 0;
    }

    public static AsyncAttemptStatus fromStreamRecordQueueEntry(StreamRecordQueueEntry entry) {
        return new AsyncAttemptStatus(
                entry.getCurrentAttempts(),
                entry.getStartTimeMillis(),
                entry.getBackoffTimeMillis());
    }
}

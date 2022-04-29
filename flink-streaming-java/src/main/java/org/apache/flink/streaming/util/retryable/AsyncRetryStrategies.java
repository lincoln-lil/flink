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

package org.apache.flink.streaming.util.retryable;

import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

/** Utility class to create concrete AsyncRetryStrategy. */
public class AsyncRetryStrategies {
    public static final NoRetryStrategy NO_RETRY_STRATEGY = new NoRetryStrategy();

    /** NoRetryStrategy. */
    public static class NoRetryStrategy implements AsyncRetryStrategy {

        private NoRetryStrategy() {}

        @Override
        public boolean canRetry(int currentAttempts) {
            return false;
        }

        @Override
        public long getBackoffTimeMillis() {
            return -1;
        }
    }

    /** FixedDelayRetryStrategy. */
    public static class FixedDelayRetryStrategy<OUT> implements AsyncRetryStrategy<OUT> {
        private final int maxAttempts;
        private final long backoffTimeMillis;
        private final Optional<Predicate<Collection<OUT>>> resultPredicate;
        private final Optional<Predicate<Throwable>> exceptionPredicate;

        private FixedDelayRetryStrategy(
                int maxAttempts,
                long backoffTimeMillis,
                Optional<Predicate<Collection<OUT>>> resultPredicate,
                Optional<Predicate<Throwable>> exceptionPredicate) {
            this.maxAttempts = maxAttempts;
            this.backoffTimeMillis = backoffTimeMillis;
            this.resultPredicate = resultPredicate;
            this.exceptionPredicate = exceptionPredicate;
        }

        @Override
        public boolean canRetry(int currentAttempts) {
            return currentAttempts <= maxAttempts;
        }

        @Override
        public Optional<Predicate<Collection<OUT>>> resultPredicate() {
            return this.resultPredicate;
        }

        @Override
        public Optional<Predicate<Throwable>> exceptionPredicate() {
            return this.exceptionPredicate;
        }

        @Override
        public long getBackoffTimeMillis() {
            return backoffTimeMillis;
        }
    }

    /** FixedDelayRetryStrategyBuilder for building a FixedDelayRetryStrategy. */
    public static class FixedDelayRetryStrategyBuilder<OUT> {
        private int maxAttempts;
        private long backoffTimeMillis;
        private Optional<Predicate<Collection<OUT>>> resultPredicate = Optional.empty();
        private Optional<Predicate<Throwable>> exceptionPredicate = Optional.empty();

        public FixedDelayRetryStrategyBuilder(int maxAttempts, long backoffTimeMillis) {
            Preconditions.checkArgument(maxAttempts > 0, "");
            Preconditions.checkArgument(backoffTimeMillis > 0, "");
            this.maxAttempts = maxAttempts;
            this.backoffTimeMillis = backoffTimeMillis;
        }

        public FixedDelayRetryStrategyBuilder ifResult(
                @Nonnull Predicate<Collection<OUT>> resultRetryPredicate) {
            this.resultPredicate = Optional.of(resultRetryPredicate);
            return this;
        }

        public FixedDelayRetryStrategyBuilder ifException(
                @Nonnull Predicate<Throwable> exceptionRetryPredicate) {
            this.exceptionPredicate = Optional.of(exceptionRetryPredicate);
            return this;
        }

        public FixedDelayRetryStrategy build() {
            return new FixedDelayRetryStrategy(
                    maxAttempts, backoffTimeMillis, resultPredicate, exceptionPredicate);
        }
    }
}

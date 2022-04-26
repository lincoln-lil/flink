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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;

import java.io.Serializable;
import java.util.Objects;

/**
 * This class defines methods to generate RetryStrategyConfigurations. These configurations are used
 * to create RetryStrategies for lookup join at runtime.
 */
@PublicEvolving
public class RetryStrategies {

    public static NoRetryStrategyConfiguration noRetry() {
        new RestartStrategies.NoRestartStrategyConfiguration();
        return new NoRetryStrategyConfiguration();
    }

    public static FixedDelayRetryStrategyConfiguration fixedDelayRetry(
            int maxAttempts, long delayIntervalMs) {
        return new FixedDelayRetryStrategyConfiguration(maxAttempts, delayIntervalMs);
    }

    public static AdaptiveDelayRetryStrategyConfiguration adaptiveDelayRetry(
            int maxAttempts, long timeoutMs) {
        return new AdaptiveDelayRetryStrategyConfiguration(maxAttempts, timeoutMs);
    }

    /** Abstract configuration class for retry strategies. */
    public abstract static class RetryStrategyConfiguration implements Serializable {
        private static final long serialVersionUID = 1L;

        private RetryStrategyConfiguration() {}

        public abstract String getDescription();

        @Override
        public String toString() {
            return getDescription();
        }
    }

    /** Configuration representing no retry strategy. */
    public static final class NoRetryStrategyConfiguration extends RetryStrategyConfiguration {
        private static final long serialVersionUID = 1L;

        @Override
        public String getDescription() {
            return "Retry deactivated.";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof NoRetryStrategyConfiguration;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }
    }

    /** Configuration representing fixed-delay retry strategy. */
    public static final class FixedDelayRetryStrategyConfiguration
            extends RetryStrategyConfiguration {
        private static final long serialVersionUID = 1L;
        private final int maxAttempts;
        private final long delayIntervalMs;

        public FixedDelayRetryStrategyConfiguration(int maxAttempts, long delayIntervalMs) {
            this.maxAttempts = maxAttempts;
            this.delayIntervalMs = delayIntervalMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof FixedDelayRetryStrategyConfiguration) {
                FixedDelayRetryStrategyConfiguration oth = (FixedDelayRetryStrategyConfiguration) o;
                return this.delayIntervalMs == oth.delayIntervalMs
                        && this.maxAttempts == oth.maxAttempts;
            }
            return false;
        }

        @Override
        public String getDescription() {
            return String.format(
                    "Retry with fixed delay (%d) ms. #%d max attempts.",
                    delayIntervalMs, maxAttempts);
        }
    }

    /** Configuration representing adaptive-delay retry strategy. */
    public static final class AdaptiveDelayRetryStrategyConfiguration
            extends RetryStrategyConfiguration {
        private static final long serialVersionUID = 1L;
        private final int maxAttempts;
        private final long timeoutMs;

        public AdaptiveDelayRetryStrategyConfiguration(int maxAttempts, long timeoutMs) {
            this.maxAttempts = maxAttempts;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof AdaptiveDelayRetryStrategyConfiguration) {
                AdaptiveDelayRetryStrategyConfiguration oth =
                        (AdaptiveDelayRetryStrategyConfiguration) o;
                return this.timeoutMs == oth.timeoutMs && this.maxAttempts == oth.maxAttempts;
            }
            return false;
        }

        @Override
        public String getDescription() {
            return String.format(
                    "Retry with adaptive delay, total timeout (%d) ms. #%d max attempts.",
                    timeoutMs, maxAttempts);
        }
    }
}

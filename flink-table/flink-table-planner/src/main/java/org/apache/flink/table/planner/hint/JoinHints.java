/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.calcite.rel.hint.RelHint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for Join hints. */
public class JoinHints {
    // ~ Join hint name
    public static final String HINT_LOOKUP_MISS_RETRY = "LOOKUP_MISS_RETRY";

    public static boolean isJoinHintSupported(RelHint hint) {
        JoinStrategy joinStrategy = JoinStrategy.getJoinStrategy(hint.hintName);
        switch (joinStrategy) {
            case LOOKUP_MISS_RETRY:
                return true;
            default:
                return false;
        }
    }

    private static final Map<String, JoinStrategy> joinHintAlias = new HashMap<>();

    /** Currently available join strategy. */
    public enum JoinStrategy {
        LOOKUP_MISS_RETRY(HINT_LOOKUP_MISS_RETRY);

        private List<String> aliasName = new ArrayList<>();

        JoinStrategy(String... alias) {
            for (String name : alias) {
                aliasName.add(name);
                joinHintAlias.put(name, this);
            }
        }

        public List<String> getAlias() {
            return aliasName;
        }

        public static JoinStrategy getJoinStrategy(String hintName) {
            return joinHintAlias.get(hintName.toUpperCase());
        }
    }

    public static boolean validHintOptions(String hintName, List<String> options) {
        JoinStrategy strategy = JoinStrategy.getJoinStrategy(hintName);
        switch (strategy) {
            case LOOKUP_MISS_RETRY:
                return options.size() > 0;
        }
        return false;
    }

    public static List<String> mergeJoinOptions(
            JoinStrategy joinStrategy, List<RelHint> joinHints) {
        List<String> hintAlias = joinStrategy.getAlias();
        return joinHints.stream()
                .filter(h -> hintAlias.contains(h.hintName.toUpperCase()))
                .flatMap(h -> h.listOptions.stream().map(String::toUpperCase))
                .collect(Collectors.toList());
    }
}

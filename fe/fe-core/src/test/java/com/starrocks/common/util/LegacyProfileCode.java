// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.starrocks.common.Pair;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

// The merge and the formatters of RuntimeProfile as they were before they were rewritten for fewer allocations,
// kept as the reference that the rewrite must reproduce. The code is the old code with these changes: private
// fields are read through getters (removeRedundantMinMaxMetrics iterates a copy of the counter names), the min and
// max of Counter are read through hasMinValue/getMinValue, and Counter.mergeIsomorphicCounters is the current one,
// whose arithmetic is the old one.
final class LegacyProfileCode {
    private static final Logger LOG = LogManager.getLogger(LegacyProfileCode.class);

    private LegacyProfileCode() {
    }

    private static String printCounter(long value, TUnit type) {
        StringBuilder builder = new StringBuilder();
        try (Formatter fmt = new Formatter()) {
            if (type == TUnit.UNIT || type == TUnit.UNIT_PER_SECOND) {
                Pair<Double, String> pair = DebugUtil.getUint(value);
                if (pair.second.isEmpty()) {
                    builder.append(value);
                } else {
                    builder.append(fmt.format("%.3f", pair.first)).append(pair.second)
                            .append(" (").append(value).append(")");
                }
                if (type == TUnit.UNIT_PER_SECOND) {
                    builder.append(" /sec");
                }
            } else if (type == TUnit.BYTES || type == TUnit.BYTES_PER_SECOND) {
                Pair<Double, String> pair = DebugUtil.getByteUint(value);
                builder.append(fmt.format("%.3f", pair.first)).append(" ").append(pair.second);
                if (type == TUnit.BYTES_PER_SECOND) {
                    builder.append("/sec");
                }
            } else if (type == TUnit.TIME_NS) {
                if (value >= DebugUtil.BILLION) {
                    // If the time is over a second, print it up to ms.
                    value /= DebugUtil.MILLION;
                    DebugUtil.printTimeMs(value, builder);
                } else if (value >= DebugUtil.MILLION) {
                    // if the time is over a ms, print it up to microsecond in the unit of ms.
                    value /= DebugUtil.THOUSAND;
                    long remain = value % DebugUtil.THOUSAND;
                    if (remain == 0) {
                        builder.append(value / DebugUtil.THOUSAND).append("ms");
                    } else {
                        builder.append(value / DebugUtil.THOUSAND).append(".")
                                .append(String.format("%03d", remain)).append("ms");
                    }
                } else if (value >= DebugUtil.THOUSAND) {
                    // if the time is over a microsecond, print it using unit microsecond
                    long remain = value % DebugUtil.THOUSAND;
                    if (remain == 0) {
                        builder.append(value / DebugUtil.THOUSAND).append("us");
                    } else {
                        builder.append(value / DebugUtil.THOUSAND).append(".")
                                .append(String.format("%03d", remain)).append("us");
                    }
                } else {
                    builder.append(value).append("ns");
                }
            } else if (type == TUnit.DOUBLE_VALUE) {
                builder.append(fmt.format("%.3f", (double) value));
            } else if (type == TUnit.NONE) {
                // Do nothing
            } else {
                Preconditions.checkState(false, "type=" + type);
            }
        }
        return builder.toString();
    }


    public static RuntimeProfile mergeIsomorphicProfiles(List<RuntimeProfile> profiles,
                                                         Set<String> excludedInfoStrings) {
        if (CollectionUtils.isEmpty(profiles)) {
            return null;
        }

        RuntimeProfile mergedProfile = new RuntimeProfile(profiles.get(0).getName());

        for (RuntimeProfile runtimeProfile : profiles) {
            mergedProfile.copyAllInfoStringsFrom(runtimeProfile, excludedInfoStrings);
        }

        // Find all counters, although these profiles are expected to be isomorphic,
        // some counters are only attached to one of them
        List<Map<String, Pair<TUnit, String>>> allLevelCounters = Lists.newArrayList();
        for (RuntimeProfile profile : profiles) {
            // Level order traverse starts with root
            Queue<String> nameQueue = Lists.newLinkedList();
            nameQueue.offer(RuntimeProfile.ROOT_COUNTER);
            int levelIdx = -1;
            while (!nameQueue.isEmpty()) {
                levelIdx++;
                List<String> currentNames = Lists.newArrayList(nameQueue);
                nameQueue.clear();
                for (String name : currentNames) {
                    Set<String> childNames = profile.getChildCounterMap().get(name);
                    if (childNames != null) {
                        for (String childName : childNames) {
                            nameQueue.offer(childName);
                        }
                    }

                    if (Objects.equals(RuntimeProfile.ROOT_COUNTER, name)) {
                        continue;
                    }
                    Pair<Counter, String> pair = profile.getCounterPair(name);
                    if (pair == null) {
                        LOG.warn("missing counter, profileName={}, counterName={}", profile.getName(), name);
                        continue;
                    }
                    Counter counter = pair.first;
                    String parentName = pair.second;

                    while (allLevelCounters.size() <= levelIdx) {
                        allLevelCounters.add(Maps.newHashMap());
                    }

                    Map<String, Pair<TUnit, String>> levelCounters = allLevelCounters.get(levelIdx);
                    if (!levelCounters.containsKey(name)) {
                        levelCounters.put(name, Pair.create(counter.getType(), parentName));
                        continue;
                    }
                    TUnit existType = levelCounters.get(name).first;
                    if (!existType.equals(counter.getType())) {
                        LOG.warn(
                                "find non-isomorphic counter, profileName={}, counterName={}, existType={}, " +
                                        "anotherType={}",
                                mergedProfile.getName(), name, existType.name(), counter.getType().name());
                        continue;
                    }
                }
            }
        }

        List<Triple<TUnit, String, String>> levelOrderedCounters = Lists.newArrayList();
        for (Map<String, Pair<TUnit, String>> levelCounters : allLevelCounters) {
            for (Map.Entry<String, Pair<TUnit, String>> entry : levelCounters.entrySet()) {
                String name = entry.getKey();
                TUnit type = entry.getValue().first;
                String parentName = entry.getValue().second;
                levelOrderedCounters.add(Triple.of(type, name, parentName));
            }
        }

        for (Triple<TUnit, String, String> triple : levelOrderedCounters) {
            TUnit type = triple.getLeft();
            String name = triple.getMiddle();
            String parentName = triple.getRight();

            // We don't need to calculate sum or average of counter's extra info (min value and max value) created by be
            if (name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)
                    || name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
                continue;
            }

            List<Counter> counters = Lists.newArrayList();
            long minValue = Long.MAX_VALUE;
            long maxValue = Long.MIN_VALUE;
            boolean alreadyMerged = false;
            boolean skipMerge = false;
            Counter skipMergeCounter = null;
            TCounterStrategy strategy = null;
            for (RuntimeProfile profile : profiles) {
                Counter counter = profile.getCounter(name);

                // Allow some counters which only attach to one of the isomorphic profiles
                // E.g. A bunch of ExchangeSinkOperators may share one SinkBuffer, so the metrics
                // of SinkBuffer only attach to the first ExchangeSinkOperator's profile
                if (counter == null) {
                    continue;
                }
                if (!type.equals(counter.getType())) {
                    LOG.warn(
                            "find non-isomorphic counter, profileName={}, counterName={}, existType={}, anotherType={}",
                            mergedProfile.getName(), name, type.name(), counter.getType().name());
                    continue;
                }
                strategy = counter.getStrategy();
                if (counter.isSkipMerge()) {
                    skipMerge = true;
                    skipMergeCounter = counter;
                    break;
                }

                if (!counter.isSkipMinMax()) {
                    if (counter.hasMinValue()) {
                        alreadyMerged = true;
                        minValue = Math.min(counter.getMinValue(), minValue);
                    } else {
                        // TODO: keep compatible with older version backend, can be removed in next version
                        Counter minCounter = profile.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MIN + name);
                        if (minCounter != null) {
                            alreadyMerged = true;
                            if (minCounter.getValue() < minValue) {
                                minValue = minCounter.getValue();
                            }
                        }
                    }
                    if (counter.hasMaxValue()) {
                        alreadyMerged = true;
                        maxValue = Math.max(counter.getMaxValue(), maxValue);
                    } else {
                        // TODO: keep compatible with older version backend, can be removed in next version
                        Counter maxCounter = profile.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + name);
                        if (maxCounter != null) {
                            alreadyMerged = true;
                            if (maxCounter.getValue() > maxValue) {
                                maxValue = maxCounter.getValue();
                            }
                        }
                    }

                }

                counters.add(counter);
            }
            Counter mergedCounter;
            if (!Objects.equals(RuntimeProfile.ROOT_COUNTER, parentName) && mergedProfile.getCounter(parentName) != null) {
                mergedCounter = mergedProfile.addCounter(name, type, strategy, parentName);
            } else {
                if (!Objects.equals(RuntimeProfile.ROOT_COUNTER, parentName)) {
                    LOG.warn("missing parent counter, profileName={}, counterName={}, parentCounterName={}",
                            mergedProfile.getName(), name, parentName);
                }
                mergedCounter = mergedProfile.addCounter(name, type, strategy);
            }
            if (skipMerge) {
                mergedCounter.setValue(skipMergeCounter.getValue());
            } else {
                Counter.MergedInfo mergedInfo = Counter.mergeIsomorphicCounters(counters);
                final long mergedValue = mergedInfo.mergedValue;
                if (!alreadyMerged) {
                    minValue = mergedInfo.minValue;
                    maxValue = mergedInfo.maxValue;
                }
                mergedCounter.setValue(mergedValue);

                if (!mergedCounter.isSkipMinMax()) {
                    Counter minCounter =
                            mergedProfile.addCounter(RuntimeProfile.MERGED_INFO_PREFIX_MIN + name, type,
                                    mergedCounter.getStrategy(),
                                    name);
                    Counter maxCounter =
                            mergedProfile.addCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + name, type,
                                    mergedCounter.getStrategy(),
                                    name);
                    if (minValue != Integer.MAX_VALUE) {
                        mergedCounter.setMinValue(minValue);
                        minCounter.setValue(minValue);
                    }
                    if (maxValue != Integer.MIN_VALUE) {
                        mergedCounter.setMaxValue(maxValue);
                        maxCounter.setValue(maxValue);
                    }
                }
            }

        }

        // merge children
        int maxChildSize = 0;
        RuntimeProfile profileWithFullChild = null;
        for (RuntimeProfile profile : profiles) {
            if (profile.getChildList().size() > maxChildSize) {
                maxChildSize = profile.getChildList().size();
                profileWithFullChild = profile;
            }
        }
        if (profileWithFullChild != null) {
            boolean identical = true;
            for (int i = 0; i < maxChildSize; i++) {
                Pair<RuntimeProfile, Boolean> prototypeKv = profileWithFullChild.getChildList().get(i);
                String childName = prototypeKv.first.getName();
                List<RuntimeProfile> subProfiles = Lists.newArrayList();
                for (RuntimeProfile profile : profiles) {
                    RuntimeProfile child = profile.getChild(childName);
                    if (child == null) {
                        identical = false;
                        LOG.debug("find non-isomorphic children, profileName={}, requiredChildName={}",
                                profile.getName(), childName);
                        continue;
                    }
                    subProfiles.add(child);
                }
                RuntimeProfile mergedChild = mergeIsomorphicProfiles(subProfiles, excludedInfoStrings);
                mergedProfile.addChild(mergedChild);
            }
            if (!identical) {
                mergedProfile.addInfoString("NotIdentical", "");
            }
        }

        return mergedProfile;
    }


    public static void removeRedundantMinMaxMetrics(RuntimeProfile profile) {
        for (String name : profile.getCounterMap().keySet()) {
            Counter counter = profile.getCounter(name);
            Counter minCounter = profile.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MIN + name);
            Counter maxCounter = profile.getCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + name);
            if (counter == null || minCounter == null || maxCounter == null) {
                continue;
            }

            if (Objects.equals(minCounter.getValue(), maxCounter.getValue()) &&
                    Objects.equals(minCounter.getValue(), counter.getValue())) {
                profile.removeCounter(RuntimeProfile.MERGED_INFO_PREFIX_MIN + name);
                profile.removeCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + name);
            }
        }

        profile.getChildList().forEach(pair -> removeRedundantMinMaxMetrics(pair.first));
    }


    static final class DefaultProfileFormatter {

        private final StringBuilder builder;

        DefaultProfileFormatter(StringBuilder builder) {
            this.builder = builder;
        }

        DefaultProfileFormatter() {
            this.builder = new StringBuilder();
        }

        public String format(RuntimeProfile profile, String prefix) {
            this.doFormat(profile, prefix);
            return builder.toString();
        }

        public String format(RuntimeProfile profile) {
            this.doFormat(profile, "");
            return builder.toString();
        }

        private void doFormat(RuntimeProfile profile, String prefix) {
            Counter totalTimeCounter = profile.getCounterMap().get(RuntimeProfile.TOTAL_TIME_COUNTER);
            Preconditions.checkState(totalTimeCounter != null);
            // 1. profile name
            builder.append(prefix).append(profile.getName()).append(":");
            // total time
            if (totalTimeCounter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    builder.append("(Active: ")
                            .append(printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType()));
                    if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                        // TotalTime in nanosecond concated if it's larger than 1000ns
                        builder.append("[").append(totalTimeCounter.getValue()).append("ns]");
                    }
                    builder.append(", % non-child: ").append(fmt.format("%.2f", profile.getLocalTimePercent()))
                            .append("%)");
                }
            }
            builder.append("\n");

            // 2. info String
            for (Map.Entry<String, String> infoPair : profile.getInfoStrings().entrySet()) {
                String key = infoPair.getKey();
                String value = infoPair.getValue();
                builder.append(prefix).append("   - ").append(key);
                if (StringUtils.isNotBlank(value)) {
                    builder.append(": ").append(profile.getInfoString(key));
                }
                builder.append("\n");
            }

            // 3. counters
            printChildCounters(profile, prefix, RuntimeProfile.ROOT_COUNTER);

            // 4. children
            for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
                boolean indent = childPair.second;
                RuntimeProfile childProfile = childPair.first;
                doFormat(childProfile, prefix + (indent ? "  " : ""));
            }
        }

        private void printChildCounters(RuntimeProfile profile, String prefix, String counterName) {
            Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();
            if (childCounterMap.get(counterName) == null) {
                return;
            }
            List<String> childNames = Lists.newArrayList(childCounterMap.get(counterName));
            childNames.sort(String::compareTo);

            // Keep MIN/MAX metrics head of other child counters
            List<String> minMaxChildNames = Lists.newArrayListWithCapacity(2);
            List<String> otherChildNames = Lists.newArrayListWithCapacity(childNames.size());
            for (String childName : childNames) {
                if (childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)
                        || childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
                    minMaxChildNames.add(childName);
                } else {
                    otherChildNames.add(childName);
                }
            }
            List<String> reorderedChildNames = Lists.newArrayListWithCapacity(childNames.size());
            reorderedChildNames.addAll(minMaxChildNames);
            reorderedChildNames.addAll(otherChildNames);

            Map<String, Counter> counterMap1 = profile.getCounterMap();
            for (String childName : reorderedChildNames) {
                Counter childCounter = counterMap1.get(childName);
                Preconditions.checkState(childCounter != null);
                builder.append(prefix).append("   - ").append(childName).append(": ")
                        .append(printCounter(childCounter.getValue(), childCounter.getType())).append("\n");
                this.printChildCounters(profile, prefix + "  ", childName);
            }
        }
    }


    static final class JsonProfileFormatter {
        private final JsonObject builder;

        JsonProfileFormatter() {
            this.builder = new JsonObject();
        }

        public String format(RuntimeProfile profile, String prefix) {
            //may parse prefix to json
            return this.format(profile);
        }

        public String format(RuntimeProfile profile) {
            addRuntimeProfile(profile, builder);
            Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
            return gson.toJson(builder);
        }

        public void addRuntimeProfile(RuntimeProfile profile, JsonObject jsonObject) {
            Counter totalTimeCounter = profile.getCounterMap().get(RuntimeProfile.TOTAL_TIME_COUNTER);
            Preconditions.checkState(totalTimeCounter != null);
            // 1. profile name
            JsonObject innerJsonObject = new JsonObject();
            jsonObject.add(profile.getName(), innerJsonObject);
            // total time
            if (totalTimeCounter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    innerJsonObject.addProperty("Active",
                            printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType()));
                    if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                        innerJsonObject.addProperty("Active_ns", totalTimeCounter.getValue());
                    }
                    innerJsonObject.addProperty("non-child", fmt.format("%.2f", profile.getLocalTimePercent()) + "%");
                }
            }
            if (profile.getInfoStrings().size() > 0) {
                // 2. info String
                for (Map.Entry<String, String> infoPair : profile.getInfoStrings().entrySet()) {
                    String key = infoPair.getKey();
                    innerJsonObject.addProperty(key, profile.getInfoString(key));
                }
            }

            // 3. counters
            addChildCounters(profile, RuntimeProfile.ROOT_COUNTER, innerJsonObject);

            // 4. children
            if (profile.getChildList().size() > 0) {
                for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
                    RuntimeProfile childProfile = childPair.first;
                    addRuntimeProfile(childProfile, innerJsonObject);
                }
            }
        }

        private void addChildCounters(RuntimeProfile profile, String counterName, JsonObject childJsonObject) {
            Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();

            if (childCounterMap.get(counterName) == null) {
                return;
            }
            List<String> childNames = Lists.newArrayList(childCounterMap.get(counterName));
            childNames.sort(String::compareTo);

            // Keep MIN/MAX metrics head of other child counters
            List<String> minMaxChildNames = Lists.newArrayListWithCapacity(2);
            List<String> otherChildNames = Lists.newArrayListWithCapacity(childNames.size());
            for (String childName : childNames) {
                if (childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)
                        || childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
                    minMaxChildNames.add(childName);
                } else {
                    otherChildNames.add(childName);
                }
            }
            List<String> reorderedChildNames = Lists.newArrayListWithCapacity(childNames.size());
            reorderedChildNames.addAll(minMaxChildNames);
            reorderedChildNames.addAll(otherChildNames);

            for (String childName : reorderedChildNames) {
                Counter childCounter = profile.getCounterMap().get(childName);
                Preconditions.checkState(childCounter != null);
                childJsonObject.addProperty(childName,
                        printCounter(childCounter.getValue(), childCounter.getType()));
                this.addChildCounters(profile, childName, childJsonObject);
            }
        }
    }
}

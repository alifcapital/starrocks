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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/RuntimeProfile.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * It is accessed by two kinds of thread, one is to create this RuntimeProfile
 * , named 'query thread', the other is to call
 * {@link com.starrocks.common.proc.CurrentQueryInfoProvider}.
 */
public class RuntimeProfile {

    private static final Logger LOG = LogManager.getLogger(RuntimeProfile.class);
    public static final String ROOT_COUNTER = "";
    public static final String TOTAL_TIME_COUNTER = "TotalTime";
    public static final String MERGED_INFO_PREFIX_MIN = "__MIN_OF_";
    public static final String MERGED_INFO_PREFIX_MAX = "__MAX_OF_";

    // The merge, removeRedundantMinMaxMetrics and getMaxCounter look up the min and max counter of every counter of
    // every profile. Most counter names are the names of the operator metrics of the backend, so the two names are
    // built once per counter name and not once per lookup. Some names carry ids (the runtime filters), and the
    // cap keeps them from growing the map without end; the names of min and max counters themselves are not kept.
    private static final int MIN_MAX_NAMES_CAPACITY = 16384;
    private static final ConcurrentHashMap<String, String[]> MIN_MAX_NAMES = new ConcurrentHashMap<>();

    private static String[] minMaxNames(String name) {
        String[] names = MIN_MAX_NAMES.get(name);
        if (names == null) {
            names = new String[] {MERGED_INFO_PREFIX_MIN + name, MERGED_INFO_PREFIX_MAX + name};
            if (!name.startsWith(MERGED_INFO_PREFIX_MIN) && !name.startsWith(MERGED_INFO_PREFIX_MAX)
                    && MIN_MAX_NAMES.size() < MIN_MAX_NAMES_CAPACITY) {
                String[] existing = MIN_MAX_NAMES.putIfAbsent(name, names);
                if (existing != null) {
                    names = existing;
                }
            }
        }
        return names;
    }

    static String minCounterName(String name) {
        return minMaxNames(name)[0];
    }

    static String maxCounterName(String name) {
        return minMaxNames(name)[1];
    }

    private final Counter counterTotalTime;

    private final Map<String, String> infoStrings = Collections.synchronizedMap(Maps.newLinkedHashMap());

    // These will be hold by other thread.
    private final Map<String, Pair<Counter, String>> counterMap = Maps.newConcurrentMap();
    private final Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();

    private final Map<String, Set<String>> childCounterMap = Maps.newConcurrentMap();
    private final List<Pair<RuntimeProfile, Boolean>> childList = Lists.newCopyOnWriteArrayList();

    private String name;
    private double localTimePercent;
    // The version of this profile. It is used to prevent updating this profile
    // from an old one.
    private volatile long version = 0;

    public RuntimeProfile(String name) {
        this();
        this.name = name;
    }

    public RuntimeProfile() {
        this.counterTotalTime = new Counter(TUnit.TIME_NS, null, 0);
        this.localTimePercent = 0;
        this.counterMap.put(TOTAL_TIME_COUNTER, Pair.create(counterTotalTime, ROOT_COUNTER));
    }

    public Counter getCounterTotalTime() {
        return counterTotalTime;
    }

    public Map<String, Counter> getCounterMap() {
        return counterMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().first));
    }

    public List<Pair<RuntimeProfile, Boolean>> getChildList() {
        return childList;
    }

    public Map<String, RuntimeProfile> getChildMap() {
        return childMap;
    }

    public RuntimeProfile getChild(String childName) {
        return childMap.get(childName);
    }

    public void removeAllChildren() {
        childList.clear();
        childMap.clear();
    }

    public Counter addCounter(String name, TUnit type, TCounterStrategy strategy) {
        return addCounter(name, type, strategy, ROOT_COUNTER);
    }

    @VisibleForTesting
    public void addCounter(String name, String parentName, Counter counter) {
        this.counterMap.put(name, Pair.create(counter, parentName));
        if (!childCounterMap.containsKey(parentName)) {
            childCounterMap.putIfAbsent(parentName, Sets.newConcurrentHashSet());
        }
        Set<String> childNames = childCounterMap.get(parentName);
        childNames.add(name);
    }

    public Counter addCounter(String name, TUnit type, TCounterStrategy strategy, String parentName) {
        if (strategy == null) {
            strategy = Counter.createStrategy(type);
        }
        Pair<Counter, String> pair = this.counterMap.get(name);
        if (pair != null) {
            return pair.first;
        } else {
            Preconditions.checkState(parentName.equals(ROOT_COUNTER) || this.counterMap.containsKey(parentName),
                    "dangling counter %s->%s", parentName, name);
            Counter newCounter = new Counter(type, strategy, 0);
            this.counterMap.put(name, Pair.create(newCounter, parentName));

            if (!childCounterMap.containsKey(parentName)) {
                childCounterMap.putIfAbsent(parentName, Sets.newConcurrentHashSet());
            }
            Set<String> childNames = childCounterMap.get(parentName);
            childNames.add(name);
            return newCounter;
        }
    }

    public void removeCounter(String name) {
        if (!counterMap.containsKey(name)) {
            return;
        }

        // Remove from its parent sub sets
        Pair<Counter, String> pair = counterMap.get(name);
        if (pair == null) {
            return;
        }
        String parentName = pair.second;
        if (childCounterMap.containsKey(parentName)) {
            Set<String> childNames = childCounterMap.get(parentName);
            childNames.remove(name);
        }

        // Remove child counter recursively
        Queue<String> nameQueue = Lists.newLinkedList();
        nameQueue.offer(name);
        while (!nameQueue.isEmpty()) {
            String topName = nameQueue.poll();
            Set<String> childNames = childCounterMap.get(topName);
            if (childNames != null) {
                for (String childName : childNames) {
                    nameQueue.offer(childName);
                }
            }
            counterMap.remove(topName);
            childCounterMap.remove(topName);
        }
    }

    public Counter getCounter(String name) {
        Pair<Counter, String> pair = counterMap.get(name);
        if (pair != null) {
            return pair.first;
        }
        return null;
    }

    public Pair<Counter, String> getCounterPair(String name) {
        return counterMap.get(name);
    }

    public Counter getMaxCounter(String name) {
        Counter counter;
        if ((counter = getCounter(maxCounterName(name))) != null) {
            return counter;
        }

        return getCounter(name);
    }

    // Copy all the counters from src profile
    public void copyAllCountersFrom(RuntimeProfile srcProfile) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        // The name of a counter and the name of its parent, in breadth-first order.
        ArrayDeque<String> names = new ArrayDeque<>();
        ArrayDeque<String> parentNames = new ArrayDeque<>();
        names.add(ROOT_COUNTER);
        parentNames.add(ROOT_COUNTER);
        while (!names.isEmpty()) {
            String name = names.poll();
            String parentName = parentNames.poll();

            if (!Objects.equals(ROOT_COUNTER, name)) {
                Counter srcCounter = srcProfile.counterMap.get(name).first;
                Counter newCounter = addCounter(name, srcCounter.getType(), srcCounter.getStrategy(), parentName);
                newCounter.setValue(srcCounter.getValue());
                if (srcCounter.hasMinValue()) {
                    newCounter.setMinValue(srcCounter.getMinValue());
                }
                if (srcCounter.hasMaxValue()) {
                    newCounter.setMaxValue(srcCounter.getMaxValue());
                }
            }

            Set<String> childNames = srcProfile.childCounterMap.get(name);
            if (childNames != null) {
                for (String childName : childNames) {
                    names.add(childName);
                    parentNames.add(name);
                }
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public void update(final TRuntimeProfileTree thriftProfile) {
        Reference<Integer> idx = new Reference<>(0);
        update(thriftProfile.nodes, idx, false);
        Preconditions.checkState(idx.getRef().equals(thriftProfile.nodes.size()));
    }

    // Update a subtree of profiles from nodes, rooted at idx. It will do a preorder
    // traversal, and modify idx in the traversal process. idx will point to the node
    // immediately following this subtree after the traversal. If the version of the
    // parent node, or the version of root node for this subtree is older, skip to update
    // the profile of subtree, but still traverse the nodes to get the node immediately
    // following this subtree.
    private void update(List<TRuntimeProfileNode> nodes, Reference<Integer> idx, boolean isParentNodeOld) {
        TRuntimeProfileNode node = nodes.get(idx.getRef());

        boolean isNodeOld;
        if (isParentNodeOld || (node.isSetVersion() && node.version < version)) {
            isNodeOld = true;
        } else {
            isNodeOld = false;
            if (node.isSetVersion()) {
                version = node.version;
            }
        }

        // update this level's counters
        if (!isNodeOld && node.counters != null) {
            // mapping from counterName to parentCounterName
            Map<String, String> child2ParentMap = Maps.newHashMap();
            if (node.child_counters_map != null) {
                // update childCounters
                for (Map.Entry<String, Set<String>> entry : node.child_counters_map.entrySet()) {
                    String parentName = entry.getKey();
                    for (String childName : entry.getValue()) {
                        child2ParentMap.put(childName, parentName);
                    }
                }
            }
            // First processing counters by hierarchy
            Map<String, TCounter> tCounterMap = node.counters.stream()
                    .collect(Collectors.toMap(TCounter::getName, t -> t));
            Queue<String> nameQueue = Lists.newLinkedList();
            nameQueue.offer(ROOT_COUNTER);
            while (!nameQueue.isEmpty()) {
                String topName = nameQueue.poll();

                if (!Objects.equals(ROOT_COUNTER, topName)) {
                    Pair<Counter, String> pair = counterMap.get(topName);
                    TCounter tcounter = tCounterMap.get(topName);
                    String parentName = child2ParentMap.get(topName);
                    Counter counter = null;
                    if (pair == null && tcounter != null && parentName != null) {
                        counter = addCounter(topName, tcounter.type, tcounter.strategy, parentName);
                        counter.setValue(tcounter.value);
                        counter.setStrategy(tcounter.strategy);
                    } else if (pair != null && tcounter != null) {
                        counter = pair.first;
                        if (pair.first.getType() != tcounter.type) {
                            LOG.error("Cannot update counters with the same name but different types"
                                    + " type=" + tcounter.type);
                        } else {
                            pair.first.setValue(tcounter.value);
                        }
                    }

                    if (counter != null) {
                        // Running profile will report multiple times, we only need the last time value
                        if (tcounter.isSetMin_value()) {
                            counter.setMinValue(tcounter.getMin_value());
                        } else {
                            counter.clearMinValue();
                        }
                        if (tcounter.isSetMax_value()) {
                            counter.setMaxValue(tcounter.getMax_value());
                        } else {
                            counter.clearMaxValue();
                        }
                        tCounterMap.remove(topName);
                    }
                }

                if (node.child_counters_map != null) {
                    Set<String> childNames = node.child_counters_map.get(topName);
                    if (childNames != null) {
                        for (String childName : childNames) {
                            nameQueue.offer(childName);
                        }
                    }
                }
            }
            // Second, processing the remaining counters, set ROOT_COUNTER as it's parent
            for (TCounter tcounter : tCounterMap.values()) {
                Pair<Counter, String> pair = counterMap.get(tcounter.name);
                Counter counter = null;
                if (pair == null) {
                    counter = addCounter(tcounter.name, tcounter.type, tcounter.strategy);
                    counter.setValue(tcounter.value);
                    counter.setStrategy(tcounter.strategy);
                } else {
                    counter = pair.first;
                    if (pair.first.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else {
                        pair.first.setValue(tcounter.value);
                    }
                }

                // Running profile will report multiple times, we only need the last time value
                if (tcounter.isSetMin_value()) {
                    counter.setMinValue(tcounter.getMin_value());
                } else {
                    counter.clearMinValue();
                }
                if (tcounter.isSetMax_value()) {
                    counter.setMaxValue(tcounter.getMax_value());
                } else {
                    counter.clearMaxValue();
                }
            }
        }

        if (!isNodeOld && node.info_strings_display_order != null) {
            Map<String, String> nodeInfoStrings = node.info_strings;
            for (String key : node.info_strings_display_order) {
                String value = nodeInfoStrings.get(key);
                Preconditions.checkState(value != null);
                addInfoString(key, value);
            }
        }

        idx.setRef(idx.getRef() + 1);

        for (int i = 0; i < node.num_children; i++) {
            TRuntimeProfileNode tchild = nodes.get(idx.getRef());
            String childName = tchild.name;
            RuntimeProfile childProfile = this.childMap.get(childName);
            if (childProfile == null) {
                childProfile = new RuntimeProfile(childName);
                addChild(childProfile);
            }
            childProfile.update(nodes, idx, isNodeOld);
        }
    }

    // Print the profile:
    //  1. Profile Name
    //  2. Info Strings
    //  3. Counters
    //  4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        ProfileFormatter formatter = new DefaultProfileFormatter(builder);
        formatter.format(this, prefix);
    }

    public String toString() {
        ProfileFormatter formater = new DefaultProfileFormatter();
        return formater.format(this);
    }

    public static String printCounter(Counter counter) {
        if (counter == null) {
            return null;
        }
        return printCounter(counter.getValue(), counter.getType());
    }

    private static String printCounter(long value, TUnit type) {
        StringBuilder builder = new StringBuilder();
        try (Formatter fmt = new Formatter(builder)) {
            appendCounter(builder, fmt, value, type);
        }
        return builder.toString();
    }

    // Appends the counter as printCounter prints it. fmt writes into builder, so a number formatted with it lands
    // in place, without a string of its own.
    private static void appendCounter(StringBuilder builder, Formatter fmt, long value, TUnit type) {
        if (type == TUnit.UNIT || type == TUnit.UNIT_PER_SECOND) {
            Pair<Double, String> pair = DebugUtil.getUint(value);
            if (pair.second.isEmpty()) {
                builder.append(value);
            } else {
                fmt.format("%.3f", pair.first);
                builder.append(pair.second).append(" (").append(value).append(")");
            }
            if (type == TUnit.UNIT_PER_SECOND) {
                builder.append(" /sec");
            }
        } else if (type == TUnit.BYTES || type == TUnit.BYTES_PER_SECOND) {
            Pair<Double, String> pair = DebugUtil.getByteUint(value);
            fmt.format("%.3f", pair.first);
            builder.append(" ").append(pair.second);
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
                builder.append(value / DebugUtil.THOUSAND);
                if (remain != 0) {
                    builder.append(".");
                    fmt.format("%03d", remain);
                }
                builder.append("ms");
            } else if (value >= DebugUtil.THOUSAND) {
                // if the time is over a microsecond, print it using unit microsecond
                long remain = value % DebugUtil.THOUSAND;
                builder.append(value / DebugUtil.THOUSAND);
                if (remain != 0) {
                    builder.append(".");
                    fmt.format("%03d", remain);
                }
                builder.append("us");
            } else {
                builder.append(value).append("ns");
            }
        } else if (type == TUnit.DOUBLE_VALUE) {
            fmt.format("%.3f", (double) value);
        } else if (type == TUnit.NONE) {
            // Do nothing
        } else {
            Preconditions.checkState(false, "type=%s", type);
        }
    }

    // The names of the child counters of counterName in the order the formatters print them: sorted, with the
    // MIN and MAX counters first. Null when the counter has no children.
    private static String[] orderedChildCounterNames(RuntimeProfile profile, String counterName) {
        Set<String> childNameSet = profile.childCounterMap.get(counterName);
        if (childNameSet == null) {
            return null;
        }
        String[] childNames = childNameSet.toArray(new String[0]);
        Arrays.sort(childNames);
        // Keep MIN/MAX metrics head of other child counters, each group in sorted order
        int numMinMax = 0;
        for (String childName : childNames) {
            if (childName.startsWith(MERGED_INFO_PREFIX_MIN) || childName.startsWith(MERGED_INFO_PREFIX_MAX)) {
                numMinMax++;
            }
        }
        if (numMinMax == 0 || numMinMax == childNames.length) {
            return childNames;
        }
        String[] ordered = new String[childNames.length];
        int minMaxIdx = 0;
        int otherIdx = numMinMax;
        for (String childName : childNames) {
            if (childName.startsWith(MERGED_INFO_PREFIX_MIN) || childName.startsWith(MERGED_INFO_PREFIX_MAX)) {
                ordered[minMaxIdx++] = childName;
            } else {
                ordered[otherIdx++] = childName;
            }
        }
        return ordered;
    }

    // concurrency safe
    public void addChild(RuntimeProfile child) {
        if (child == null) {
            return;
        }

        childMap.put(child.name, child);
        Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
        childList.add(pair);
    }

    // concurrency safe
    public void addChildren(List<RuntimeProfile> children) {
        if (children.isEmpty()) {
            return;
        }
        final RuntimeProfile child = children.get(0);
        childMap.put(child.name, child);
        List<Pair<RuntimeProfile, Boolean>> childList =
                children.stream().map(c -> new Pair<>(c, true)).collect(Collectors.toList());
        this.childList.addAll(childList);
    }

    public void removeChild(String childName) {
        RuntimeProfile childProfile = childMap.remove(childName);
        if (childProfile == null) {
            return;
        }
        childList.removeIf(childPair -> childPair.first == childProfile);
    }

    // Because the profile of summary and child fragment is not a real parent-child relationship
    // Each child profile needs to calculate the time proportion consumed by itself
    public void computeTimeInChildProfile() {
        childMap.values().forEach(RuntimeProfile::computeTimeInProfile);
    }

    public void computeTimeInProfile() {
        computeTimeInProfile(this.counterTotalTime.getValue());
    }

    public void computeTimeInProfile(long total) {
        if (total == 0) {
            return;
        }

        // Add all the total times in all the children
        long totalChildTime = 0;

        for (Pair<RuntimeProfile, Boolean> pair : this.childList) {
            totalChildTime += pair.first.getCounterTotalTime().getValue();
        }
        long localTime = this.getCounterTotalTime().getValue() - totalChildTime;
        // Counters have some margin, set to 0 if it was negative.
        localTime = Math.max(0, localTime);
        this.localTimePercent = (double) localTime / (double) total;
        this.localTimePercent = Math.min(1.0, this.localTimePercent) * 100;

        // Recurse on children
        for (Pair<RuntimeProfile, Boolean> pair : this.childList) {
            pair.first.computeTimeInProfile(total);
        }
    }

    // from bigger to smaller
    public void sortChildren() {
        this.childList.sort((profile1, profile2) ->
                Long.compare(profile2.first.getCounterTotalTime().getValue(),
                        profile1.first.getCounterTotalTime().getValue()));
    }

    public void addInfoString(String key, String value) {
        this.infoStrings.put(key, value);
    }

    public void removeInfoString(String key) {
        infoStrings.remove(key);
    }

    // Copy all info strings from src profile
    public void copyAllInfoStringsFrom(RuntimeProfile srcProfile, Set<String> excludedInfoStrings) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        srcProfile.infoStrings.forEach((key, value) -> {
            if (CollectionUtils.isNotEmpty(excludedInfoStrings) && excludedInfoStrings.contains(key)) {
                return;
            }
            if (!this.infoStrings.containsKey(key)) {
                this.infoStrings.put(key, value);
            } else if (!Objects.equals(value, this.infoStrings.get(key))) {
                String originalKey = key;
                int pos;
                if ((pos = key.indexOf("__DUP(")) != -1) {
                    originalKey = key.substring(0, pos);
                }
                int offset = -1;
                int previousOffset;
                int step = 1;
                while (true) {
                    previousOffset = offset;
                    offset += step;
                    String indexedKey = String.format("%s__DUP(%d)", originalKey, offset);
                    if (!this.infoStrings.containsKey(indexedKey)) {
                        if (step == 1) {
                            this.infoStrings.put(indexedKey, value);
                            break;
                        }
                        // Forward too much, try to forward half of the former size
                        offset = previousOffset;
                        step >>= 1;
                        continue;
                    }
                    step <<= 1;
                }
            }
        });
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getInfoStrings() {
        return infoStrings;
    }

    public Map<String, Set<String>> getChildCounterMap() {
        return childCounterMap;
    }

    public double getLocalTimePercent() {
        return localTimePercent;
    }

    public boolean containsInfoString(String key) {
        return infoStrings.containsKey(key);
    }

    // Returns the value to which the specified key is mapped;
    // or null if this map contains no mapping for the key.
    public String getInfoString(String key) {
        return infoStrings.get(key);
    }

    // Serializes profile to thrift. Not threadsafe.
    public TRuntimeProfileTree toThrift() {
        TRuntimeProfileTree profileTree = new TRuntimeProfileTree();
        profileTree.setNodes(new ArrayList<>());
        toThrift(profileTree.nodes);
        return profileTree;
    }

    // Flatten the tree of runtime profiles by in-order traversal. Not threadsafe.
    private void toThrift(List<TRuntimeProfileNode> nodes) {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        nodes.add(node);

        node.setName(name);
        node.setNum_children(childMap.size());
        node.setIndent(true);
        node.setVersion(version);

        for (Map.Entry<String, Pair<Counter, String>> entry : counterMap.entrySet()) {
            Counter counter = entry.getValue().first;
            TCounter tCounter = new TCounter();
            tCounter.setName(entry.getKey());
            tCounter.setValue(counter.getValue());
            tCounter.setType(counter.getType());
            tCounter.setStrategy(counter.getStrategy());
            if (counter.hasMinValue()) {
                tCounter.setMin_value(counter.getMinValue());
            }
            if (counter.hasMaxValue()) {
                tCounter.setMax_value(counter.getMaxValue());
            }
            node.addToCounters(tCounter);
        }

        for (Map.Entry<String, Set<String>> entry : childCounterMap.entrySet()) {
            node.putToChild_counters_map(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Map.Entry<String, String> entry : infoStrings.entrySet()) {
            node.putToInfo_strings(entry.getKey(), entry.getValue());
            node.addToInfo_strings_display_order(entry.getKey());
        }

        for (RuntimeProfile child : childMap.values()) {
            child.toThrift(nodes);
        }
    }

    // Merge all the isomorphic sub profiles and the caller must know for sure
    // that all the children are isomorphic, otherwise, the behavior is undefined
    public static RuntimeProfile mergeIsomorphicProfiles(List<RuntimeProfile> profiles,
                                                         Set<String> excludedInfoStrings) {
        return mergeIsomorphicProfiles(profiles, excludedInfoStrings, Collections.emptySet(), false);
    }

    // excludedCounters are left out of the merge of these profiles, with the counters below them; the children are
    // merged with all their counters. The caller reads these counters itself, and the profiles stay unchanged,
    // because they are the live profiles of a running query that other readers merge at the same time.
    //
    // With dropRedundantMinMax the merge does not add the min and max counters of a counter whose min, max and value
    // are equal, which are the ones removeRedundantMinMaxMetrics removes. A caller that runs it after the merge
    // gets the same profile without building counters only to remove them.
    public static RuntimeProfile mergeIsomorphicProfiles(List<RuntimeProfile> profiles,
                                                         Set<String> excludedInfoStrings,
                                                         Set<String> excludedCounters,
                                                         boolean dropRedundantMinMax) {
        if (CollectionUtils.isEmpty(profiles)) {
            return null;
        }

        RuntimeProfile mergedProfile = new RuntimeProfile(profiles.get(0).getName());

        for (RuntimeProfile runtimeProfile : profiles) {
            mergedProfile.copyAllInfoStringsFrom(runtimeProfile, excludedInfoStrings);
        }

        // Find all counters, although these profiles are expected to be isomorphic,
        // some counters are only attached to one of them.
        // Level by level: on each level the first profile that has a counter gives its type and parent.
        List<Map<String, Pair<TUnit, String>>> allLevelCounters = new ArrayList<>();
        ArrayDeque<String> nameQueue = new ArrayDeque<>();
        for (RuntimeProfile profile : profiles) {
            // Level order traverse starts with root
            nameQueue.clear();
            nameQueue.add(ROOT_COUNTER);
            int levelIdx = -1;
            while (!nameQueue.isEmpty()) {
                levelIdx++;
                int levelSize = nameQueue.size();
                for (int i = 0; i < levelSize; i++) {
                    String name = nameQueue.poll();
                    Set<String> childNames = profile.childCounterMap.get(name);
                    if (childNames != null) {
                        for (String childName : childNames) {
                            if (!excludedCounters.contains(childName)) {
                                nameQueue.add(childName);
                            }
                        }
                    }

                    if (Objects.equals(ROOT_COUNTER, name)) {
                        continue;
                    }
                    Pair<Counter, String> pair = profile.counterMap.get(name);
                    if (pair == null) {
                        LOG.warn("missing counter, profileName={}, counterName={}", profile.name, name);
                        continue;
                    }
                    Counter counter = pair.first;

                    while (allLevelCounters.size() <= levelIdx) {
                        allLevelCounters.add(new LinkedHashMap<>());
                    }

                    Map<String, Pair<TUnit, String>> levelCounters = allLevelCounters.get(levelIdx);
                    Pair<TUnit, String> exist = levelCounters.get(name);
                    if (exist == null) {
                        levelCounters.put(name, Pair.create(counter.getType(), pair.second));
                        continue;
                    }
                    if (!exist.first.equals(counter.getType())) {
                        LOG.warn(
                                "find non-isomorphic counter, profileName={}, counterName={}, existType={}, " +
                                        "anotherType={}",
                                mergedProfile.name, name, exist.first.name(), counter.getType().name());
                    }
                }
            }
        }

        // A counter that sits on different levels in different profiles is merged once per level. Its min and max
        // counters are then made by the first merge and get the values of the last one, so with
        // dropRedundantMinMax they are decided after all levels.
        Map<String, PendingMinMax> pendingMinMax = dropRedundantMinMax ? new LinkedHashMap<>() : null;
        for (Map<String, Pair<TUnit, String>> levelCounters : allLevelCounters) {
            for (Map.Entry<String, Pair<TUnit, String>> entry : levelCounters.entrySet()) {
                String name = entry.getKey();
                TUnit type = entry.getValue().first;
                String parentName = entry.getValue().second;
                mergeCounter(mergedProfile, profiles, name, type, parentName, excludedCounters, pendingMinMax);
            }
        }
        if (pendingMinMax != null) {
            for (Map.Entry<String, PendingMinMax> entry : pendingMinMax.entrySet()) {
                String name = entry.getKey();
                PendingMinMax minMax = entry.getValue();
                long value = mergedProfile.getCounter(name).getValue();
                if (minMax.minValue == minMax.maxValue && minMax.minValue == value) {
                    continue;
                }
                mergedProfile.addCounter(minCounterName(name), minMax.type, minMax.strategy, name)
                        .setValue(minMax.minValue);
                mergedProfile.addCounter(maxCounterName(name), minMax.type, minMax.strategy, name)
                        .setValue(minMax.maxValue);
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
                List<RuntimeProfile> subProfiles = new ArrayList<>(profiles.size());
                for (RuntimeProfile profile : profiles) {
                    RuntimeProfile child = profile.getChild(childName);
                    if (child == null) {
                        identical = false;
                        LOG.debug("find non-isomorphic children, profileName={}, requiredChildName={}",
                                profile.name, childName);
                        continue;
                    }
                    subProfiles.add(child);
                }
                RuntimeProfile mergedChild = mergeIsomorphicProfiles(subProfiles, excludedInfoStrings,
                        Collections.emptySet(), dropRedundantMinMax);
                mergedProfile.addChild(mergedChild);
            }
            if (!identical) {
                mergedProfile.addInfoString("NotIdentical", "");
            }
        }

        return mergedProfile;
    }

    // The min and max counters of a merged counter, made after all levels are merged: the type and strategy of the
    // first merge of the counter, the values of the last one.
    private static final class PendingMinMax {
        final TUnit type;
        final TCounterStrategy strategy;
        long minValue;
        long maxValue;

        PendingMinMax(TUnit type, TCounterStrategy strategy) {
            this.type = type;
            this.strategy = strategy;
        }
    }

    // True when the counter `name` of the profile is one of excludedCounters or below one of them.
    private static boolean isExcluded(RuntimeProfile profile, String name, Set<String> excludedCounters) {
        String current = name;
        while (!Objects.equals(ROOT_COUNTER, current)) {
            if (excludedCounters.contains(current)) {
                return true;
            }
            Pair<Counter, String> pair = profile.counterMap.get(current);
            if (pair == null) {
                return false;
            }
            current = pair.second;
        }
        return false;
    }

    // Merges the counter `name` of the profiles into mergedProfile. The value, min and max are accumulated in place,
    // in the order and with the rules of Counter.mergeIsomorphicCounters. With pendingMinMax the min and max
    // counters are recorded there and not made.
    private static void mergeCounter(RuntimeProfile mergedProfile, List<RuntimeProfile> profiles, String name,
                                     TUnit type, String parentName, Set<String> excludedCounters,
                                     Map<String, PendingMinMax> pendingMinMax) {
        // We don't need to calculate sum or average of counter's extra info (min value and max value) created by be
        if (name.startsWith(MERGED_INFO_PREFIX_MIN) || name.startsWith(MERGED_INFO_PREFIX_MAX)) {
            return;
        }

        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        boolean alreadyMerged = false;
        Counter skipMergeCounter = null;
        TCounterStrategy strategy = null;
        // what Counter.mergeIsomorphicCounters computes over the merged counters
        Counter firstCounter = null;
        int numCounters = 0;
        long sum = 0;
        long counterMin = Long.MAX_VALUE;
        long counterMax = Long.MIN_VALUE;
        for (RuntimeProfile profile : profiles) {
            Counter counter = profile.getCounter(name);
            if (counter != null && !excludedCounters.isEmpty() && isExcluded(profile, name, excludedCounters)) {
                counter = null;
            }

            // Allow some counters which only attach to one of the isomorphic profiles
            // E.g. A bunch of ExchangeSinkOperators may share one SinkBuffer, so the metrics
            // of SinkBuffer only attach to the first ExchangeSinkOperator's profile
            if (counter == null) {
                continue;
            }
            if (!type.equals(counter.getType())) {
                LOG.warn(
                        "find non-isomorphic counter, profileName={}, counterName={}, existType={}, anotherType={}",
                        mergedProfile.name, name, type.name(), counter.getType().name());
                continue;
            }
            strategy = counter.getStrategy();
            if (counter.isSkipMerge()) {
                skipMergeCounter = counter;
                break;
            }

            if (!counter.isSkipMinMax()) {
                if (counter.hasMinValue()) {
                    alreadyMerged = true;
                    minValue = Math.min(counter.getMinValue(), minValue);
                } else {
                    // TODO: keep compatible with older version backend, can be removed in next version
                    Counter minCounter = profile.getCounter(minCounterName(name));
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
                    Counter maxCounter = profile.getCounter(maxCounterName(name));
                    if (maxCounter != null) {
                        alreadyMerged = true;
                        if (maxCounter.getValue() > maxValue) {
                            maxValue = maxCounter.getValue();
                        }
                    }
                }
            }

            long value = counter.getValue();
            if (firstCounter == null) {
                firstCounter = counter;
            }
            numCounters++;
            sum += value;
            if (value < counterMin) {
                counterMin = value;
            }
            if (counter.hasMinValue() && counter.getMinValue() < counterMin) {
                counterMin = counter.getMinValue();
            }
            if (value > counterMax) {
                counterMax = value;
            }
            if (counter.hasMaxValue() && counter.getMaxValue() > counterMax) {
                counterMax = counter.getMaxValue();
            }
        }
        Counter mergedCounter;
        if (!Objects.equals(ROOT_COUNTER, parentName) && mergedProfile.getCounter(parentName) != null) {
            mergedCounter = mergedProfile.addCounter(name, type, strategy, parentName);
        } else {
            if (!Objects.equals(ROOT_COUNTER, parentName)) {
                LOG.warn("missing parent counter, profileName={}, counterName={}, parentCounterName={}",
                        mergedProfile.name, name, parentName);
            }
            mergedCounter = mergedProfile.addCounter(name, type, strategy);
        }
        if (skipMergeCounter != null) {
            mergedCounter.setValue(skipMergeCounter.getValue());
            return;
        }
        Preconditions.checkState(firstCounter != null, "no counter %s to merge in %s", name, mergedProfile.name);
        long mergedValue = firstCounter.isAvg() ? sum / numCounters : sum;
        if (!alreadyMerged) {
            minValue = counterMin;
            maxValue = counterMax;
        }
        mergedCounter.setValue(mergedValue);

        if (mergedCounter.isSkipMinMax()) {
            return;
        }
        // The min and max are compared with the int bounds, so a min equal to Integer.MAX_VALUE or a max equal to
        // Integer.MIN_VALUE leaves its counter at 0; the bounds are kept as they are to keep the merged profiles.
        boolean setMin = minValue != Integer.MAX_VALUE;
        boolean setMax = maxValue != Integer.MIN_VALUE;
        if (pendingMinMax != null) {
            PendingMinMax minMax = pendingMinMax.computeIfAbsent(name,
                    k -> new PendingMinMax(type, mergedCounter.getStrategy()));
            if (setMin) {
                mergedCounter.setMinValue(minValue);
                minMax.minValue = minValue;
            }
            if (setMax) {
                mergedCounter.setMaxValue(maxValue);
                minMax.maxValue = maxValue;
            }
            return;
        }
        Counter minCounter = mergedProfile.addCounter(minCounterName(name), type, mergedCounter.getStrategy(), name);
        Counter maxCounter = mergedProfile.addCounter(maxCounterName(name), type, mergedCounter.getStrategy(), name);
        if (setMin) {
            mergedCounter.setMinValue(minValue);
            minCounter.setValue(minValue);
        }
        if (setMax) {
            mergedCounter.setMaxValue(maxValue);
            maxCounter.setValue(maxValue);
        }
    }

    public static void removeRedundantMinMaxMetrics(RuntimeProfile profile) {
        for (Map.Entry<String, Pair<Counter, String>> entry : profile.counterMap.entrySet()) {
            String name = entry.getKey();
            Counter counter = entry.getValue().first;
            String minName = minCounterName(name);
            String maxName = maxCounterName(name);
            Counter minCounter = profile.getCounter(minName);
            Counter maxCounter = profile.getCounter(maxName);
            if (counter == null || minCounter == null || maxCounter == null) {
                continue;
            }

            if (minCounter.getValue() == maxCounter.getValue() && minCounter.getValue() == counter.getValue()) {
                profile.removeCounter(minName);
                profile.removeCounter(maxName);
            }
        }

        profile.getChildList().forEach(pair -> removeRedundantMinMaxMetrics(pair.first));
    }

    interface ProfileFormatter {

        // Print the profile:
        //  1. Profile Name
        //  2. Info Strings
        //  3. Counters
        //  4. Children
        String format(RuntimeProfile profile, String prefix);

        String format(RuntimeProfile profile);
    }

    static class DefaultProfileFormatter implements ProfileFormatter {

        private final StringBuilder builder;
        // Writes into builder.
        private final Formatter fmt;

        DefaultProfileFormatter(StringBuilder builder) {
            this.builder = builder;
            this.fmt = new Formatter(builder);
        }

        DefaultProfileFormatter() {
            this(new StringBuilder());
        }

        @Override
        public String format(RuntimeProfile profile, String prefix) {
            this.doFormat(profile, prefix);
            return builder.toString();
        }

        @Override
        public String format(RuntimeProfile profile) {
            this.doFormat(profile, "");
            return builder.toString();
        }

        private void doFormat(RuntimeProfile profile, String prefix) {
            Counter totalTimeCounter = profile.getCounter(TOTAL_TIME_COUNTER);
            Preconditions.checkState(totalTimeCounter != null);
            // 1. profile name
            builder.append(prefix).append(profile.getName()).append(":");
            // total time
            if (totalTimeCounter.getValue() != 0) {
                builder.append("(Active: ");
                appendCounter(builder, fmt, totalTimeCounter.getValue(), totalTimeCounter.getType());
                if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                    // TotalTime in nanosecond concated if it's larger than 1000ns
                    builder.append("[").append(totalTimeCounter.getValue()).append("ns]");
                }
                builder.append(", % non-child: ");
                fmt.format("%.2f", profile.getLocalTimePercent());
                builder.append("%)");
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
            String childPrefix = null;
            for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
                boolean indent = childPair.second;
                RuntimeProfile childProfile = childPair.first;
                if (indent && childPrefix == null) {
                    childPrefix = prefix + "  ";
                }
                doFormat(childProfile, indent ? childPrefix : prefix);
            }
        }

        private void printChildCounters(RuntimeProfile profile, String prefix, String counterName) {
            String[] childNames = orderedChildCounterNames(profile, counterName);
            if (childNames == null) {
                return;
            }
            String childPrefix = null;
            for (String childName : childNames) {
                Counter childCounter = profile.getCounter(childName);
                Preconditions.checkState(childCounter != null);
                builder.append(prefix).append("   - ").append(childName).append(": ");
                appendCounter(builder, fmt, childCounter.getValue(), childCounter.getType());
                builder.append("\n");
                if (profile.childCounterMap.containsKey(childName)) {
                    if (childPrefix == null) {
                        childPrefix = prefix + "  ";
                    }
                    this.printChildCounters(profile, childPrefix, childName);
                }
            }
        }
    }

    static class JsonProfileFormatter implements ProfileFormatter {
        private final JsonObject builder;

        JsonProfileFormatter() {
            this.builder = new JsonObject();
        }

        @Override
        public String format(RuntimeProfile profile, String prefix) {
            //may parse prefix to json
            return this.format(profile);
        }

        @Override
        public String format(RuntimeProfile profile) {
            addRuntimeProfile(profile, builder);
            Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
            return gson.toJson(builder);
        }

        public void addRuntimeProfile(RuntimeProfile profile, JsonObject jsonObject) {
            Counter totalTimeCounter = profile.getCounter(TOTAL_TIME_COUNTER);
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
            String[] childNames = orderedChildCounterNames(profile, counterName);
            if (childNames == null) {
                return;
            }
            for (String childName : childNames) {
                Counter childCounter = profile.getCounter(childName);
                Preconditions.checkState(childCounter != null);
                childJsonObject.addProperty(childName,
                        printCounter(childCounter.getValue(), childCounter.getType()));
                this.addChildCounters(profile, childName, childJsonObject);
            }
        }
    }
}

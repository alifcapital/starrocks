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

import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TCounterMergeType;
import com.starrocks.thrift.TCounterMinMaxType;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// The merge and the formatters must print the same profile as the code they replace (LegacyProfileCode) for any
// input. The inputs are random instance profiles built from one seed twice, one copy for each side.
public class RuntimeProfileMergeEquivalenceTest {
    private static final int ROUNDS = 400;
    private static final TUnit[] TYPES = {TUnit.UNIT, TUnit.UNIT_PER_SECOND, TUnit.BYTES, TUnit.BYTES_PER_SECOND,
            TUnit.TIME_NS, TUnit.DOUBLE_VALUE, TUnit.NONE};
    private static final long[] EDGE_VALUES = {0, 1, 7, 999, 1000, 1001, 999_999, 1_000_000, 1_000_001,
            999_999_999L, 1_000_000_000L, 1_234_567_890_123L, Integer.MAX_VALUE, Integer.MIN_VALUE, -5, 3_000_000_005L};
    private static final Set<String> QUERY_LEVEL = Set.of("QueryCumulativeCpuTime", "QueryPeakMemoryUsage");

    private static final class CounterSpec {
        final String name;
        final String parent;
        final TUnit type;
        final TCounterStrategy strategy;

        CounterSpec(String name, String parent, TUnit type, TCounterStrategy strategy) {
            this.name = name;
            this.parent = parent;
            this.type = type;
            this.strategy = strategy;
        }
    }

    private static TCounterStrategy randomStrategy(Random random, TUnit type) {
        TCounterStrategy strategy = Counter.createStrategy(type);
        int aggregate = random.nextInt(6);
        if (aggregate == 0) {
            strategy.aggregate_type = TCounterAggregateType.AVG;
        } else if (aggregate == 1) {
            strategy.aggregate_type = TCounterAggregateType.SUM;
        }
        int merge = random.nextInt(12);
        if (merge == 0) {
            strategy.merge_type = TCounterMergeType.SKIP_ALL;
        } else if (merge == 1) {
            strategy.merge_type = TCounterMergeType.SKIP_SECOND_MERGE;
        }
        if (random.nextInt(8) == 0) {
            strategy.min_max_type = TCounterMinMaxType.SKIP_ALL;
        }
        return strategy;
    }

    private static long randomValue(Random random) {
        if (random.nextInt(4) == 0) {
            return EDGE_VALUES[random.nextInt(EDGE_VALUES.length)];
        }
        return random.nextInt(3) == 0 ? random.nextLong() >>> random.nextInt(64) : random.nextInt(5000);
    }

    // The counters of one isomorphic group: names, parents and types shared by the instances.
    private static List<CounterSpec> randomSchema(Random random) {
        List<CounterSpec> specs = new ArrayList<>();
        int numCounters = 1 + random.nextInt(12);
        for (int i = 0; i < numCounters; i++) {
            String parent = RuntimeProfile.ROOT_COUNTER;
            if (!specs.isEmpty() && random.nextInt(3) == 0) {
                parent = specs.get(random.nextInt(specs.size())).name;
            }
            TUnit type = TYPES[random.nextInt(TYPES.length)];
            specs.add(new CounterSpec("C" + i, parent, type, randomStrategy(random, type)));
        }
        if (random.nextInt(3) == 0) {
            specs.add(new CounterSpec("QueryCumulativeCpuTime", RuntimeProfile.ROOT_COUNTER, TUnit.TIME_NS,
                    Counter.createStrategy(TUnit.TIME_NS)));
            specs.add(new CounterSpec("QueryPeakMemoryUsage", RuntimeProfile.ROOT_COUNTER, TUnit.BYTES,
                    Counter.createStrategy(TUnit.BYTES)));
            specs.add(new CounterSpec("PeakChild", "QueryPeakMemoryUsage", TUnit.BYTES,
                    Counter.createStrategy(TUnit.BYTES)));
        }
        return specs;
    }

    private static void addCounters(Random random, RuntimeProfile profile, List<CounterSpec> specs) {
        for (CounterSpec spec : specs) {
            boolean parentPresent = spec.parent.equals(RuntimeProfile.ROOT_COUNTER)
                    || profile.getCounter(spec.parent) != null;
            if (!parentPresent || random.nextInt(10) == 0) {
                continue;
            }
            TUnit type = random.nextInt(50) == 0 ? TUnit.UNIT_PER_SECOND : spec.type;
            // now and then an instance puts a counter on another level than the others do
            String parent = random.nextInt(25) == 0 ? RuntimeProfile.ROOT_COUNTER : spec.parent;
            Counter counter = profile.addCounter(spec.name, type, spec.strategy, parent);
            counter.setValue(randomValue(random));
            if (random.nextBoolean()) {
                long min = counter.getValue() - random.nextInt(100);
                long max = counter.getValue() + random.nextInt(100);
                if (random.nextInt(4) != 0) {
                    counter.setMinValue(min);
                }
                if (random.nextInt(4) != 0) {
                    counter.setMaxValue(max);
                }
            } else if (random.nextInt(5) == 0) {
                // the min and max counters of a backend of an older version
                profile.addCounter(RuntimeProfile.MERGED_INFO_PREFIX_MIN + spec.name, type, spec.strategy, spec.name)
                        .setValue(counter.getValue() - random.nextInt(10));
                profile.addCounter(RuntimeProfile.MERGED_INFO_PREFIX_MAX + spec.name, type, spec.strategy, spec.name)
                        .setValue(counter.getValue() + random.nextInt(10));
            }
        }
    }

    private static RuntimeProfile randomTree(Random random, String name, List<CounterSpec> specs, int depth,
                                             List<List<CounterSpec>> childSchemas) {
        RuntimeProfile profile = new RuntimeProfile(name);
        profile.addInfoString("Shared", "same");
        if (random.nextInt(3) == 0) {
            profile.addInfoString("Differs", "v" + random.nextInt(3));
        }
        if (random.nextInt(4) == 0) {
            profile.addInfoString("Empty", "");
        }
        profile.getCounterTotalTime().setValue(random.nextInt(4) == 0 ? 0 : randomValue(random) & 0xffffffffffL);
        addCounters(random, profile, specs);
        if (depth > 0) {
            for (int c = 0; c < childSchemas.size(); c++) {
                if (random.nextInt(20) == 0) {
                    continue;
                }
                RuntimeProfile child = randomTree(random, name.charAt(0) + "-child" + c, childSchemas.get(c),
                        depth - 1, Collections.emptyList());
                profile.addChild(child);
            }
        }
        return profile;
    }

    // Instance profiles of one fragment, the same for the same seed.
    private static List<RuntimeProfile> instances(long seed) {
        Random schemaRandom = new Random(seed);
        List<CounterSpec> topSchema = randomSchema(schemaRandom);
        int numChildren = schemaRandom.nextInt(4);
        List<List<CounterSpec>> childSchemas = new ArrayList<>();
        for (int i = 0; i < numChildren; i++) {
            childSchemas.add(randomSchema(schemaRandom));
        }
        int numInstances = 1 + schemaRandom.nextInt(5);
        List<RuntimeProfile> profiles = new ArrayList<>();
        Random random = new Random(seed * 31 + 7);
        for (int i = 0; i < numInstances; i++) {
            profiles.add(randomTree(random, "Instance", topSchema, 1, childSchemas));
        }
        return profiles;
    }

    private static String legacyText(RuntimeProfile profile) {
        return new LegacyProfileCode.DefaultProfileFormatter().format(profile);
    }

    private static String legacyJson(RuntimeProfile profile) {
        return new LegacyProfileCode.JsonProfileFormatter().format(profile);
    }

    private static String text(RuntimeProfile profile) {
        return new RuntimeProfile.DefaultProfileFormatter().format(profile);
    }

    private static String json(RuntimeProfile profile) {
        return new RuntimeProfile.JsonProfileFormatter().format(profile);
    }

    @Test
    public void formattersPrintTheSameProfile() {
        for (long seed = 0; seed < ROUNDS; seed++) {
            for (RuntimeProfile profile : instances(seed)) {
                Assertions.assertEquals(legacyText(profile), text(profile), "seed " + seed);
                Assertions.assertEquals(legacyJson(profile), json(profile), "seed " + seed);
                StringBuilder prefixed = new StringBuilder();
                profile.prettyPrint(prefixed, "  ");
                Assertions.assertEquals(new LegacyProfileCode.DefaultProfileFormatter().format(profile, "  "),
                        prefixed.toString(), "seed " + seed);
            }
        }
    }

    @Test
    public void mergePrintsTheSameProfile() {
        Set<String> excludedInfoStrings = Set.of("Differs");
        for (long seed = 0; seed < ROUNDS; seed++) {
            RuntimeProfile legacy = LegacyProfileCode.mergeIsomorphicProfiles(instances(seed), excludedInfoStrings);
            RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(instances(seed), excludedInfoStrings);
            Assertions.assertEquals(legacyText(legacy), text(merged), "seed " + seed);
            Assertions.assertEquals(legacyJson(legacy), json(merged), "seed " + seed);
        }
    }

    @Test
    public void mergeWithoutRedundantMinMaxPrintsTheSameProfile() {
        for (long seed = 0; seed < ROUNDS; seed++) {
            RuntimeProfile legacy = LegacyProfileCode.mergeIsomorphicProfiles(instances(seed), Collections.emptySet());
            LegacyProfileCode.removeRedundantMinMaxMetrics(legacy);
            RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(instances(seed), Collections.emptySet(),
                    Collections.emptySet(), true);
            RuntimeProfile.removeRedundantMinMaxMetrics(merged);
            Assertions.assertEquals(legacyText(legacy), text(merged), "seed " + seed);
        }
    }

    // The order of QueryRuntimeProfile.buildQueryProfile: the merged instance is copied into the fragment profile,
    // and the redundant min and max counters are removed from the fragment profile and its children.
    @Test
    public void mergeInTheOrderOfTheQueryProfilePrintsTheSameProfile() {
        for (long seed = 0; seed < ROUNDS; seed++) {
            RuntimeProfile legacyMerged = LegacyProfileCode.mergeIsomorphicProfiles(instances(seed),
                    Collections.emptySet());
            RuntimeProfile legacy = new RuntimeProfile("Fragment 0");
            legacy.copyAllInfoStringsFrom(legacyMerged, null);
            legacy.copyAllCountersFrom(legacyMerged);
            legacyMerged.getChildList().forEach(pair -> legacy.addChild(pair.first));
            LegacyProfileCode.removeRedundantMinMaxMetrics(legacy);

            RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(instances(seed), Collections.emptySet(),
                    Collections.emptySet(), true);
            RuntimeProfile fragment = new RuntimeProfile("Fragment 0");
            fragment.copyAllInfoStringsFrom(merged, null);
            fragment.copyAllCountersFrom(merged);
            merged.getChildList().forEach(pair -> fragment.addChild(pair.first));
            RuntimeProfile.removeRedundantMinMaxMetrics(fragment);
            Assertions.assertEquals(legacyText(legacy), text(fragment), "seed " + seed);
        }
    }

    // Formatting and merging read profiles that report threads change at the same time; they must not fail.
    @Test
    public void readersRunWhileTheProfilesChange() throws Exception {
        List<RuntimeProfile> live = instances(7);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            Random random = new Random(1);
            try {
                while (!stop.get()) {
                    for (RuntimeProfile profile : live) {
                        for (String name : profile.getCounterMap().keySet()) {
                            Counter counter = profile.getCounter(name);
                            if (counter != null) {
                                counter.setValue(random.nextInt(1000));
                                counter.setMinValue(random.nextInt(10));
                                counter.setMaxValue(1000 + random.nextInt(10));
                            }
                        }
                        profile.addCounter("Added" + random.nextInt(50), TUnit.UNIT, null).setValue(1);
                    }
                }
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        writer.start();
        try {
            for (int i = 0; i < 300; i++) {
                RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(live, Collections.emptySet(),
                        QUERY_LEVEL, true);
                RuntimeProfile.removeRedundantMinMaxMetrics(merged);
                Assertions.assertNotNull(text(merged));
                Assertions.assertNotNull(json(live.get(0)));
            }
        } finally {
            stop.set(true);
            writer.join();
        }
        Assertions.assertNull(failure.get());
    }

    // The merge leaves out the excluded counters as the removal of them from the instances did, and does not change
    // the instances.
    @Test
    public void excludedCountersMatchRemovedCountersAndKeepTheInstances() {
        for (long seed = 0; seed < ROUNDS; seed++) {
            List<RuntimeProfile> removedFrom = instances(seed);
            for (RuntimeProfile instance : removedFrom) {
                for (String name : QUERY_LEVEL) {
                    instance.removeCounter(name);
                }
            }
            RuntimeProfile legacy = LegacyProfileCode.mergeIsomorphicProfiles(removedFrom, Collections.emptySet());
            LegacyProfileCode.removeRedundantMinMaxMetrics(legacy);

            List<RuntimeProfile> kept = instances(seed);
            List<String> before = new ArrayList<>();
            for (RuntimeProfile instance : kept) {
                before.add(legacyText(instance));
            }
            RuntimeProfile merged = RuntimeProfile.mergeIsomorphicProfiles(kept, Collections.emptySet(), QUERY_LEVEL,
                    true);
            RuntimeProfile.removeRedundantMinMaxMetrics(merged);
            Assertions.assertEquals(legacyText(legacy), text(merged), "seed " + seed);
            for (int i = 0; i < kept.size(); i++) {
                Assertions.assertEquals(before.get(i), legacyText(kept.get(i)), "seed " + seed);
            }
        }
    }
}

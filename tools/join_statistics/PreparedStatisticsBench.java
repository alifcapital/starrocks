// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
package com.starrocks.sql.optimizer.statistics;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.statistic.JoinStatisticsMeta;
import org.openjdk.jol.info.GraphLayout;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Standalone benchmark against payloads produced by the SQL collector; never used by the server. */
public class PreparedStatisticsBench {
    private static volatile Object retained;
    private static final com.sun.management.ThreadMXBean MEMORY =
            (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    private static long allocated() {
        return MEMORY.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private static long collections() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(b -> Math.max(0, b.getCollectionCount())).sum();
    }

    private static double median(double[] values) {
        double[] copy = values.clone();
        Arrays.sort(copy);
        return copy[copy.length / 2];
    }

    public static void main(String[] args) throws Exception {
        Path directory = Path.of(args[0]);
        Map<String, Object> results = new LinkedHashMap<>();
        for (int file = 1; file < args.length; file++) {
            String name = args[file];
            var meta = GsonUtils.GSON.fromJson(Files.readString(directory.resolve(name + ".meta.json")), JoinStatisticsMeta.class);
            byte[] payload = Files.readAllBytes(directory.resolve(name + ".bin"));
            int maximum = 256 * 1024 * 1024;
            JoinStatisticsData data = JoinStatisticsCodec.decode(payload, maximum, meta.getId(), meta.getGeneration());
            long actual = GraphLayout.parseInstance(data).totalSize();
            Cache<String, JoinStatisticsData> cache = Caffeine.newBuilder().executor(Runnable::run).maximumWeight(512L << 20)
                    .weigher((String key, JoinStatisticsData value) -> Math.toIntExact(value.estimatedSize() + 192)).build();
            long emptyCache = GraphLayout.parseInstance(cache).totalSize();
            cache.put(name, data);
            cache.cleanUp();
            long occupiedCache = GraphLayout.parseInstance(cache).totalSize();
            for (int i = 0; i < 5; i++) {
                retained = JoinStatisticsCodec.decode(payload, maximum, meta.getId(), meta.getGeneration());
            }
            double[] decodeMs = new double[9];
            double[] decodeAllocations = new double[9];
            long gc = collections();
            for (int i = 0; i < decodeMs.length; i++) {
                long before = allocated();
                long started = System.nanoTime();
                retained = JoinStatisticsCodec.decode(payload, maximum, meta.getId(), meta.getGeneration());
                decodeMs[i] = (System.nanoTime() - started) / 1e6;
                decodeAllocations[i] = allocated() - before;
            }
            Map<String, Object> record = new LinkedHashMap<>();
            record.put("payload_bytes", payload.length);
            record.put("prepared_heap_bytes", actual);
            record.put("estimated_bytes", data.estimatedSize());
            record.put("cache_increment_bytes", occupiedCache - emptyCache);
            record.put("decode_ms", median(decodeMs));
            record.put("decode_samples_ms", decodeMs);
            record.put("decode_allocated_bytes", median(decodeAllocations));
            record.put("decode_gc_count", collections() - gc);
            record.put("sources", data.getSources().stream().map(s -> Map.of("rows", s.getRows(),
                    "slices", s.getTuples().size(), "keys", s.getDegrees().size())).toList());
            Map<String, Object> estimates = new LinkedHashMap<>();
            for (boolean partial : new boolean[] {false, true}) {
                List<JoinStatisticsEstimate.Selection> selections = new ArrayList<>();
                for (JoinStatisticsData.Source source : data.getSources()) {
                    List<Integer> slices = new ArrayList<>();
                    long rows = 0;
                    for (int i = 0; i < source.getTuples().size(); i++) {
                        if (i == 0 || (partial && (source.getColumns().isEmpty()
                                || Objects.equals(source.getTuples().get(0).get(0), source.getTuples().get(i).get(0))))) {
                            slices.add(i);
                            rows += source.getTupleRows(i);
                        }
                    }
                    selections.add(new JoinStatisticsEstimate.Selection(slices.stream().mapToInt(Integer::intValue).toArray(),
                            0, rows));
                }
                int mask = (1 << selections.size()) - 1;
                for (int outputs : new int[] {mask, 1}) {
                    for (int warmup = 0; warmup < 100; warmup++) {
                        retained = JoinStatisticsEstimate.estimate(meta.getDefinition(), data, selections, mask, outputs,
                                TimeUnit.SECONDS.toNanos(5));
                    }
                    double[] times = new double[9];
                    double[] bytes = new double[9];
                    double answer = 0;
                    gc = collections();
                    for (int i = 0; i < times.length; i++) {
                        long before = allocated();
                        long start = System.nanoTime();
                        answer = JoinStatisticsEstimate.estimate(meta.getDefinition(), data, selections, mask, outputs,
                                TimeUnit.SECONDS.toNanos(5)).orElseThrow();
                        times[i] = (System.nanoTime() - start) / 1e6;
                        bytes[i] = allocated() - before;
                    }
                    estimates.put((partial ? "partial" : "full") + (outputs == 1 ? "_rf" : "_join"),
                            Map.of("estimate", answer, "median_ms", median(times), "samples_ms", times,
                                    "allocated_bytes", median(bytes), "gc_count", collections() - gc));
                }
            }
            record.put("estimates", estimates);
            results.put(name, record);
        }
        System.out.println(GsonUtils.GSON.toJson(results));
    }
}

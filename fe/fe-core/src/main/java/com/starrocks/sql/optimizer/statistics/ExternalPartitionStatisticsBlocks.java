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

package com.starrocks.sql.optimizer.statistics;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.common.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Immutable, byte-weighed blocks. No file metadata, global partition index or independent key registry. */
public final class ExternalPartitionStatisticsBlocks {
    private ExternalPartitionStatisticsBlocks() {
    }

    public static final class Block implements ExternalColumnStatistics {
        public final ExternalStatisticsCacheKey key;
        public final ExternalColumnStatistics.Partition summary;
        private final long[] rows;
        private final long loadedNanos;

        Block(ExternalStatisticsCacheKey key, ExternalColumnStatistics.Partition summary, long[] rows) {
            this(key, summary, rows, System.nanoTime());
        }

        Block(ExternalStatisticsCacheKey key, ExternalColumnStatistics.Partition summary, long[] rows, long loadedNanos) {
            if (rows.length != key.partitions.size()) {
                throw new IllegalArgumentException("Block coverage does not match its domain");
            }
            this.key = key;
            this.summary = summary;
            this.rows = rows.clone();
            this.loadedNanos = loadedNanos;
        }

        public static Block fromRows(ExternalStatisticsCacheKey key, ExternalColumnStatistics.Partition summary,
                                     String coverageJson) {
            long[] rows = new long[key.partitions.size()];
            Arrays.fill(rows, -1);
            long total = 0;
            for (JsonElement element : JsonParser.parseString(coverageJson).getAsJsonArray()) {
                JsonArray pair = element.getAsJsonArray();
                String partition = pair.get(0).getAsString();
                int position = java.util.Collections.binarySearch(key.partitions, partition);
                long count = pair.get(1).getAsLong();
                if (position < 0 || rows[position] >= 0 || count < 0) {
                    throw new IllegalArgumentException("Invalid/duplicate block coverage: " + partition);
                }
                rows[position] = count;
                total = Math.addExact(total, count);
            }
            if (total != summary.getRowCount()) {
                throw new IllegalArgumentException("Block row total disagrees with coverage");
            }
            return new Block(key, summary, rows);
        }

        public static Block merge(ExternalStatisticsCacheKey key,
                Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cached) {
            return merge(key, cached, System.nanoTime());
        }

        public static Block merge(ExternalStatisticsCacheKey key,
                Map<ExternalStatisticsCacheKey, Optional<ExternalColumnStatistics>> cached, long loadedNanos) {
            long[] rows = new long[key.partitions.size()];
            Arrays.fill(rows, -1);
            long count = 0;
            long nulls = 0;
            double size = 0;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            String type = null;
            StatisticsHll.Union hll = new StatisticsHll.Union();
            for (int i = 0; i < rows.length; i++) {
                ExternalStatisticsCacheKey part = new ExternalStatisticsCacheKey(
                        key.tableUUID, key.partitions.get(i), key.columnName);
                ExternalColumnStatistics value = cached.getOrDefault(part, Optional.empty()).orElse(null);
                if (value == null) {
                    continue;
                }
                ExternalColumnStatistics.Partition stats = (ExternalColumnStatistics.Partition) value;
                rows[i] = stats.getRowCount();
                count = Math.addExact(count, stats.getRowCount());
                nulls = Math.addExact(nulls, stats.getNullCount());
                size += stats.getDataSize();
                min = Math.min(min, stats.getMinValue());
                max = Math.max(max, stats.getMaxValue());
                type = type == null ? stats.getSourceType() : type.equals(stats.getSourceType()) ? type : "";
                hll.merge(stats.getHll());
            }
            return new Block(key, new ExternalColumnStatistics.Partition(type == null ? "" : type, count, size,
                    nulls, hll.snapshot(), min, max), rows, loadedNanos);
        }

        long rows(int position) {
            return rows[position];
        }

        boolean needsRefresh(long now) {
            return Config.enable_statistic_cache_refresh_after_write
                    && now - loadedNanos >= TimeUnit.SECONDS.toNanos(Config.statistic_update_interval_sec);
        }

        boolean expired(long now) {
            return now - loadedNanos >= TimeUnit.SECONDS.toNanos(Config.statistic_update_interval_sec * 2L);
        }

        String first() {
            return key.partitions.get(0);
        }

        String last() {
            return key.partitions.get(key.partitions.size() - 1);
        }

        @Override
        public String getSourceType() {
            return summary.getSourceType();
        }

        @Override
        public int retainedBytes() {
            long bytes = 192L + summary.retainedBytes() + 16L * rows.length;
            for (String name : key.partitions) {
                bytes += 40L + 2L * name.length();
            }
            return (int) Math.min(Integer.MAX_VALUE, bytes);
        }
    }

    /** One cache entry per table/column. Replacement retains a generation token across block additions. */
    public static final class Directory implements ExternalColumnStatistics {
        public final Object generation;
        private final List<Block> blocks;
        private final int bytes;

        public Directory() {
            this(new Object(), List.of());
        }

        private Directory(Object generation, List<Block> blocks) {
            this.generation = generation;
            this.blocks = List.copyOf(blocks);
            long weight = 96;
            for (Block block : blocks) {
                weight += 8L + block.retainedBytes();
            }
            this.bytes = (int) Math.min(Integer.MAX_VALUE, weight);
        }

        // A later query can reveal holes inside a block first built for a sparse set. Retire
        // those blocks so their envelopes do not prevent caching useful finer-grained blocks.
        public Directory refine(List<String> requested) {
            List<Block> kept = new ArrayList<>(blocks.size());
            for (Block block : blocks) {
                int position = java.util.Collections.binarySearch(requested, block.first());
                position = position >= 0 ? position : -position - 1;
                boolean hole = false;
                while (position < requested.size() && requested.get(position).compareTo(block.last()) <= 0) {
                    if (java.util.Collections.binarySearch(block.key.partitions, requested.get(position)) < 0) {
                        hole = true;
                        break;
                    }
                    position++;
                }
                if (!hole) {
                    kept.add(block);
                }
            }
            return kept.size() == blocks.size() ? this : new Directory(new Object(), kept);
        }

        public List<Block> covering(List<String> requested, Set<String> membership, long now) {
            List<Block> result = new ArrayList<>();
            if (requested.isEmpty()) {
                return result;
            }
            String first = requested.get(0);
            String last = requested.get(requested.size() - 1);
            int lo = 0;
            int hi = blocks.size();
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (blocks.get(mid).last().compareTo(first) < 0) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            for (int i = lo; i < blocks.size(); i++) {
                Block block = blocks.get(i);
                if (block.first().compareTo(last) > 0) {
                    break;
                }
                if (!block.expired(now) && membership.containsAll(block.key.partitions)) {
                    result.add(block);
                }
            }
            return result;
        }

        public Directory withBlocks(List<Block> incoming, long now) {
            List<Block> kept = new ArrayList<>();
            for (Block block : blocks) {
                if (!block.expired(now)) {
                    kept.add(block);
                }
            }
            for (Block block : incoming) {
                int position = java.util.Collections.binarySearch(kept, block, Comparator.comparing(Block::first));
                if (position >= 0 && kept.get(position).key.equals(block.key)) {
                    kept.set(position, block);
                    continue;
                }
                if (position >= 0) {
                    continue;
                }
                position = -position - 1;
                // Conservative interval exclusion also handles sparse sets: no proliferation of
                // overlapping combinations. Uncacheable boundary blocks are still used by this request.
                if ((position > 0 && kept.get(position - 1).last().compareTo(block.first()) >= 0)
                        || (position < kept.size() && kept.get(position).first().compareTo(block.last()) <= 0)) {
                    continue;
                }
                kept.add(position, block);
            }
            return new Directory(generation, kept);
        }

        @Override
        public String getSourceType() {
            return "";
        }

        @Override
        public int retainedBytes() {
            return bytes;
        }
    }
}

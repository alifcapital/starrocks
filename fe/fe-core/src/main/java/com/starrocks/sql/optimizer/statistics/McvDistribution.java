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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** An owned immutable MCV snapshot. String keys retain the existing statistics encoding. */
public final class McvDistribution extends AbstractMap<String, Long> {
    private static final McvDistribution EMPTY = new McvDistribution(Map.of());

    private final Map<String, Long> values;
    private volatile FrequencyOrder frequencyOrder;
    private final long totalRows;

    private McvDistribution(Map<String, Long> source) {
        Map<String, Long> copy = new LinkedHashMap<>();
        long sum = 0;
        for (Entry<String, Long> entry : source.entrySet()) {
            long count = Objects.requireNonNull(entry.getValue());
            copy.put(entry.getKey(), entry.getValue());
            sum += count;
        }
        values = Collections.unmodifiableMap(copy);
        totalRows = sum;
    }

    private static final class FrequencyOrder {
        private final String[] keys;
        private final long[] counts;

        private FrequencyOrder(Map<String, Long> values) {
            var ordered = new ArrayList<>(values.entrySet());
            // Stable sorting preserves encounter order for equally frequent values.
            ordered.sort(Entry.comparingByValue(Comparator.reverseOrder()));
            keys = new String[ordered.size()];
            counts = new long[ordered.size()];
            for (int i = 0; i < ordered.size(); i++) {
                keys[i] = ordered.get(i).getKey();
                counts[i] = ordered.get(i).getValue();
            }
        }
    }

    /** Cache loaders prepare eagerly; derived distributions only pay if frequency order is used. */
    public void prepareFrequencyOrder() {
        frequencyOrder();
    }

    private FrequencyOrder frequencyOrder() {
        FrequencyOrder prepared = frequencyOrder;
        if (prepared == null) {
            synchronized (this) {
                prepared = frequencyOrder;
                if (prepared == null) {
                    prepared = new FrequencyOrder(values);
                    frequencyOrder = prepared;
                }
            }
        }
        return prepared;
    }

    /** Reuse a prepared snapshot; mutable inputs are copied and never retained. */
    public static McvDistribution copyOf(Map<String, Long> source) {
        if (source == null || source.isEmpty()) {
            return EMPTY;
        }
        return source instanceof McvDistribution prepared ? prepared : new McvDistribution(source);
    }

    public long getTotalRows() {
        return totalRows;
    }

    public String getKeyByFrequency(int rank) {
        return frequencyOrder().keys[rank];
    }

    public long getCountByFrequency(int rank) {
        return frequencyOrder().counts[rank];
    }

    /** Conservative retained-size estimate excluding key strings owned by the enclosing record. */
    public long retainedBytesExcludingKeys() {
        // Map entries, boxed counts, table slots, key/count arrays, and object/array headers.
        return 256L + 112L * size();
    }

    @Override
    public Set<Entry<String, Long>> entrySet() {
        return values.entrySet();
    }

    @Override
    public Long get(Object key) {
        return values.get(key);
    }

    @Override
    public boolean containsKey(Object key) {
        return values.containsKey(key);
    }

    @Override
    public int size() {
        return values.size();
    }
}

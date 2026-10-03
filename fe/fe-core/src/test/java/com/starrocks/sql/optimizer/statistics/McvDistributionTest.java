// Copyright 2021-present StarRocks, Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.statistics;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class McvDistributionTest {
    @Test
    void ownsSnapshotAndImmutableViews() {
        Map<String, Long> source = new LinkedHashMap<>();
        source.put("first", 7L);
        source.put("second", 7L);
        source.put("small", 2L);
        McvDistribution prepared = McvDistribution.copyOf(source);
        source.put("first", 100L);
        source.remove("second");
        assertEquals(16, prepared.getTotalRows());
        assertEquals(7L, prepared.get("first"));
        assertEquals("first", prepared.getKeyByFrequency(0));
        assertEquals("second", prepared.getKeyByFrequency(1));
        assertEquals(2, prepared.getCountByFrequency(2));
        assertSame(prepared, McvDistribution.copyOf(prepared));
        assertThrows(UnsupportedOperationException.class, () -> prepared.put("new", 1L));
        assertThrows(UnsupportedOperationException.class, () -> prepared.remove("first"));
        assertThrows(UnsupportedOperationException.class,
                () -> prepared.entrySet().iterator().next().setValue(1L));
        assertThrows(UnsupportedOperationException.class, () -> prepared.values().remove(7L));
    }

    @Test
    void emptyAndLargeCountsKeepLongSemantics() {
        assertSame(McvDistribution.copyOf(null), McvDistribution.copyOf(Map.of()));
        assertEquals(0, McvDistribution.copyOf(Map.of()).getTotalRows());
        Map<String, Long> source = new LinkedHashMap<>();
        source.put("small", 1L);
        source.put("large", Long.MAX_VALUE);
        McvDistribution prepared = McvDistribution.copyOf(source);
        assertEquals("large", prepared.getKeyByFrequency(0));
        assertEquals(Long.MIN_VALUE, prepared.getTotalRows());
        assertThrows(IndexOutOfBoundsException.class, () -> prepared.getCountByFrequency(2));
    }

    @Test
    void concurrentPreparationHasStableWeightAndOrder() throws Exception {
        McvDistribution prepared = McvDistribution.copyOf(Map.of("a", 1L, "b", 9L));
        long bytes = prepared.retainedBytesExcludingKeys();
        var executor = Executors.newFixedThreadPool(4);
        try {
            Callable<String> reader = () -> {
                for (int i = 0; i < 1000; i++) {
                    prepared.prepareFrequencyOrder();
                    assertEquals(9, prepared.getCountByFrequency(0));
                    assertEquals(10, prepared.getTotalRows());
                }
                return prepared.getKeyByFrequency(0);
            };
            for (var future : executor.invokeAll(List.of(reader, reader, reader, reader))) {
                assertEquals("b", future.get());
            }
        } finally {
            executor.shutdownNow();
        }
        assertEquals(bytes, prepared.retainedBytesExcludingKeys());
    }
}

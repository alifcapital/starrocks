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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HistogramMcvSnapshotTest {
    @Test
    void histogramOwnsBucketsAndMcvAndReusesPreparedValues() {
        Map<String, Long> source = new LinkedHashMap<>();
        source.put("a", 3L);
        source.put("b", 9L);
        List<Bucket> buckets = new ArrayList<>(List.of(new Bucket(10, 20, 30L, 1L)));
        Histogram histogram = new Histogram(buckets, source);
        source.clear();
        buckets.clear();
        assertEquals(42, histogram.getTotalRows());
        assertEquals("MCV: [[b:9][a:3]]", histogram.getMcvString());
        assertThrows(UnsupportedOperationException.class, () -> histogram.getBuckets().clear());
        assertThrows(UnsupportedOperationException.class, () -> histogram.getMCV().clear());
        Histogram shared = Histogram.ofSingleBucket(0, 20, 100, histogram.getMCV());
        assertSame(histogram.getMcvDistribution(), shared.getMcvDistribution());
        assertEquals(100, shared.getTotalRows());
    }

    @Test
    void derivedDistributionGetsItsOwnFrequencyIndex() {
        Histogram original = new Histogram(Map.of("a", 3L, "b", 9L));
        original.getMcvDistribution().prepareFrequencyOrder();
        Map<String, Long> filtered = new LinkedHashMap<>(original.getMCV());
        filtered.remove("b");
        Histogram derived = new Histogram(filtered);
        filtered.put("b", 100L);
        assertEquals("a", derived.getMcvDistribution().getKeyByFrequency(0));
        assertEquals("b", original.getMcvDistribution().getKeyByFrequency(0));
        assertEquals(3, derived.getTotalRows());
        assertEquals(12, original.getTotalRows());
    }
}

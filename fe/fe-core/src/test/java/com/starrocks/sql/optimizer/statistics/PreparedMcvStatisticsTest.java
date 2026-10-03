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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class PreparedMcvStatisticsTest {
    private static ExternalMcvStatistics.Group group() {
        return new ExternalMcvStatistics.Group(List.of("c"), 1000, 91,
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("30"), 100, List.of(100L))),
                List.of(List.of("10", "20", "900", "10", "90")), List.of(0L));
    }

    @Test
    public void testPlannerViewsShareOnlyTheImmutableDistribution() {
        ExternalMcvStatistics.Group group = group();
        group.prepare(VarcharType.VARCHAR);
        ColumnStatistic a = group.columnStatistic(VarcharType.VARCHAR,
                ColumnStatistic.builder().setAverageRowSize(7).build()).orElseThrow();
        ColumnStatistic b = group.columnStatistic(VarcharType.VARCHAR,
                ColumnStatistic.builder().setAverageRowSize(27).build()).orElseThrow();
        Assertions.assertNotSame(a, b);
        Assertions.assertSame(a.getHistogram(), b.getHistogram());
        Assertions.assertEquals(7, a.getAverageRowSize());
        Assertions.assertEquals(27, b.getAverageRowSize());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> a.getHistogram().getMCV().clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> a.getHistogram().getBuckets().clear());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new ExternalMcvStatistics(List.of(group)).getGroups().clear());
    }

    @Test
    public void testFilteringCannotChangeTheCachedDistribution() {
        ExternalMcvStatistics.Group group = group();
        Histogram original = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow().getHistogram();
        Histogram filtered = StringHistogramEstimator.filter(original, "20", BinaryType.GE);
        Assertions.assertEquals(110, StringHistogramEstimator.rows(filtered));
        Assertions.assertEquals(1000, StringHistogramEstimator.rows(original));
        Assertions.assertSame(original,
                group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow().getHistogram());
        Assertions.assertEquals("10", ((StringBucket) original.getBuckets().get(0)).getLowerString());
        Assertions.assertEquals(900, original.getBuckets().get(0).getCount());
    }

    @Test
    public void testPreparedViewIsBoundToItsSourceType() {
        ExternalMcvStatistics.Group group = group();
        ColumnStatistic string = group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow();
        ColumnStatistic number = group.columnStatistic(IntegerType.INT, ColumnStatistic.unknown()).orElseThrow();
        Assertions.assertTrue(string.getHistogram().hasStringValues());
        Assertions.assertFalse(number.getHistogram().hasStringValues());
        Assertions.assertNotSame(string.getHistogram(), number.getHistogram());
        Assertions.assertEquals(10, number.getMinValue());
        Assertions.assertEquals(30, number.getMaxValue());
        Assertions.assertTrue(group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown())
                .orElseThrow().getHistogram().hasStringValues());
    }

    @Test
    public void testBucketTextRoundTripsWithoutRetainingMutableInput() {
        List<String> fields = new ArrayList<>(List.of("10", "20", "900", "10", "90"));
        ExternalMcvStatistics.Group group = new ExternalMcvStatistics.Group(List.of("c"), 900, 90,
                List.of(), List.of(fields), List.of(0L));
        fields.set(2, "1");
        Assertions.assertEquals(List.of(List.of("10", "20", "900", "10", "90")), group.getBuckets());
        Assertions.assertEquals(900, group.columnStatistic(IntegerType.INT, ColumnStatistic.unknown())
                .orElseThrow().getHistogram().getTotalRows());
    }

    @Test
    public void testConcurrentColdViewsShareOneHistogram() {
        ExternalMcvStatistics.Group group = group();
        List<CompletableFuture<Histogram>> futures = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            futures.add(CompletableFuture.supplyAsync(() ->
                    group.columnStatistic(VarcharType.VARCHAR, ColumnStatistic.unknown()).orElseThrow().getHistogram()));
        }
        Histogram first = futures.get(0).join();
        for (CompletableFuture<Histogram> future : futures) {
            Assertions.assertSame(first, future.join());
        }
    }

    @Test
    public void testUtf8ComparisonBoundariesAndMalformedSurrogates() {
        List<String> values = List.of("", "a", utf16(97, 0x0000), "?", utf16(0x007f), utf16(0x0080), utf16(0x07ff), utf16(0x0800),
                utf16(0xd7ff), utf16(0xe000), utf16(0xffff), utf16(0xd800, 0xdc00), utf16(0xdbff, 0xdfff),
                utf16(0xd800), utf16(0xdc00),
                utf16(120, 0xd800, 122), "x?z", utf16(0xd800, 0xd800, 0xdc00));
        for (String left : values) {
            for (String right : values) {
                assertUtf8Order(left, right);
            }
        }
        Assertions.assertTrue(utf16(0xd800, 0xdc00).compareTo(utf16(0xe000)) < 0);
        Assertions.assertTrue(StringBucket.compare(utf16(0xd800, 0xdc00), utf16(0xe000)) > 0);
    }

    @Test
    public void testUtf8ComparisonMatchesEncodedOrderForRandomUtf16() {
        Random random = new Random(20260921);
        for (int i = 0; i < 5000; i++) {
            String left = randomUtf16(random);
            String right = randomUtf16(random);
            assertUtf8Order(left, right);
            assertUtf8Order(left, left + right);
        }
    }

    private static String utf16(int... units) {
        char[] chars = new char[units.length];
        for (int i = 0; i < units.length; i++) {
            chars[i] = (char) units[i];
        }
        return new String(chars);
    }

    private static String randomUtf16(Random random) {
        char[] chars = new char[random.nextInt(16)];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) random.nextInt(65536);
        }
        return new String(chars);
    }

    private static void assertUtf8Order(String left, String right) {
        int expected = Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals(Integer.signum(expected), Integer.signum(StringBucket.compare(left, right)));
    }
}

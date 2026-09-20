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

package com.starrocks.statistic;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class ExternalHistogramStatisticsCollectJobTest {
    private static ExternalHistogramStatisticsCollectJob newJob(Map<String, String> properties) {
        return new ExternalHistogramStatisticsCollectJob("hive0", new Database(1, "db"), null,
                List.of("amount", "status"), List.of(IntegerType.BIGINT, VarcharType.VARCHAR),
                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE, properties);
    }

    @Test
    public void testParseFrequentItemsAndQuantiles() {
        Assertions.assertEquals(List.of("5", "7"),
                ExternalHistogramStatisticsCollectJob.parseFrequentItems("[[\"5\",\"501\"],[\"7\",\"301\"]]"));
        Assertions.assertTrue(ExternalHistogramStatisticsCollectJob.parseFrequentItems(null).isEmpty());
        Assertions.assertEquals(List.of("0", "2500", "9999"),
                ExternalHistogramStatisticsCollectJob.parseQuantiles("[\"0\",\"2500\",\"9999\"]"));
        Assertions.assertTrue(ExternalHistogramStatisticsCollectJob.parseQuantiles("[]").isEmpty());
    }

    @Test
    public void testParseHistogramDropsEmptyMcvAndAccumulatesBucketCounts() {
        ExternalHistogramStatisticsCollectJob.ColumnHistogram histogram =
                ExternalHistogramStatisticsCollectJob.parseHistogram(
                        "{\"mcv\":[[\"5\",\"51\"],[\"9\",\"0\"],[\"60\",\"110\"]],"
                                + "\"buckets\":[[\"0\",\"50\",\"50\",\"1\",\"50\"],[\"51\",\"99\",\"59\",\"1\",\"49\"]]}");
        Assertions.assertEquals(List.of(List.of("60", "110"), List.of("5", "51")), histogram.mcv);
        Assertions.assertEquals(2, histogram.buckets.size());
        Assertions.assertEquals(List.of("0", "50", "50", "1", "50"), histogram.buckets.get(0));
        Assertions.assertEquals(List.of("51", "99", "109", "1", "49"), histogram.buckets.get(1));

        Assertions.assertEquals("[[\"60\",\"110\"],[\"5\",\"51\"]]",
                ExternalHistogramStatisticsCollectJob.toJson(histogram.mcv));
        Assertions.assertEquals("[[\"0\",\"50\",\"50\",\"1\",\"50\"],[\"51\",\"99\",\"109\",\"1\",\"49\"]]",
                ExternalHistogramStatisticsCollectJob.toJson(histogram.buckets));
    }

    @Test
    public void testParseHistogramWithoutBuckets() {
        ExternalHistogramStatisticsCollectJob.ColumnHistogram histogram =
                ExternalHistogramStatisticsCollectJob.parseHistogram(
                        "{\"mcv\":[[\"approved\",\"900\"],[\"a\\\"b\",\"7\"]],\"buckets\":[]}");
        Assertions.assertEquals(List.of(List.of("approved", "900"), List.of("a\"b", "7")), histogram.mcv);
        Assertions.assertTrue(histogram.buckets.isEmpty());
        Assertions.assertEquals("[[\"approved\",\"900\"],[\"a\\\"b\",\"7\"]]",
                ExternalHistogramStatisticsCollectJob.toJson(histogram.mcv));
    }

    @Test
    public void testTailBucketHoldsRowsOutsideMcv() {
        List<List<String>> mcv = List.of(List.of("approved", "900"), List.of("declined", "70"));
        ExternalHistogramStatisticsCollectJob.ColumnHistogram histogram =
                ExternalHistogramStatisticsCollectJob.withTailBucket(mcv, 1000);
        Assertions.assertSame(mcv, histogram.mcv);
        Assertions.assertEquals(List.of(List.of("Infinity", "Infinity", "30", "0")), histogram.buckets);
        Assertions.assertEquals("[[\"Infinity\",\"Infinity\",\"30\",\"0\"]]",
                ExternalHistogramStatisticsCollectJob.toJson(histogram.buckets));

        histogram = ExternalHistogramStatisticsCollectJob.withTailBucket(List.of(), 0);
        Assertions.assertTrue(histogram.mcv.isEmpty());
        Assertions.assertEquals(List.of(List.of("Infinity", "Infinity", "0", "0")), histogram.buckets);
    }

    @Test
    public void testDefaultsComeFromConfig() {
        ExternalHistogramStatisticsCollectJob job = newJob(Map.of());
        Assertions.assertEquals("ExternalHistogram", job.getName());
        Assertions.assertEquals("hive0", job.getCatalogName());
        Assertions.assertTrue(Config.histogram_mcv_size > 0);
        Assertions.assertTrue(Config.histogram_buckets_size > 0);
    }
}

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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.VarcharType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A column of strings of the format 'yyyy-MM-dd HH:mm:ss' with 864 rows in each of the 92 days from 2026-07-01, one
 * every 100 seconds, and a histogram of strings in buckets of 3000 rows whose ends fall inside the days. The rows of a
 * range of days are then 864 for each day of it.
 */
final class DailyStrings {
    static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    static final StringDateFormat FORMAT = StringDateFormat.parse("%Y-%m-%d %H:%i:%s");
    static final DateTimeFormatter TEXT_OF_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final LocalDate FIRST_DAY = LocalDate.of(2026, 7, 1);
    static final int DAYS = 92;
    static final int ROWS_OF_DAY = 864;
    static final int ROWS_OF_BUCKET = 3000;

    private DailyStrings() {
    }

    static String at(long row) {
        return FIRST_DAY.atStartOfDay().plusSeconds(100L * row).format(TEXT_OF_TIME);
    }

    static Statistics statistics() {
        return statistics(Map.of());
    }

    // The same column with most common values: the rows of each are not in the buckets, and add to the rows of the table
    static Statistics statistics(Map<String, Long> mostCommon) {
        long rows = (long) ROWS_OF_DAY * DAYS + mostCommon.values().stream().mapToLong(Long::longValue).sum();
        return statistics(ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(19)
                .setDistinctValuesCount((double) rows)
                .setStringBounds(new StringBounds(at(0), at((long) ROWS_OF_DAY * DAYS - 1), StringBounds.WHOLE, false))
                .setHistogram(histogram(mostCommon)).build(), rows);
    }

    static Statistics statistics(ColumnStatistic statistic) {
        return statistics(statistic, (long) ROWS_OF_DAY * DAYS);
    }

    private static Statistics statistics(ColumnStatistic statistic, long rows) {
        return Statistics.builder().setOutputRowCount((double) rows).addColumnStatistic(TEXT, statistic).build();
    }

    private static Histogram histogram(Map<String, Long> mostCommon) {
        long total = (long) ROWS_OF_DAY * DAYS;
        List<Bucket> buckets = new ArrayList<>();
        for (long low = 0; low < total; low += ROWS_OF_BUCKET) {
            long high = Math.min(low + ROWS_OF_BUCKET, total) - 1;
            buckets.add(new StringBucket(at(low), at(high), high + 1, 1, high - low + 1));
        }
        return Histogram.forStrings(buckets, mostCommon);
    }
}

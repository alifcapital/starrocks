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

package com.starrocks.connector.iceberg.cost;

import com.starrocks.catalog.Column;
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.ExpressionStatisticCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StringBounds;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// The statistics of the columns of an Iceberg table stay as they were for every reader, except the bounds of a column
// that a file left out, which are dropped. Only the estimates of strings and casts get the string bounds for
// estimates, which know how the metrics mode cuts the bounds.
public class IcebergColumnBoundsTest {
    private static final Types.NestedField TIMESTAMP =
            Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone());
    private static final Types.NestedField TIMESTAMP_TZ =
            Types.NestedField.optional(2, "tstz", Types.TimestampType.withZone());
    private static final Types.NestedField DATE = Types.NestedField.optional(3, "d", Types.DateType.get());
    private static final Types.NestedField TEXT = Types.NestedField.optional(4, "created_at", Types.StringType.get());
    private static final Types.NestedField NUMBER = Types.NestedField.optional(5, "n", Types.IntegerType.get());
    private static final Types.NestedField MISSING = Types.NestedField.optional(6, "s", Types.StringType.get());
    private static final Types.NestedField NUMBER_1 = Types.NestedField.optional(1, "n", Types.IntegerType.get());
    private static final List<Types.NestedField> FIELDS = List.of(TIMESTAMP, TIMESTAMP_TZ, DATE, TEXT, NUMBER, MISSING);
    private static final Map<Integer, Column> COLUMNS = Map.of(
            1, new Column("ts", DateType.DATETIME), 2, new Column("tstz", DateType.DATETIME),
            3, new Column("d", DateType.DATE), 4, new Column("created_at", VarcharType.VARCHAR),
            5, new Column("n", IntegerType.INT), 6, new Column("s", VarcharType.VARCHAR));
    private static final LocalDateTime FIRST = LocalDateTime.of(2018, 8, 15, 13, 16, 23, 509_905_000);
    private static final LocalDateTime LAST = LocalDateTime.of(2026, 10, 13, 2, 52, 47, 123_000_000);

    @BeforeEach
    public void setUp() {
        // A session zone away from UTC: the bounds do not depend on it
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("Asia/Dushanbe");
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private static long micros(LocalDateTime time) {
        return ChronoUnit.MICROS.between(LocalDateTime.of(1970, 1, 1, 0, 0), time);
    }

    // Two files: both have bounds for the times, the date and created_at; the second has values of n and s without
    // bounds for them, as a file written before these columns had metrics.
    private static IcebergFileStats files() {
        Map<Integer, Type.PrimitiveType> types = new HashMap<>();
        FIELDS.forEach(field -> types.put(field.fieldId(), field.type().asPrimitiveType()));
        Map<Integer, Object> min = new HashMap<>(Map.of(1, micros(FIRST), 2, micros(FIRST),
                3, (int) LocalDate.of(2018, 8, 15).toEpochDay(), 4, "2018-08-15T13:16", 5, 1, 6, "a"));
        Map<Integer, Object> max = new HashMap<>(Map.of(1, micros(FIRST.plusDays(1)), 2, micros(FIRST.plusDays(1)),
                3, (int) LocalDate.of(2018, 8, 16).toEpochDay(), 4, "2018-08-16T00:0:", 5, 100, 6, "m"));
        Map<Integer, Long> nulls = new HashMap<>(Map.of(1, 0L, 2, 0L, 3, 0L, 4, 0L, 5, 0L, 6, 0L));
        IcebergFileStats stats = new IcebergFileStats(types, FIELDS, 10, 100, min, max, nulls,
                new HashMap<>(Map.of(4, 270L)));
        Map<Integer, Object> secondMin = Map.of(1, micros(LAST.minusDays(1)), 2, micros(LAST.minusDays(1)),
                3, (int) LocalDate.of(2026, 10, 12).toEpochDay(), 4, "2026-10-12T02:52");
        Map<Integer, Object> secondMax = Map.of(1, micros(LAST), 2, micros(LAST),
                3, (int) LocalDate.of(2026, 10, 13).toEpochDay(), 4, "2026-10-13T02:53");
        Map<Integer, Long> secondNulls = Map.of(1, 0L, 2, 1L, 3, 0L, 4, 0L, 5, 0L, 6, 0L);
        stats.incrementRecordCount(10);
        stats.updateStats(stats.getMinValues(), secondMin, secondNulls, 10, i -> i > 0);
        stats.updateStats(stats.getMaxValues(), secondMax, secondNulls, 10, i -> i < 0);
        stats.updateNullCount(secondNulls);
        return stats;
    }

    @Test
    public void testColumnStatisticsAreThoseOfTheBase() {
        IcebergFileStats stats = files();
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        // n and s lost their bounds in the second file, see testBoundsAFileLeftOutAreDropped
        for (Types.NestedField field : List.of(TIMESTAMP, TIMESTAMP_TZ, DATE, TEXT)) {
            Column column = COLUMNS.get(field.fieldId());
            for (Map<Integer, Long> ndv : List.of(Map.<Integer, Long>of(), Map.of(field.fieldId(), 7L))) {
                for (MetricsModes.MetricsMode mode : new MetricsModes.MetricsMode[] {
                        null, MetricsModes.Truncate.withLength(16), MetricsModes.Full.get()}) {
                    ColumnStatistic actual = provider.buildColumnStatistic(field.fieldId(), column, stats, ndv, mode);
                    ColumnStatistic expected = Base.buildColumnStatistic(field.fieldId(), column, stats, ndv);
                    String name = field.name() + " " + ndv + " " + mode;
                    Assertions.assertEquals(0, Double.compare(expected.getMinValue(), actual.getMinValue()), name);
                    Assertions.assertEquals(0, Double.compare(expected.getMaxValue(), actual.getMaxValue()), name);
                    Assertions.assertEquals(expected.getMinString(), actual.getMinString(), name);
                    Assertions.assertEquals(expected.getMaxString(), actual.getMaxString(), name);
                    Assertions.assertEquals(0, Double.compare(expected.getDistinctValuesCount(),
                            actual.getDistinctValuesCount()), name);
                    Assertions.assertEquals(0, Double.compare(expected.getNullsFraction(), actual.getNullsFraction()),
                            name);
                    Assertions.assertEquals(0, Double.compare(expected.getAverageRowSize(),
                            actual.getAverageRowSize()), name);
                    Assertions.assertEquals(expected.getType(), actual.getType(), name);
                    Assertions.assertEquals(expected.getHistogram(), actual.getHistogram(), name);
                    Assertions.assertEquals(expected.toString(), actual.toString(), name);
                }
            }
        }
        // The times are the UTC seconds of the metrics with their fraction, whatever the zone of the session
        ColumnStatistic tstz = provider.buildColumnStatistic(2, COLUMNS.get(2), stats, Map.of(), null);
        Assertions.assertEquals(micros(FIRST) / 1e6, tstz.getMinValue());
        Assertions.assertEquals(FIRST.toEpochSecond(ZoneOffset.UTC), (long) tstz.getMinValue());
        Assertions.assertEquals(0.509905, tstz.getMinValue() - Math.floor(tstz.getMinValue()), 1e-6);
        Assertions.assertEquals(micros(LAST) / 1e6,
                provider.buildColumnStatistic(1, COLUMNS.get(1), stats, Map.of(), null).getMaxValue());
        Assertions.assertEquals(LocalDate.of(2018, 8, 15).toEpochDay() * 86400.0,
                provider.buildColumnStatistic(3, COLUMNS.get(3), stats, Map.of(), null).getMinValue());
        ColumnStatistic text = provider.buildColumnStatistic(4, COLUMNS.get(4), stats, Map.of(),
                MetricsModes.Truncate.withLength(16));
        Assertions.assertEquals("2018-08-15T13:16", text.getMinString());
        Assertions.assertEquals("2026-10-13T02:53", text.getMaxString());
    }

    @Test
    public void testBoundsAFileLeftOutAreDropped() {
        IcebergFileStats stats = files();
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        // n: the base kept the upper bound of the first file, which misses the values of the second file
        ColumnStatistic numberBefore = Base.buildColumnStatistic(5, COLUMNS.get(5), stats, Map.of());
        ColumnStatistic numberAfter = provider.buildColumnStatistic(5, COLUMNS.get(5), stats, Map.of(), null);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, numberBefore.getMinValue());
        Assertions.assertEquals(100, numberBefore.getMaxValue());
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, numberAfter.getMinValue());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, numberAfter.getMaxValue());
        // s: the same for the connector string bounds
        ColumnStatistic textBefore = Base.buildColumnStatistic(6, COLUMNS.get(6), stats, Map.of());
        ColumnStatistic textAfter = provider.buildColumnStatistic(6, COLUMNS.get(6), stats, Map.of(),
                MetricsModes.Truncate.withLength(16));
        Assertions.assertNull(textBefore.getMinString());
        Assertions.assertEquals("m", textBefore.getMaxString());
        Assertions.assertNull(textAfter.getMinString());
        Assertions.assertNull(textAfter.getMaxString());
        Assertions.assertNull(textAfter.getStringBounds());
        // The other fields stay
        for (List<ColumnStatistic> pair : List.of(List.of(numberBefore, numberAfter), List.of(textBefore, textAfter))) {
            Assertions.assertEquals(pair.get(0).getNullsFraction(), pair.get(1).getNullsFraction());
            Assertions.assertEquals(pair.get(0).getAverageRowSize(), pair.get(1).getAverageRowSize());
            Assertions.assertEquals(pair.get(0).getType(), pair.get(1).getType());
        }
        Assertions.assertEquals(7, provider.buildColumnStatistic(5, COLUMNS.get(5), stats, Map.of(5, 7L), null)
                .getDistinctValuesCount());
    }

    @Test
    public void testIdentityPartitionColumnWithoutBoundsInTheFirstFile() throws Exception {
        Types.NestedField region = Types.NestedField.optional(2, "region", Types.StringType.get());
        Map<Integer, Type.PrimitiveType> types = Map.of(1, Types.IntegerType.get(), 2, Types.StringType.get());
        List<PartitionField> fields = PartitionSpec.builderFor(new Schema(NUMBER_1, region)).identity("region")
                .build().fields();
        // IcebergMetadata leaves the identity partition columns out of the columns it passes. The first file has
        // values of region and no bounds for them.
        IcebergFileStats stats = new IcebergFileStats(types, List.of(NUMBER_1), 10, 100,
                new HashMap<>(Map.of(1, 1)), new HashMap<>(Map.of(1, 9)), new HashMap<>(Map.of(1, 0L)), Map.of());
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        Method min = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMin", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        Method max = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMax", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        min.setAccessible(true);
        max.setAccessible(true);
        // The second file has region from 'm' to 'z'
        stats.incrementRecordCount(10);
        Map<Integer, Long> nulls = Map.of(1, 0L, 2, 0L);
        min.invoke(provider, stats, fields, Map.of(1, 1, 2, "m"), nulls, 10L);
        max.invoke(provider, stats, fields, Map.of(1, 9, 2, "z"), nulls, 10L);
        stats.updateNullCount(nulls);
        ColumnStatistic statistic = provider.buildColumnStatistic(2, new Column("region", VarcharType.VARCHAR), stats,
                Map.of(), MetricsModes.Full.get());
        // The first file may hold 'a', below 'm'
        Assertions.assertNull(statistic.getMinString(), statistic.getMinString());
        Assertions.assertNull(statistic.getMaxString(), statistic.getMaxString());
        Assertions.assertEquals(1, provider.buildColumnStatistic(1, new Column("n", IntegerType.INT), stats, Map.of(),
                null).getMinValue());
    }

    @Test
    public void testFilesThatKeepOrLoseTheBounds() throws Exception {
        Map<Integer, Type.PrimitiveType> types = Map.of(1, Types.IntegerType.get(), 6, Types.StringType.get());
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        Method min = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMin", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        Method max = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMax", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        min.setAccessible(true);
        max.setAccessible(true);
        // A first file with a lower bound of s and no upper one, as when Iceberg cannot increase a cut maximum
        IcebergFileStats noUpper = new IcebergFileStats(types, List.of(NUMBER_1, MISSING), 10, 100,
                new HashMap<>(Map.of(1, 1, 6, "a")), new HashMap<>(Map.of(1, 9)),
                new HashMap<>(Map.of(1, 0L, 6, 0L)), Map.of());
        noUpper.incrementRecordCount(10);
        min.invoke(provider, noUpper, List.of(), Map.of(1, 1, 6, "b"), Map.of(1, 0L, 6, 0L), 10L);
        max.invoke(provider, noUpper, List.of(), Map.of(1, 9, 6, "c"), Map.of(1, 0L, 6, 0L), 10L);
        ColumnStatistic text = provider.buildColumnStatistic(6, COLUMNS.get(6), noUpper, Map.of(), null);
        Assertions.assertNull(text.getMinString());
        Assertions.assertNull(text.getMaxString());
        Assertions.assertEquals(9, provider.buildColumnStatistic(1, COLUMNS.get(5), noUpper, Map.of(), null)
                .getMaxValue());
        // A second file without any metrics leaves out the bounds of every column, the NULL rows stay as before
        IcebergFileStats noMetrics = new IcebergFileStats(types, List.of(NUMBER_1, MISSING), 10, 100,
                new HashMap<>(Map.of(1, 1, 6, "a")), new HashMap<>(Map.of(1, 9, 6, "c")),
                new HashMap<>(Map.of(1, 2L, 6, 0L)), Map.of());
        noMetrics.incrementRecordCount(10);
        min.invoke(provider, noMetrics, List.of(), null, null, 10L);
        max.invoke(provider, noMetrics, List.of(), null, null, 10L);
        noMetrics.updateNullCount(null);
        ColumnStatistic number = provider.buildColumnStatistic(1, COLUMNS.get(5), noMetrics, Map.of(), null);
        Assertions.assertTrue(number.isInfiniteRange(), number.toString());
        Assertions.assertEquals(Base.buildColumnStatistic(1, COLUMNS.get(5), noMetrics, Map.of()).getNullsFraction(),
                number.getNullsFraction());
        Assertions.assertNull(provider.buildColumnStatistic(6, COLUMNS.get(6), noMetrics, Map.of(), null)
                .getMaxString());
    }

    @Test
    public void testNullPartitionKeepsTheBounds() throws Exception {
        // region is an identity partition column; the file of the NULL partition has only NULL rows of it
        Types.NestedField region = Types.NestedField.optional(2, "region", Types.StringType.get());
        Map<Integer, Type.PrimitiveType> types = Map.of(1, Types.IntegerType.get(), 2, Types.StringType.get());
        List<PartitionField> fields = PartitionSpec.builderFor(new Schema(NUMBER_1, region)).identity("region")
                .build().fields();
        IcebergFileStats stats = new IcebergFileStats(types, List.of(NUMBER_1), 10, 100,
                new HashMap<>(Map.of(1, 1, 2, "a")), new HashMap<>(Map.of(1, 9, 2, "c")),
                new HashMap<>(Map.of(1, 0L, 2, 0L)), Map.of());
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        Method min = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMin", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        Method max = IcebergStatisticProvider.class.getDeclaredMethod("updateSummaryMax", IcebergFileStats.class,
                List.class, Map.class, Map.class, long.class);
        min.setAccessible(true);
        max.setAccessible(true);
        stats.incrementRecordCount(10);
        Map<Integer, Long> nulls = Map.of(1, 0L, 2, 10L);
        min.invoke(provider, stats, fields, Map.of(1, 1), nulls, 10L);
        max.invoke(provider, stats, fields, Map.of(1, 9), nulls, 10L);
        stats.updateNullCount(nulls);
        ColumnStatistic statistic = provider.buildColumnStatistic(2, new Column("region", VarcharType.VARCHAR), stats,
                Map.of(), MetricsModes.Full.get());
        Assertions.assertEquals("a", statistic.getMinString());
        Assertions.assertEquals("c", statistic.getMaxString());
    }

    @Test
    public void testStringBoundsForEstimates() {
        IcebergFileStats stats = files();
        IcebergStatisticProvider provider = new IcebergStatisticProvider();
        ColumnStatistic truncated = provider.buildColumnStatistic(4, COLUMNS.get(4), stats, Map.of(),
                MetricsModes.Truncate.withLength(16));
        Assertions.assertEquals(new StringBounds("2018-08-15T13:16", "2026-10-13T02:53", 16, true),
                truncated.getStringBounds());
        // Bounds of 16 characters may come from files written with the default truncate(16) before the mode
        // became full
        Assertions.assertEquals(new StringBounds("2018-08-15T13:16", "2026-10-13T02:53", 16, true),
                provider.buildColumnStatistic(4, COLUMNS.get(4), stats, Map.of(), MetricsModes.Full.get())
                        .getStringBounds());
        IcebergFileStats whole = new IcebergFileStats(Map.of(4, Types.StringType.get()), List.of(TEXT), 10, 100,
                new HashMap<>(Map.of(4, "2018-08-15T13:16:23")), new HashMap<>(Map.of(4, "2026-10-13T02:52:47")),
                new HashMap<>(Map.of(4, 0L)), Map.of());
        Assertions.assertEquals(new StringBounds("2018-08-15T13:16:23", "2026-10-13T02:52:47", StringBounds.WHOLE,
                false), provider.buildColumnStatistic(4, COLUMNS.get(4), whole, Map.of(), MetricsModes.Full.get())
                .getStringBounds());
        Assertions.assertEquals(8, provider.buildColumnStatistic(4, COLUMNS.get(4), whole, Map.of(),
                MetricsModes.Truncate.withLength(8)).getStringBounds().cutLength());
        Assertions.assertTrue(provider.buildColumnStatistic(4, COLUMNS.get(4), stats, Map.of(), null)
                .getStringBounds().maxMayBeCut());
        // A file left the bounds of s out: the estimates get none
        Assertions.assertNull(provider.buildColumnStatistic(6, COLUMNS.get(6), stats, Map.of(),
                MetricsModes.Truncate.withLength(16)).getStringBounds());
        Assertions.assertNull(provider.buildColumnStatistic(1, COLUMNS.get(1), stats, Map.of(), null)
                .getStringBounds());
        // The cast of created_at to DATETIME reaches to the end of the minute the cut upper bound holds
        ColumnRefOperator ref = new ColumnRefOperator(4, VarcharType.VARCHAR, "created_at", true);
        Statistics statistics = Statistics.builder().setOutputRowCount(20).addColumnStatistic(ref, truncated).build();
        ColumnStatistic cast = ExpressionStatisticCalculator.calculate(new CastOperator(DateType.DATETIME, ref),
                statistics);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2018, 8, 15, 13, 16)), cast.getMinValue());
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2026, 10, 13, 2, 53, 59)),
                cast.getMaxValue());
    }

    // IcebergStatisticProvider.buildColumnStatistic and its helpers as they are on the base: the reference that the
    // statistics of the connector keep.
    private static final class Base {
        static ColumnStatistic buildColumnStatistic(Integer fieldId, Column column, IcebergFileStats icebergStats,
                                                    Map<Integer, Long> colIdToNdv) {
            ColumnStatistic.Builder builder = ColumnStatistic.builder();
            if (icebergStats.canUseStats(fieldId, icebergStats.getMinValues())) {
                if (column.getType().isStringType()) {
                    builder.setMinString(icebergStats.getMinValues().get(fieldId).toString());
                } else {
                    icebergStats.getMinValue(fieldId).ifPresent(builder::setMinValue);
                }
            }
            if (icebergStats.canUseStats(fieldId, icebergStats.getMaxValues())) {
                if (column.getType().isStringType()) {
                    builder.setMaxString(icebergStats.getMaxValues().get(fieldId).toString());
                } else {
                    icebergStats.getMaxValue(fieldId).ifPresent(builder::setMaxValue);
                }
            }
            Long nullCount = icebergStats.getNullCounts() == null ? null : icebergStats.getNullCounts().get(fieldId);
            builder.setNullsFraction(nullCount != null ? nullCount * 1.0 / Math.max(icebergStats.getRecordCount(), 1)
                    : 0);
            Map<Integer, Long> columnSizes = icebergStats.getColumnSizes();
            long columnSizeRecordCount = icebergStats.getColumnSizeRecordCount(fieldId);
            if (columnSizes != null && columnSizes.containsKey(fieldId) && columnSizeRecordCount > 0) {
                double physicalSize = (double) columnSizes.get(fieldId) / columnSizeRecordCount;
                builder.setAverageRowSize(Math.max(physicalSize, column.getType().getTypeSize()));
            } else {
                builder.setAverageRowSize(column.getType().getTypeSize());
            }
            Long ndv = colIdToNdv.get(fieldId);
            if (ndv != null) {
                builder.setDistinctValuesCount(Math.max(Math.min(ndv, icebergStats.getRecordCount()), 1));
            } else {
                builder.setDistinctValuesCount(estimateNdvFallback(fieldId, icebergStats));
            }
            builder.setType(ColumnStatistic.StatisticType.ESTIMATE);
            return builder.build();
        }

        private static double estimateNdvFallback(Integer fieldId, IcebergFileStats icebergStats) {
            Optional<Double> minVal = icebergStats.getMinValue(fieldId);
            Optional<Double> maxVal = icebergStats.getMaxValue(fieldId);
            Map<Integer, Long> columnSizes = icebergStats.getColumnSizes();
            long colSizeBytes = (columnSizes != null && columnSizes.containsKey(fieldId))
                    ? columnSizes.get(fieldId) : -1L;
            return ConnectorNdvEstimator.estimate(toTypeCategory(fieldId, icebergStats), minVal.orElse(Double.NaN),
                    maxVal.orElse(Double.NaN), colSizeBytes, icebergStats.getColumnSizeRecordCount(fieldId),
                    icebergStats.getRecordCount(), icebergTypeWidthBytes(fieldId, icebergStats));
        }

        private static int icebergTypeWidthBytes(Integer fieldId, IcebergFileStats icebergStats) {
            Type.PrimitiveType type = icebergStats.getIdToTypeMapping().get(fieldId);
            if (type instanceof Types.BooleanType) {
                return 1;
            } else if (type instanceof Types.LongType || type instanceof Types.DoubleType
                    || type instanceof Types.TimestampType || type instanceof Types.TimeType) {
                return 8;
            } else if (type instanceof Types.IntegerType || type instanceof Types.FloatType
                    || type instanceof Types.DateType) {
                return 4;
            }
            return 0;
        }

        private static ConnectorNdvEstimator.TypeCategory toTypeCategory(Integer fieldId, IcebergFileStats stats) {
            Type.PrimitiveType type = stats.getIdToTypeMapping().get(fieldId);
            if (type instanceof Types.BooleanType) {
                return ConnectorNdvEstimator.TypeCategory.BOOLEAN;
            } else if (type instanceof Types.IntegerType || type instanceof Types.LongType) {
                return ConnectorNdvEstimator.TypeCategory.INTEGER_LIKE;
            } else if (type instanceof Types.FloatType || type instanceof Types.DoubleType) {
                return ConnectorNdvEstimator.TypeCategory.FLOAT_LIKE;
            } else if (type instanceof Types.DateType) {
                return ConnectorNdvEstimator.TypeCategory.DATE_IN_EPOCH_SECONDS;
            } else if (type instanceof Types.TimestampType) {
                return ConnectorNdvEstimator.TypeCategory.TIMESTAMP_IN_EPOCH_MICROS;
            } else if (type instanceof Types.StringType || type instanceof Types.BinaryType) {
                return ConnectorNdvEstimator.TypeCategory.STRING_LIKE;
            }
            return ConnectorNdvEstimator.TypeCategory.OTHER;
        }
    }
}

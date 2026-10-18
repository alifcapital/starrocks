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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The key of the estimate of the predicate of a scan over the statistics of the scan, see
 * ExpressionStatisticsCache#scanEstimate. The estimate is a function of the predicate, the statistics and the session,
 * and the cache that keeps it lives in the optimizer context of one query, so the key holds:
 * - the predicate with, per operator, its class, its own properties, its type, the flags the estimators read and its
 *   children, and for a column reference its id, type, nullability and name;
 * - every value of the statistics, the doubles as their bits, and the identity of the immutable objects they refer to:
 *   histograms, MCV heads and statistics on expressions;
 * - the session values the estimate reads, also through the constant folding it runs on head values, see #session().
 * Two scans of one table that read it through other column references, such as two copies of one CTE, have other
 * keys. We compare operators by the classes we know to copy deeply, see #copy(), and do not key a predicate with
 * another operator.
 */
final class ScanEstimateKey {
    private static final Set<Class<?>> KNOWN = Set.of(ColumnRefOperator.class, ConstantOperator.class,
            BinaryPredicateOperator.class, InPredicateOperator.class, IsNullPredicateOperator.class,
            LikePredicateOperator.class, CompoundPredicateOperator.class, BetweenPredicateOperator.class,
            CallOperator.class, CastOperator.class, CaseWhenOperator.class);

    private final ScalarOperator predicate;
    private final List<Object> values;
    private final int hash;

    record ColumnKey(int id, Type type, boolean nullable, String name) {
        static ColumnKey of(ColumnRefOperator column) {
            return column == null ? null
                    : new ColumnKey(column.getId(), column.getType(), column.isNullable(), column.getName());
        }
    }

    private record ColumnValue(long min, long max, long nulls, long size, long ndv, long collection,
                               long sourceNdv, ExpressionStatisticsCache.Identity histogram,
                               ColumnStatistic.StatisticType type, String minString, String maxString,
                               boolean defaultSelectivity) {
        static ColumnValue of(ColumnStatistic statistic) {
            return new ColumnValue(bits(statistic.getMinValue()), bits(statistic.getMaxValue()),
                    bits(statistic.getNullsFraction()), bits(statistic.getAverageRowSize()),
                    bits(statistic.getDistinctValuesCount()), bits(statistic.getCollectionSize()),
                    bits(statistic.getSourceDistinctValuesCount()),
                    new ExpressionStatisticsCache.Identity(statistic.getHistogram()), statistic.getType(),
                    statistic.getMinString(), statistic.getMaxString(),
                    statistic.isDistinctValuesFromDefaultSelectivity());
        }
    }

    private record GroupValue(long ndv, long rowCount, List<ColumnKey> columns,
                              ExpressionStatisticsCache.Identity mcv, List<Long> nullCounts) {
        static GroupValue of(MultiColumnCombinedStats group) {
            List<ColumnKey> columns = new ArrayList<>(group.getColumns().size());
            for (ColumnRefOperator column : group.getColumns()) {
                columns.add(ColumnKey.of(column));
            }
            return new GroupValue(group.getNdv(), bits(group.getRowCount()), columns,
                    new ExpressionStatisticsCache.Identity(group.getMcvDistribution()), group.getNullCounts());
        }
    }

    private ScanEstimateKey(ScalarOperator predicate, List<Object> values, int hash) {
        this.predicate = predicate;
        this.values = values;
        this.hash = hash;
    }

    /** The key of the estimate, null when the predicate holds an operator the key does not compare. */
    static ScanEstimateKey of(ScalarOperator predicate, Statistics statistics) {
        if (!known(predicate)) {
            return null;
        }
        List<Object> values = List.of(session(), content(statistics));
        return new ScanEstimateKey(predicate, values, 31 * hash(predicate) + values.hashCode());
    }

    // A key that a later change of the predicate cannot change, to keep in the cache.
    ScanEstimateKey copy() {
        return new ScanEstimateKey(predicate.clone(), values, hash);
    }

    private static boolean known(ScalarOperator operator) {
        if (!KNOWN.contains(operator.getClass())) {
            return false;
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (!known(child)) {
                return false;
            }
        }
        return true;
    }

    private static long bits(double value) {
        return Double.doubleToRawLongBits(value);
    }

    // The session values the estimate reads: the switches of the estimators, and what the constant folding of a call
    // on a head value reads, the time zone, the base type of an equality of a number with a string, the
    // consolidation of LIKE and the start time of the query for now(). The check of the ranges implied by another
    // conjunct reads the IS NULL alternatives of the range extraction. The estimate is a function of them, so the key
    // holds them, even though the cache lives in the optimizer context of one query.
    static List<Object> session() {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            return List.of();
        }
        SessionVariable variables = context.getSessionVariable();
        return Arrays.asList(variables.isCboEnableMcvEstimate(), variables.isUseCorrelatedPredicateEstimate(),
                variables.isDisableFunctionFoldConstants(), variables.isCboDecimalCastStringStrict(),
                variables.isCboEnableHistogramJoinEstimation(), variables.isEnableMonotonicPredicateEstimate(),
                variables.isEnableMonotonicPredicateRewrite(), variables.getSqlMode(), variables.getTimeZone(),
                variables.getCboEqBaseType(), variables.getLikePredicateConsolidateMin(),
                variables.isCboDerivePredicateNullAlternative(), context.getStartTimeInstant());
    }

    static List<Object> content(Statistics statistics) {
        Map<ColumnKey, ColumnValue> columns = new HashMap<>();
        statistics.getColumnStatistics().forEach((column, statistic) ->
                columns.put(ColumnKey.of(column), ColumnValue.of(statistic)));
        Map<Set<ColumnKey>, GroupValue> groups = new HashMap<>();
        statistics.getMultiColumnCombinedStats().forEach((key, group) ->
                groups.put(columnKeys(key), GroupValue.of(group)));
        List<Object> content = new ArrayList<>();
        content.add(bits(statistics.getOutputRowCount()));
        content.add(statistics.isTableRowCountMayInaccurate());
        content.add(statistics.getStatsSource());
        content.add(statistics.isPartitionRestricted());
        content.add(new ExpressionStatisticsCache.Identity(statistics.getJoinStatisticsScope()));
        content.add(new ExpressionStatisticsCache.Identity(statistics.getJoinStatisticsPlanner()));
        content.add(new ExpressionStatisticsCache.Identity(statistics.getScanExpressionStatistics()));
        content.add(columnKeys(statistics.getShadowColumns()));
        content.add(columns);
        content.add(groups);
        return content;
    }

    private static Set<ColumnKey> columnKeys(Collection<ColumnRefOperator> columns) {
        Set<ColumnKey> keys = new HashSet<>();
        for (ColumnRefOperator column : columns) {
            keys.add(ColumnKey.of(column));
        }
        return keys;
    }

    private static int hash(ScalarOperator operator) {
        int result = 31 * operator.getClass().hashCode() + operator.hashCodeSelf();
        result = 31 * result + Objects.hashCode(operator.getType());
        result = 31 * result + flags(operator);
        for (ScalarOperator child : operator.getChildren()) {
            result = 31 * result + hash(child);
        }
        return result;
    }

    // The flags of an operator that the estimators read.
    private static int flags(ScalarOperator operator) {
        int flags = (operator.isNotEvalEstimate() ? 1 : 0) | (operator.isFromPredicateRangeDerive() ? 2 : 0)
                | (operator.isRedundant() ? 4 : 0) | (operator.isPushdown() ? 8 : 0)
                | (operator.isCorrelated() ? 16 : 0) | (operator.isJoinDerived() ? 32 : 0);
        if (operator instanceof ColumnRefOperator column) {
            flags |= column.isNullable() ? 64 : 0;
        }
        return flags;
    }

    private static boolean same(ScalarOperator left, ScalarOperator right) {
        if (left.getClass() != right.getClass() || !left.equalsSelf(right)
                || !Objects.equals(left.getType(), right.getType()) || flags(left) != flags(right)
                || left.getChildren().size() != right.getChildren().size()) {
            return false;
        }
        if (left instanceof ColumnRefOperator column
                && !Objects.equals(column.getName(), ((ColumnRefOperator) right).getName())) {
            return false;
        }
        for (int i = 0; i < left.getChildren().size(); i++) {
            if (!same(left.getChildren().get(i), right.getChildren().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ScanEstimateKey key && key.hash == hash && values.equals(key.values)
                && same(predicate, key.predicate);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}

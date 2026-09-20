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

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

/**
 * Estimates the selectivity of an inner equi-join on a composite key from the MCV lists of the two
 * key groups, as PostgreSQL's eqjoinsel_inner does with single-column MCV lists: head tuples that
 * match on both sides join exactly; a head tuple without a match on the other side joins that side's
 * tail, spread over its tail distinct tuples; the two tails join each other at the same rate. The
 * smaller of the two directional estimates is taken. A tuple with a NULL component never joins.
 */
public class MultiColumnJoinMcvEstimator {
    /**
     * The join selectivity over the cross product, with leftKey.get(i) = rightKey.get(i) the join
     * predicates. Empty unless both groups are exactly their side's key and carry an MCV list.
     */
    public static OptionalDouble estimateSelectivity(MultiColumnCombinedStats left, List<ColumnRefOperator> leftKey,
                                                     double leftNullsFraction, MultiColumnCombinedStats right,
                                                     List<ColumnRefOperator> rightKey, double rightNullsFraction) {
        if (!coversKey(left, leftKey) || !coversKey(right, rightKey) || leftKey.size() != rightKey.size()) {
            return OptionalDouble.empty();
        }
        Side leftSide = new Side(left, leftKey, leftNullsFraction);
        Side rightSide = new Side(right, rightKey, rightNullsFraction);

        double matchProduct = 0;
        double matchedLeft = 0;
        double matchedRight = 0;
        int matches = 0;
        for (Map.Entry<List<String>, Double> entry : leftSide.head.entrySet()) {
            Double rightShare = rightSide.head.get(entry.getKey());
            if (rightShare == null) {
                continue;
            }
            matchProduct += entry.getValue() * rightShare;
            matchedLeft += entry.getValue();
            matchedRight += rightShare;
            matches++;
        }
        double unmatchedLeft = Math.max(0, leftSide.headShare - matchedLeft);
        double unmatchedRight = Math.max(0, rightSide.headShare - matchedRight);

        double selectivity = Math.min(
                directional(matchProduct, unmatchedLeft, leftSide.tailShare, rightSide, unmatchedRight, matches),
                directional(matchProduct, unmatchedRight, rightSide.tailShare, leftSide, unmatchedLeft, matches));
        return OptionalDouble.of(Math.min(1.0, Math.max(0.0, selectivity)));
    }

    // The rows of one side against the other side's tail: its unmatched head tuples each meet one
    // tail tuple on average, and its tail meets the other side's tail and unmatched head at the rate
    // of the other side's tail tuples.
    private static double directional(double matchProduct, double unmatchedHead, double tailShare, Side other,
                                      double otherUnmatchedHead, int matches) {
        double selectivity = matchProduct;
        if (other.ndv > other.headTuples) {
            selectivity += unmatchedHead * other.tailShare / (other.ndv - other.headTuples);
        }
        if (other.ndv > matches) {
            selectivity += tailShare * (other.tailShare + otherUnmatchedHead) / (other.ndv - matches);
        }
        return selectivity;
    }

    private static boolean coversKey(MultiColumnCombinedStats stats, List<ColumnRefOperator> key) {
        return stats != null && stats.hasMcv() && stats.isComplete() && stats.getNdv() > 0
                && stats.getColumns().size() == key.size() && stats.getColumns().containsAll(key);
    }

    private static class Side {
        // Head tuples without a NULL component, projected onto the key in predicate order.
        final Map<List<String>, Double> head = new HashMap<>();
        // Share of the head tuples without a NULL.
        final double headShare;
        // Share of the rows outside the head that hold no NULL in the key.
        final double tailShare;
        final double ndv;
        final double headTuples;

        Side(MultiColumnCombinedStats stats, List<ColumnRefOperator> key, double nullsFraction) {
            List<Integer> positions = new ArrayList<>(key.size());
            for (ColumnRefOperator column : key) {
                positions.add(stats.getColumns().indexOf(column));
            }
            double total = 0;
            double nonNull = 0;
            for (MultiColumnCombinedStats.McvEntry entry : stats.getMcv()) {
                double share = entry.getCount() / (double) stats.getRowCount();
                total += share;
                List<String> projection = new ArrayList<>(key.size());
                for (int i = 0; i < key.size(); i++) {
                    String value = entry.getValues().get(positions.get(i));
                    if (value == null) {
                        projection = null;
                        break;
                    }
                    projection.add(canonical(key.get(i).getType(), value));
                }
                if (projection != null) {
                    head.merge(projection, share, Double::sum);
                    nonNull += share;
                }
            }
            this.headShare = nonNull;
            // The head tuples with a NULL are already out; the rest of the NULL rows are in the tail.
            this.tailShare = Math.max(0, 1.0 - nullsFraction - nonNull);
            this.ndv = stats.getNdv();
            this.headTuples = stats.getMcv().size();
        }
    }

    // The text of a value that two sides of different numeric types agree on: exact for integers and
    // decimals, since a double cannot tell large or high-precision values apart.
    static String canonical(Type type, String value) {
        try {
            if (type.isFixedPointType() || type.isDecimalV3() || type.isDecimalV2()) {
                return new BigDecimal(value).stripTrailingZeros().toPlainString();
            }
            if (type.isFloatingPointType()) {
                return Double.toString(Double.parseDouble(value));
            }
            if (type.isDate() || type.isDatetime()) {
                Optional<Double> number = StatisticUtils.convertStatisticsToDouble(type, value);
                if (number.isPresent()) {
                    return Double.toString(number.get());
                }
            }
        } catch (RuntimeException e) {
            // keep the text
        }
        return value;
    }
}

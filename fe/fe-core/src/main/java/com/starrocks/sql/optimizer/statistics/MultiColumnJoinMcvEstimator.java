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
import java.util.Collections;
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
        final Map<List<String>, Double> head;
        // Share of the head tuples without a NULL.
        final double headShare;
        // Share of the rows outside the head that hold no NULL in the key.
        final double tailShare;
        final double ndv;
        final double headTuples;

        Side(MultiColumnCombinedStats stats, List<ColumnRefOperator> key, double nullsFraction) {
            PreparedHead prepared = stats.getJoinHead(key);
            this.head = prepared.head;
            this.headShare = prepared.nonNull;
            this.tailShare = Math.max(0, Math.min(1.0 - stats.getMcvDistribution().getTotalShare(),
                    1.0 - nullsFraction - prepared.nonNull));
            this.ndv = stats.getNdv();
            this.headTuples = stats.getMcv().size();
        }
    }

    static final class PreparedHead {
        private final List<Integer> positions;
        private final List<Type> types;
        private final Map<List<String>, Double> head;
        private final double nonNull;

        PreparedHead(MultiColumnCombinedStats stats, List<ColumnRefOperator> key) {
            positions = new ArrayList<>(key.size());
            types = new ArrayList<>(key.size());
            for (ColumnRefOperator column : key) {
                positions.add(stats.getColumns().indexOf(column));
                types.add(column.getType().clone());
            }
            Map<List<String>, Double> values = new HashMap<>();
            double nonNullSum = 0;
            for (int t = 0; t < stats.getMcv().size(); t++) {
                MultiColumnCombinedStats.McvEntry entry = stats.getMcv().get(t);
                List<String> projection = new ArrayList<>(key.size());
                for (int i = 0; i < key.size(); i++) {
                    String value = entry.getValues().get(positions.get(i));
                    if (value == null) {
                        projection = null;
                        break;
                    }
                    projection.add(canonical(types.get(i), value));
                }
                if (projection != null) {
                    double share = stats.getMcvDistribution().getShare(t);
                    values.merge(projection, share, Double::sum);
                    nonNullSum += share;
                }
            }
            head = Collections.unmodifiableMap(values);
            nonNull = nonNullSum;
        }

        boolean matches(MultiColumnCombinedStats stats, List<ColumnRefOperator> key) {
            if (positions.size() != key.size()) {
                return false;
            }
            for (int i = 0; i < key.size(); i++) {
                if (positions.get(i) != stats.getColumns().indexOf(key.get(i))
                        || !types.get(i).equals(key.get(i).getType())) {
                    return false;
                }
            }
            return true;
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
                double number = Double.parseDouble(value);
                if (type.isFloat()) {
                    number = (float) number;
                }
                return Double.toString(number == 0 ? 0 : number);
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

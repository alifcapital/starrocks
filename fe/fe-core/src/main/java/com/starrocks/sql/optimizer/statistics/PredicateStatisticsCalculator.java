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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.sql.spm.SPMFunctions;
import com.starrocks.type.BooleanType;
import org.apache.commons.math3.util.Precision;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.HistogramStatisticsUtils.estimateInPredicateWithHistogram;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateUtils.computeCompoundStatsWithMultiColumnOptimize;

public class PredicateStatisticsCalculator {
    private static final Logger LOG = LogManager.getLogger(PredicateStatisticsCalculator.class);

    public static Statistics statisticsCalculate(ScalarOperator predicate, Statistics statistics) {
        return statisticsCalculate(predicate, statistics, true);
    }

    /**
     * The estimate of the predicate of a scan with statistics on expressions of its columns. The conditions that
     * can be written over the virtual columns are estimated over them on a copy of the predicate; the predicate and
     * the plan do not change. The row count comes from that estimate. The statistics of the real columns come from
     * the estimate of the predicate as written, bounded by that row count: x knows how many rows a prefix holds,
     * not how many distinct values of c they have. Any failure on this path leaves the estimate as before.
     *
     * @param expressions the statistics on expressions of the scan; the caller passes them only for a scan
     */
    public static Statistics statisticsCalculate(ScalarOperator predicate, Statistics statistics, boolean useMcv,
                                                 ScanExpressionStatistics expressions) {
        Statistics plain = statisticsCalculate(predicate, statistics, useMcv);
        if (expressions == null || predicate == null) {
            return plain;
        }
        try {
            ExpressionStatisticsMatcher.Result match = ExpressionStatisticsMatcher.match(predicate, expressions,
                    statistics);
            if (match == null) {
                Set<ColumnRefOperator> describing = Sets.newHashSet();
                for (ScanExpressionStatistics.Entry entry : expressions.getEntries()) {
                    if (ExpressionStatisticsMatcher.describesFilteredRows(predicate, entry, statistics)) {
                        describing.add(entry.getColumn());
                    }
                }
                return Statistics.buildFrom(plain).setScanExpressionStatistics(
                        expressions.afterFilter(Map.of(), describing, null, false)).build();
            }
            Statistics.Builder virtual = Statistics.buildFrom(
                    expressions.withVirtualColumns(statistics, match.getUsedColumns()));
            // The share of x leaves out the NULL rows of a source column with a written condition that is never true
            // on NULL, so a kept condition on that column must not take them out again.
            for (ColumnRefOperator column : match.getNonNullColumns()) {
                ColumnStatistic statistic = statistics.getColumnStatistics().get(column);
                if (statistic != null && !statistic.isUnknown()) {
                    virtual.addColumnStatistic(column, ColumnStatistic.buildFrom(statistic).setNullsFraction(0).build());
                }
            }
            Statistics withVirtual = virtual.build();
            Statistics estimated = expressions.getCache().within(
                    () -> statisticsCalculate(match.getPredicate(), withVirtual, useMcv));
            double rowCount = estimated.getOutputRowCount();
            // The plain estimate gives only the form of the column statistics here, not the row count; a default
            // selectivity mark on it describes a guess that the row count does not use, unless the estimate over
            // x makes the same guess on the column, as for a condition on another column it cannot write.
            Statistics.Builder columns = Statistics.buildFrom(plain);
            double distinctBound = Math.max(1, rowCount);
            Set<ColumnRefOperator> nonNullColumns = match.getNonNullColumns();
            // The columns of the conditions the copy keeps as they are: both estimates evaluate them.
            Set<ColumnRefOperator> keptColumns = Sets.newHashSet();
            List<ScalarOperator> keptConjuncts = new ArrayList<>();
            for (ScalarOperator conjunct : Utils.extractConjuncts(match.getPredicate())) {
                List<ColumnRefOperator> read = Utils.extractColumnRef(conjunct);
                if (read.stream().noneMatch(ExpressionStatisticsEstimator::isVirtualColumn)) {
                    keptColumns.addAll(read);
                    keptConjuncts.add(conjunct);
                }
            }
            // The plain NDV of a kept column can still hold the guess of a condition on f, so a guessed column takes
            // its NDV from the kept conditions alone, estimated once when a column needs it.
            Statistics[] keptOnly = new Statistics[1];
            plain.getColumnStatistics().forEach((column, statistic) -> {
                ColumnStatistic overX = estimated.getColumnStatistics().get(column);
                boolean kept = keptColumns.contains(column);
                boolean plainGuess = statistic.isDistinctValuesFromDefaultSelectivity();
                boolean xGuess = overX != null && overX.isDistinctValuesFromDefaultSelectivity();
                // The mark stays when the estimate over x makes the same guess on the column.
                boolean guessed = plainGuess && !xGuess;
                boolean nonNull = nonNullColumns.contains(column);
                double ndv = statistic.getDistinctValuesCount();
                boolean mark = plainGuess && xGuess;
                if (kept) {
                    if (plainGuess) {
                        if (keptOnly[0] == null) {
                            keptOnly[0] = statisticsCalculate(Utils.compoundAnd(keptConjuncts), statistics, useMcv);
                        }
                        ColumnStatistic alone = keptOnly[0].getColumnStatistics().get(column);
                        if (alone != null && !alone.isUnknown()) {
                            ndv = alone.getDistinctValuesCount();
                            mark = alone.isDistinctValuesFromDefaultSelectivity() && (overX == null || xGuess);
                        }
                    }
                    // The estimate over x evaluates the kept conditions as well and can keep fewer values.
                    if (overX != null && !overX.isUnknown()) {
                        ndv = Math.min(ndv, overX.getDistinctValuesCount());
                    }
                } else if (guessed || !statistic.isUnknown()
                        && statistic.getDistinctValuesCount() >= Math.max(1, plain.getOutputRowCount()) * (1 - 1e-9)) {
                    // The NDV of the plain estimate follows its guessed row count, so the column takes its NDV from
                    // the estimate over x; the scan gives it when the estimate over x does not have the column. An NDV
                    // at the plain row count is the cap by that row count, which a formula on f can make tiny without
                    // a mark, so it is taken the same way; an NDV below the plain row count is a narrowing the plain
                    // estimate counted, and it stays.
                    ColumnStatistic source = overX != null ? overX : statistics.getColumnStatistics().get(column);
                    if (source != null && !source.isUnknown()) {
                        ndv = source.getDistinctValuesCount();
                    }
                }
                // With a column of unknown statistics the adjustment below leaves every column as is, so the NDV
                // of a known column is bounded by the row count here.
                ndv = Math.min(ndv, distinctBound);
                mark |= !statistic.isUnknown() && keepsGuessOverX(ndv, overX, distinctBound);
                boolean changed = !statistic.isUnknown() && ndv != statistic.getDistinctValuesCount();
                if (!changed && !nonNull && mark == plainGuess) {
                    return;
                }
                ColumnStatistic.Builder adjusted = ColumnStatistic.buildFrom(statistic)
                        .setDistinctValuesFromDefaultSelectivity(mark);
                if (!statistic.isUnknown()) {
                    adjusted.setDistinctValuesCount(ndv);
                }
                if (nonNull) {
                    // A written condition on the column is never true on NULL, and the row count from x leaves
                    // the NULL rows out.
                    adjusted.setNullsFraction(0);
                }
                columns.addColumnStatistic(column, adjusted.build());
            });
            Statistics result = StatisticsEstimateUtils.adjustStatisticsByRowCount(
                    columns.setOutputRowCount(rowCount).build(), rowCount);
            Map<ColumnRefOperator, ColumnStatistic> filtered = new HashMap<>();
            for (ColumnRefOperator column : match.getUsedColumns()) {
                ColumnStatistic statistic = estimated.getColumnStatistics().get(column);
                if (statistic != null) {
                    filtered.put(column, statistic);
                }
            }
            List<ScalarOperator> virtualConjuncts = Utils.extractConjuncts(match.getPredicate()).stream()
                    .filter(conjunct -> Utils.extractColumnRef(conjunct).stream()
                            .anyMatch(ExpressionStatisticsEstimator::isVirtualColumn))
                    .collect(Collectors.toList());
            return Statistics.buildFrom(result).setOutputRowCount(result.getOutputRowCount())
                    .setScanExpressionStatistics(expressions.afterFilter(filtered, match.getFilteredColumns(),
                            virtualConjuncts.isEmpty() ? null : Utils.compoundAnd(virtualConjuncts), match.hasOr()))
                    .build();
        } catch (RuntimeException e) {
            LOG.warn("Failed to estimate the predicate of table {} with statistics on expressions, predicate {}",
                    expressions.getTableName(), predicate, e);
            return plain;
        }
    }

    /**
     * Whether the NDV of a column after the merge is the guess the estimate over x made on it: the NDV over x under
     * its mark, or the bound by the row count from x, which the guess set. An NDV between them is a value the plain
     * estimate counted, so it does not take the mark.
     */
    static boolean keepsGuessOverX(double ndv, ColumnStatistic overX, double distinctBound) {
        return overX != null && overX.isDistinctValuesFromDefaultSelectivity()
                && (ndv == overX.getDistinctValuesCount() || ndv == distinctBound);
    }

    /**
     * @param useMcv whether predicates on the columns of an MCV list are estimated from the list; off for
     *               the plain estimates the MCV estimation itself is built from
     */
    public static Statistics statisticsCalculate(ScalarOperator predicate, Statistics statistics, boolean useMcv) {
        if (predicate == null) {
            return statistics;
        }
        return finishEstimate(predicate, statistics, visit(predicate, statistics, useMcv), useMcv);
    }

    /**
     * The row count of statisticsCalculate(predicate, statistics, false). The MCV estimators ask for the plain row
     * count of a condition many times, once per value of a head tuple, so we skip the propagation of the MCV lists
     * that the estimate ends with: it builds the distributions and the column statistics of the output and keeps
     * its row count.
     */
    static double plainRowCount(ScalarOperator predicate, Statistics statistics) {
        if (predicate == null) {
            return statistics.getOutputRowCount();
        }
        return visit(predicate, statistics, false).getOutputRowCount();
    }

    private static Statistics visit(ScalarOperator predicate, Statistics statistics, boolean useMcv) {
        // The time-complexity of PredicateStatisticsCalculatingVisitor OR row-count is O(2^n), n is OR number
        if (countDisConsecutiveOr(predicate, 0, false) > StatisticsEstimateCoefficient.DEFAULT_OR_OPERATOR_LIMIT) {
            return predicate.accept(new LargeOrCalculatingVisitor(statistics, useMcv), null);
        }
        return predicate.accept(new BaseCalculatingVisitor(statistics, useMcv), null);
    }

    private static Statistics finishEstimate(ScalarOperator predicate, Statistics statistics, Statistics output,
                                            boolean useMcv) {
        if (useMcv) {
            java.util.OptionalDouble exact = McvStatisticsPropagation.completeHeadRows(predicate, statistics);
            if (exact.isPresent()) {
                output = Statistics.buildFrom(output).setOutputRowCount(Math.max(1, exact.getAsDouble())).build();
            }
        }
        return McvStatisticsPropagation.filter(predicate, statistics, output);
    }

    static boolean allConstantOperators(List<ScalarOperator> operators, int from) {
        for (int i = from; i < operators.size(); i++) {
            if (!(operators.get(i) instanceof ConstantOperator)) {
                return false;
            }
        }
        return true;
    }

    // The distinct values of operators[from..] after the mapping, in the order of their first occurrence. An IN list
    // can hold thousands of literals and its statistics are derived many times per query, so we use a presized set
    // and a plain loop instead of a stream.
    static List<ScalarOperator> distinctFrom(List<ScalarOperator> operators, int from,
                                             UnaryOperator<ScalarOperator> mapping) {
        int count = operators.size() - from;
        if (count <= 0) {
            return List.of();
        }
        List<ScalarOperator> result = new ArrayList<>(count);
        Set<ScalarOperator> seen = Sets.newHashSetWithExpectedSize(count);
        for (int i = from; i < operators.size(); i++) {
            ScalarOperator value = mapping.apply(operators.get(i));
            if (seen.add(value)) {
                result.add(value);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static long countDisConsecutiveOr(ScalarOperator root, long count, boolean isConsecutive) {
        boolean isOr = OperatorType.COMPOUND.equals(root.getOpType()) && ((CompoundPredicateOperator) root).isOr();
        if (isOr && !isConsecutive) {
            count = count + 1;
        }

        for (ScalarOperator child : root.getChildren()) {
            count = countDisConsecutiveOr(child, count, isOr);
        }

        return count;
    }

    // Preserve signed zero and NaN payloads as well as numerical values when deciding whether to reuse.
    private static boolean sameOrColumnValues(ColumnStatistic column, double min, double max,
                                              double distinct, double nulls) {
        return Double.doubleToRawLongBits(column.getMinValue()) == Double.doubleToRawLongBits(min)
                && Double.doubleToRawLongBits(column.getMaxValue()) == Double.doubleToRawLongBits(max)
                && Double.doubleToRawLongBits(column.getDistinctValuesCount()) == Double.doubleToRawLongBits(distinct)
                && Double.doubleToRawLongBits(column.getNullsFraction()) == Double.doubleToRawLongBits(nulls);
    }

    private static Statistics finishOrStatistics(Statistics input, Statistics.Builder builder, double rowCount) {
        if (builder != null) {
            return builder.build();
        }
        // withOutputRowCount treats NaNs as equal. The old OR builder retained the newly computed
        // payload, so keep its behavior when only the raw NaN row-count bits differ.
        if (Double.isNaN(rowCount) && Double.isNaN(input.getOutputRowCount())
                && Double.doubleToRawLongBits(rowCount) != Double.doubleToRawLongBits(input.getOutputRowCount())) {
            return Statistics.buildFrom(input).setOutputRowCount(rowCount).build();
        }
        return input.withOutputRowCount(rowCount);
    }

    private static class BaseCalculatingVisitor extends ScalarOperatorVisitor<Statistics, Void> {
        protected final Statistics statistics;
        protected final boolean useMcv;

        public BaseCalculatingVisitor(Statistics statistics, boolean useMcv) {
            this.statistics = statistics;
            this.useMcv = useMcv;
        }

        /**
         * A predicate on a column of an MCV list, estimated from the list: the row count comes from the
         * MCV estimate, the column statistics from the plain estimate of the predicate. Empty when no
         * MCV list answers the predicate.
         */
        protected Optional<Statistics> estimateWithMcv(ScalarOperator predicate) {
            if (!useMcv) {
                return Optional.empty();
            }
            Optional<MultiColumnMcvEstimator.Result> mcv =
                    MultiColumnMcvEstimator.estimate(List.of(predicate), statistics);
            if (mcv.isEmpty()) {
                return Optional.empty();
            }
            double rowCount = statistics.getOutputRowCount() * mcv.get().getSelectivity();
            Statistics plain = predicate.accept(new BaseCalculatingVisitor(statistics, false), null);
            Statistics estimated = Statistics.buildFrom(plain).setOutputRowCount(rowCount).build();
            return Optional.of(StatisticsEstimateUtils.adjustStatisticsByRowCount(estimated, rowCount));
        }

        protected boolean checkNeedEvalEstimate(ScalarOperator predicate) {
            if (predicate == null) {
                return false;
            }
            // check predicate need to eval
            if (predicate.isNotEvalEstimate()) {
                return false;
            }
            // extract range predicate scalar operator with unknown column statistics will not eval
            if (predicate.isFromPredicateRangeDerive()) {
                for (ColumnStatistic cs : statistics.getColumnStatistics().values()) {
                    if (cs.isUnknown()) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public Statistics visit(ScalarOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            double outputRowCount =
                    statistics.getOutputRowCount() * StatisticsEstimateCoefficient.PREDICATE_UNKNOWN_FILTER_COEFFICIENT;
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                    statistics.withOutputRowCount(outputRowCount),
                    outputRowCount, true);
        }

        @Override
        public Statistics visitVariableReference(ColumnRefOperator variable, Void context) {
            if (!checkNeedEvalEstimate(variable)) {
                return statistics;
            }

            if (!variable.getType().isBoolean()) {
                return visit(variable, context);
            }

            try {
                BinaryPredicateOperator binaryPredicateOperator = new BinaryPredicateOperator(
                        BinaryType.EQ, variable, ConstantOperator.createBoolean(true));
                return visitBinaryPredicate(binaryPredicateOperator, context);
            } catch (Exception e) {
                return visit(variable, context);
            }
        }

        @Override
        public Statistics visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            Optional<Statistics> mcv = estimateWithMcv(predicate);
            if (mcv.isPresent()) {
                return mcv.get();
            }
            Optional<Statistics> substring = ExpressionStatisticsEstimator.estimate(predicate, statistics);
            if (substring.isPresent()) {
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(substring.get(),
                        substring.get().getOutputRowCount());
            }
            if (SPMFunctions.isSPMFunctions(predicate)) {
                if (SPMFunctions.canRevert2ScalarOperator(predicate)) {
                    predicate = (InPredicateOperator) SPMFunctions.revertSPMFunctions(predicate).get(0);
                } else {
                    return statistics;
                }
            }
            double selectivity;

            ScalarOperator firstChild = getChildForCastOperator(predicate.getChild(0));
            // 1. compute the inPredicate children column statistics
            ColumnStatistic inColumnStatistic = getExpressionStatistic(firstChild);

            // Unknown columns use fixed selectivity. With input NDV <= 1, the final NDV is
            // unchanged by duplicate non-null literals: min(input NDV, positive count) is
            // already saturated. Bounds are duplicate-insensitive, so avoid building a set.
            List<ScalarOperator> children = predicate.getChildren();
            boolean skipLiteralDedup = inColumnStatistic.isUnknown()
                    && inColumnStatistic.getDistinctValuesCount() <= 1
                    && !Double.isNaN(statistics.getOutputRowCount())
                    && allConstantOperators(children, 1);
            List<ScalarOperator> otherChildrenList = skipLiteralDedup
                    ? children.subList(1, children.size())
                    : distinctFrom(children, 1, this::getChildForCastOperator);
            boolean allConstants = skipLiteralDedup || allConstantOperators(otherChildrenList, 0);

            if (!predicate.isSubquery() && firstChild.isColumnRef() && !inColumnStatistic.isUnknown() &&
                    inColumnStatistic.getHistogram() != null && allConstants) {
                return estimateInPredicateWithHistogram(
                        (ColumnRefOperator) firstChild,
                        inColumnStatistic,
                        otherChildrenList.stream()
                                .map(op -> (ConstantOperator) op)
                                .collect(Collectors.toList()),
                        predicate.isNotIn(),
                        statistics
                );
            }

            // using ndv to estimate string col inPredicate
            if (!predicate.isNotIn() && firstChild.getType().getPrimitiveType().isCharFamily()
                    && firstChild.isColumnRef()
                    && !inColumnStatistic.isUnknown()) {
                selectivity = Math.min(otherChildrenList.size() / inColumnStatistic.getDistinctValuesCount(), 1);

                double rowCount = Math.max(1, statistics.getOutputRowCount() * selectivity);

                // only columnRefOperator could add column statistic to statistics.
                ColumnRefOperator childOpt = (ColumnRefOperator) firstChild;
                ColumnStatistic newInColumnStatistic =
                        ColumnStatistic.builder()
                                .setDistinctValuesCount(Math.min(inColumnStatistic.getDistinctValuesCount(),
                                        otherChildrenList.size()))
                                .setSourceDistinctValuesCount(inColumnStatistic.getSourceDistinctValuesCount())
                                .setAverageRowSize(inColumnStatistic.getAverageRowSize())
                                .setNullsFraction(0)
                                .build();

                Statistics inStatistics = Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                        addColumnStatistic(childOpt, newInColumnStatistic).build();
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(inStatistics, rowCount);
            }

            double columnMaxVal = inColumnStatistic.getMaxValue();
            double columnMinVal = inColumnStatistic.getMinValue();
            double columnDistinctValues = inColumnStatistic.getDistinctValuesCount();

            double otherChildrenMaxValue;
            double otherChildrenMinValue;
            double otherChildrenDistinctValues;
            boolean hasUnknownOrNaN;
            if (allConstants && !Double.isNaN(statistics.getOutputRowCount())) {
                // IN consumes only bounds/NDV here. General expression statistics also build a
                // singleton histogram and string key for each literal, neither of which is read.
                otherChildrenMaxValue = Double.NEGATIVE_INFINITY;
                otherChildrenMinValue = Double.POSITIVE_INFINITY;
                otherChildrenDistinctValues = 0;
                for (ScalarOperator child : otherChildrenList) {
                    ConstantOperator constant = (ConstantOperator) child;
                    OptionalDouble value = constant.isNull() ? OptionalDouble.empty()
                            : ConstantOperatorUtils.doubleValueFromConstant(constant);
                    otherChildrenMaxValue = Math.max(otherChildrenMaxValue,
                            value.orElse(Double.POSITIVE_INFINITY));
                    otherChildrenMinValue = Math.min(otherChildrenMinValue,
                            value.orElse(Double.NEGATIVE_INFINITY));
                    // Literal NDVs are 0 or 1: their sum is exact for any Java list size.
                    if (!constant.isNull()) {
                        otherChildrenDistinctValues++;
                    }
                }
                if (otherChildrenList.isEmpty()) {
                    otherChildrenMaxValue = Double.POSITIVE_INFINITY;
                    otherChildrenMinValue = Double.NEGATIVE_INFINITY;
                }
                hasUnknownOrNaN = Double.isNaN(otherChildrenMaxValue) || Double.isNaN(otherChildrenMinValue);
            } else {
                List<ColumnStatistic> valueStatistics = otherChildrenList.stream()
                        .map(this::getExpressionStatistic).toList();
                otherChildrenMaxValue = valueStatistics.stream().mapToDouble(ColumnStatistic::getMaxValue).max()
                        .orElse(Double.POSITIVE_INFINITY);
                otherChildrenMinValue = valueStatistics.stream().mapToDouble(ColumnStatistic::getMinValue).min()
                        .orElse(Double.NEGATIVE_INFINITY);
                // Preserve DoubleStream's compensated summation for arbitrary expression NDVs.
                otherChildrenDistinctValues = valueStatistics.stream()
                        .mapToDouble(ColumnStatistic::getDistinctValuesCount).sum();
                hasUnknownOrNaN = valueStatistics.stream().anyMatch(value -> value.hasNaNValue() || value.isUnknown());
            }
            boolean hasOverlap =
                    Math.max(columnMinVal, otherChildrenMinValue) <= Math.min(columnMaxVal, otherChildrenMaxValue);

            // 2 .compute the in predicate selectivity
            boolean defaultSelectivity = false;
            if (inColumnStatistic.isUnknown() || inColumnStatistic.hasNaNValue() ||
                    hasUnknownOrNaN ||
                    !(firstChild.isColumnRef())) {
                // use default selectivity if column statistic is unknown or has NaN values.
                // can not get accurate column statistics if it is not ColumnRef operator
                defaultSelectivity = true;
                selectivity = predicate.isNotIn() ?
                        1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            } else {
                // children column statistics are not unknown.
                selectivity = hasOverlap ?
                        Math.min(1.0, otherChildrenDistinctValues / inColumnStatistic.getDistinctValuesCount()) : 0.0;
                selectivity = predicate.isNotIn() ? 1 - selectivity : selectivity;
            }
            // avoid not in predicate too small
            if (predicate.isNotIn() && Precision.equals(selectivity, 0.0, 0.000001d)) {
                defaultSelectivity = true;
                selectivity = 1 - StatisticsEstimateCoefficient.IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            }

            double rowCount = Math.min(statistics.getOutputRowCount() * selectivity, statistics.getOutputRowCount());

            // 3. compute the inPredicate first child column statistics after in predicate
            if (!hasUnknownOrNaN &&
                    !predicate.isNotIn() && hasOverlap) {
                columnMaxVal = Math.min(columnMaxVal, otherChildrenMaxValue);
                columnMinVal = Math.max(columnMinVal, otherChildrenMinValue);
                columnDistinctValues = Math.min(columnDistinctValues, otherChildrenDistinctValues);
            }
            ColumnStatistic newInColumnStatistic =
                    ColumnStatistic.buildFrom(inColumnStatistic).setDistinctValuesCount(columnDistinctValues)
                            .setMinValue(columnMinVal)
                            .setMaxValue(columnMaxVal).build();

            // only columnRefOperator could add column statistic to statistics
            Optional<ColumnRefOperator> childOpt =
                    firstChild.isColumnRef() ? Optional.of((ColumnRefOperator) firstChild) : Optional.empty();

            Statistics inStatistics = childOpt.map(operator ->
                            Statistics.buildFrom(statistics).setOutputRowCount(rowCount).
                                    addColumnStatistic(operator, newInColumnStatistic).build()).
                    orElseGet(() -> statistics.withOutputRowCount(rowCount));
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(inStatistics, rowCount, defaultSelectivity);
        }

        @Override
        public Statistics visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            Optional<Statistics> mcv = estimateWithMcv(predicate);
            if (mcv.isPresent()) {
                return ExpressionStatisticsEstimator.withNullCheckedVirtualColumn(predicate, statistics, mcv.get());
            }
            double selectivity = 1;
            List<ColumnRefOperator> children = Utils.extractColumnRef(predicate);
            if (children.size() != 1) {
                selectivity = predicate.isNotNull() ?
                        1 - StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
                double rowCount = statistics.getOutputRowCount() * selectivity;
                return statistics.withOutputRowCount(rowCount);
            }
            ColumnStatistic isNullColumnStatistic = statistics.getColumnStatistic(children.get(0));
            boolean defaultSelectivity = false;
            if (isNullColumnStatistic.isUnknown()) {
                defaultSelectivity = true;
                selectivity = predicate.isNotNull() ?
                        1 - StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT :
                        StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT;
            } else {
                selectivity = predicate.isNotNull() ? 1 - isNullColumnStatistic.getNullsFraction() :
                        isNullColumnStatistic.getNullsFraction();
            }
            // avoid estimate selectivity too small because of the error of null fraction
            selectivity =
                    Math.max(selectivity, StatisticsEstimateCoefficient.IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT);
            double rowCount = statistics.getOutputRowCount() * selectivity;
            Statistics.Builder builder = Statistics.buildFrom(statistics).setOutputRowCount(rowCount);
            builder.addColumnStatistic(children.get(0), ColumnStatistic.buildFrom(isNullColumnStatistic)
                    .setNullsFraction(predicate.isNotNull() ? 0.0 : 1.0)
                    .build());
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                    ExpressionStatisticsEstimator.withNullCheckedVirtualColumn(predicate, statistics, builder.build()),
                    rowCount, defaultSelectivity);
        }

        @Override
        public Statistics visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            if (predicate.getChild(0) instanceof ColumnRefOperator column) {
                ColumnStatistic statistic = statistics.getColumnStatistic(column);
                Optional<String> prefix = LikePatternEstimator.pattern(predicate)
                        .flatMap(LikePatternEstimator.LikePattern::fixedPrefix);
                if (statistic.getHistogram() != null && statistic.getHistogram().hasStringValues() && prefix.isPresent()) {
                    if (ExpressionStatisticsEstimator.isVirtualColumn(column)) {
                        return ExpressionStatisticsEstimator.prefix(predicate, column, statistic, prefix.get(),
                                statistics);
                    }
                    return StringHistogramEstimator.prefix(column, statistic, prefix.get(), statistics);
                }
            }
            Optional<Statistics> mcv = estimateWithMcv(predicate);
            if (mcv.isPresent()) {
                return mcv.get();
            }
            OptionalDouble selectivity = LikePatternEstimator.selectivity(predicate, statistics);
            if (selectivity.isEmpty()) {
                return visit(predicate, context);
            }
            ColumnRefOperator column = LikePatternEstimator.column(predicate).orElseThrow();
            double rowCount = statistics.getOutputRowCount() * selectivity.getAsDouble();
            Statistics.Builder builder = Statistics.buildFrom(statistics).setOutputRowCount(rowCount);
            // The NULL rows are out unless the expression turns NULL into a matching value.
            if (!LikePatternEstimator.nullRowsMatch(predicate, column).orElse(true)) {
                builder.addColumnStatistic(column,
                        ColumnStatistic.buildFrom(statistics.getColumnStatistic(column)).setNullsFraction(0).build());
            }
            return StatisticsEstimateUtils.adjustStatisticsByRowCount(builder.build(), rowCount);
        }

        @Override
        public Statistics visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }
            // A comparison of a function of a column with a constant is estimated by the bounds on the column that
            // follow from it, see columnBoundsForEstimate. The check of the shape comes first, because it rejects most
            // comparisons for free.
            if (predicate.getChild(0) instanceof CallOperator && predicate.getChild(1).isConstantRef()) {
                MonotonicFilterDerivation.ColumnBounds bounds =
                        MonotonicFilterDerivation.columnBoundsForEstimate(predicate);
                if (bounds != null) {
                    List<ScalarOperator> columnBounds = bounds.bounds();
                    ScalarOperator bound = columnBounds.size() == 1 ? columnBounds.get(0)
                            : Utils.compoundAnd(columnBounds);
                    Statistics byBounds = statisticsCalculate(bound, statistics, useMcv);
                    if (!bounds.wider()) {
                        return byBounds;
                    }
                    // A wider bound admits rows that the comparison does not hold, such as the rest of the day of a
                    // month shift of a DATETIME. The comparison on the statistics of the function can give
                    // fewer rows, and we take the lower row count. The columns keep their statistics by the bound,
                    // as the estimate on the function does not narrow the column.
                    ColumnStatistic function = getExpressionStatistic(predicate.getChild(0));
                    if (function.isUnknown() || function.isInfiniteRange() || function.hasNaNValue()) {
                        return byBounds;
                    }
                    double rows = estimateBinaryPredicate(predicate, function).getOutputRowCount();
                    return rows < byBounds.getOutputRowCount()
                            ? StatisticsEstimateUtils.adjustStatisticsByRowCount(byBounds.withOutputRowCount(rows), rows)
                            : byBounds;
                }
            }
            return estimateBinaryPredicate(predicate, null);
        }

        // knownLeftStatistic: the statistics of the left child when the caller has computed them, or null
        private Statistics estimateBinaryPredicate(BinaryPredicateOperator predicate,
                                                   ColumnStatistic knownLeftStatistic) {
            Optional<Statistics> mcv = estimateWithMcv(predicate);
            if (mcv.isPresent()) {
                return mcv.get();
            }
            Optional<Statistics> substring = ExpressionStatisticsEstimator.estimate(predicate, statistics);
            if (substring.isPresent()) {
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(substring.get(),
                        substring.get().getOutputRowCount());
            }
            ScalarOperator leftChild = predicate.getChild(0);
            ScalarOperator rightChild = predicate.getChild(1);
            Preconditions.checkState(!(leftChild.isConstantRef() && rightChild.isConstantRef()),
                    "ConstantRef-cmp-ConstantRef not supported here, %s should be eliminated earlier",
                    predicate);
            Preconditions.checkState(!(leftChild.isConstant() && rightChild.isVariable()),
                    "Constant-cmp-Column not supported here, %s should be deal earlier", predicate);
            // Unwrap only casts that preserve the values and the statistics domain.
            leftChild = getChildForCastOperator(leftChild);
            rightChild = getChildForCastOperator(rightChild);

            // For SPM functions, we try to revert to origin scalar operator
            // in actually, SPMFunction also support the correct statistics, but the implement of binary
            // predicate depend on ConstantOperator, not ConstantExpression, it's take SPM's plan is
            // different with origin plan.
            if (SPMFunctions.isSPMFunctions(leftChild) && SPMFunctions.canRevert2ScalarOperator(leftChild)) {
                leftChild = SPMFunctions.revertSPMFunctions(leftChild).get(0);
            }
            if (SPMFunctions.isSPMFunctions(rightChild) && SPMFunctions.canRevert2ScalarOperator(rightChild)) {
                rightChild = SPMFunctions.revertSPMFunctions(rightChild).get(0);
            }

            // compute left and right column statistics
            ColumnStatistic leftColumnStatistic = knownLeftStatistic != null && leftChild == predicate.getChild(0)
                    ? knownLeftStatistic : getExpressionStatistic(leftChild);
            ColumnStatistic rightColumnStatistic = getExpressionStatistic(rightChild);
            // do not use NaN to estimate predicate
            if (leftColumnStatistic.hasNaNValue()) {
                leftColumnStatistic =
                        ColumnStatistic.buildFrom(leftColumnStatistic).setMaxValue(Double.POSITIVE_INFINITY)
                                .setMinValue(Double.NEGATIVE_INFINITY).build();
            }
            if (rightColumnStatistic.hasNaNValue()) {
                rightColumnStatistic =
                        ColumnStatistic.buildFrom(rightColumnStatistic).setMaxValue(Double.POSITIVE_INFINITY)
                                .setMinValue(Double.NEGATIVE_INFINITY).build();
            }

            if (leftChild.isVariable()) {
                Optional<ColumnRefOperator> leftChildOpt;
                // only columnRefOperator could add column statistic to statistics
                leftChildOpt = leftChild.isColumnRef() ? Optional.of((ColumnRefOperator) leftChild) : Optional.empty();

                if (rightChild.isConstantRef()) {
                    Optional<ConstantOperator> constantOperator = Optional.of((ConstantOperator) rightChild);
                    Statistics binaryStats =
                            BinaryPredicateStatisticCalculator.estimateColumnToConstantComparison(leftChildOpt,
                                    leftColumnStatistic, predicate, constantOperator, statistics);
                    return StatisticsEstimateUtils.adjustStatisticsByRowCount(binaryStats,
                            binaryStats.getOutputRowCount());
                } else {
                    Statistics binaryStats = BinaryPredicateStatisticCalculator.estimateColumnToColumnComparison(
                            leftChild, leftColumnStatistic,
                            rightChild, rightColumnStatistic,
                            predicate, statistics);
                    return StatisticsEstimateUtils.adjustStatisticsByRowCount(binaryStats,
                            binaryStats.getOutputRowCount());
                }
            } else {
                // constant compare constant
                double outputRowCount = statistics.getOutputRowCount() *
                        StatisticsEstimateCoefficient.CONSTANT_TO_CONSTANT_PREDICATE_COEFFICIENT;
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        statistics.withOutputRowCount(outputRowCount), outputRowCount);
            }
        }

        @Override
        public Statistics visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }

            if (predicate.isAnd()) {
                Pair<Map<ColumnRefOperator, ConstantOperator>, List<ScalarOperator>> extracted =
                        Utils.separateEqualityPredicates(predicate);
                Optional<MultiColumnMcvEstimator.Result> mcvEstimate =
                        useMcv ? MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics)
                                : Optional.empty();

                if (extracted.first.size() > 1 || mcvEstimate.isPresent()) {
                    return computeCompoundStatsWithMultiColumnOptimize(predicate, statistics, mcvEstimate, useMcv);
                }

                Statistics leftStatistics = finishEstimate(predicate.getChild(0), statistics,
                        predicate.getChild(0).accept(this, null), useMcv);
                Statistics andStatistics = finishEstimate(predicate.getChild(1), leftStatistics,
                        predicate.getChild(1).accept(new BaseCalculatingVisitor(leftStatistics, useMcv), null), useMcv);
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(andStatistics,
                        andStatistics.getOutputRowCount());
            } else if (predicate.isOr()) {
                List<ScalarOperator> disjunctive = Utils.extractDisjunctive(predicate);
                Statistics cumulativeStatistics = statisticsCalculate(disjunctive.get(0), statistics, useMcv);
                double rowCount = cumulativeStatistics.getOutputRowCount();

                for (int i = 1; i < disjunctive.size(); ++i) {
                    Statistics orItemStatistics = statisticsCalculate(disjunctive.get(i), statistics, useMcv);
                    Statistics andStatistics = statisticsCalculate(disjunctive.get(i), cumulativeStatistics, useMcv);
                    rowCount = cumulativeStatistics.getOutputRowCount() + orItemStatistics.getOutputRowCount() -
                            andStatistics.getOutputRowCount();
                    rowCount = Math.min(rowCount, statistics.getOutputRowCount());
                    cumulativeStatistics =
                            computeOrPredicateStatistics(cumulativeStatistics, orItemStatistics, andStatistics, rowCount);
                    cumulativeStatistics = McvStatisticsPropagation.filter(
                            Utils.compoundOr(disjunctive.subList(0, i + 1)), statistics, cumulativeStatistics);
                }

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(cumulativeStatistics, rowCount);
            } else {
                Statistics inputStatistics = predicate.getChild(0).accept(this, null);
                // The negation of a condition on a virtual column holds on its non-NULL rows only.
                double rowCount = Math.max(0, statistics.getOutputRowCount()
                        * ExpressionStatisticsEstimator.negationShare(predicate.getChild(0), statistics)
                        - inputStatistics.getOutputRowCount());
                Statistics negated = ExpressionStatisticsEstimator.withNegatedVirtualColumn(predicate.getChild(0),
                        statistics, statistics.withOutputRowCount(rowCount));
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(negated, rowCount);
            }
        }

        protected Statistics computeOrPredicateStatistics(Statistics cumulativeStatistics, Statistics orItemStatistics,
                                                          Statistics andStatistics, double rowCount) {
            Statistics.Builder builder = null;
            for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry :
                    cumulativeStatistics.getColumnStatistics().entrySet()) {
                ColumnRefOperator columnRefOperator = entry.getKey();
                ColumnStatistic columnStatistic = entry.getValue();
                ColumnStatistic rightColumnStatistic = orItemStatistics.getColumnStatistic(columnRefOperator);
                double min = Math.min(columnStatistic.getMinValue(), rightColumnStatistic.getMinValue());
                double max = Math.max(columnStatistic.getMaxValue(), rightColumnStatistic.getMaxValue());
                double originalNdv = statistics.getColumnStatistic(columnRefOperator).getDistinctValuesCount();
                double accumulatedNdv = columnStatistic.getDistinctValuesCount() + rightColumnStatistic.getDistinctValuesCount();
                double distinct = Math.min(originalNdv, accumulatedNdv);
                double origNulls =
                        statistics.getColumnStatistic(columnRefOperator).getNullsFraction() * statistics.getOutputRowCount();
                double leftNulls = cumulativeStatistics.getOutputRowCount() * columnStatistic.getNullsFraction();
                double rightNulls = orItemStatistics.getOutputRowCount() * rightColumnStatistic.getNullsFraction();
                // Without counting intersection, overlapping null rows are counted twice and the propagated
                // null fraction is inflated.
                double intersectionNulls = andStatistics == null ? 0.0 : andStatistics.getOutputRowCount() *
                        andStatistics.getColumnStatistic(columnRefOperator).getNullsFraction();
                // The intersection can't hold more null rows than either arm
                double cappedIntersectionNulls = Math.min(intersectionNulls, Math.min(leftNulls, rightNulls));
                double unionNulls = Math.max(0.0, leftNulls + rightNulls - cappedIntersectionNulls);
                double cappedUnionNulls = Math.min(unionNulls, Math.min(origNulls, rowCount));
                double nullsFraction = rowCount > 0 ? Math.min(1.0, cappedUnionNulls / rowCount) : 0.0;
                if (!sameOrColumnValues(columnStatistic, min, max, distinct, nullsFraction)) {
                    if (builder == null) {
                        builder = Statistics.buildFrom(cumulativeStatistics).setOutputRowCount(rowCount);
                    }
                    builder.addColumnStatistic(columnRefOperator, ColumnStatistic.buildFrom(columnStatistic)
                            .setMinValue(min).setMaxValue(max).setDistinctValuesCount(distinct)
                            .setNullsFraction(nullsFraction).build());
                }
            }
            return finishOrStatistics(cumulativeStatistics, builder, rowCount);
        }

        @Override
        public Statistics visitConstant(ConstantOperator constant, Void context) {
            if (constant.getBoolean()) {
                return statistics;
            } else {
                return statistics.withOutputRowCount(0.0);
            }
        }

        @Override
        public Statistics visitCall(CallOperator call, Void context) {
            if (call.getType() != BooleanType.BOOLEAN) {
                return visit(call, context);
            }

            if (call.getFnName().equalsIgnoreCase(FunctionSet.IF)) {
                return ifPredicate(call);
            }

            return visit(call, context);
        }

        // The statistics are computed by building the equivalent predicate using AND and OR, and computing its statistics.
        // example:
        //                       IF
        //               /       |        \
        //      condition   predicate1     predicate2
        //
        // equivalent predicate:
        //                            OR
        //                   /                   \
        //                AND                     AND
        //               /   \                   /   \
        //      condition     predicate1       OR     predicate2
        //                                    /  \
        //                   condition IS NULL    NOT ( condition )
        private Statistics ifPredicate(CallOperator predicate) {
            List<ScalarOperator> children = predicate.getChildren();
            ScalarOperator trueBranch = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    children.get(0), children.get(1));

            ScalarOperator isNullCondition = new IsNullPredicateOperator(false, children.get(0));
            ScalarOperator notCondition = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                    children.get(0));
            ScalarOperator falseBranchCondition = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    isNullCondition, notCondition);
            ScalarOperator falseBranch = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    falseBranchCondition, children.get(2));

            CompoundPredicateOperator equivalentCompoundPredicate =
                    new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, trueBranch, falseBranch);

            return equivalentCompoundPredicate.accept(this, null);
        }

        private ScalarOperator getChildForCastOperator(ScalarOperator operator) {
            if (operator instanceof CastOperator && !castsNumberToDate((CastOperator) operator)) {
                if (operator.getChild(0) instanceof ConstantOperator constant) {
                    return StatisticsCastEvaluator.cast(constant, operator.getType())
                            .<ScalarOperator>map(value -> value).orElse(operator);
                }
                if (CastStatisticsUtils.preservesValues(operator.getChild(0).getType(), operator.getType())) {
                    return getChildForCastOperator(operator.getChild(0));
                }
            }
            return operator;
        }

        // We keep a cast of a number to a date and do not take the statistics of the number: the statistics of a date
        // are seconds since the epoch, while a number such as 20240101 as an INT means something else.
        // ExpressionStatisticCalculator estimates the cast itself.
        private static boolean castsNumberToDate(CastOperator cast) {
            return cast.getChild(0).getType().isNumericType() && cast.getType().isDateType();
        }

        private ColumnStatistic getExpressionStatistic(ScalarOperator operator) {
            return ExpressionStatisticCalculator.calculate(operator, statistics);
        }
    }

    private static class LargeOrCalculatingVisitor extends BaseCalculatingVisitor {
        public LargeOrCalculatingVisitor(Statistics statistics, boolean useMcv) {
            super(statistics, useMcv);
        }

        @Override
        public Statistics visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (!checkNeedEvalEstimate(predicate)) {
                return statistics;
            }

            if (predicate.isAnd()) {
                Pair<Map<ColumnRefOperator, ConstantOperator>, List<ScalarOperator>> extracted =
                        Utils.separateEqualityPredicates(predicate);
                Optional<MultiColumnMcvEstimator.Result> mcvEstimate =
                        useMcv ? MultiColumnMcvEstimator.estimate(Utils.extractConjuncts(predicate), statistics)
                                : Optional.empty();

                if (extracted.first.size() > 1 || mcvEstimate.isPresent()) {
                    return computeCompoundStatsWithMultiColumnOptimize(predicate, statistics, mcvEstimate, useMcv);
                }

                Statistics leftStatistics = finishEstimate(predicate.getChild(0), statistics,
                        predicate.getChild(0).accept(this, null), useMcv);
                Statistics andStatistics = finishEstimate(predicate.getChild(1), leftStatistics,
                        predicate.getChild(1).accept(new LargeOrCalculatingVisitor(leftStatistics, useMcv), null), useMcv);
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(andStatistics,
                        andStatistics.getOutputRowCount());
            } else if (predicate.isOr()) {
                List<ScalarOperator> disjunctive = Utils.extractDisjunctive(predicate);
                Statistics baseStatistics = disjunctive.get(0).accept(this, null);
                double rowCount = baseStatistics.getOutputRowCount();

                for (int i = 1; i < disjunctive.size(); ++i) {
                    Statistics orStatistics = disjunctive.get(i).accept(this, null);
                    rowCount = (baseStatistics.getOutputRowCount() + orStatistics.getOutputRowCount()) / 2;
                    rowCount = Math.max(rowCount, baseStatistics.getOutputRowCount());
                    rowCount = Math.max(rowCount, orStatistics.getOutputRowCount());
                    rowCount = Math.min(rowCount, statistics.getOutputRowCount());
                    // This path uses an averaging heuristic and does not estimate the arms' intersection,
                    // so no inclusion/exclusion adjustment is applied here (andStatistics is null).
                    baseStatistics = computeOrPredicateStatistics(baseStatistics, orStatistics, null, rowCount);
                }

                return StatisticsEstimateUtils.adjustStatisticsByRowCount(baseStatistics, rowCount);
            } else {
                Statistics inputStatistics = predicate.getChild(0).accept(this, null);
                // The negation of a condition on a virtual column holds on its non-NULL rows only.
                double rowCount = Math.max(0, statistics.getOutputRowCount()
                        * ExpressionStatisticsEstimator.negationShare(predicate.getChild(0), statistics)
                        - inputStatistics.getOutputRowCount());
                Statistics negated = ExpressionStatisticsEstimator.withNegatedVirtualColumn(predicate.getChild(0),
                        statistics, statistics.withOutputRowCount(rowCount));
                return StatisticsEstimateUtils.adjustStatisticsByRowCount(negated, rowCount);
            }
        }

        @Override
        protected Statistics computeOrPredicateStatistics(Statistics baseStatistics, Statistics orItemStatistics,
                                                          Statistics andStatistics, double rowCount) {
            // support simple avg statistics
            Statistics.Builder builder = null;
            for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : baseStatistics.getColumnStatistics().entrySet()) {
                ColumnRefOperator columnRefOperator = entry.getKey();
                ColumnStatistic columnStatistic = entry.getValue();
                ColumnStatistic rightColumnStatistic = orItemStatistics.getColumnStatistic(columnRefOperator);
                double min = Math.min(columnStatistic.getMinValue(), rightColumnStatistic.getMinValue());
                double max = Math.max(columnStatistic.getMaxValue(), rightColumnStatistic.getMaxValue());
                double distinct = Math.max(1,
                        (columnStatistic.getDistinctValuesCount() + rightColumnStatistic.getDistinctValuesCount()) / 2);
                // nullsFraction is a ratio, not a count like distinct/NDV above it - cap at 1
                // instead of flooring at 1, which forced every merge to the constant 1.0.
                double nulls = Math.min(1.0,
                        (columnStatistic.getNullsFraction() + rightColumnStatistic.getNullsFraction()) / 2);
                if (!sameOrColumnValues(columnStatistic, min, max, distinct, nulls)) {
                    if (builder == null) {
                        builder = Statistics.buildFrom(baseStatistics).setOutputRowCount(rowCount);
                    }
                    builder.addColumnStatistic(columnRefOperator, ColumnStatistic.buildFrom(columnStatistic)
                            .setMinValue(min).setMaxValue(max).setDistinctValuesCount(distinct)
                            .setNullsFraction(nulls).build());
                }
            }
            return finishOrStatistics(baseStatistics, builder, rowCount);
        }

    }
}

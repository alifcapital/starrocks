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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Estimates the selectivity of a conjunction with the most common value (MCV) list of multi-column
 * statistics.
 *
 * The MCV list is a point sample of the joint distribution of a column group, so it answers any
 * predicate that can be evaluated on a value tuple: equality on all or some of the group's columns,
 * IN, ranges, IS NULL, on the column itself or on a cast or a function of it, evaluated on the
 * tuple component. The head of the distribution is counted exactly by summing the rows of the
 * tuples that satisfy the predicates. The rest is the independence estimate of the predicates minus
 * what independence attributes to the matching tuples, the formula PostgreSQL uses for
 * pg_mcv_list: sel = mcv_sel + clamp(simple_sel - mcv_basesel, 0, 1 - mcv_totalsel).
 *
 * The independence estimate uses the collected component counts where it can: an equality whose
 * value is a component of some head tuple has the exact share of that value, and the rows outside
 * the head that satisfy it are at most that share less the matching head tuples. The head answers
 * predicates on the columns the query reads even when it reads only some of the group's columns,
 * because the component counts make the independence share of a tuple independent of the query.
 *
 * When every column of the group has an equality predicate and no tuple matches, the value tuple is
 * in the tail of the distribution: its share is bounded by the smallest MCV share and, on average,
 * by the tail mass spread over the tail distinct values.
 */
public class MultiColumnMcvEstimator {
    public static class Result {
        private final double selectivity;
        private final Set<ScalarOperator> consumed;
        private final Set<ColumnRefOperator> consumedColumns;

        Result(double selectivity, Set<ScalarOperator> consumed, Set<ColumnRefOperator> consumedColumns) {
            this.selectivity = selectivity;
            this.consumed = consumed;
            this.consumedColumns = consumedColumns;
        }

        public double getSelectivity() {
            return selectivity;
        }

        /** Conjuncts whose effect is already inside the selectivity. */
        public Set<ScalarOperator> getConsumed() {
            return consumed;
        }

        public Set<ColumnRefOperator> getConsumedColumns() {
            return consumedColumns;
        }
    }

    /**
     * Estimates the conjuncts that fall on column groups with an MCV list. Empty when no group has an
     * MCV list, when fewer than two columns of any group carry supported predicates, or when a value
     * cannot be interpreted.
     */
    public static Optional<Result> estimate(List<ScalarOperator> conjuncts, Statistics statistics) {
        if (!isEnabled() || !hasMcvStats(statistics)) {
            return Optional.empty();
        }
        Map<ColumnRefOperator, List<ScalarOperator>> byColumn = groupSupportedConjuncts(conjuncts);
        if (byColumn.size() < 2) {
            return Optional.empty();
        }

        double selectivity = 1.0;
        Set<ScalarOperator> consumed = new HashSet<>();
        Set<ColumnRefOperator> consumedColumns = new HashSet<>();
        Map<String, Double> valueSelectivityCache = new HashMap<>();
        Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups = statistics.getMultiColumnCombinedStats();
        while (true) {
            Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> best = findBestGroup(byColumn, groups);
            if (best == null) {
                break;
            }
            Set<ColumnRefOperator> covered = new HashSet<>(best.getKey());
            covered.retainAll(byColumn.keySet());
            List<ScalarOperator> groupConjuncts = new ArrayList<>();
            for (ColumnRefOperator column : covered) {
                groupConjuncts.addAll(byColumn.get(column));
            }
            OptionalDouble groupSelectivity =
                    groupSelectivity(best.getValue(), covered, groupConjuncts, statistics, valueSelectivityCache);
            if (groupSelectivity.isEmpty()) {
                return Optional.empty();
            }
            selectivity *= groupSelectivity.getAsDouble();
            consumed.addAll(groupConjuncts);
            consumedColumns.addAll(covered);
            byColumn.keySet().removeAll(covered);
        }
        if (consumed.isEmpty()) {
            return Optional.empty();
        }
        double floor = 1.0 / Math.max(1.0, statistics.getOutputRowCount());
        return Optional.of(new Result(Math.min(1.0, Math.max(floor, selectivity)), consumed, consumedColumns));
    }

    /**
     * The distinct value tuples of some columns of a group, from its head: the distinct projections
     * of the head tuples, plus the tail tuples projected at the same rate as the head. When several
     * groups hold every column, the narrowest one projects; ties go to the one whose head covers
     * more rows. Empty when no group with an MCV list holds every column.
     */
    public static OptionalDouble projectedNdv(Collection<ColumnRefOperator> columns, Statistics statistics) {
        if (!isEnabled() || columns.isEmpty()) {
            return OptionalDouble.empty();
        }
        MultiColumnCombinedStats best = null;
        double bestCoverage = -1;
        for (MultiColumnCombinedStats stats : statistics.getMultiColumnCombinedStats().values()) {
            if (!stats.hasMcv() || stats.getNdv() <= 0 || !stats.getColumns().containsAll(columns)) {
                continue;
            }
            double coverage = mcvTotalRows(stats) / (double) stats.getRowCount();
            if (best == null || stats.getColumns().size() < best.getColumns().size()
                    || (stats.getColumns().size() == best.getColumns().size() && coverage > bestCoverage)) {
                best = stats;
                bestCoverage = coverage;
            }
        }
        if (best == null) {
            return OptionalDouble.empty();
        }
        List<Integer> positions = new ArrayList<>();
        for (ColumnRefOperator column : columns) {
            positions.add(best.getColumns().indexOf(column));
        }
        Set<List<String>> projections = new HashSet<>();
        for (MultiColumnCombinedStats.McvEntry entry : best.getMcv()) {
            List<String> projection = new ArrayList<>(positions.size());
            for (int position : positions) {
                projection.add(entry.getValues().get(position));
            }
            projections.add(projection);
        }
        double headTuples = best.getMcv().size();
        double tailTuples = Math.max(0, best.getNdv() - headTuples);
        return OptionalDouble.of(projections.size() + tailTuples * projections.size() / headTuples);
    }

    private static boolean isEnabled() {
        ConnectContext context = ConnectContext.get();
        return context == null || context.getSessionVariable().isCboEnableMcvEstimate();
    }

    private static boolean hasMcvStats(Statistics statistics) {
        for (MultiColumnCombinedStats stats : statistics.getMultiColumnCombinedStats().values()) {
            if (stats.hasMcv()) {
                return true;
            }
        }
        return false;
    }

    /**
     * The group covering the most predicate columns; ties go to the group whose MCV list covers more rows.
     */
    private static Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> findBestGroup(
            Map<ColumnRefOperator, List<ScalarOperator>> byColumn,
            Map<Set<ColumnRefOperator>, MultiColumnCombinedStats> groups) {
        Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> best = null;
        int bestCovered = 1;
        double bestCoverage = -1;
        for (Map.Entry<Set<ColumnRefOperator>, MultiColumnCombinedStats> entry : groups.entrySet()) {
            if (!entry.getValue().hasMcv()) {
                continue;
            }
            int covered = 0;
            for (ColumnRefOperator column : entry.getKey()) {
                if (byColumn.containsKey(column)) {
                    covered++;
                }
            }
            double coverage = mcvTotalRows(entry.getValue()) / (double) entry.getValue().getRowCount();
            if (covered > bestCovered || (covered == bestCovered && best != null && coverage > bestCoverage)) {
                best = entry;
                bestCovered = covered;
                bestCoverage = coverage;
            }
        }
        return best;
    }

    private static double mcvTotalRows(MultiColumnCombinedStats stats) {
        double total = 0;
        for (MultiColumnCombinedStats.McvEntry entry : stats.getMcv()) {
            total += entry.getCount();
        }
        return total;
    }

    private static OptionalDouble groupSelectivity(MultiColumnCombinedStats stats, Set<ColumnRefOperator> covered,
                                                   List<ScalarOperator> conjuncts, Statistics statistics,
                                                   Map<String, Double> valueSelectivityCache) {
        List<ColumnRefOperator> columns = stats.getColumns();
        double rowCount = stats.getRowCount();
        double mcvSel = 0;
        double mcvTotalSel = 0;
        double mcvBaseSel = 0;
        double minHeadShare = 1.0;
        for (MultiColumnCombinedStats.McvEntry entry : stats.getMcv()) {
            double share = entry.getCount() / rowCount;
            mcvTotalSel += share;
            minHeadShare = Math.min(minHeadShare, share);
            Optional<Boolean> matches = matchesAll(entry, columns, conjuncts);
            if (matches.isEmpty()) {
                return OptionalDouble.empty();
            }
            if (!matches.get()) {
                continue;
            }
            mcvSel += share;
            OptionalDouble base = baseShare(entry, columns, rowCount, statistics, valueSelectivityCache);
            if (base.isEmpty()) {
                return OptionalDouble.empty();
            }
            mcvBaseSel += base.getAsDouble();
        }

        ComponentShares shares = new ComponentShares(stats);
        double simpleSel = 1.0;
        double exactSel = 1.0;
        boolean hasExactSel = false;
        for (ScalarOperator conjunct : conjuncts) {
            ColumnRefOperator column = predicateColumn(conjunct);
            // An expression of the column may map several values to one; only the column itself has
            // an exact share.
            OptionalDouble exact = conjunct.getChild(0).isColumnRef()
                    ? shares.selectivity(conjunct, columns.indexOf(column), column.getType()) : OptionalDouble.empty();
            double sel = exact.isPresent() ? exact.getAsDouble()
                    : StatisticsEstimateUtils.getPredicateSelectivity(conjunct, statistics);
            simpleSel *= sel;
            if (exact.isPresent()) {
                exactSel = Math.min(exactSel, sel);
                hasExactSel = true;
            }
        }
        double otherSel = Math.min(Math.max(0.0, 1.0 - mcvTotalSel), Math.max(0.0, simpleSel - mcvBaseSel));
        if (hasExactSel) {
            // The matching head tuples all satisfy an exactly known conjunct, so the rows outside the
            // head that satisfy the conjunction are at most its rows less the matching head tuples.
            otherSel = Math.min(otherSel, Math.max(0.0, exactSel - mcvSel));
        }

        if (mcvSel == 0 && covered.size() == columns.size() && allEquality(conjuncts)) {
            double tailNdv = Math.max(1.0, stats.getNdv() - stats.getMcv().size());
            double uniformTail = Math.max(0.0, 1.0 - mcvTotalSel) / tailNdv;
            otherSel = Math.min(otherSel, Math.min(uniformTail, minHeadShare));
        }
        return OptionalDouble.of(Math.min(1.0, mcvSel + otherSel));
    }

    /**
     * Whether the tuple satisfies every conjunct. Empty when a conjunct falls outside the tuple or a
     * component cannot be compared with the constant.
     */
    private static Optional<Boolean> matchesAll(MultiColumnCombinedStats.McvEntry entry, List<ColumnRefOperator> columns,
                                                List<ScalarOperator> conjuncts) {
        for (ScalarOperator conjunct : conjuncts) {
            ColumnRefOperator column = predicateColumn(conjunct);
            int index = columns.indexOf(column);
            if (index < 0 || index >= entry.getValues().size()) {
                return Optional.empty();
            }
            Optional<Boolean> match = matchesComponent(conjunct, column, entry.getValues().get(index));
            if (match.isEmpty()) {
                return Optional.empty();
            }
            if (!match.get()) {
                return Optional.of(false);
            }
        }
        return Optional.of(true);
    }

    /**
     * The share of the rows that independence attributes to a tuple: the product of the shares of its
     * components. Exact from the collected component counts; otherwise estimated from the single-column
     * statistics, which needs every column of the group to be read by the query.
     */
    private static OptionalDouble baseShare(MultiColumnCombinedStats.McvEntry entry, List<ColumnRefOperator> columns,
                                            double rowCount, Statistics statistics, Map<String, Double> cache) {
        double base = 1.0;
        if (entry.hasComponentCounts()) {
            for (Long count : entry.getComponentCounts()) {
                base *= count / rowCount;
            }
            return OptionalDouble.of(base);
        }
        for (int i = 0; i < columns.size() && i < entry.getValues().size(); i++) {
            ColumnRefOperator column = columns.get(i);
            if (column == null) {
                return OptionalDouble.empty();
            }
            base *= columnValueSelectivity(column, entry.getValues().get(i), statistics, cache);
        }
        return OptionalDouble.of(base);
    }

    // Equality on the columns themselves: one value tuple satisfies the conjunction.
    private static boolean allEquality(List<ScalarOperator> conjuncts) {
        for (ScalarOperator conjunct : conjuncts) {
            if (!(conjunct instanceof BinaryPredicateOperator) ||
                    ((BinaryPredicateOperator) conjunct).getBinaryType() != BinaryType.EQ ||
                    !conjunct.getChild(0).isColumnRef()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Selectivity of column = value on the input statistics; the independence view of one tuple component.
     */
    private static double columnValueSelectivity(ColumnRefOperator column, String value, Statistics statistics,
                                                 Map<String, Double> cache) {
        ColumnStatistic columnStatistic = statistics.getColumnStatistic(column);
        if (value == null) {
            return columnStatistic.getNullsFraction();
        }
        String key = column.getId() + "\u0000" + value;
        Double cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        double selectivity;
        Optional<ConstantOperator> constant = ConstantOperator.createVarchar(value).castTo(column.getType());
        if (constant.isPresent() && !constant.get().isNull()) {
            selectivity = StatisticsEstimateUtils.getPredicateSelectivity(
                    new BinaryPredicateOperator(BinaryType.EQ, column, constant.get()), statistics);
        } else {
            selectivity = 1.0 / Math.max(1.0, columnStatistic.getDistinctValuesCount());
        }
        cache.put(key, selectivity);
        return selectivity;
    }

    /**
     * The shares of the head component values in their own columns, from the collected component
     * counts: for each tuple position, the rows whose column holds the value (a null key for NULL).
     */
    static class ComponentShares {
        private final double rowCount;
        private final List<Map<String, Long>> counts;

        ComponentShares(MultiColumnCombinedStats stats) {
            this.rowCount = stats.getRowCount();
            int width = stats.getColumns().size();
            this.counts = new ArrayList<>(width);
            for (int i = 0; i < width; i++) {
                counts.add(new HashMap<>());
            }
            for (MultiColumnCombinedStats.McvEntry entry : stats.getMcv()) {
                if (!entry.hasComponentCounts()) {
                    continue;
                }
                for (int i = 0; i < width; i++) {
                    counts.get(i).putIfAbsent(entry.getValues().get(i), entry.getComponentCounts().get(i));
                }
            }
        }

        /**
         * The exact selectivity of column = constant, column IN (constants), column IS NULL or column IS
         * NOT NULL at the tuple position; empty when a value is not a head component there.
         */
        OptionalDouble selectivity(ScalarOperator conjunct, int position, Type type) {
            if (position < 0 || position >= counts.size() || counts.get(position).isEmpty()) {
                return OptionalDouble.empty();
            }
            Map<String, Long> known = counts.get(position);
            if (conjunct instanceof IsNullPredicateOperator) {
                Long nulls = known.get(null);
                if (nulls == null) {
                    return OptionalDouble.empty();
                }
                double share = nulls / rowCount;
                return OptionalDouble.of(((IsNullPredicateOperator) conjunct).isNotNull() ? 1.0 - share : share);
            }
            List<ConstantOperator> constants = new ArrayList<>();
            if (conjunct instanceof BinaryPredicateOperator
                    && ((BinaryPredicateOperator) conjunct).getBinaryType() == BinaryType.EQ) {
                constants.add((ConstantOperator) conjunct.getChild(1));
            } else if (conjunct instanceof InPredicateOperator && !((InPredicateOperator) conjunct).isNotIn()) {
                for (int i = 1; i < conjunct.getChildren().size(); i++) {
                    constants.add((ConstantOperator) conjunct.getChild(i));
                }
            } else {
                return OptionalDouble.empty();
            }
            Set<String> matched = new HashSet<>();
            double share = 0;
            for (ConstantOperator constant : constants) {
                Optional<String> value = findComponent(known, type, constant);
                if (value.isEmpty()) {
                    return OptionalDouble.empty();
                }
                if (matched.add(value.get())) {
                    share += known.get(value.get()) / rowCount;
                }
            }
            return OptionalDouble.of(Math.min(1.0, share));
        }

        private static Optional<String> findComponent(Map<String, Long> known, Type type, ConstantOperator constant) {
            for (String value : known.keySet()) {
                if (value == null) {
                    continue;
                }
                Optional<Integer> cmp = compare(type, value, constant);
                if (cmp.isPresent() && cmp.get() == 0) {
                    return Optional.of(value);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Conjuncts of the forms expr op constant, expr [NOT] IN (constants), expr LIKE 'pattern' and expr IS [NOT] NULL, where
     * expr is a column or a cast or a function of one column, keyed by that column. Other conjuncts
     * are left to the regular estimation.
     */
    private static Map<ColumnRefOperator, List<ScalarOperator>> groupSupportedConjuncts(List<ScalarOperator> conjuncts) {
        Map<ColumnRefOperator, List<ScalarOperator>> byColumn = new LinkedHashMap<>();
        for (ScalarOperator conjunct : conjuncts) {
            ColumnRefOperator column = predicateColumn(conjunct);
            if (column != null && column.getType().canStatistic()) {
                byColumn.computeIfAbsent(column, k -> new ArrayList<>()).add(conjunct);
            }
        }
        return byColumn;
    }

    private static ColumnRefOperator predicateColumn(ScalarOperator conjunct) {
        if (conjunct instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator predicate = (BinaryPredicateOperator) conjunct;
            if (predicate.getBinaryType() == BinaryType.EQ_FOR_NULL) {
                return null;
            }
            if (isNonNullConstant(predicate.getChild(1))) {
                return columnOf(predicate.getChild(0));
            }
            return null;
        }
        if (conjunct instanceof InPredicateOperator) {
            InPredicateOperator predicate = (InPredicateOperator) conjunct;
            if (predicate.isSubquery()) {
                return null;
            }
            for (int i = 1; i < predicate.getChildren().size(); i++) {
                if (!isNonNullConstant(predicate.getChild(i))) {
                    return null;
                }
            }
            return columnOf(predicate.getChild(0));
        }
        if (conjunct instanceof IsNullPredicateOperator) {
            return columnOf(conjunct.getChild(0));
        }
        if (conjunct instanceof LikePredicateOperator
                && LikePatternEstimator.pattern((LikePredicateOperator) conjunct).isPresent()) {
            return columnOf(conjunct.getChild(0));
        }
        return null;
    }

    // The one column an expression of casts and function calls with constant arguments is built on.
    static ColumnRefOperator columnOf(ScalarOperator expr) {
        if (expr.isColumnRef()) {
            return (ColumnRefOperator) expr;
        }
        if (!(expr instanceof CastOperator) && !(expr instanceof CallOperator)) {
            return null;
        }
        ColumnRefOperator column = null;
        for (ScalarOperator child : expr.getChildren()) {
            if (child.isConstantRef()) {
                continue;
            }
            ColumnRefOperator childColumn = columnOf(child);
            if (childColumn == null || (column != null && !column.equals(childColumn))) {
                return null;
            }
            column = childColumn;
        }
        return column;
    }

    // The functions that pick one of their arguments, folded here because the optimizer's rules leave
    // a NULL first argument alone. Empty for any other function.
    private static Optional<ConstantOperator> chooseAmongConstants(String function, List<ScalarOperator> arguments) {
        List<ConstantOperator> constants = new ArrayList<>(arguments.size());
        for (ScalarOperator argument : arguments) {
            constants.add((ConstantOperator) argument);
        }
        if (FunctionSet.COALESCE.equalsIgnoreCase(function)) {
            for (ConstantOperator constant : constants) {
                if (!constant.isNull()) {
                    return Optional.of(constant);
                }
            }
            return Optional.of(constants.get(constants.size() - 1));
        }
        if (FunctionSet.IFNULL.equalsIgnoreCase(function) && constants.size() == 2) {
            return Optional.of(constants.get(0).isNull() ? constants.get(1) : constants.get(0));
        }
        if (FunctionSet.IF.equalsIgnoreCase(function) && constants.size() == 3) {
            ConstantOperator condition = constants.get(0);
            boolean holds = !condition.isNull() && condition.getType().isBoolean() && condition.getBoolean();
            return Optional.of(holds ? constants.get(1) : constants.get(2));
        }
        return Optional.empty();
    }

    /**
     * Whether a tuple component satisfies the conjunct, whose left side is the column or an
     * expression of it evaluated on the component. Empty when the expression cannot be evaluated
     * or the result cannot be compared with the constant.
     */
    static Optional<Boolean> matchesComponent(ScalarOperator conjunct, ColumnRefOperator column, String value) {
        ScalarOperator expr = conjunct.getChild(0);
        if (expr.isColumnRef()) {
            return matches(conjunct, column.getType(), value);
        }
        Optional<ConstantOperator> result = evaluate(expr, column, value);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        return matches(conjunct, expr.getType(), result.get().isNull() ? null : constantText(result.get()));
    }

    // The expression with the column replaced by the component value, folded to a constant.
    static Optional<ConstantOperator> evaluate(ScalarOperator expr, ColumnRefOperator column, String value) {
        try {
            if (expr.isColumnRef()) {
                if (value == null) {
                    return Optional.of(ConstantOperator.createNull(column.getType()));
                }
                return ConstantOperator.createVarchar(value).castTo(column.getType());
            }
            List<ScalarOperator> children = new ArrayList<>(expr.getChildren().size());
            for (ScalarOperator child : expr.getChildren()) {
                if (child.isConstantRef()) {
                    children.add(child);
                    continue;
                }
                Optional<ConstantOperator> folded = evaluate(child, column, value);
                if (folded.isEmpty()) {
                    return Optional.empty();
                }
                children.add(folded.get());
            }
            if (expr instanceof CastOperator) {
                ConstantOperator child = (ConstantOperator) children.get(0);
                if (child.isNull()) {
                    return Optional.of(ConstantOperator.createNull(expr.getType()));
                }
                // A value the target type cannot hold casts to NULL, as on the BE.
                return Optional.of(child.castTo(expr.getType()).orElse(ConstantOperator.createNull(expr.getType())));
            }
            if (expr instanceof CallOperator) {
                CallOperator call = (CallOperator) expr;
                Optional<ConstantOperator> chosen = chooseAmongConstants(call.getFnName(), children);
                if (chosen.isPresent()) {
                    return chosen.get().isNull() || chosen.get().getType().equals(expr.getType())
                            ? chosen : chosen.get().castTo(expr.getType());
                }
                // The optimizer's own folding of a call over constants.
                ScalarOperator result = new ScalarOperatorRewriter().rewrite(
                        new CallOperator(call.getFnName(), call.getType(), children, call.getFunction()),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                if (result instanceof ConstantOperator) {
                    return Optional.of((ConstantOperator) result);
                }
            }
            return Optional.empty();
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static boolean isNonNullConstant(ScalarOperator operator) {
        return operator.isConstantRef() && !((ConstantOperator) operator).isNull();
    }

    /**
     * Whether a tuple component satisfies the conjunct. Empty when the component cannot be compared with
     * the constant.
     */
    static Optional<Boolean> matches(ScalarOperator conjunct, Type type, String value) {
        if (conjunct instanceof IsNullPredicateOperator) {
            boolean isNull = value == null;
            return Optional.of(((IsNullPredicateOperator) conjunct).isNotNull() != isNull);
        }
        if (value == null) {
            return Optional.of(false);
        }
        if (conjunct instanceof LikePredicateOperator) {
            return LikePatternEstimator.pattern((LikePredicateOperator) conjunct).map(pattern -> pattern.matches(value));
        }
        if (conjunct instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator predicate = (BinaryPredicateOperator) conjunct;
            Optional<Integer> cmp = compare(type, value, (ConstantOperator) predicate.getChild(1));
            if (cmp.isEmpty()) {
                return Optional.empty();
            }
            switch (predicate.getBinaryType()) {
                case EQ:
                    return Optional.of(cmp.get() == 0);
                case NE:
                    return Optional.of(cmp.get() != 0);
                case LT:
                    return Optional.of(cmp.get() < 0);
                case LE:
                    return Optional.of(cmp.get() <= 0);
                case GT:
                    return Optional.of(cmp.get() > 0);
                case GE:
                    return Optional.of(cmp.get() >= 0);
                default:
                    return Optional.empty();
            }
        }
        if (conjunct instanceof InPredicateOperator) {
            InPredicateOperator predicate = (InPredicateOperator) conjunct;
            boolean found = false;
            for (int i = 1; i < predicate.getChildren().size() && !found; i++) {
                Optional<Integer> cmp = compare(type, value, (ConstantOperator) predicate.getChild(i));
                if (cmp.isEmpty()) {
                    return Optional.empty();
                }
                found = cmp.get() == 0;
            }
            return Optional.of(predicate.isNotIn() != found);
        }
        return Optional.empty();
    }

    /**
     * Compares a stored tuple component with a predicate constant. Numbers, dates and booleans compare
     * as numbers, so the text forms of the BE and the FE need not agree on trailing zeros; strings
     * compare bytewise like the BE does.
     */
    static Optional<Integer> compare(Type type, String value, ConstantOperator constant) {
        if (type.isBoolean()) {
            return Optional.of(Double.compare(booleanValue(value), booleanValue(constantText(constant))));
        }
        if (type.isFixedPointType() || type.isDecimalV3() || type.isDecimalV2()) {
            // Exact: a double cannot tell large integers or high-precision decimals apart.
            try {
                return Optional.of(Integer.signum(
                        new BigDecimal(value).compareTo(new BigDecimal(constantText(constant)))));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }
        if (type.isNumericType() || type.isDate() || type.isDatetime()) {
            try {
                Optional<Double> left = StatisticUtils.convertStatisticsToDouble(type, value);
                Optional<Double> right =
                        StatisticUtils.convertStatisticsToDouble(constant.getType(), constantText(constant));
                if (left.isPresent() && right.isPresent()) {
                    return Optional.of(Double.compare(left.get(), right.get()));
                }
                return Optional.empty();
            } catch (RuntimeException e) {
                return Optional.empty();
            }
        }
        // Bytewise over UTF-8, as the BE compares strings; UTF-16 units would order supplementary
        // characters below the basic plane.
        return Optional.of(Integer.signum(Arrays.compareUnsigned(value.getBytes(StandardCharsets.UTF_8),
                constantText(constant).getBytes(StandardCharsets.UTF_8))));
    }

    private static double booleanValue(String text) {
        return "1".equals(text) || "true".equalsIgnoreCase(text) ? 1.0 : 0.0;
    }

    static String constantText(ConstantOperator constant) {
        if (constant.getType().isBoolean()) {
            return constant.getBoolean() ? "1" : "0";
        }
        return constant.toString();
    }
}

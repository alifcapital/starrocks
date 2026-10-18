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

import com.starrocks.sql.optimizer.Utils;
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
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Writes the conditions of a scan predicate over the virtual columns of the statistics on expressions, for the
 * estimate only. The predicate of the scan, the plan and the execution do not change: the result is a copy of the
 * predicate that only the statistics estimate reads.
 *
 * A condition is written over x when its expression can be written as g(f): an exact match of a part of the
 * condition with the canonical form of f, or the prefix rule for f = substr(c, 1, K). An atom is written only when
 * no column of f remains in it. Inside an AND, an OR or a NOT, the atoms that are written go over x and the others
 * stay as they are, so a conjunct is written when at least one of its atoms is.
 *
 * We expect the conditions on c and on the columns that share an MCV group with c to be estimated together by one
 * source, so we do not replace a source that counts them together with x unless x counts them together too:
 * - the statistics of c, when every condition with f reduces to bounds on c and the other conditions compare c
 *   itself with constants: the bounds narrow c, and the next condition is estimated on the narrowed c;
 * - the head of a group without x that holds every column of the conditions: f is evaluated on its head values,
 *   so the conditions with f are written in the canonical form, which the FE can fold;
 * - the head of x or of a group of x and other columns: the conditions with f and conditions on the other columns.
 * When several sources count all conditions, x is taken. When no source counts them all, the conditions with f are
 * estimated over x and the others as before, and the two shares multiply as independent.
 */
public final class ExpressionStatisticsMatcher {
    // We expect predicates of hundreds of atoms over x to be generated, and fear that their estimate takes longer than
    // the planner waits: every atom written over x makes the estimate of the AND, OR or NOT around it filter the head
    // of x again, so the work is the number of atoms times the size of the head, and 600 atoms of x <> p took 2.4 to
    // 3.0 s on the stand against the 3 s the planner gives. The atoms of one axis, such as the equalities of an OR, are
    // one set before they are counted, and estimated in one pass. The other atoms written over x are counted over the
    // predicate, after the source of each column is chosen, and an atom past this number stays as it is, to be
    // estimated without x, while the atoms before it are still written. The atoms of the worst form that do not
    // merge, x >= p, take 0.24 s for 64 of them in an AND on the stand, and 0.15 s for an OR or an AND of 64 on a head
    // of 600 values in ExpressionStatisticsLargeOrTest.
    static final int MAX_ATOMS_OVER_X = 64;

    private ExpressionStatisticsMatcher() {
    }

    // The atoms that can still be written over x in a predicate; the cost of a written atom is paid here.
    private static final class AtomBudget {
        private int remaining;
        private int spent;

        private AtomBudget(int remaining) {
            this.remaining = remaining;
        }

        // The budget of a count: how many atoms a conjunct would cost, without limit.
        private static AtomBudget unlimited() {
            return new AtomBudget(Integer.MAX_VALUE);
        }

        private boolean take() {
            return take(1);
        }

        private boolean take(int atoms) {
            if (remaining < atoms) {
                return false;
            }
            remaining -= atoms;
            spent += atoms;
            return true;
        }
    }

    /** The copy of the predicate for the estimate. */
    public static final class Result {
        private final ScalarOperator predicate;
        private final Set<ColumnRefOperator> usedColumns;
        private final boolean hasOr;
        private final Set<ColumnRefOperator> filteredColumns;
        private final Set<ColumnRefOperator> nonNullColumns;
        private final Set<ColumnRefOperator> nullRowsLeftOutColumns;
        // By the position of a conjunct of the predicate: the condition written for it, null to keep it as it is,
        // and whether the copy drops it.
        private final ScalarOperator[] replacement;
        private final boolean[] dropped;

        Result(List<ScalarOperator> conjuncts, ScalarOperator[] replacement, boolean[] dropped,
                Set<ColumnRefOperator> usedColumns, boolean hasOr, Set<ColumnRefOperator> filteredColumns,
                Set<ColumnRefOperator> nonNullColumns, Set<ColumnRefOperator> nullRowsLeftOutColumns) {
            this.replacement = replacement;
            this.dropped = dropped;
            this.predicate = copy(conjuncts, replacement, dropped);
            this.usedColumns = Collections.unmodifiableSet(usedColumns);
            this.hasOr = hasOr;
            this.filteredColumns = Collections.unmodifiableSet(filteredColumns);
            this.nonNullColumns = Collections.unmodifiableSet(nonNullColumns);
            this.nullRowsLeftOutColumns = Collections.unmodifiableSet(nullRowsLeftOutColumns);
        }

        /**
         * The same match for a predicate of conjuncts equal to these, one by one in the same order and with the same
         * flags: the copy keeps the conjunct objects of that predicate and the conditions written here.
         */
        Result forConjuncts(List<ScalarOperator> conjuncts) {
            return new Result(conjuncts, replacement, dropped, usedColumns, hasOr, filteredColumns, nonNullColumns,
                    nullRowsLeftOutColumns);
        }

        private static ScalarOperator copy(List<ScalarOperator> conjuncts, ScalarOperator[] replacement,
                                           boolean[] dropped) {
            List<ScalarOperator> copy = new ArrayList<>(conjuncts.size());
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!dropped[i]) {
                    copy.add(replacement[i] != null ? replacement[i] : conjuncts.get(i));
                }
            }
            return copy.isEmpty() ? ConstantOperator.createBoolean(true) : Utils.compoundAnd(copy);
        }

        public ScalarOperator getPredicate() {
            return predicate;
        }

        /** The virtual columns the copy uses. */
        public Set<ColumnRefOperator> getUsedColumns() {
            return usedColumns;
        }

        /**
         * Whether a condition written over a virtual column has an OR, over it alone or with a part on a real
         * column.
         */
        public boolean hasOr() {
            return hasOr;
        }

        /**
         * The virtual columns whose statistics after the estimate describe the rows of the scan: every estimated
         * condition on the columns of their expression is written over them.
         */
        public Set<ColumnRefOperator> getFilteredColumns() {
            return filteredColumns;
        }

        /**
         * The source columns with a condition written over a virtual column that is never true when the column is
         * NULL: the rows after the filter have no NULL in them.
         */
        public Set<ColumnRefOperator> getNonNullColumns() {
            return nonNullColumns;
        }

        /**
         * The source columns whose NULL rows the estimate over x already leaves out: those with a condition that
         * reads only x and is never true when the column is NULL, so a kept condition on the column must not leave
         * them out again. A condition with a part kept on the column is not among them: we expect that part to leave
         * the NULL rows out in its own estimate, and a NULL fraction of 0 would raise its share.
         */
        public Set<ColumnRefOperator> getNullRowsLeftOutColumns() {
            return nullRowsLeftOutColumns;
        }
    }

    private enum Source { X, COLUMN, GROUP }

    /**
     * The copy of the predicate with the conditions written over the virtual columns, or null when no condition
     * is written and the estimate goes as before.
     */
    public static Result match(ScalarOperator predicate, ScanExpressionStatistics expressions, Statistics statistics) {
        if (predicate == null || expressions == null || expressions.getEntries().isEmpty()) {
            return null;
        }
        if (Collections.disjoint(Utils.extractColumnRef(predicate), expressions.getSourceColumns())) {
            return null;
        }
        return expressions.getCache().match(predicate, expressions, statistics,
                () -> matchUncached(predicate, expressions, statistics));
    }

    private static Result matchUncached(ScalarOperator predicate, ScanExpressionStatistics expressions,
                                        Statistics statistics) {
        ExpressionStatisticsCache cache = expressions.getCache();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        int size = conjuncts.size();
        boolean anyUnknown = statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown);
        boolean[] estimated = new boolean[size];
        List<Set<ColumnRefOperator>> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ScalarOperator conjunct = conjuncts.get(i);
            // A bound kept only to skip files is not estimated by any path. A range derived from other conditions
            // is estimated as before when every column has statistics, so it counts as a condition on its column.
            estimated[i] = !conjunct.isNotEvalEstimate() && !(conjunct.isFromPredicateRangeDerive() && anyUnknown);
            columns.add(new HashSet<>(Utils.extractColumnRef(conjunct)));
        }

        // The conditions each expression can write, by the position of the conjunct, and the atoms each of them costs.
        // The cost is not paid here: the expression that is chosen for a column pays it below.
        Map<ScanExpressionStatistics.Entry, Map<Integer, Integer>> costs = new HashMap<>();
        Map<ScanExpressionStatistics.Entry, Map<Integer, ScalarOperator>> candidates = new LinkedHashMap<>();
        Map<ScanExpressionStatistics.Entry, Set<ColumnRefOperator>> entrySources = new HashMap<>();
        for (ScanExpressionStatistics.Entry entry : expressions.getEntries()) {
            Set<ColumnRefOperator> sources = new HashSet<>(Utils.extractColumnRef(entry.getExpression()));
            entrySources.put(entry, sources);
            PrefixDerivationRule rule = PrefixDerivationRule.of(entry);
            Map<Integer, ScalarOperator> written = new LinkedHashMap<>();
            Map<Integer, Integer> cost = new HashMap<>();
            for (int i = 0; i < size; i++) {
                ScalarOperator conjunct = conjuncts.get(i);
                if (!estimated[i] || conjunct.isFromPredicateRangeDerive() || Collections.disjoint(columns.get(i), sources)) {
                    continue;
                }
                AtomBudget count = AtomBudget.unlimited();
                ScalarOperator rewritten = rewriteTree(conjunct, entry, rule, cache, count);
                if (rewritten != null) {
                    written.put(i, rewritten);
                    cost.put(i, count.spent);
                }
            }
            if (!written.isEmpty()) {
                candidates.put(entry, written);
                costs.put(entry, cost);
            }
        }
        if (candidates.isEmpty()) {
            return null;
        }

        // One expression per column: the one that writes more conditions, then the shortest prefix.
        List<ScanExpressionStatistics.Entry> order = new ArrayList<>(candidates.keySet());
        order.sort(Comparator.comparing((ScanExpressionStatistics.Entry entry) -> -candidates.get(entry).size())
                .thenComparing(entry -> {
                    PrefixDerivationRule rule = PrefixDerivationRule.of(entry);
                    return rule == null ? Integer.MAX_VALUE : rule.getLength();
                }));
        ScalarOperator[] replacement = new ScalarOperator[size];
        boolean[] dropped = new boolean[size];
        Set<Integer> claimed = new HashSet<>();
        Set<ColumnRefOperator> handledSources = new HashSet<>();
        Set<ColumnRefOperator> used = new HashSet<>();
        Set<ColumnRefOperator> nonNull = new HashSet<>();
        Set<ColumnRefOperator> nullRowsLeftOut = new HashSet<>();
        AtomBudget budget = new AtomBudget(MAX_ATOMS_OVER_X);
        boolean hasOr = false;
        boolean changed = false;
        for (ScanExpressionStatistics.Entry entry : order) {
            Set<ColumnRefOperator> sources = entrySources.get(entry);
            if (!Collections.disjoint(sources, handledSources)) {
                continue;
            }
            Map<Integer, ScalarOperator> written = new LinkedHashMap<>(candidates.get(entry));
            written.keySet().removeAll(claimed);
            if (written.isEmpty()) {
                continue;
            }
            handledSources.addAll(sources);
            Set<ColumnRefOperator> component = component(entry, sources, expressions, statistics);
            List<Integer> related = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                if (estimated[i] && !claimed.contains(i) && !Collections.disjoint(columns.get(i), component)) {
                    related.add(i);
                }
            }
            Set<Integer> implied = new HashSet<>();
            List<Integer> others = new ArrayList<>();
            for (int i : related) {
                if (written.containsKey(i)) {
                    continue;
                }
                if (impliedNotNull(conjuncts.get(i), sources, written.keySet(), conjuncts)) {
                    implied.add(i);
                } else {
                    others.add(i);
                }
            }
            Source source = chooseSource(entry, sources, written.keySet(), others, conjuncts, columns, expressions,
                    statistics);
            if (source == Source.COLUMN) {
                claimed.addAll(related);
                continue;
            }
            if (source == Source.GROUP) {
                for (int i : written.keySet()) {
                    ScalarOperator executable = executableTree(conjuncts.get(i), cache);
                    if (executable != null && executable != conjuncts.get(i)) {
                        replacement[i] = executable;
                        changed = true;
                    }
                }
                claimed.addAll(related);
                continue;
            }
            Map<Integer, ScalarOperator> chosen = payForAtoms(written, costs.get(entry), conjuncts, dropped, entry,
                    PrefixDerivationRule.of(entry), cache, budget);
            for (Map.Entry<Integer, ScalarOperator> item : chosen.entrySet()) {
                replacement[item.getKey()] = item.getValue();
                // A condition with a part kept on a real column is not over x only. We do not narrow the
                // distribution of x by it, as the values of x cannot tell which rows the kept part takes, and the
                // NULL rows of the column are left out by the whole condition, not by the estimate over x.
                boolean overXOnly = Utils.extractColumnRef(item.getValue()).stream().allMatch(entry.getColumn()::equals);
                hasOr |= containsOr(item.getValue());
                // A prefix no value can have is written as a constant, which does not read x.
                if (Utils.extractColumnRef(item.getValue()).contains(entry.getColumn())) {
                    used.add(entry.getColumn());
                }
                for (ColumnRefOperator sourceColumn : sources) {
                    boolean known = overXOnly ? nullRowsLeftOut.contains(sourceColumn) : nonNull.contains(sourceColumn);
                    if (!known && rejectsNull(sourceColumn, conjuncts.get(item.getKey()))) {
                        nonNull.add(sourceColumn);
                        if (overXOnly) {
                            nullRowsLeftOut.add(sourceColumn);
                        }
                    }
                }
            }
            for (int i : implied) {
                dropped[i] = true;
            }
            claimed.addAll(written.keySet());
            claimed.addAll(implied);
            changed = true;
        }
        if (!changed) {
            return null;
        }

        // x describes the rows after the scan filter when every estimated condition on its sources is on x.
        Set<ColumnRefOperator> filtered = new HashSet<>();
        for (ScanExpressionStatistics.Entry entry : expressions.getEntries()) {
            Set<ColumnRefOperator> sources = entrySources.get(entry);
            boolean described = true;
            for (int i = 0; i < size && described; i++) {
                if (!estimated[i] || dropped[i] || Collections.disjoint(columns.get(i), sources)) {
                    continue;
                }
                described = replacement[i] != null && Utils.extractColumnRef(replacement[i]).contains(entry.getColumn());
            }
            if (described) {
                filtered.add(entry.getColumn());
            }
        }
        return new Result(conjuncts, replacement, dropped, used, hasOr, filtered, nonNull, nullRowsLeftOut);
    }

    /**
     * The conditions of the conjuncts that the chosen expression writes, within the atoms that are left of the budget
     * of the predicate. The negations x <> a, x NOT IN and NOT of a prefix atom, of different conjuncts, are the atoms
     * of one axis in an AND, so they are one set at the place of the first of them, and the others are dropped; the set
     * costs one atom. A conjunct is paid in the order of the conjuncts: when the atoms of a conjunct are more than are
     * left, its atoms are written again one by one while the budget lasts, and the others stay as they are.
     */
    private static Map<Integer, ScalarOperator> payForAtoms(Map<Integer, ScalarOperator> written,
                                                            Map<Integer, Integer> costs, List<ScalarOperator> conjuncts,
                                                            boolean[] dropped, ScanExpressionStatistics.Entry entry,
                                                            PrefixDerivationRule rule, ExpressionStatisticsCache cache,
                                                            AtomBudget budget) {
        ColumnRefOperator x = entry.getColumn();
        Map<AxisSets.Axis, List<Integer>> negations = new LinkedHashMap<>();
        Map<Integer, AxisSets.Member> members = new HashMap<>();
        for (Map.Entry<Integer, ScalarOperator> item : written.entrySet()) {
            Optional<AxisSets.Member> member = AxisSets.memberOf(item.getValue(), x, true);
            if (member.isPresent()) {
                members.put(item.getKey(), member.get());
                negations.computeIfAbsent(member.get().axis(), axis -> new ArrayList<>()).add(item.getKey());
            }
        }
        Map<Integer, List<Integer>> sets = new HashMap<>();
        negations.values().stream().filter(indexes -> indexes.size() > 1)
                .forEach(indexes -> sets.put(indexes.get(0), indexes));
        Set<Integer> merged = new HashSet<>();
        sets.values().forEach(indexes -> merged.addAll(indexes.subList(1, indexes.size())));
        Map<Integer, ScalarOperator> chosen = new LinkedHashMap<>();
        for (Map.Entry<Integer, ScalarOperator> item : written.entrySet()) {
            int index = item.getKey();
            if (merged.contains(index)) {
                continue;
            }
            List<Integer> set = sets.get(index);
            if (set != null) {
                ScalarOperator one = AxisSets.setOf(members.get(index).axis(),
                        set.stream().map(members::get).toList(), x, true);
                if (one != null && budget.take()) {
                    chosen.put(index, one);
                    set.subList(1, set.size()).forEach(other -> dropped[other] = true);
                    continue;
                }
                // The set is not written: its negations are estimated each as it stands, one by one.
                for (int other : set) {
                    ScalarOperator single = payForConjunct(written.get(other), costs.get(other), conjuncts.get(other),
                            entry, rule, cache, budget);
                    if (single != null) {
                        chosen.put(other, single);
                    }
                }
                continue;
            }
            ScalarOperator paid = payForConjunct(item.getValue(), costs.get(index), conjuncts.get(index), entry, rule,
                    cache, budget);
            if (paid != null) {
                chosen.put(index, paid);
            }
        }
        return chosen;
    }

    // The written conjunct when the budget holds its atoms; else the conjunct written again within what is left, null
    // when no atom of it is.
    private static ScalarOperator payForConjunct(ScalarOperator written, int cost, ScalarOperator conjunct,
                                                 ScanExpressionStatistics.Entry entry, PrefixDerivationRule rule,
                                                 ExpressionStatisticsCache cache, AtomBudget budget) {
        if (budget.take(cost)) {
            return written;
        }
        return rewriteTree(conjunct, entry, rule, cache, budget);
    }

    /**
     * Whether x describes the rows of the scan after the predicate when the predicate is estimated as before: no
     * estimated condition reads a column of its expression.
     */
    public static boolean describesFilteredRows(ScalarOperator predicate, ScanExpressionStatistics.Entry entry,
                                                Statistics statistics) {
        if (predicate == null) {
            return true;
        }
        boolean anyUnknown = statistics.getColumnStatistics().values().stream().anyMatch(ColumnStatistic::isUnknown);
        List<ColumnRefOperator> sources = Utils.extractColumnRef(entry.getExpression());
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            boolean estimated = !conjunct.isNotEvalEstimate() && !(conjunct.isFromPredicateRangeDerive() && anyUnknown);
            if (estimated && !Collections.disjoint(Utils.extractColumnRef(conjunct), sources)) {
                return false;
            }
        }
        return true;
    }

    // The columns whose conditions are estimated together with the conditions on the sources of the expression.
    private static Set<ColumnRefOperator> component(ScanExpressionStatistics.Entry entry, Set<ColumnRefOperator> sources,
                                                    ScanExpressionStatistics expressions, Statistics statistics) {
        Set<ColumnRefOperator> component = new HashSet<>(sources);
        for (MultiColumnCombinedStats group : statistics.getMultiColumnCombinedStats().values()) {
            if (group.hasMcv() && group.getColumns().stream().anyMatch(sources::contains)) {
                group.getColumns().stream().filter(java.util.Objects::nonNull).forEach(component::add);
            }
        }
        for (Set<ColumnRefOperator> key : expressions.getGroups().keySet()) {
            if (key.contains(entry.getColumn())) {
                key.stream().filter(column -> expressions.getEntry(column) == null).forEach(component::add);
            }
        }
        return component;
    }

    private static Source chooseSource(ScanExpressionStatistics.Entry entry, Set<ColumnRefOperator> sources,
                                       Set<Integer> written, List<Integer> others, List<ScalarOperator> conjuncts,
                                       List<Set<ColumnRefOperator>> columns, ScanExpressionStatistics expressions,
                                       Statistics statistics) {
        if (others.isEmpty() || xGroupCounts(entry, sources, others, conjuncts, columns, expressions)) {
            return Source.X;
        }
        if (columnCounts(sources, written, others, conjuncts, statistics)) {
            return Source.COLUMN;
        }
        Set<ColumnRefOperator> needed = new HashSet<>();
        written.forEach(i -> needed.addAll(columns.get(i)));
        others.forEach(i -> needed.addAll(columns.get(i)));
        for (MultiColumnCombinedStats group : statistics.getMultiColumnCombinedStats().values()) {
            if (group.hasMcv() && group.getColumns().containsAll(needed)) {
                return Source.GROUP;
            }
        }
        return Source.X;
    }

    // A group of x and other columns counts the other conditions when they read only its other columns.
    private static boolean xGroupCounts(ScanExpressionStatistics.Entry entry, Set<ColumnRefOperator> sources,
                                        List<Integer> others, List<ScalarOperator> conjuncts,
                                        List<Set<ColumnRefOperator>> columns, ScanExpressionStatistics expressions) {
        for (int i : others) {
            if (!Collections.disjoint(columns.get(i), sources)
                    || MultiColumnMcvEstimator.predicateColumn(conjuncts.get(i)) == null) {
                return false;
            }
            boolean inGroup = false;
            for (Set<ColumnRefOperator> key : expressions.getGroups().keySet()) {
                if (key.contains(entry.getColumn()) && key.containsAll(columns.get(i))) {
                    inGroup = true;
                    break;
                }
            }
            if (!inGroup) {
                return false;
            }
        }
        return true;
    }

    // The statistics of c count the conditions together when every condition with f reduces to bounds on c and
    // the other conditions compare c itself with constants.
    private static boolean columnCounts(Set<ColumnRefOperator> sources, Set<Integer> written, List<Integer> others,
                                        List<ScalarOperator> conjuncts, Statistics statistics) {
        if (sources.size() != 1) {
            return false;
        }
        ColumnRefOperator column = sources.iterator().next();
        for (int i : others) {
            if (!comparesColumn(conjuncts.get(i), column)) {
                return false;
            }
        }
        ColumnStatistic statistic = statistics.getColumnStatistics().get(column);
        for (int i : written) {
            ScalarOperator conjunct = conjuncts.get(i);
            if (conjunct instanceof BinaryPredicateOperator && conjunct.getChild(0) instanceof CallOperator
                    && conjunct.getChild(1).isConstantRef()
                    && MonotonicFilterDerivation.reducesToColumnBounds((BinaryPredicateOperator) conjunct)) {
                continue;
            }
            if (conjunct instanceof LikePredicateOperator && column.equals(conjunct.getChild(0))
                    && statistic != null && statistic.getHistogram() != null
                    && statistic.getHistogram().hasStringValues()
                    && LikePatternEstimator.pattern((LikePredicateOperator) conjunct)
                    .flatMap(LikePatternEstimator.LikePattern::fixedPrefix).isPresent()) {
                continue;
            }
            return false;
        }
        return true;
    }

    // c op constant, c [NOT] IN (constants) or c IS [NOT] NULL, with c bare or under a cast.
    private static boolean comparesColumn(ScalarOperator conjunct, ColumnRefOperator column) {
        if (conjunct.getChildren().isEmpty()) {
            return false;
        }
        ScalarOperator left = conjunct.getChild(0);
        while (left instanceof CastOperator) {
            left = left.getChild(0);
        }
        if (!column.equals(left)) {
            return false;
        }
        if (conjunct instanceof IsNullPredicateOperator) {
            return true;
        }
        if (conjunct instanceof BinaryPredicateOperator) {
            return conjunct.getChild(1).isConstantRef();
        }
        if (conjunct instanceof InPredicateOperator && !((InPredicateOperator) conjunct).isSubquery()) {
            return conjunct.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstantRef);
        }
        return false;
    }

    /**
     * Whether a conjunct is c IS NOT NULL that follows from a condition written over x, for example the NULL check
     * of a join key next to lower(c) = 'a'. We fear dropping the check next to a condition that holds on NULL, such
     * as ifnull(c, 'u') = 'u', so the check goes only when a written condition is never true with c NULL, as the
     * constant folding of the condition with c = NULL shows.
     */
    private static boolean impliedNotNull(ScalarOperator conjunct, Set<ColumnRefOperator> sources,
                                          Set<Integer> written, List<ScalarOperator> conjuncts) {
        if (!(conjunct instanceof IsNullPredicateOperator) || !((IsNullPredicateOperator) conjunct).isNotNull()
                || !conjunct.getChild(0).isColumnRef() || !sources.contains(conjunct.getChild(0))) {
            return false;
        }
        ColumnRefOperator column = (ColumnRefOperator) conjunct.getChild(0);
        for (int i : written) {
            if (rejectsNull(column, conjuncts.get(i))) {
                return true;
            }
        }
        return false;
    }

    // Whether a condition is never true with the column NULL: a structural NULL, else the constant folding.
    private static boolean rejectsNull(ColumnRefOperator column, ScalarOperator condition) {
        if (ExpressionStatisticsEstimator.nullOnNull(condition, column).orElse(false)) {
            return true;
        }
        return Utils.canEliminateNull(Set.of(column), condition);
    }

    static boolean containsOr(ScalarOperator operator) {
        if (operator instanceof CompoundPredicateOperator && ((CompoundPredicateOperator) operator).isOr()) {
            return true;
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (containsOr(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The condition with every atom that can be written over x replaced by its form over x, or null when no atom
     * can. An atom is written only when no column of f remains in it: the estimate over x knows nothing of how x
     * relates to another column, as in concat(lower(c), d) = 'q' with the statistic of lower(c).
     *
     * We want an OR such as left(c, 5) = 'a' OR c = 'b' estimated by x for the part that x knows and by the
     * ordinary estimate for the rest, and not by the ordinary estimate for the whole. So an atom that cannot be
     * written stays as it is inside its AND, OR or NOT, and the estimate of the compound predicate adds up the
     * parts as it does for any other columns.
     */
    private static ScalarOperator rewriteTree(ScalarOperator operator, ScanExpressionStatistics.Entry entry,
                                              PrefixDerivationRule rule, ExpressionStatisticsCache cache,
                                              AtomBudget budget) {
        ScalarOperator rewritten = rewritePart(operator, entry, rule, cache, budget);
        return rewritten == operator ? null : rewritten;
    }

    // The same object when no atom of the operator is written. Each atom written over x is paid from the budget, and
    // an atom the budget cannot pay stays as it is.
    private static ScalarOperator rewritePart(ScalarOperator operator, ScanExpressionStatistics.Entry entry,
                                              PrefixDerivationRule rule, ExpressionStatisticsCache cache,
                                              AtomBudget budget) {
        if (operator instanceof CompoundPredicateOperator compound && !compound.isNot()) {
            return rewriteChain(operator, compound.isOr(), entry, rule, cache, budget);
        }
        if (operator instanceof CompoundPredicateOperator negation) {
            ScalarOperator child = rewritePart(negation.getChild(0), entry, rule, cache, budget);
            if (child == negation.getChild(0)) {
                return operator;
            }
            boolean ofAnd = negation.getChild(0) instanceof CompoundPredicateOperator inner && inner.isAnd();
            if (ConstantOperator.FALSE.equals(child) && !ofAnd) {
                // A prefix no value can have is false on every value of x and NULL on NULL, so its negation is
                // true exactly on the rows where x is not NULL. A conjunction that is false for such a part is false
                // on the rows with x NULL only when its other parts are, so its negation is not this.
                return new IsNullPredicateOperator(true, entry.getColumn());
            }
            return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, child);
        }
        ScalarOperator rewritten = rewriteWritableAtom(operator, entry, rule, cache);
        if (rewritten == null) {
            return operator;
        }
        return reads(rewritten, entry) && !budget.take() ? operator : rewritten;
    }

    // The atom written over x, or null when it cannot be written over x alone.
    private static ScalarOperator rewriteWritableAtom(ScalarOperator atom, ScanExpressionStatistics.Entry entry,
                                                      PrefixDerivationRule rule, ExpressionStatisticsCache cache) {
        ScalarOperator rewritten = rewriteAtom(atom, entry, rule, cache);
        if (rewritten == null || !Utils.extractColumnRef(rewritten).stream().allMatch(entry.getColumn()::equals)) {
            return null;
        }
        return rewritten;
    }

    private static boolean reads(ScalarOperator operator, ScanExpressionStatistics.Entry entry) {
        return Utils.extractColumnRef(operator).contains(entry.getColumn());
    }

    // Whether a part was written as false: a prefix no value can have, which is false on every value of x.
    private static boolean isWrittenFalse(ScalarOperator written, ScalarOperator part) {
        return written != part && ConstantOperator.FALSE.equals(written);
    }

    /**
     * An OR or an AND with its parts written. The estimate of an OR adds up its parts in their order, and it keeps the
     * statistics of x after the first part. We want the same estimate whatever order the query lists the parts in,
     * and the statistics of x to be those of the parts over x, so when some parts read x and some do not, the parts
     * over x go first, each group in the order of the query.
     *
     * The atoms of one axis are one set before they are paid: the atoms of an OR that are values of x or prefixes of
     * x, and the negations of them in an AND, see AxisSets. The set costs one atom of the budget, and when the budget
     * cannot pay it the atoms stay as they are. A compound part pays its own atoms.
     *
     * A part written as false adds no row to an OR, so it is left out; an OR of only such parts is false. We fear the
     * estimate of the OR taking the rows of the part that is false among the rows of the others. An AND with a part
     * written as false is false.
     */
    private static ScalarOperator rewriteChain(ScalarOperator operator, boolean or, ScanExpressionStatistics.Entry entry,
                                               PrefixDerivationRule rule, ExpressionStatisticsCache cache,
                                               AtomBudget budget) {
        List<ScalarOperator> parts = or ? Utils.extractDisjunctive(operator) : Utils.extractConjuncts(operator);
        int size = parts.size();
        ColumnRefOperator x = entry.getColumn();
        ScalarOperator[] written = new ScalarOperator[size];
        boolean[] atom = new boolean[size];
        Map<AxisSets.Axis, List<Integer>> axes = new LinkedHashMap<>();
        Map<Integer, AxisSets.Member> members = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ScalarOperator part = parts.get(i);
            if (part instanceof CompoundPredicateOperator) {
                written[i] = rewritePart(part, entry, rule, cache, budget);
                continue;
            }
            atom[i] = true;
            ScalarOperator rewritten = rewriteWritableAtom(part, entry, rule, cache);
            written[i] = rewritten == null ? part : rewritten;
            Optional<AxisSets.Member> member = rewritten == null ? Optional.empty()
                    : AxisSets.memberOf(rewritten, x, !or);
            if (member.isPresent()) {
                members.put(i, member.get());
                axes.computeIfAbsent(member.get().axis(), axis -> new ArrayList<>()).add(i);
            }
        }
        // The atoms of one axis are one set at the place of the first of them.
        Set<Integer> merged = new HashSet<>();
        Map<Integer, List<Integer>> sets = new HashMap<>();
        for (List<Integer> indexes : axes.values()) {
            if (indexes.size() > 1) {
                ScalarOperator set = AxisSets.setOf(members.get(indexes.get(0)).axis(),
                        indexes.stream().map(members::get).toList(), x, !or);
                if (set != null) {
                    sets.put(indexes.get(0), indexes);
                    written[indexes.get(0)] = set;
                    merged.addAll(indexes.subList(1, indexes.size()));
                }
            }
        }
        // The atoms are paid in the order of the parts. What the budget cannot pay stays as it is: a set stays as its atoms.
        for (int i = 0; i < size; i++) {
            if (!atom[i] || merged.contains(i) || written[i] == parts.get(i) || !reads(written[i], entry)
                    || budget.take()) {
                continue;
            }
            written[i] = parts.get(i);
            List<Integer> set = sets.remove(i);
            if (set != null) {
                set.forEach(other -> {
                    written[other] = parts.get(other);
                    merged.remove(other);
                });
            }
        }
        boolean changed = !merged.isEmpty();
        boolean dropped = false;
        List<ScalarOperator> kept = new ArrayList<>(size);
        int readX = 0;
        for (int i = 0; i < size; i++) {
            if (merged.contains(i)) {
                continue;
            }
            changed |= written[i] != parts.get(i);
            if (isWrittenFalse(written[i], parts.get(i))) {
                if (!or) {
                    return ConstantOperator.FALSE;
                }
                dropped = true;
                continue;
            }
            readX += reads(written[i], entry) ? 1 : 0;
            kept.add(written[i]);
        }
        if (!changed) {
            return operator;
        }
        if (kept.isEmpty()) {
            return ConstantOperator.FALSE;
        }
        boolean sameParts = merged.isEmpty() && !dropped;
        if (!or || readX == 0 || readX == kept.size()) {
            return sameParts ? rebuildChain(operator, or, kept.iterator()) : compound(or, kept);
        }
        List<ScalarOperator> ordered = new ArrayList<>(kept.size());
        for (ScalarOperator part : kept) {
            if (reads(part, entry)) {
                ordered.add(part);
            }
        }
        for (ScalarOperator part : kept) {
            if (!reads(part, entry)) {
                ordered.add(part);
            }
        }
        return Utils.compoundOr(ordered);
    }

    private static ScalarOperator compound(boolean or, List<ScalarOperator> parts) {
        return or ? Utils.compoundOr(parts) : Utils.compoundAnd(parts);
    }

    // The tree of ORs or ANDs of the original with its parts taken in turn from the written ones.
    private static ScalarOperator rebuildChain(ScalarOperator original, boolean or, Iterator<ScalarOperator> parts) {
        if (original instanceof CompoundPredicateOperator compound && !compound.isNot() && compound.isOr() == or) {
            ScalarOperator first = rebuildChain(original.getChild(0), or, parts);
            ScalarOperator second = rebuildChain(original.getChild(1), or, parts);
            return new CompoundPredicateOperator(compound.getCompoundType(), first, second);
        }
        return parts.next();
    }

    private static ScalarOperator rewriteAtom(ScalarOperator atom, ScanExpressionStatistics.Entry entry,
                                              PrefixDerivationRule rule, ExpressionStatisticsCache cache) {
        if (!isSupportedAtom(atom)) {
            return null;
        }
        ScalarOperator left = atom.getChild(0);
        if (atom instanceof IsNullPredicateOperator && left.isColumnRef()) {
            // A NULL check on the column itself stays on the column.
            return null;
        }
        ScalarOperator canonical;
        try {
            canonical = cache.canonical(left);
        } catch (RuntimeException e) {
            return null;
        }
        ScalarOperator replaced = replace(canonical, entry, cache);
        if (replaced != canonical) {
            return PrefixDerivationRule.withLeft(atom, replaced);
        }
        return rule == null ? null : rule.rewrite(atom, canonical);
    }

    // A comparison with a constant, IN with constants, LIKE with a constant pattern or IS [NOT] NULL.
    private static boolean isSupportedAtom(ScalarOperator atom) {
        if (atom instanceof IsNullPredicateOperator) {
            return true;
        }
        if (atom instanceof BinaryPredicateOperator) {
            return atom.getChild(1).isConstantRef() && !((ConstantOperator) atom.getChild(1)).isNull();
        }
        if (atom instanceof InPredicateOperator) {
            return !((InPredicateOperator) atom).isSubquery() && atom.getChildren().stream().skip(1)
                    .allMatch(child -> child.isConstantRef() && !((ConstantOperator) child).isNull());
        }
        if (atom instanceof LikePredicateOperator) {
            return LikePatternEstimator.pattern((LikePredicateOperator) atom).isPresent();
        }
        return false;
    }

    // The canonical expression with every part equal to f replaced by x; the same object when no part is f.
    private static ScalarOperator replace(ScalarOperator canonical, ScanExpressionStatistics.Entry entry,
                                          ExpressionStatisticsCache cache) {
        ExpressionStatisticsCanonicalizer.Key key = cache.key(canonical);
        if (entry.getKey().equals(key)) {
            return entry.getColumn();
        }
        ScalarOperator copy = null;
        for (int i = 0; i < canonical.getChildren().size(); i++) {
            ScalarOperator child = canonical.getChild(i);
            ScalarOperator replaced = replace(child, entry, cache);
            if (replaced != child) {
                if (copy == null) {
                    copy = canonical.clone();
                }
                copy.setChild(i, replaced);
            }
        }
        return copy == null ? canonical : copy;
    }

    /**
     * The condition with the left side of each comparison in the canonical form, which the FE can fold on the
     * values of an MCV head: left(c, 3) becomes substr(c, 1, 3). Null when a part cannot be canonicalized.
     */
    static ScalarOperator executableTree(ScalarOperator operator, ExpressionStatisticsCache cache) {
        if (operator instanceof CompoundPredicateOperator) {
            List<ScalarOperator> children = new ArrayList<>(operator.getChildren().size());
            for (ScalarOperator child : operator.getChildren()) {
                ScalarOperator executable = executableTree(child, cache);
                if (executable == null) {
                    return null;
                }
                children.add(executable);
            }
            return new CompoundPredicateOperator(((CompoundPredicateOperator) operator).getCompoundType(), children);
        }
        if (!isSupportedAtom(operator) || !(operator.getChild(0) instanceof CallOperator)) {
            return operator;
        }
        try {
            return PrefixDerivationRule.withLeft(operator, cache.canonical(operator.getChild(0)));
        } catch (RuntimeException e) {
            return null;
        }
    }
}

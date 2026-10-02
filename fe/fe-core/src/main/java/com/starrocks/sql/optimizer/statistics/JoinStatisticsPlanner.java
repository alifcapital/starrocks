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

import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

/** Query-local matching, generation snapshots and bounded entropy solves. */
public final class JoinStatisticsPlanner {
    private static final Logger LOG = LogManager.getLogger(JoinStatisticsPlanner.class);
    private record Request(Map<String, JoinStatisticsScope.Source> sources,
                           Set<JoinStatisticsScope.Equality> equalities, Set<String> outputs,
                           Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns) {
    }

    private record KeyRequest(JoinStatisticsScope scope, ColumnRefOperator column) {
    }

    private record SkewRequest(Request scope, List<ColumnRefOperator> columns, double rows, int limit) { }

    private final Map<SkewRequest, Optional<SkewJoinStatistics.Distribution>> skew = new HashMap<>();

    private record OuterRequest(Request request, String optionalSource) { }

    public record KeyStatistics(double rows, DegreeStatistics degree) {
    }

    record OuterEstimate(double rows, double matchedRows) { }

    private final Map<Long, Optional<JoinStatisticsData>> snapshots = new HashMap<>();
    private final Map<Request, OptionalDouble> estimates = new LinkedHashMap<>();
    private final Map<KeyRequest, KeyStatistics> keys = new HashMap<>();
    private final Map<OuterRequest, Boolean> outerPreservation = new HashMap<>();
    private final Map<OuterRequest, Optional<OuterEstimate>> outerEstimates = new HashMap<>();
    private final JoinStatisticsCorrelation.Evaluation correlations = new JoinStatisticsCorrelation.Evaluation();
    private List<JoinStatisticsMeta> definitions;
    private final Map<Long, JoinStatisticsMeta> definitionsById = new HashMap<>();
    private Set<String> definitionTables;
    private ColumnRefFactory scopeFactory;
    private long scopeTableVersion = -1;
    private boolean scopeRelevant;
    private boolean scopeFailed;
    private long elapsedNanos;
    private long retainedSnapshotBytes;

    public synchronized boolean hasDefinitions() {
        if (scopeFailed) {
            return false;
        }
        if (definitions == null) {
            ConnectContext context = ConnectContext.get();
            definitions = context != null && (context.isStatisticsJob()
                    || !context.getSessionVariable().isCboEnableJoinStatistics()) ? List.of()
                    : context != null && context.getJoinStatisticsReplay() != null
                    ? context.getJoinStatisticsReplay().entries().stream().map(e -> e.meta()).toList()
                    : GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsRegistry().snapshot();
        }
        if (definitionsById.isEmpty()) {
            definitions.forEach(meta -> definitionsById.put(meta.getId(), meta));
        }
        return !definitions.isEmpty();
    }

    /** Keep optional provenance failures out of ordinary cardinality estimation. */
    synchronized JoinStatisticsScope deriveScope(ExpressionContext context, ColumnRefFactory factory,
                                                 boolean includePostProcessing) {
        try {
            if (!hasDefinitions() || !scopeRelevant(factory)) {
                return null;
            }
            return JoinStatisticsScope.derive(context, includePostProcessing);
        } catch (RuntimeException e) {
            scopeFailed = true;
            LOG.warn("Disabling JOIN statistics for this planning attempt after provenance failure", e);
            return null;
        }
    }

    private boolean scopeRelevant(ColumnRefFactory factory) {
        // Direct estimator callers may not have registered a query's tables. Stay conservative there.
        if (factory == null || factory.getColumnRefToTable().isEmpty()) {
            return true;
        }
        if (scopeFactory == factory && scopeTableVersion == factory.getTableMappingVersion()) {
            return scopeRelevant;
        }
        if (definitionTables == null) {
            definitionTables = new HashSet<>();
            for (JoinStatisticsMeta meta : definitions) {
                for (JoinStatisticsDefinition.Source source : meta.getDefinition().getSources()) {
                    definitionTables.add(source.getTableUuid());
                }
            }
        }
        scopeRelevant = false;
        // All query sources remain in scope once any source is relevant: AB must still help ABC.
        // Recheck when rewrites add/replace tables, including native materialized views.
        for (Table table : new HashSet<>(factory.getColumnRefToTable().values())) {
            if (definitionTables.contains(table.getUUID())) {
                scopeRelevant = true;
                break;
            }
        }
        scopeFactory = factory;
        scopeTableVersion = factory.getTableMappingVersion();
        return scopeRelevant;
    }

    /** Executable plans retain scalar decisions, not all generations touched during join enumeration. */
    public synchronized void finishPlanning() {
        snapshots.clear();
        retainedSnapshotBytes = 0;
        estimates.clear();
        keys.clear();
        skew.clear();
        outerPreservation.clear();
        outerEstimates.clear();
        correlations.clear();
        definitions = List.of();
        definitionsById.clear();
        definitionTables = null;
        scopeFactory = null;
    }

    /**
     * A LEFT JOIN against at most one matching row preserves every left row exactly once.
     * Prove fanout on the participating key support; do not infer uniqueness from average NDV.
     * Additional ON conditions can only remove matches and do not filter the preserved side.
     */
    synchronized boolean preservesOuterRows(JoinStatisticsScope preserved, JoinStatisticsScope optional,
                                            ScalarOperator on) {
        if (preserved == null || optional == null || optional.getSources().size() != 1 || !hasDefinitions()) {
            return false;
        }
        String optionalId = optional.getSources().keySet().iterator().next();
        String preservedId = null;
        List<ScalarOperator> equalities = new ArrayList<>();
        for (ScalarOperator conjunct : Utils.extractConjuncts(on)) {
            if (!(conjunct instanceof BinaryPredicateOperator binary) || binary.getBinaryType() != BinaryType.EQ) {
                continue;
            }
            var a = JoinStatisticsScope.sourceColumn(binary.getChild(0));
            var b = JoinStatisticsScope.sourceColumn(binary.getChild(1));
            if (a == null || b == null) {
                continue;
            }
            var left = preserved.getColumns().get(a);
            var right = optional.getColumns().get(b);
            if (left == null || right == null) {
                left = preserved.getColumns().get(b);
                right = optional.getColumns().get(a);
            }
            if (left == null || right == null) {
                continue;
            }
            if (preservedId != null && !preservedId.equals(left.tableUuid())) {
                return false;
            }
            preservedId = left.tableUuid();
            equalities.add(conjunct);
        }
        if (equalities.isEmpty()) {
            return false;
        }
        JoinStatisticsScope pair = JoinStatisticsScope.join(preserved.restrict(Set.of(preservedId)), optional,
                JoinOperator.INNER_JOIN, Utils.compoundAnd(equalities));
        if (pair == null) {
            return false;
        }
        optionalId = pair.getColumns().get(optional.getColumns().keySet().iterator().next()).tableUuid();
        OuterRequest request = new OuterRequest(new Request(pair.getSources(), pair.getEqualities(),
                pair.getOutputs(), pair.getColumns()), optionalId);
        Boolean cached = outerPreservation.get(request);
        if (cached != null) {
            return cached;
        }
        long budget = TimeUnit.MILLISECONDS.toNanos(Config.statistic_join_optimizer_budget_ms) - elapsedNanos;
        if (budget <= 0 || outerPreservation.size() >= 1024) {
            return false;
        }
        long start = System.nanoTime();
        long[] loading = {0};
        boolean result = false;
        try {
            for (var meta : JoinStatisticsBindings.bind(definitions, pair, budget)) {
                if (System.nanoTime() - start - loading[0] >= budget || Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (meta.getGeneration() == 0) {
                    continue;
                }
                var definition = meta.getDefinition();
                int[] domains = JoinStatisticsKeyLayout.match(definition, pair);
                if (domains == null) {
                    continue;
                }
                Optional<JoinStatisticsData> value = snapshot(meta, loading);
                if (value.isEmpty()) {
                    continue;
                }
                var data = value.get();
                int left = -1;
                int right = -1;
                for (int source = 0; source < definition.getSources().size(); source++) {
                    String uuid = definition.getSources().get(source).getUuid();
                    if (uuid.equals(preservedId)) {
                        left = source;
                    }
                    if (uuid.equals(optionalId)) {
                        right = source;
                    }
                }
                if (left < 0 || right < 0
                        || !data.getSources().get(left).getTableUuid().equals(preservedId)
                        || !data.getSources().get(right).getTableUuid().equals(optionalId)) {
                    continue;
                }
                if (!pair.getSources().get(preservedId).tableState().matches(data.getSources().get(left))
                        || !pair.getSources().get(optionalId).tableState().matches(data.getSources().get(right))) {
                    continue;
                }
                var leftSelection = select(data.getSources().get(left), pair.getSources().get(preservedId), pair.getColumns());
                var rightSelection = select(data.getSources().get(right), pair.getSources().get(optionalId), pair.getColumns());
                if (rightSelection == null) {
                    continue;
                }
                for (var basis : data.getBases()) {
                    if ((domains[basis.getDomain()] & (1 << left | 1 << right)) != (1 << left | 1 << right)) {
                        continue;
                    }
                    int l = basis.getSources().indexOf(left);
                    int r = basis.getSources().indexOf(right);
                    if (l < 0 || r < 0) {
                        continue;
                    }
                    double maximum = rightSelection.maximumFrequency(basis, r, leftSelection, l, correlations);
                    if (maximum <= 1) {
                        result = true;
                        Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.RowPreservingOuterJoins", 1);
                        Tracers.log(Tracers.Module.OPTIMIZER,
                                "JOIN statistics {}: row-preserving outer join, maximum matching fanout={}",
                                definition.getName(), maximum);
                        break;
                    }
                }
                if (result) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("Cannot establish row preservation for outer JOIN", e);
        } finally {
            long spent = System.nanoTime() - start - loading[0];
            elapsedNanos += spent;
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.EstimationMicros", spent / 1000);
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.LoadMicros", loading[0] / 1000);
        }
        outerPreservation.put(request, result);
        return result;
    }

    /** Two base inputs, pure equality: inner rows + preserved rows - known matched preserved rows.
     * Never subtract a SEMI upper bound: doing so could manufacture an underestimate of unmatched rows.
     * Duplicating outer joins end provenance above this operator; only their own cardinality is refined.
     */
    synchronized Optional<OuterEstimate> estimateOuter(JoinStatisticsScope preserved, JoinStatisticsScope optional,
                                               ScalarOperator on) {
        if (preserved == null || optional == null || preserved.getSources().size() != 1
                || optional.getSources().size() != 1 || preserved.getOutputs().size() != 1
                || optional.getOutputs().size() != 1 || !hasDefinitions()) {
            return Optional.empty();
        }
        for (var conjunct : Utils.extractConjuncts(on)) {
            if (!(conjunct instanceof BinaryPredicateOperator binary) || binary.getBinaryType() != BinaryType.EQ) {
                return Optional.empty();
            }
        }
        var scope = JoinStatisticsScope.join(preserved, optional, JoinOperator.INNER_JOIN, on);
        if (scope == null || scope.getEqualities().isEmpty()) {
            return Optional.empty();
        }
        String leftId = scope.getColumns().get(preserved.getColumns().keySet().iterator().next()).tableUuid();
        String rightId = scope.getColumns().get(optional.getColumns().keySet().iterator().next()).tableUuid();
        var request = new OuterRequest(new Request(scope.getSources(), scope.getEqualities(), scope.getOutputs(),
                scope.getColumns()), rightId);
        var cached = outerEstimates.get(request);
        if (cached != null) {
            return cached;
        }
        long budget = TimeUnit.MILLISECONDS.toNanos(Config.statistic_join_optimizer_budget_ms) - elapsedNanos;
        if (budget <= 0 || outerEstimates.size() >= 1024) {
            return Optional.empty();
        }
        long start = System.nanoTime();
        long[] loading = {0};
        Optional<OuterEstimate> result = Optional.empty();
        try {
            for (var meta : JoinStatisticsBindings.bind(definitions, scope, budget)) {
                if (System.nanoTime() - start - loading[0] >= budget) {
                    break;
                }
                var domains = JoinStatisticsKeyLayout.match(meta.getDefinition(), scope);
                // A head proves simultaneous matches only for one complete (possibly compound) key.
                if (domains == null || Arrays.stream(domains).filter(d -> Integer.bitCount(d) >= 2).count() != 1) {
                    continue;
                }
                var loaded = snapshot(meta, loading);
                if (loaded.isEmpty()) {
                    continue;
                }
                var data = loaded.get();
                int left = -1;
                int right = -1;
                for (int i = 0; i < data.getSources().size(); i++) {
                    if (data.getSources().get(i).getTableUuid().equals(leftId)) {
                        left = i;
                    }
                    if (data.getSources().get(i).getTableUuid().equals(rightId)) {
                        right = i;
                    }
                }
                if (left < 0 || right < 0) {
                    continue;
                }
                if (!scope.getSources().get(leftId).tableState().matches(data.getSources().get(left))
                        || !scope.getSources().get(rightId).tableState().matches(data.getSources().get(right))) {
                    continue;
                }
                var l = select(data.getSources().get(left), scope.getSources().get(leftId), scope.getColumns());
                var r = select(data.getSources().get(right), scope.getSources().get(rightId), scope.getColumns());
                if (l == null || r == null || !l.hasExactRows() || !r.hasExactRows()) {
                    continue;
                }
                var selections = new ArrayList<JoinStatisticsEstimate.Selection>(
                        java.util.Collections.nCopies(data.getSources().size(), null));
                selections.set(left, l);
                selections.set(right, r);
                int mask = (1 << left) | (1 << right);
                // The inner upper estimate and matched-row subtraction must refer to the same
                // generation. Combining minima across different snapshots can underestimate.
                var inner = JoinStatisticsEstimate.estimate(meta.getDefinition(), data, selections, mask, mask,
                        domains, budget - (System.nanoTime() - start - loading[0]), correlations);
                if (inner.isEmpty()) {
                    continue;
                }
                for (var basis : data.getBases()) {
                    if (Integer.bitCount(domains[basis.getDomain()]) != 2) {
                        continue;
                    }
                    int ls = basis.getSources().indexOf(left);
                    int rs = basis.getSources().indexOf(right);
                    if (ls < 0 || rs < 0) {
                        continue;
                    }
                    double matched = l.knownMatches(basis, ls, r, rs, correlations);
                    double rows = Math.max(l.rowLimit(), inner.getAsDouble() + l.rowLimit() - matched);
                    if (result.isEmpty() || rows < result.get().rows()) {
                        result = Optional.of(new OuterEstimate(rows, matched));
                    }
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("Cannot refine outer JOIN cardinality", e);
        } finally {
            long spent = System.nanoTime() - start - loading[0];
            elapsedNanos += spent;
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.EstimationMicros", spent / 1000);
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.LoadMicros", loading[0] / 1000);
        }
        if (result.isPresent()) {
            Tracers.log(Tracers.Module.OPTIMIZER, "JOIN statistics outer: preserved={}, rows={}",
                    leftId, result.get().rows());
        }
        outerEstimates.put(request, result);
        return result;
    }

    public synchronized OptionalDouble estimate(JoinStatisticsScope scope) {
        if (scope == null || scope.getSources().size() < 2 || !hasDefinitions()) {
            return OptionalDouble.empty();
        }
        Request request = new Request(scope.getSources(), scope.getEqualities(), scope.getOutputs(), scope.getColumns());
        OptionalDouble cached = estimates.get(request);
        if (cached != null) {
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.MemoHits", 1);
            return cached;
        }
        long budget = TimeUnit.MILLISECONDS.toNanos(Config.statistic_join_optimizer_budget_ms) - elapsedNanos;
        if (budget <= 0 || estimates.size() >= 4096) {
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.BudgetFallbacks", 1);
            return OptionalDouble.empty();
        }
        long started = System.nanoTime();
        long[] loadNanos = {0};
        OptionalDouble result = OptionalDouble.empty();
        try {
            for (JoinStatisticsMeta meta : JoinStatisticsBindings.bind(definitions, scope, budget)) {
                if (System.nanoTime() - started - loadNanos[0] >= budget) {
                    break;
                }
                if (meta.getGeneration() == 0) {
                    continue;
                }
                JoinStatisticsDefinition definition = meta.getDefinition();
                Map<String, Integer> positions = new HashMap<>();
                for (int i = 0; i < definition.getSources().size(); i++) {
                    positions.put(definition.getSources().get(i).getUuid(), i);
                }
                if (!positions.keySet().containsAll(scope.getSources().keySet())) {
                    continue;
                }
                int[] domainSources = JoinStatisticsKeyLayout.match(definition, scope);
                if (domainSources == null) {
                    continue;
                }
                Optional<JoinStatisticsData> snapshot = snapshot(meta, loadNanos);
                if (snapshot.isEmpty()) {
                    Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.LoadMisses", 1);
                    continue;
                }
                JoinStatisticsData data = snapshot.get();
                List<JoinStatisticsEstimate.Selection> selections = new ArrayList<>();
                int sources = 0;
                int outputs = 0;
                boolean usable = true;
                for (int source = 0; source < definition.getSources().size(); source++) {
                    String uuid = definition.getSources().get(source).getUuid();
                    if (!data.getSources().get(source).getTableUuid().equals(uuid)) {
                        usable = false;
                        break;
                    }
                    JoinStatisticsScope.Source scan = scope.getSources().get(uuid);
                    if (scan == null) {
                        selections.add(null);
                        continue;
                    }
                    sources |= 1 << source;
                    if (scope.getOutputs().contains(uuid)) {
                        outputs |= 1 << source;
                    }
                    JoinStatisticsEstimate.Selection selection = select(data.getSources().get(source), scan, scope.getColumns());
                    selections.add(selection);
                    usable &= selection != null;
                }
                if (!usable) {
                    continue;
                }
                OptionalDouble candidate = JoinStatisticsEstimate.estimate(definition, data, selections, sources, outputs,
                        domainSources, budget - (System.nanoTime() - started - loadNanos[0]), correlations);
                candidate = scaleEstimate(candidate, data.getSources(), scope);
                if (candidate.isPresent() && (result.isEmpty() || candidate.getAsDouble() < result.getAsDouble())) {
                    result = candidate;
                    Tracers.log(Tracers.Module.OPTIMIZER, "JOIN statistics {} generation {}: sources={}, outputs={}, rows={}",
                            definition.getName(), data.getGeneration(), sources, outputs, result.getAsDouble());
                }
            }
            OptionalDouble combined = compose(scope, budget - (System.nanoTime() - started - loadNanos[0]), loadNanos);
            if (combined.isPresent() && (result.isEmpty() || combined.getAsDouble() < result.getAsDouble())) {
                result = combined;
            }
        } catch (RuntimeException e) {
            LOG.debug("Cannot apply JOIN statistics to this subgraph", e);
        } finally {
            long spent = System.nanoTime() - started - loadNanos[0];
            elapsedNanos += spent;
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.EstimationMicros", spent / 1000);
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.LoadMicros", loadNanos[0] / 1000);
        }
        if (result.isPresent()) {
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.Estimates", 1);
        }
        estimates.put(request, result);
        return result;
    }

    private record Part(JoinStatisticsMeta meta, JoinStatisticsData data, JoinStatisticsScope scope,
                        List<JoinStatisticsEstimate.Selection> selections, int sources, int outputs,
                        int[] domains) { }

    /** Compose constraints over shared identities, not estimates or joined intermediate distributions. */
    private OptionalDouble compose(JoinStatisticsScope scope, long budget, long[] loadNanos) {
        ConnectContext context = ConnectContext.get();
        if (budget <= 0 || scope.getSources().size() < 3
                || (context != null && !context.getSessionVariable().isCboEnableJoinStatisticsComposition())) {
            return OptionalDouble.empty();
        }
        long start = System.nanoTime();
        long initialLoad = loadNanos[0];
        JoinStatisticsKeyLayout layout = new JoinStatisticsKeyLayout(scope);
        int attributes = layout.sources.size() + layout.domains.size();
        if (attributes > JoinStatisticsEntropyModel.MAX_ATTRIBUTES) {
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.CompositionSizeFallbacks", 1);
            return OptionalDouble.empty();
        }
        List<Part> parts = new ArrayList<>();
        Set<List<Object>> duplicates = new HashSet<>();
        // Gather bounded alternatives before choosing a snapshot cohort and a cover. One
        // recent object must not veto an otherwise usable older pair of definitions.
        List<JoinStatisticsMeta> candidates = JoinStatisticsBindings.bind(definitions, scope, budget).stream()
                .filter(meta -> meta.getGeneration() != 0)
                .sorted(java.util.Comparator.comparingLong(JoinStatisticsMeta::getCollectedAt).reversed()
                        .thenComparingLong(JoinStatisticsMeta::getId)).toList();
        for (JoinStatisticsMeta meta : candidates) {
            if (parts.size() >= 64 || System.nanoTime() - start - (loadNanos[0] - initialLoad) >= budget) {
                break;
            }
            JoinStatisticsDefinition definition = meta.getDefinition();
            Set<String> included = new HashSet<>();
            definition.getSources().forEach(source -> included.add(source.getUuid()));
            included.retainAll(scope.getSources().keySet());
            if (included.size() < 2 || included.size() == scope.getSources().size()) {
                continue; // Whole-scope definitions already used by the direct path.
            }
            JoinStatisticsScope local = scope.restrict(included);
            if (local.getEqualities().isEmpty()) {
                continue;
            }
            int[] domains = JoinStatisticsKeyLayout.match(definition, local);
            if (domains == null) {
                continue;
            }
            boolean usable = true;
            for (int domain = 0; domain < domains.length; domain++) {
                if (domains[domain] != 0 && layout.domain(definition, domain, included) < 0) {
                    usable = false;
                }
            }
            if (!usable) {
                continue;
            }
            Optional<JoinStatisticsData> loaded = snapshot(meta, loadNanos);
            if (loaded.isEmpty()) {
                continue;
            }
            JoinStatisticsData data = loaded.get();
            List<JoinStatisticsEstimate.Selection> selected = new ArrayList<>();
            int sourceMask = 0;
            int outputMask = 0;
            for (int source = 0; source < definition.getSources().size(); source++) {
                String uuid = definition.getSources().get(source).getUuid();
                if (!included.contains(uuid)) {
                    selected.add(null);
                    continue;
                }
                var stored = data.getSources().get(source);
                if (!stored.getTableUuid().equals(uuid)) {
                    usable = false;
                    Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.CompositionSnapshotConflicts", 1);
                }
                var selection = select(stored, scope.getSources().get(uuid), scope.getColumns());
                selected.add(selection);
                usable &= selection != null;
                sourceMask |= 1 << source;
                if (scope.getOutputs().contains(uuid)) {
                    outputMask |= 1 << source;
                }
            }
            if (!usable) {
                continue;
            }
            List<Object> identity = new ArrayList<>();
            for (int source = 0; source < definition.getSources().size(); source++) {
                var declared = definition.getSources().get(source);
                identity.add(List.of(declared.getUuid(), declared.getPredicates(), data.getSources().get(source).getSnapshot()));
            }
            for (var domain : definition.getDomains()) {
                identity.add(List.of(domain.getColumns(), domain.getTypes()));
            }
            if (duplicates.add(identity)) {
                parts.add(new Part(meta, data, local, selected, sourceMask, outputMask, domains));
            }
        }
        parts = selectCompositionCover(parts, scope, start, initialLoad, loadNanos, budget);
        if (parts.isEmpty()) {
            return OptionalDouble.empty();
        }
        JoinStatisticsEntropyModel model = layout.domains.size() == 1
                ? JoinStatisticsEntropyModel.commonKeyStar(attributes) : new JoinStatisticsEntropyModel(attributes);
        int objective = 0;
        for (int source = 0; source < layout.sources.size(); source++) {
            if (scope.getOutputs().contains(layout.sources.get(source))) {
                objective |= 1 << (layout.domains.size() + source);
            }
        }
        for (Part part : parts) {
            long remaining = budget - (System.nanoTime() - start - (loadNanos[0] - initialLoad));
            if (remaining <= 0) {
                return OptionalDouble.empty();
            }
            var definition = part.meta.getDefinition();
            var prepared = JoinStatisticsEstimate.prepare(definition, part.data, part.selections, part.sources,
                    part.outputs, part.domains, remaining, correlations, false);
            if (prepared == null) {
                return OptionalDouble.empty();
            }
            int count = Integer.bitCount(part.sources);
            for (int key : prepared.keys()) {
                if (key != 0) {
                    count++;
                }
            }
            int[] mapping = new int[count];
            for (int source = 0; source < prepared.rows().length; source++) {
                if (prepared.rows()[source] != 0) {
                    int global = layout.sources.indexOf(definition.getSources().get(source).getUuid());
                    mapping[Integer.numberOfTrailingZeros(prepared.rows()[source])] = 1 << (layout.domains.size() + global);
                }
            }
            for (int domain = 0; domain < prepared.keys().length; domain++) {
                if (prepared.keys()[domain] != 0) {
                    int global = layout.domain(definition, domain, part.scope.getSources().keySet());
                    mapping[Integer.numberOfTrailingZeros(prepared.keys()[domain])] = 1 << global;
                }
            }
            model.include(prepared.model(), mapping);
        }
        OptionalDouble result = model.estimate(objective,
                budget - (System.nanoTime() - start - (loadNanos[0] - initialLoad)));
        Map<String, JoinStatisticsData.Source> collectedSources = new HashMap<>();
        for (Part part : parts) {
            for (var source : part.data.getSources()) {
                if (part.scope.getSources().containsKey(source.getTableUuid())) {
                    collectedSources.putIfAbsent(source.getTableUuid(), source);
                }
            }
        }
        result = scaleEstimate(result, collectedSources.values(), scope);
        if (result.isPresent()) {
            Tracers.count(Tracers.Module.OPTIMIZER, "JoinStatistics.Compositions", 1);
            Tracers.log(Tracers.Module.OPTIMIZER, "JOIN statistics composition {}: sources={}, outputs={}, rows={}",
                    parts.stream().map(part -> part.meta.getDefinition().getName()).toList(),
                    scope.getSources().size(), scope.getOutputs().size(), result.getAsDouble());
        }
        return result;
    }

    private static List<Part> selectCompositionCover(List<Part> candidates, JoinStatisticsScope scope,
                                                      long started, long initialLoad, long[] loadNanos, long budget) {
        // Try every bounded seed: greedy coverage alone cannot repair an incompatible first snapshot.
        for (Part seed : candidates) {
            Map<String, Long> versions = new HashMap<>();
            Set<String> sources = new HashSet<>();
            Set<JoinStatisticsScope.Equality> edges = new HashSet<>();
            List<Part> selected = new ArrayList<>();
            Part next = seed;
            while (next != null && selected.size() < 16) {
                if (System.nanoTime() - started - (loadNanos[0] - initialLoad) >= budget) {
                    return List.of();
                }
                selected.add(next);
                sources.addAll(next.scope.getSources().keySet());
                edges.addAll(next.scope.getEqualities());
                for (var source : next.data.getSources()) {
                    if (next.scope.getSources().containsKey(source.getTableUuid())) {
                        versions.put(source.getTableUuid(), source.getSnapshot());
                    }
                }
                next = null;
                int bestGain = -1;
                for (Part candidate : candidates) {
                    if (selected.contains(candidate) || !compatible(candidate, versions)) {
                        continue;
                    }
                    int gain = 0;
                    for (String source : candidate.scope.getSources().keySet()) {
                        if (!sources.contains(source)) {
                            gain++;
                        }
                    }
                    for (var edge : candidate.scope.getEqualities()) {
                        if (!edges.contains(edge)) {
                            gain++;
                        }
                    }
                    if (gain > bestGain) {
                        next = candidate;
                        bestGain = gain;
                    }
                }
            }
            if (selected.size() >= 2 && sources.containsAll(scope.getSources().keySet())
                    && edges.containsAll(scope.getEqualities())) {
                return selected;
            }
        }
        return List.of();
    }

    private static boolean compatible(Part part, Map<String, Long> versions) {
        for (var source : part.data.getSources()) {
            if (part.scope.getSources().containsKey(source.getTableUuid())) {
                Long version = versions.get(source.getTableUuid());
                if (version != null && version != source.getSnapshot()) {
                    return false;
                }
            }
        }
        return true;
    }

    /** A base-scan RF denominator and, for one exact slice, NDV need not inherit a weaker local estimate. */
    public synchronized KeyStatistics keyStatistics(JoinStatisticsScope scope, ColumnRefOperator column) {
        if (scope == null || column == null || scope.getSources().size() != 1 || scope.getOutputs().isEmpty()
                || !hasDefinitions()) {
            return null;
        }
        JoinStatisticsScope.ColumnOrigin origin = scope.getColumns().get(column);
        if (origin == null) {
            return null;
        }
        KeyRequest request = new KeyRequest(scope, column);
        if (keys.containsKey(request)) {
            return keys.get(request);
        }
        long budget = TimeUnit.MILLISECONDS.toNanos(Config.statistic_join_optimizer_budget_ms) - elapsedNanos;
        if (budget <= 0 || keys.size() >= 4096) {
            return null;
        }
        long started = System.nanoTime();
        long[] loadNanos = {0};
        KeyStatistics result = null;
        long newest = -1;
        try {
            for (JoinStatisticsMeta meta : JoinStatisticsBindings.bind(definitions, scope, budget)) {
                if (System.nanoTime() - started - loadNanos[0] >= budget) {
                    break;
                }
                if (meta.getGeneration() == 0 || meta.getCollectedAt() < newest) {
                    continue;
                }
                JoinStatisticsDefinition definition = meta.getDefinition();
                for (int source = 0; source < definition.getSources().size(); source++) {
                    if (!definition.getSources().get(source).getUuid().equals(origin.tableUuid())) {
                        continue;
                    }
                    for (int domain = 0; domain < definition.getDomains().size(); domain++) {
                        var key = definition.getDomains().get(domain);
                        if (!List.of(origin.name()).equals(key.getColumns().get(source))
                                || !JoinStatisticsDefinition.matchesKeyType(origin.type(), key.getTypes().get(0))) {
                            continue;
                        }
                        var value = snapshot(meta, loadNanos);
                        if (value.isEmpty()) {
                            continue;
                        }
                        var data = value.get().getSources().get(source);
                        if (!data.getTableUuid().equals(origin.tableUuid())) {
                            continue;
                        }
                        var selection = select(data, scope.getSources().get(origin.tableUuid()), scope.getColumns());
                        KeyStatistics candidate = selection == null ? null : selection.keyStatistics(data, domain);
                        if (candidate != null) {
                            result = new KeyStatistics(candidate.rows()
                                    * scope.getSources().get(origin.tableUuid()).tableState().scale(data), candidate.degree());
                            newest = meta.getCollectedAt();
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("Cannot apply conditional JOIN key statistics", e);
        } finally {
            elapsedNanos += System.nanoTime() - started - loadNanos[0];
        }
        keys.put(request, result);
        return result;
    }

    /** Exact retained-key frequencies. Different-key chains require joint key frequencies, not marginal products. */
    public synchronized SkewJoinStatistics.Distribution skewStatistics(JoinStatisticsScope scope,
            List<ColumnRefOperator> columns, double estimatedRows, int limit) {
        if (scope == null || scope.getOutputs().isEmpty() || !hasDefinitions()) {
            return null;
        }
        List<JoinStatisticsScope.ColumnOrigin> origins = columns.stream().map(scope.getColumns()::get).toList();
        if (origins.stream().anyMatch(java.util.Objects::isNull)
                || origins.stream().map(JoinStatisticsScope.ColumnOrigin::tableUuid).distinct().count() != 1) {
            return null;
        }
        var request = new SkewRequest(new Request(scope.getSources(), scope.getEqualities(),
                scope.getOutputs(), scope.getColumns()), List.copyOf(columns), estimatedRows, limit);
        if (skew.containsKey(request)) {
            return skew.get(request).orElse(null);
        }
        if (skew.size() >= 1024) {
            return null;
        }
        long budget = TimeUnit.MILLISECONDS.toNanos(Config.statistic_join_optimizer_budget_ms) - elapsedNanos;
        if (budget <= 0) {
            return null;
        }
        long started = System.nanoTime();
        long[] loading = {0};
        SkewJoinStatistics.Distribution result = null;
        long newest = -1;
        try {
            for (var meta : JoinStatisticsBindings.bind(definitions, scope, budget)) {
                if (System.nanoTime() - started - loading[0] >= budget || Thread.currentThread().isInterrupted()) {
                    break;
                }
                if (meta.getGeneration() == 0 || meta.getCollectedAt() < newest) {
                    continue;
                }
                var definition = meta.getDefinition();
                int source = -1;
                for (int i = 0; i < definition.getSources().size(); i++) {
                    if (definition.getSources().get(i).getUuid().equals(origins.get(0).tableUuid())) {
                        source = i;
                    }
                }
                if (source < 0) {
                    continue;
                }
                for (int domain = 0; domain < definition.getDomains().size(); domain++) {
                    var key = definition.getDomains().get(domain);
                    var names = key.getColumns().get(source);
                    if (names == null || names.size() != columns.size()) {
                        continue;
                    }
                    int[] positions = origins.stream().mapToInt(o -> names.indexOf(o.name())).toArray();
                    boolean compatible = java.util.Arrays.stream(positions).distinct().count() == positions.length;
                    for (int i = 0; i < positions.length; i++) {
                        compatible &= positions[i] >= 0 && JoinStatisticsDefinition.matchesKeyType(
                                origins.get(i).type(), key.getTypes().get(Math.max(0, positions[i])));
                    }
                    // All inputs must be joined on this entire domain. Do not multiply unrelated marginals.
                    for (var edge : scope.getEqualities()) {
                        int a = domainComponent(definition, key, edge.left());
                        compatible &= a >= 0 && a == domainComponent(definition, key, edge.right());
                    }
                    if (!compatible || (scope.getSources().size() > 1
                            && (JoinStatisticsKeyLayout.match(definition, scope) == null
                            || scope.getEqualities().size() < columns.size() * (scope.getSources().size() - 1)))) {
                        continue;
                    }
                    var loaded = snapshot(meta, loading);
                    if (loaded.isEmpty()) {
                        continue;
                    }
                    var data = loaded.get();
                    final int domainId = domain;
                    var basis = data.getBases().stream().filter(b -> b.getDomain() == domainId).findFirst().orElse(null);
                    if (basis == null || basis.getHeadKeys() == null) {
                        continue;
                    }
                    double[] frequencies = new double[basis.getHeadKeys().size()];
                    java.util.Arrays.fill(frequencies, 1);
                    double rows = estimatedRows;
                    int included = 0;
                    double nullRows = 0;
                    for (int side = 0; side < basis.getSources().size(); side++) {
                        int id = basis.getSources().get(side);
                        String role = definition.getSources().get(id).getUuid();
                        var scan = scope.getSources().get(role);
                        if (scan == null) {
                            continue;
                        }
                        // Ordinary equality rejects NULL keys. Removing its redundant scan check is safe:
                        // head frequencies already contain only non-NULL keys; the row denominator stays conservative.
                        var keyNames = key.getColumns().get(id);
                        var predicates = scan.predicates().stream().filter(predicate -> {
                            if (predicate instanceof IsNullPredicateOperator check && check.isNotNull()
                                    && check.getChild(0) instanceof ColumnRefOperator ref) {
                                var origin = scope.getColumns().get(ref);
                                return origin == null || !origin.tableUuid().equals(role)
                                        || !keyNames.contains(origin.name());
                            }
                            return true;
                        }).toList();
                        var selectedScan = new JoinStatisticsScope.Source(role, scan.estimatedRows(), predicates,
                                scan.physicalUuid(), scan.tableState());
                        var selection = select(data.getSources().get(id), selectedScan, scope.getColumns());
                        if (selection == null || !selection.hasExactRows()) {
                            compatible = false;
                            break;
                        }
                        included++;
                        if (scope.getSources().size() == 1) {
                            rows = selection.rowLimit() * scan.tableState().scale(data.getSources().get(id));
                            if (columns.size() == 1 && predicates.size() == scan.predicates().size()) {
                                nullRows = selection.nullRows(data.getSources().get(id), domain)
                                        * scan.tableState().scale(data.getSources().get(id));
                            }
                        }
                        long[] head = selection.head(basis, side);
                        boolean output = scope.getOutputs().contains(role);
                        double scale = scan.tableState().scale(data.getSources().get(id));
                        for (int h = 0; h < head.length; h++) {
                            frequencies[h] *= scale == 0 ? 0 : output ? head[h] * scale : head[h] > 0 ? 1 : 0;
                        }
                    }
                    if (!compatible || included != scope.getSources().size()) {
                        continue;
                    }
                    // For a JOIN input use its bound as denominator, never divide by an unrelated base cardinality.
                    rows = Math.max(rows, java.util.Arrays.stream(frequencies).sum());
                    if (!Double.isFinite(rows) || rows <= 0) {
                        continue;
                    }
                    // Only retained winners are boxed; a 16K head must not allocate 16K Java Integers per lookup.
                    java.util.PriorityQueue<Integer> top = new java.util.PriorityQueue<>(
                            java.util.Comparator.<Integer>comparingDouble(i -> frequencies[i]).thenComparingInt(i -> -i));
                    double maximumOmittedRows = 0;
                    for (int h = 0; h < frequencies.length; h++) {
                        // Missing text is not SQL NULL and must not displace an available skew key.
                        if (!basis.getHeadKeys().hasValue(h)) {
                            maximumOmittedRows = Math.max(maximumOmittedRows, frequencies[h]);
                            continue;
                        }
                        if (frequencies[h] > 0 && (top.size() < limit || frequencies[h] > frequencies[top.peek()])) {
                            top.add(h);
                            if (top.size() > limit) {
                                top.remove();
                            }
                        }
                    }
                    List<SkewJoinStatistics.Entry> entries = new ArrayList<>();
                    while (!top.isEmpty()) {
                        int h = top.remove();
                        List<String> values = basis.getHeadKeys().tuple(h, columns.size());
                        if (values.size() != columns.size()) {
                            continue;
                        }
                        entries.add(new SkewJoinStatistics.Entry(
                                java.util.Arrays.stream(positions).mapToObj(values::get).toList(), frequencies[h]));
                    }
                    if (nullRows > 0) {
                        entries.add(new SkewJoinStatistics.Entry(java.util.Collections.singletonList(null), nullRows));
                    }
                    entries.sort(java.util.Comparator.comparingDouble(SkewJoinStatistics.Entry::rows).reversed());
                    if (entries.size() > limit) {
                        entries = entries.subList(0, limit);
                    }
                    result = new SkewJoinStatistics.Distribution(rows, entries, "JOIN_STATISTICS", maximumOmittedRows);
                    newest = meta.getCollectedAt();
                }
            }
        } catch (RuntimeException e) {
            LOG.debug("Cannot apply JOIN heavy-key statistics", e);
        } finally {
            long spent = System.nanoTime() - started - loading[0];
            elapsedNanos += spent;
            Tracers.count(Tracers.Module.OPTIMIZER, "SkewStatistics.LookupMicros", spent / 1000);
        }
        if (result != null) {
            Tracers.log(Tracers.Module.OPTIMIZER, "Skew JOIN statistics: columns={}, rows={}, keys={}",
                    columns, result.rows(), result.entries());
        }
        skew.put(request, Optional.ofNullable(result));
        return result;
    }

    private static int domainComponent(JoinStatisticsDefinition definition, JoinStatisticsDefinition.KeyDomain domain,
                                       JoinStatisticsScope.ColumnOrigin origin) {
        for (int source = 0; source < definition.getSources().size(); source++) {
            if (definition.getSources().get(source).getUuid().equals(origin.tableUuid())) {
                var columns = domain.getColumns().get(source);
                return columns == null ? -1 : columns.indexOf(origin.name());
            }
        }
        return -1;
    }

    private static long loadTimeoutMillis() {
        ConnectContext context = ConnectContext.get();
        if (context == null || context.getStartTime() <= 0) {
            return 30000;
        }
        long remaining = context.getSessionVariable().getQueryTimeoutS() * 1000L
                - (System.currentTimeMillis() - context.getStartTime());
        return Math.max(0, Math.min(30000, remaining));
    }

    private Optional<JoinStatisticsData> snapshot(JoinStatisticsMeta meta, long[] loadNanos) {
        JoinStatisticsMeta original = definitionsById.getOrDefault(meta.getId(), meta);
        Optional<JoinStatisticsData> result = snapshots.computeIfAbsent(meta.getId(), ignored -> {
            long loading = System.nanoTime();
            try {
                ConnectContext context = ConnectContext.get();
                Optional<JoinStatisticsData> data;
                if (context != null && context.getJoinStatisticsReplay() != null) {
                    data = Optional.ofNullable(context.getJoinStatisticsReplay().get(meta.getId())).map(e -> e.data());
                } else {
                    data = GlobalStateMgr.getCurrentState().getAnalyzeMgr().getJoinStatisticsManager()
                            .get(original, loadTimeoutMillis());
                }
                if (data.isPresent()) {
                    long bytes = data.get().estimatedSize();
                    // Eviction must not let one optimizer pin an unbounded number of retired cache entries.
                    if (retainedSnapshotBytes + bytes > 512L * 1024 * 1024) {
                        return Optional.empty();
                    }
                    retainedSnapshotBytes += bytes;
                    if (context != null && context.shouldDumpQuery() && context.getDumpInfo() instanceof
                            com.starrocks.sql.optimizer.dump.QueryDumpInfo dump) {
                        dump.getJoinStatistics().add(original, data.get());
                    }
                }
                return data;
            } finally {
                loadNanos[0] += System.nanoTime() - loading;
            }
        });
        return result.map(data -> data.withSourceRoles(meta.getDefinition().getSources().stream()
                .map(JoinStatisticsDefinition.Source::getUuid).toList()));
    }

    public OptionalDouble membership(JoinStatisticsScope build, ColumnRefOperator buildKey,
                                     JoinStatisticsScope probe, ColumnRefOperator probeKey) {
        if (build == null || probe == null || buildKey == null || probeKey == null) {
            return OptionalDouble.empty();
        }
        JoinStatisticsScope joined = JoinStatisticsScope.join(probe, build, JoinOperator.LEFT_SEMI_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, probeKey, buildKey));
        OptionalDouble result = estimate(joined);
        Tracers.count(Tracers.Module.OPTIMIZER,
                result.isPresent() ? "JoinStatistics.RfEstimates" : "JoinStatistics.RfFallbacks", 1);
        return result;
    }

    /** Frequency-preserving best effort, not a bound on a changed key distribution.
     * Scale output multiplicities only: build duplicates do not multiply SEMI/RF membership.
     * All constraints are solved in collection-time units, including composed objects.
     */
    private static OptionalDouble scaleEstimate(OptionalDouble estimate,
            java.util.Collection<JoinStatisticsData.Source> stored, JoinStatisticsScope scope) {
        if (estimate.isEmpty()) {
            return estimate;
        }
        double rows = estimate.getAsDouble();
        for (var source : stored) {
            var scan = scope.getSources().get(source.getTableUuid());
            if (scan == null) {
                continue;
            }
            double factor = scan.tableState().scale(source);
            if (!Double.isFinite(factor)) {
                return OptionalDouble.empty();
            }
            if (factor == 0) {
                return OptionalDouble.of(0);
            }
            if (scope.getOutputs().contains(source.getTableUuid())) {
                rows *= factor;
            }
        }
        if (rows == 0 && scope.getSources().values().stream().allMatch(scan -> scan.estimatedRows() > 0)) {
            return OptionalDouble.empty();
        }
        return Double.isFinite(rows) ? OptionalDouble.of(rows) : OptionalDouble.empty();
    }

    static JoinStatisticsEstimate.Selection select(JoinStatisticsData.Source data, JoinStatisticsScope.Source scan,
                                                    Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns) {
        double scale = scan.tableState().scale(data);
        // No distribution can be extrapolated from an empty collection, regardless of metadata freshness.
        if (data.getRows() == 0 || !Double.isFinite(scale)) {
            return null;
        }
        double estimatedRows = scale > 0 ? scan.estimatedRows() / scale : 0;
        List<IntPredicate> tests = new ArrayList<>();
        Set<Integer> fixed = new HashSet<>();
        boolean extraPredicate = false;
        record InCoverage(int test, int column, Set<ConstantOperator> requested) { }
        List<InCoverage> inChecks = new ArrayList<>();
        for (ScalarOperator predicate : scan.predicates()) {
            List<ColumnRefOperator> refs = Utils.extractColumnRef(predicate);
            if (refs.size() != 1) {
                extraPredicate = true;
                continue;
            }
            JoinStatisticsScope.ColumnOrigin origin = columns.get(refs.get(0));
            int position = origin == null ? -1 : data.getColumns().indexOf(origin.name());
            IntPredicate test = position < 0 || !origin.type().equals(data.getTypes().get(position))
                    ? null : compile(predicate, refs.get(0), data, position);
            if (test == null) {
                extraPredicate = true;
                continue;
            }
            tests.add(test);
            if (predicate instanceof InPredicateOperator in && !in.isNotIn()) {
                Set<ConstantOperator> requested = new HashSet<>();
                for (int i = 1; i < in.getChildren().size(); i++) {
                    ((ConstantOperator) in.getChild(i)).castTo(data.getTypes().get(position)).ifPresent(requested::add);
                }
                inChecks.add(new InCoverage(tests.size() - 1, position, requested));
            }
            if ((predicate instanceof BinaryPredicateOperator binary && binary.getBinaryType() == BinaryType.EQ)
                    || (predicate instanceof IsNullPredicateOperator isNull && !isNull.isNotNull())
                    || predicate instanceof ColumnRefOperator
                    || (predicate instanceof CompoundPredicateOperator compound && compound.isNot()
                    && compound.getChild(0) instanceof ColumnRefOperator)) {
                fixed.add(position);
            }
        }
        List<Integer> selected = new ArrayList<>();
        long covered = 0;
        long allCovered = 0;
        int[] failures = new int[data.getTuples().size()];
        int[] lastFailure = new int[failures.length];
        for (int slice = 0; slice < data.getTuples().size(); slice++) {
            allCovered = Math.addExact(allCovered, data.getTupleRows(slice));
            for (int test = 0; test < tests.size(); test++) {
                if (!tests.get(test).test(slice)) {
                    failures[slice]++;
                    lastFailure[slice] = test;
                }
            }
            if (failures[slice] == 0) {
                selected.add(slice);
                covered = Math.addExact(covered, data.getTupleRows(slice));
            }
        }
        boolean missingValue = false;
        for (InCoverage check : inChecks) {
            Set<ConstantOperator> present = new HashSet<>();
            for (int slice = 0; slice < failures.length; slice++) {
                // Reuse the failure counts: test the context of this IN without reevaluating every predicate.
                if (failures[slice] == 0 || (failures[slice] == 1 && lastFailure[slice] == check.test())) {
                    present.add(data.predicateValue(slice, check.column()));
                }
            }
            missingValue |= !present.containsAll(check.requested());
        }
        if (!tests.isEmpty() && (selected.isEmpty() || missingValue) && estimatedRows > covered && allCovered > 0) {
            boolean[] included = new boolean[data.getTuples().size()];
            selected.forEach(id -> included[id] = true);
            boolean hasRemainder = allCovered > covered;
            int[] remaining = java.util.stream.IntStream.range(0, included.length)
                    .filter(id -> !hasRemainder || !included[id]).toArray();
            double remainingRows = hasRemainder ? allCovered - covered : allCovered;
            double missing = estimatedRows - covered;
            if (Double.isFinite(missing) && missing <= Long.MAX_VALUE) {
                return new JoinStatisticsEstimate.Selection(selected.stream().mapToInt(Integer::intValue).toArray(),
                        (long) Math.ceil(missing), covered + missing, remaining, missing / remainingRows);
            }
        }
        if (selected.isEmpty() && !tests.isEmpty()) {
            return null; // Absence from an old dictionary is not evidence of an empty current distribution.
        }
        boolean exactCombination = fixed.size() == data.getColumns().size() && !selected.isEmpty();
        long residual = exactCombination ? 0 : data.getRows() - allCovered;
        if (residual > 0 && !tests.isEmpty()) {
            // Unrecorded predicate combinations retain the ordinary local selectivity estimate.
            double unobserved = Math.max(0, estimatedRows - covered);
            residual = Math.min(residual, (long) Math.ceil(unobserved));
        }
        if (selected.isEmpty() && data.getRows() != 0 && residual == 0 && allCovered != data.getRows()) {
            return null;
        }
        double rowLimit = (double) covered + residual;
        if (extraPredicate) {
            rowLimit = Math.min(rowLimit, estimatedRows);
        }
        return new JoinStatisticsEstimate.Selection(selected.stream().mapToInt(Integer::intValue).toArray(), residual, rowLimit,
                !extraPredicate && (exactCombination || allCovered == data.getRows()));
    }

    private static IntPredicate compile(ScalarOperator predicate, ColumnRefOperator column,
                                         JoinStatisticsData.Source data, int position) {
        // Boolean equality is normalized to a bare column (or NOT column) by the optimizer.
        if (data.getTypes().get(position).isBoolean() && (predicate.equals(column)
                || (predicate instanceof CompoundPredicateOperator compound && compound.isNot()
                && compound.getChild(0).equals(column)))) {
            boolean expected = predicate.equals(column);
            return slice -> {
                ConstantOperator value = data.predicateValue(slice, position);
                return !value.isNull() && value.getBoolean() == expected;
            };
        }
        if (predicate.getChildren().isEmpty() || !predicate.getChild(0).equals(column)) {
            return null;
        }
        if (predicate instanceof IsNullPredicateOperator isNull) {
            return slice -> data.predicateValue(slice, position).isNull() != isNull.isNotNull();
        }
        if (predicate instanceof BinaryPredicateOperator binary && binary.getChild(1) instanceof ConstantOperator literal) {
            if (!List.of(BinaryType.EQ, BinaryType.NE, BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)
                    .contains(binary.getBinaryType())) {
                return null;
            }
            Optional<ConstantOperator> constant = literal.castTo(data.getTypes().get(position));
            if (constant.isEmpty() || constant.get().isNull()) {
                return null;
            }
            return slice -> {
                ConstantOperator value = data.predicateValue(slice, position);
                if (value.isNull()) {
                    return false;
                }
                int comparison = compare(value, constant.get());
                return switch (binary.getBinaryType()) {
                    case EQ -> comparison == 0;
                    case NE -> comparison != 0;
                    case LT -> comparison < 0;
                    case LE -> comparison <= 0;
                    case GT -> comparison > 0;
                    case GE -> comparison >= 0;
                    default -> false;
                };
            };
        }
        if (predicate instanceof InPredicateOperator in) {
            Set<ConstantOperator> values = new HashSet<>();
            for (int i = 1; i < in.getChildren().size(); i++) {
                if (!(in.getChild(i) instanceof ConstantOperator literal)) {
                    return null;
                }
                Optional<ConstantOperator> constant = literal.castTo(data.getTypes().get(position));
                if (constant.isEmpty() || constant.get().isNull()) {
                    return null;
                }
                values.add(constant.get());
            }
            return slice -> {
                ConstantOperator value = data.predicateValue(slice, position);
                return !value.isNull() && values.contains(value) != in.isNotIn();
            };
        }
        return null;
    }

    private static int compare(ConstantOperator left, ConstantOperator right) {
        if (!left.getType().isStringType()) {
            return left.compareTo(right);
        }
        String a = left.getVarchar();
        String b = right.getVarchar();
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int x = a.codePointAt(i);
            int y = b.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(a.length() - i, b.length() - j);
    }
}

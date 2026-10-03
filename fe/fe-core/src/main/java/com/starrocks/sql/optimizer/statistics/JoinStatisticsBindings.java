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

import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/** Bounded relation-role matching; physical tables may occur more than once in the query or definition. */
final class JoinStatisticsBindings {
    private JoinStatisticsBindings() { }

    static List<JoinStatisticsMeta> bind(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope) {
        return bind(definitions, scope, java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                com.starrocks.common.Config.statistic_join_optimizer_budget_ms));
    }

    static List<JoinStatisticsMeta> bind(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget) {
        return bind(definitions, scope, budget, System::nanoTime);
    }

    static List<JoinStatisticsMeta> bind(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget,
                                         LongSupplier clock) {
        return search(definitions, scope, budget, clock).values();
    }

    private record Result(List<JoinStatisticsMeta> values, boolean complete) { }

    private static Result search(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget,
                                  LongSupplier clock) {
        SearchBudget searchBudget = new SearchBudget(budget, clock);
        if (searchBudget.exhausted()) {
            return new Result(List.of(), false);
        }
        boolean queryRoles = scope.getSources().values().stream().anyMatch(s -> !s.tableUuid().equals(s.physicalUuid()));
        List<JoinStatisticsScope.Source> relations = scope.getSources().values().stream()
                .sorted(java.util.Comparator.comparing(JoinStatisticsScope.Source::tableUuid)).toList();
        Set<String> physicalTables = new HashSet<>();
        relations.forEach(source -> physicalTables.add(source.physicalUuid()));
        List<JoinStatisticsMeta> result = new ArrayList<>();
        for (var meta : definitions) {
            if (searchBudget.exhausted()) {
                break;
            }
            if (meta.getDefinition().getSources().stream().noneMatch(s -> physicalTables.contains(s.getTableUuid()))) {
                continue;
            }
            boolean storedRoles = meta.getDefinition().getSources().stream()
                    .anyMatch(s -> !s.getUuid().equals(s.getTableUuid()));
            if (!queryRoles && !storedRoles) {
                result.add(meta);
                continue;
            }
            List<JoinStatisticsMeta> alternatives = new ArrayList<>();
            assign(meta, scope, relations, 0, new ArrayList<>(), new HashSet<>(), alternatives, searchBudget);
            result.addAll(alternatives);
        }
        return new Result(List.copyOf(result), !searchBudget.stopped);
    }

    /** Definitions are pinned for this planner; only complete role searches enter this bounded memo. */
    static final class Prepared {
        private record Key(Map<String, String> roles, Set<JoinStatisticsScope.Equality> edges) { }
        private record Candidates(List<JoinStatisticsMeta> original, List<JoinStatisticsMeta> newest) { }
        private List<JoinStatisticsMeta> definitions;
        private final Map<String, BitSet> byTable = new HashMap<>();
        private final Map<Key, Candidates> matches = new HashMap<>();
        private final Map<JoinStatisticsMeta, Map<String, Integer>> positions = new java.util.IdentityHashMap<>();
        private long bytes;

        List<JoinStatisticsMeta> bind(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget) {
            return candidates(definitions, scope, budget, System::nanoTime).original();
        }

        List<JoinStatisticsMeta> newest(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget) {
            return candidates(definitions, scope, budget, System::nanoTime).newest();
        }

        List<JoinStatisticsMeta> bind(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget,
                                      LongSupplier clock) {
            return candidates(definitions, scope, budget, clock).original();
        }

        private Candidates candidates(List<JoinStatisticsMeta> definitions, JoinStatisticsScope scope, long budget,
                                       LongSupplier clock) {
            long started = clock.getAsLong();
            if (budget <= 0 || Thread.currentThread().isInterrupted()) {
                return new Candidates(List.of(), List.of());
            }
            if (this.definitions != definitions) {
                clear();
                for (int i = 0; i < definitions.size(); i++) {
                    if (clock.getAsLong() - started >= budget || Thread.currentThread().isInterrupted()) {
                        byTable.clear();
                        return new Candidates(List.of(), List.of());
                    }
                    for (var source : definitions.get(i).getDefinition().getSources()) {
                        byTable.computeIfAbsent(source.getTableUuid(), ignored -> new BitSet()).set(i);
                    }
                }
            }
            this.definitions = definitions;
            Map<String, String> roles = new HashMap<>();
            scope.getSources().forEach((role, source) -> roles.put(role, source.physicalUuid()));
            Key lookup = new Key(roles, scope.getEqualities());
            Candidates cached = matches.get(lookup);
            if (cached != null) {
                return cached;
            }
            BitSet included = new BitSet();
            roles.values().forEach(table -> {
                BitSet indexes = byTable.get(table);
                if (indexes != null) {
                    included.or(indexes);
                }
            });
            List<JoinStatisticsMeta> relevant = new ArrayList<>();
            for (int i = included.nextSetBit(0); i >= 0; i = included.nextSetBit(i + 1)) {
                relevant.add(definitions.get(i));
            }
            Result result = search(relevant, scope, budget - (clock.getAsLong() - started), clock);
            Candidates candidates = new Candidates(result.values(), result.values().stream()
                    .filter(meta -> meta.getGeneration() != 0)
                    .sorted(java.util.Comparator.comparingLong(JoinStatisticsMeta::getCollectedAt).reversed()
                            .thenComparingLong(JoinStatisticsMeta::getId)).toList());
            long charge = 256L + 256L * (roles.size() + scope.getEqualities().size()) + 4096L * result.values().size();
            if (result.complete() && matches.size() < 512 && bytes + charge <= 8L * 1024 * 1024) {
                Set<JoinStatisticsScope.Equality> edges = new HashSet<>();
                for (var edge : scope.getEqualities()) {
                    edges.add(new JoinStatisticsScope.Equality(own(edge.left()), own(edge.right())));
                }
                matches.put(new Key(Map.copyOf(roles), Set.copyOf(edges)), candidates);
                bytes += charge;
            }
            return candidates;
        }

        private static JoinStatisticsScope.ColumnOrigin own(JoinStatisticsScope.ColumnOrigin origin) {
            return new JoinStatisticsScope.ColumnOrigin(origin.tableUuid(), origin.name(), origin.type().clone());
        }

        Map<String, Integer> positions(JoinStatisticsMeta meta) {
            Map<String, Integer> prepared = positions.get(meta);
            if (prepared == null) {
                Map<String, Integer> result = new HashMap<>();
                for (int i = 0; i < meta.getDefinition().getSources().size(); i++) {
                    result.put(meta.getDefinition().getSources().get(i).getUuid(), i);
                }
                prepared = Map.copyOf(result);
                if (positions.size() < 512) {
                    positions.put(meta, prepared);
                }
            }
            return prepared;
        }

        void clear() {
            definitions = null;
            byTable.clear();
            matches.clear();
            positions.clear();
            bytes = 0;
        }
    }

    private static void assign(JoinStatisticsMeta meta, JoinStatisticsScope scope,
                               List<JoinStatisticsScope.Source> relations, int index, List<String> roles,
                               Set<String> used, List<JoinStatisticsMeta> result, SearchBudget budget) {
        if (result.size() >= 64 || budget.exhausted()) {
            return;
        }
        var definition = meta.getDefinition();
        if (index == definition.getSources().size()) {
            if (used.isEmpty()) {
                return;
            }
            List<JoinStatisticsDefinition.Source> sources = new ArrayList<>();
            for (int i = 0; i < roles.size(); i++) {
                sources.add(definition.getSources().get(i).withRole(roles.get(i)));
            }
            var bound = new JoinStatisticsDefinition(definition.getName(), sources, definition.getDomains(),
                    definition.getProperties());
            if (budget.exhausted()) {
                return;
            }
            if (used.size() >= 2) {
                JoinStatisticsScope restricted = scope.restrict(used);
                if (budget.exhausted() || JoinStatisticsKeyLayout.match(bound, restricted) == null) {
                    return;
                }
            }
            result.add(new JoinStatisticsMeta(meta.getId(), bound, meta.getGeneration(), meta.getParts(),
                    meta.getPayloadBytes(), meta.getChecksum(), meta.getCollectedAt()));
            return;
        }
        String physical = definition.getSources().get(index).getTableUuid();
        boolean matched = false;
        for (var relation : relations) {
            if (result.size() >= 64 || budget.exhausted()) {
                return;
            }
            if (!physical.equals(relation.physicalUuid()) || used.contains(relation.tableUuid())) {
                continue;
            }
            matched = true;
            roles.add(relation.tableUuid());
            used.add(relation.tableUuid());
            assign(meta, scope, relations, index + 1, roles, used, result, budget);
            used.remove(relation.tableUuid());
            roles.remove(roles.size() - 1);
        }
        if (result.size() >= 64 || budget.exhausted()) {
            return;
        }
        long remainingRoles = definition.getSources().subList(index, definition.getSources().size()).stream()
                .filter(source -> source.getTableUuid().equals(physical)).count();
        long availableRoles = relations.stream().filter(relation -> relation.physicalUuid().equals(physical)
                && !used.contains(relation.tableUuid())).count();
        if (!matched || remainingRoles > availableRoles) {
            roles.add("unbound:" + meta.getId() + ":" + index);
            assign(meta, scope, relations, index + 1, roles, used, result, budget);
            roles.remove(roles.size() - 1);
        }
    }

    /** Shared by all definitions and recursive branches; failed matches also consume the budget. */
    private static final class SearchBudget {
        private final long started;
        private final long nanos;
        private final LongSupplier clock;
        private boolean stopped;

        private SearchBudget(long nanos, LongSupplier clock) {
            this.nanos = nanos;
            this.clock = clock;
            this.started = clock.getAsLong();
        }

        private boolean exhausted() {
            stopped = stopped || Thread.currentThread().isInterrupted() || nanos <= 0 || clock.getAsLong() - started >= nanos;
            return stopped;
        }
    }
}

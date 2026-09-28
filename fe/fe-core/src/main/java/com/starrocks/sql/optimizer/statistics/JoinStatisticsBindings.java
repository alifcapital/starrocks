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
import java.util.HashSet;
import java.util.List;
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
        SearchBudget searchBudget = new SearchBudget(budget, clock);
        if (searchBudget.exhausted()) {
            return List.of();
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
        return result;
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

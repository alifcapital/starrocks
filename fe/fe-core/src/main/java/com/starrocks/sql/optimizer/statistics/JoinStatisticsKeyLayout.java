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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Query equality coordinates. Tuple order is matched by column provenance, never by a hash alone. */
final class JoinStatisticsKeyLayout {
    record Domain(List<Set<JoinStatisticsScope.ColumnOrigin>> components) { }

    final List<String> sources;
    final List<Domain> domains;

    JoinStatisticsKeyLayout(JoinStatisticsScope scope) {
        sources = scope.getSources().keySet().stream().sorted().toList();
        List<Set<JoinStatisticsScope.ColumnOrigin>> groups = new ArrayList<>();
        for (var edge : scope.getEqualities()) {
            Set<JoinStatisticsScope.ColumnOrigin> group = new HashSet<>(List.of(edge.left(), edge.right()));
            for (int i = groups.size() - 1; i >= 0; i--) {
                if (groups.get(i).stream().anyMatch(group::contains)) {
                    group.addAll(groups.remove(i));
                }
            }
            groups.add(group);
        }
        groups.sort(Comparator.comparing(group -> group.stream()
                .map(c -> c.tableUuid() + ":" + c.name()).sorted().reduce("", (a, b) -> a + "|" + b)));
        Map<Set<String>, List<Set<JoinStatisticsScope.ColumnOrigin>>> tuples = new LinkedHashMap<>();
        for (var group : groups) {
            Set<String> participants = new HashSet<>();
            for (var column : group) {
                if (!participants.add(column.tableUuid())) {
                    throw new IllegalArgumentException("Two columns of one source in an equality domain");
                }
            }
            tuples.computeIfAbsent(participants, ignored -> new ArrayList<>()).add(Set.copyOf(group));
        }
        domains = tuples.values().stream().map(parts -> new Domain(List.copyOf(parts))).toList();
    }

    private static boolean matches(Domain query, JoinStatisticsDefinition.KeyDomain stored,
                                   JoinStatisticsDefinition definition, Set<String> included) {
        if (query.components.size() != stored.getTypes().size()) {
            return false;
        }
        Map<String, Integer> positions = new HashMap<>();
        for (int i = 0; i < definition.getSources().size(); i++) {
            positions.put(definition.getSources().get(i).getUuid(), i);
        }
        Set<Integer> used = new HashSet<>();
        for (var component : query.components) {
            int matching = -1;
            for (int part = 0; part < stored.getTypes().size(); part++) {
                boolean all = true;
                int participants = 0;
                for (var column : component) {
                    if (!included.contains(column.tableUuid())) {
                        continue;
                    }
                    participants++;
                    Integer position = positions.get(column.tableUuid());
                    List<String> columns = position == null ? null : stored.getColumns().get(position);
                    all &= columns != null && columns.get(part).equals(column.name())
                            && JoinStatisticsDefinition.matchesKeyType(column.type(), stored.getTypes().get(part));
                }
                if (all && participants >= 2) {
                    matching = part;
                    break;
                }
            }
            if (matching < 0 || !used.add(matching)) {
                return false;
            }
        }
        return true;
    }

    int domain(JoinStatisticsDefinition definition, int storedDomain, Set<String> included) {
        for (int d = 0; d < domains.size(); d++) {
            if (matches(domains.get(d), definition.getDomains().get(storedDomain), definition, included)) {
                return d;
            }
        }
        return -1;
    }

    static int[] match(JoinStatisticsDefinition definition, JoinStatisticsScope scope) {
        JoinStatisticsKeyLayout layout = new JoinStatisticsKeyLayout(scope);
        int[] masks = new int[definition.getDomains().size()];
        for (Domain domain : layout.domains) {
            int match = find(domain, definition, scope.getSources().keySet());
            if (match >= 0) {
                masks[match] |= sourceMask(domain, definition);
                continue;
            }
            // Previously collected scalar domains remain usable for an existing compound ON.
            // Composition deliberately does not identify these scalar coordinates with a tuple coordinate.
            for (var component : domain.components) {
                Domain scalar = new Domain(List.of(component));
                match = find(scalar, definition, scope.getSources().keySet());
                if (match < 0) {
                    return null;
                }
                masks[match] |= sourceMask(scalar, definition);
            }
        }
        return masks;
    }

    private static int find(Domain domain, JoinStatisticsDefinition definition, Set<String> included) {
        for (int d = 0; d < definition.getDomains().size(); d++) {
            if (matches(domain, definition.getDomains().get(d), definition, included)) {
                return d;
            }
        }
        return -1;
    }

    private static int sourceMask(Domain domain, JoinStatisticsDefinition definition) {
        int result = 0;
        for (var column : domain.components.get(0)) {
            for (int source = 0; source < definition.getSources().size(); source++) {
                if (definition.getSources().get(source).getUuid().equals(column.tableUuid())) {
                    result |= 1 << source;
                }
            }
        }
        return result;
    }
}

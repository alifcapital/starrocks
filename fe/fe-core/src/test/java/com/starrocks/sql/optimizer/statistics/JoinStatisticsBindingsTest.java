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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.statistic.JoinStatisticsDefinition;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class JoinStatisticsBindingsTest {
    private static JoinStatisticsMeta definition(long id, int roles) {
        var sources = new ArrayList<JoinStatisticsDefinition.Source>();
        Map<Integer, List<String>> keys = new HashMap<>();
        for (int i = 0; i < roles; i++) {
            sources.add(new JoinStatisticsDefinition.Source("iceberg", "db", "t", "stored:" + i, "physical",
                    List.of()));
            keys.put(i, List.of("id"));
        }
        return new JoinStatisticsMeta(id, new JoinStatisticsDefinition("self" + id, sources,
                List.of(new JoinStatisticsDefinition.KeyDomain(keys, List.of("INT"))), Map.of()));
    }

    private static JoinStatisticsScope scope(int roles, String key) {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getUUID()).thenReturn("physical");
        var columns = new ArrayList<ColumnRefOperator>();
        JoinStatisticsScope scope = null;
        for (int i = 0; i < roles; i++) {
            var column = new ColumnRefOperator(i + 1, IntegerType.INT, key, false);
            var scan = JoinStatisticsScope.scan(table, Map.of(column, new Column(key, IntegerType.INT)), 100);
            List<ScalarOperator> equalities = new ArrayList<>();
            for (var previous : columns) {
                equalities.add(new BinaryPredicateOperator(BinaryType.EQ, previous, column));
            }
            scope = scope == null ? scan : JoinStatisticsScope.join(scope, scan, JoinOperator.INNER_JOIN,
                    Utils.compoundAnd(equalities));
            columns.add(column);
        }
        return Mockito.spy(scope);
    }

    @Test
    void rejectedMatchesConsumeBudgetWithoutProducingResults() {
        var meta = definition(1, 4);
        var scope = scope(6, "different_key");
        AtomicLong clock = new AtomicLong();
        Mockito.doAnswer(call -> {
            clock.addAndGet(10);
            return call.callRealMethod();
        }).when(scope).restrict(Mockito.anySet());

        // All 360 assignments are incompatible. The successful-result cap cannot stop this search.
        Assertions.assertTrue(JoinStatisticsBindings.bind(List.of(meta), scope, 25, clock::get).isEmpty());
        Mockito.verify(scope, Mockito.times(3)).restrict(Mockito.anySet());
        Mockito.clearInvocations(scope);
        clock.set(0);
        Assertions.assertTrue(JoinStatisticsBindings.bind(List.of(meta), scope, Long.MAX_VALUE, clock::get).isEmpty());
        Mockito.verify(scope, Mockito.times(360)).restrict(Mockito.anySet());
    }

    @Test
    void expiryKeepsOnlyFullyValidatedBindings() {
        var scope = scope(3, "id");
        var meta = definition(1, 2);
        AtomicLong clock = new AtomicLong();
        Mockito.doAnswer(call -> {
            clock.addAndGet(10);
            return call.callRealMethod();
        }).when(scope).restrict(Mockito.anySet());
        var result = JoinStatisticsBindings.bind(List.of(meta), scope, 15, clock::get);
        Assertions.assertEquals(1, result.size());
        Mockito.verify(scope, Mockito.times(2)).restrict(Mockito.anySet());

        var complete = JoinStatisticsBindings.bind(List.of(meta), scope, Long.MAX_VALUE, clock::get);
        Assertions.assertEquals(6, complete.size());
        Set<List<String>> assignments = new HashSet<>();
        for (var bound : complete) {
            List<String> roles = bound.getDefinition().getSources().stream()
                    .map(JoinStatisticsDefinition.Source::getUuid).toList();
            Assertions.assertEquals(2, new HashSet<>(roles).size());
            Assertions.assertTrue(scope.getSources().keySet().containsAll(roles));
            Assertions.assertNotNull(JoinStatisticsKeyLayout.match(bound.getDefinition(), scope.restrict(Set.copyOf(roles))));
            assignments.add(roles);
        }
        Assertions.assertEquals(6, assignments.size());
        Assertions.assertEquals(complete.get(0).getDefinition().getSources().stream()
                        .map(JoinStatisticsDefinition.Source::getUuid).toList(),
                result.get(0).getDefinition().getSources().stream()
                        .map(JoinStatisticsDefinition.Source::getUuid).toList());
    }

    @Test
    void definitionsShareTheRemainingBudget() {
        var scope = scope(2, "id");
        AtomicLong clock = new AtomicLong();
        Mockito.doAnswer(call -> {
            clock.addAndGet(10);
            return call.callRealMethod();
        }).when(scope).restrict(Mockito.anySet());
        var result = JoinStatisticsBindings.bind(List.of(definition(1, 2), definition(2, 2)), scope, 25, clock::get);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.stream().allMatch(meta -> meta.getId() == 1));
        Mockito.verify(scope, Mockito.times(3)).restrict(Mockito.anySet());
    }

    @Test
    void interruptionStopsRejectedSearchAndPreservesInterruptFlag() {
        var scope = scope(6, "different_key");
        Mockito.doAnswer(call -> {
            Thread.currentThread().interrupt();
            return call.callRealMethod();
        }).when(scope).restrict(Mockito.anySet());
        try {
            Assertions.assertTrue(JoinStatisticsBindings.bind(List.of(definition(1, 4)), scope,
                    Long.MAX_VALUE, () -> 0).isEmpty());
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
            Mockito.verify(scope, Mockito.times(1)).restrict(Mockito.anySet());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void exhaustedBudgetAndPreexistingCancellationDoNoSearch() {
        var scope = Mockito.mock(JoinStatisticsScope.class);
        var definitions = List.of(definition(1, 4));
        Assertions.assertTrue(JoinStatisticsBindings.bind(definitions, scope, 0).isEmpty());
        Assertions.assertTrue(JoinStatisticsBindings.bind(definitions, scope, -1).isEmpty());
        Thread.currentThread().interrupt();
        try {
            Assertions.assertTrue(JoinStatisticsBindings.bind(definitions, scope, Long.MAX_VALUE).isEmpty());
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        Mockito.verifyNoInteractions(scope);
    }
    @Test
    void preparedBindingsDoNotCacheIncompleteSearchesAndReuseCompleteAssignments() {
        var scope = scope(3, "id");
        var definitions = List.of(definition(1, 2));
        var prepared = new JoinStatisticsBindings.Prepared();
        AtomicLong clock = new AtomicLong();
        Mockito.doAnswer(call -> {
            clock.addAndGet(10);
            return call.callRealMethod();
        }).when(scope).restrict(Mockito.anySet());
        Assertions.assertEquals(1, prepared.bind(definitions, scope, 15, clock::get).size());
        clock.set(0);
        var full = prepared.bind(definitions, scope, Long.MAX_VALUE, clock::get);
        Assertions.assertEquals(6, full.size());
        Mockito.clearInvocations(scope);
        Assertions.assertSame(full, prepared.bind(definitions, scope, Long.MAX_VALUE, clock::get));
        Mockito.verify(scope, Mockito.never()).restrict(Mockito.anySet());
        Assertions.assertTrue(prepared.bind(definitions, scope, 0, clock::get).isEmpty());
        prepared.clear();
        Assertions.assertEquals(6, prepared.bind(definitions, scope, Long.MAX_VALUE, clock::get).size());
    }

}

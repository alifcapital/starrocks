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

import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Bounded query-local selections. Stored keys own predicates and types, never mutable plan nodes. */
final class JoinStatisticsSelectionCache {
    private static final long MAX_BYTES = 8L * 1024 * 1024;
    private record Binding(int id, JoinStatisticsScope.ColumnOrigin origin) { }
    private record Key(JoinStatisticsData.Source source, double rows, JoinStatisticsTableState state,
                       List<ScalarOperator> predicates, List<Type> types, List<Binding> bindings) { }
    private final Map<Key, Optional<JoinStatisticsEstimate.Selection>> values = new HashMap<>();
    private long bytes;

    JoinStatisticsEstimate.Selection select(JoinStatisticsData.Source source, JoinStatisticsScope.Source scan,
                                             Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns) {
        Key lookup = key(source, scan, columns, false);
        if (lookup != null) {
            Optional<JoinStatisticsEstimate.Selection> cached = values.get(lookup);
            if (cached != null) {
                return cached.orElse(null);
            }
        }
        JoinStatisticsEstimate.Selection selected = JoinStatisticsPlanner.select(source, scan, columns);
        if (lookup != null && values.size() < 1024) {
            long charge = 512L + (selected == null ? 0 : selected.estimatedSize());
            for (ScalarOperator predicate : scan.predicates()) {
                charge += 256L * (1 + predicate.getChildren().size());
                for (ScalarOperator child : predicate.getChildren()) {
                    if (child instanceof ConstantOperator constant && constant.getValue() instanceof String text) {
                        charge += 2L * text.length();
                    }
                }
            }
            if (bytes + charge <= MAX_BYTES) {
                values.put(key(source, scan, columns, true), Optional.ofNullable(selected));
                bytes += charge;
            }
        }
        return selected;
    }

    private static Key key(JoinStatisticsData.Source source, JoinStatisticsScope.Source scan,
                           Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns, boolean own) {
        List<ScalarOperator> predicates = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        List<Binding> bindings = new ArrayList<>();
        for (ScalarOperator predicate : scan.predicates()) {
            // These are exactly the shallow scalar forms understood by select(). Unknown functions
            // retain the original path rather than risk an incomplete cache key for plugin state.
            if (!cacheable(predicate)) {
                return null;
            }
            ScalarOperator stored = own ? predicate.clone() : predicate;
            predicates.add(stored);
            capture(stored, columns, own, types, bindings);
        }
        return new Key(source, scan.estimatedRows(), scan.tableState(), predicates, types, bindings);
    }

    private static boolean cacheable(ScalarOperator predicate) {
        if (predicate instanceof ColumnRefOperator) {
            return true;
        }
        if (!(predicate instanceof BinaryPredicateOperator || predicate instanceof InPredicateOperator
                || predicate instanceof IsNullPredicateOperator || predicate instanceof CompoundPredicateOperator)
                || predicate.getChildren().isEmpty() || !(predicate.getChild(0) instanceof ColumnRefOperator)) {
            return false;
        }
        if (predicate instanceof CompoundPredicateOperator compound && !compound.isNot()) {
            return false;
        }
        for (int i = 1; i < predicate.getChildren().size(); i++) {
            if (!(predicate.getChild(i) instanceof ConstantOperator)) {
                return false;
            }
        }
        return true;
    }

    private static void capture(ScalarOperator value,
                                Map<ColumnRefOperator, JoinStatisticsScope.ColumnOrigin> columns, boolean own,
                                List<Type> types, List<Binding> bindings) {
        Type type = own ? value.getType().clone() : value.getType();
        types.add(type);
        if (own) {
            value.setType(type);
        }
        if (value instanceof ColumnRefOperator column) {
            JoinStatisticsScope.ColumnOrigin origin = columns.get(column);
            if (own && origin != null) {
                origin = new JoinStatisticsScope.ColumnOrigin(origin.tableUuid(), origin.name(), origin.type().clone());
            }
            bindings.add(new Binding(column.getId(), origin));
        }
        for (ScalarOperator child : value.getChildren()) {
            capture(child, columns, own, types, bindings);
        }
    }

    long estimatedSize() {
        return bytes;
    }

    void clear() {
        values.clear();
        bytes = 0;
    }
}

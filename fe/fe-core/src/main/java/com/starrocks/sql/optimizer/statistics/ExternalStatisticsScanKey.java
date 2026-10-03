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
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Query-local base scan identity. Lookup borrows inputs; only stored keys copy mutable optimizer state. */
public final class ExternalStatisticsScanKey {
    private final String catalogName;
    private final Table table;
    private final Map<ColumnRefOperator, Column> columns;
    private final List<PartitionKey> partitionKeys;
    private final ScalarOperator predicate;
    private final long limit;
    private final TvrVersionRange version;
    private final boolean partitionAware;
    private final boolean skipConnector;

    public ExternalStatisticsScanKey(String catalogName, Table table, Map<ColumnRefOperator, Column> columns,
                                    List<PartitionKey> partitionKeys, ScalarOperator predicate, long limit,
                                    TvrVersionRange version, boolean partitionAware, boolean skipConnector) {
        this.catalogName = catalogName;
        this.table = table;
        this.columns = columns;
        this.partitionKeys = partitionKeys;
        this.predicate = predicate;
        this.limit = limit;
        this.version = version;
        this.partitionAware = partitionAware;
        this.skipConnector = skipConnector;
    }

    public ExternalStatisticsScanKey snapshot() {
        List<PartitionKey> savedPartitions = partitionKeys == null ? null : partitionKeys.stream().map(key -> {
            PartitionKey saved = new PartitionKey(key.getKeys().stream()
                    .map(value -> (LiteralExpr) value.clone()).toList(), List.copyOf(key.getTypes()));
            saved.setNullPartitionValue(key.getNullPartitionValue());
            return saved;
        }).toList();
        return new ExternalStatisticsScanKey(catalogName, table, Map.copyOf(columns), savedPartitions,
                predicate == null ? null : predicate.clone(), limit, version, partitionAware, skipConnector);
    }

    private static boolean samePartitions(List<PartitionKey> left, List<PartitionKey> right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            PartitionKey a = left.get(i);
            PartitionKey b = right.get(i);
            if (!a.getTypes().equals(b.getTypes()) || !a.getKeys().equals(b.getKeys())
                    || !Objects.equals(a.getNullPartitionValue(), b.getNullPartitionValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ExternalStatisticsScanKey)) {
            return false;
        }
        ExternalStatisticsScanKey key = (ExternalStatisticsScanKey) other;
        return table == key.table && limit == key.limit && partitionAware == key.partitionAware
                && skipConnector == key.skipConnector && Objects.equals(catalogName, key.catalogName)
                && sameColumns(key.columns) && Objects.equals(predicate, key.predicate)
                && Objects.equals(version, key.version)
                && (version == null || version.getClass() == key.version.getClass())
                && samePartitions(partitionKeys, key.partitionKeys);
    }

    private boolean sameColumns(Map<ColumnRefOperator, Column> other) {
        if (columns.size() != other.size()) {
            return false;
        }
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            // Catalog Column objects belong to the query's schema; a replacement must not reuse its estimates.
            if (entry.getValue() != other.get(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int columnsHash = 0;
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            columnsHash += entry.getKey().getId() ^ System.identityHashCode(entry.getValue());
        }
        int hash = 31 * Objects.hashCode(catalogName) + System.identityHashCode(table);
        hash = 31 * hash + columnsHash;
        hash = 31 * hash + Objects.hashCode(partitionKeys);
        hash = 31 * hash + Objects.hashCode(predicate);
        hash = 31 * hash + Long.hashCode(limit);
        hash = 31 * hash + Objects.hashCode(version);
        hash = 31 * hash + Boolean.hashCode(partitionAware);
        return 31 * hash + Boolean.hashCode(skipConnector);
    }
}

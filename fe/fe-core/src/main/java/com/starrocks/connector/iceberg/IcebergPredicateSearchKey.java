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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import java.util.Objects;

/**
 * Key of the Iceberg planning caches of one query. We want scans of a table with predicates that mean the same to
 * share one planning: PredicateSearchKey.equals compares predicates with ScalarOperator.equivalent, by column name,
 * but its hash code uses the column ref ids, which differ between scans. So equal keys of different scans landed in
 * different hash buckets and never met. This key leaves the predicate out of the hash code.
 *
 * <p>The result of Iceberg planning depends only on what the key holds: the table, the version, the predicate and
 * whether column statistics are loaded. Every Iceberg cache must build its keys with this class, because
 * PredicateSearchKey.equals compares classes.
 *
 * <p>ScalarOperator.equivalent also compares whether each column ref is nullable. A column that Iceberg declares
 * required gets a not-nullable column ref, and the statistics of a scan store their key while it is so.
 * PlanFragmentBuilder.buildIcebergScanNode then sets the column refs of the scan to nullable in place, because the
 * scan returns nullable columns, and the scan node builds its key after that. A predicate that holds one of those
 * refs no longer matched the key of the statistics, and the scan planned the files again. The Iceberg filter
 * converted from a predicate does not read the flag, so the key compares a copy of the predicate in which every
 * column ref is nullable.
 */
public class IcebergPredicateSearchKey extends PredicateSearchKey {
    private static final BaseScalarOperatorShuttle NULLABLE_COLUMN_REFS = new BaseScalarOperatorShuttle() {
        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            return variable.isNullable() ? variable
                    : new ColumnRefOperator(variable.getId(), variable.getType(), variable.getName(), true);
        }
    };

    private final ScalarOperator comparedPredicate;

    protected IcebergPredicateSearchKey(String databaseName, String tableName, GetRemoteFilesParams params) {
        super(databaseName, tableName, params);
        ScalarOperator predicate = params.getPredicate();
        this.comparedPredicate = predicate == null ? null : predicate.accept(NULLABLE_COLUMN_REFS, null);
    }

    public static IcebergPredicateSearchKey of(String databaseName, String tableName, GetRemoteFilesParams params) {
        return new IcebergPredicateSearchKey(databaseName, tableName, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergPredicateSearchKey that = (IcebergPredicateSearchKey) o;
        if (comparedPredicate == null ? that.comparedPredicate != null
                : that.comparedPredicate == null || !comparedPredicate.equivalent(that.comparedPredicate)) {
            return false;
        }
        return getParams().isEnableColumnStats() == that.getParams().isEnableColumnStats()
                && Objects.equals(getVersion(), that.getVersion())
                && Objects.equals(getDatabaseName(), that.getDatabaseName())
                && Objects.equals(getTableName(), that.getTableName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatabaseName(), getTableName(), getVersion(), getParams().isEnableColumnStats());
    }
}

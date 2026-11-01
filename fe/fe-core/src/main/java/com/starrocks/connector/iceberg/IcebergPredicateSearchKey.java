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
 */
public class IcebergPredicateSearchKey extends PredicateSearchKey {
    protected IcebergPredicateSearchKey(String databaseName, String tableName, GetRemoteFilesParams params) {
        super(databaseName, tableName, params);
    }

    public static IcebergPredicateSearchKey of(String databaseName, String tableName, GetRemoteFilesParams params) {
        return new IcebergPredicateSearchKey(databaseName, tableName, params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDatabaseName(), getTableName(), getVersion(), getParams().isEnableColumnStats());
    }
}

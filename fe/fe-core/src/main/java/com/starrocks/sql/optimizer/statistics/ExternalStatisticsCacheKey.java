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

import java.util.List;
import java.util.Objects;

/** Table summaries, partition cells and reusable block directories share the external statistics byte budget. */
public final class ExternalStatisticsCacheKey {
    public enum Scope {
        PARTITION, TABLE, BLOCK, BLOCK_DIRECTORY
    }

    public final Scope scope;
    public final String tableUUID;
    public final String partitionName;

    public final String columnName;
    public final List<String> partitions;

    public ExternalStatisticsCacheKey(String tableUUID, String partitionName, String columnName) {
        this(tableUUID, partitionName, columnName, Scope.PARTITION);
    }

    private ExternalStatisticsCacheKey(String tableUUID, String partitionName, String columnName, Scope scope) {
        this(tableUUID, partitionName, columnName, scope, List.of());
    }

    private ExternalStatisticsCacheKey(String tableUUID, String partitionName, String columnName,
                                       Scope scope, List<String> partitions) {
        this.partitions = List.copyOf(partitions);
        this.scope = scope;
        this.tableUUID = tableUUID;
        this.partitionName = partitionName;
        this.columnName = columnName;
    }

    public static ExternalStatisticsCacheKey table(String tableUUID, String columnName) {
        return new ExternalStatisticsCacheKey(tableUUID, "", columnName, Scope.TABLE);
    }

    public static ExternalStatisticsCacheKey block(String tableUUID, String columnName, List<String> partitions) {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("Empty statistics block");
        }
        return new ExternalStatisticsCacheKey(tableUUID, partitions.get(0), columnName, Scope.BLOCK, partitions);
    }

    public static ExternalStatisticsCacheKey directory(String tableUUID, String columnName) {
        return new ExternalStatisticsCacheKey(tableUUID, "", columnName, Scope.BLOCK_DIRECTORY);
    }

    public boolean isTable() {
        return scope == Scope.TABLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalStatisticsCacheKey)) {
            return false;
        }
        ExternalStatisticsCacheKey other = (ExternalStatisticsCacheKey) o;
        return scope == other.scope && tableUUID.equals(other.tableUUID) && partitionName.equals(other.partitionName)
                && columnName.equals(other.columnName) && partitions.equals(other.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, tableUUID, partitionName, columnName, partitions);
    }

    @Override
    public String toString() {
        return scope + "/" + tableUUID + "/" + partitionName + "/" + columnName;
    }
}

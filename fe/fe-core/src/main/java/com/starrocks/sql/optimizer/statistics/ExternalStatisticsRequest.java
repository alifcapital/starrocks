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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Immutable scan domain. Used only to share an in-flight/finished result within one optimization. */
public final class ExternalStatisticsRequest {
    public final String tableUUID;
    public final List<String> partitions;
    public final List<String> columns;
    public final boolean wholeTable;
    public final boolean unpartitioned;

    public ExternalStatisticsRequest(String tableUUID, Collection<String> partitions, Collection<String> columns) {
        this(tableUUID, partitions, columns, false);
    }

    public ExternalStatisticsRequest(String tableUUID, Collection<String> partitions, Collection<String> columns,
                                     boolean wholeTable) {
        this(tableUUID, partitions, columns, wholeTable, false);
    }

    public ExternalStatisticsRequest(String tableUUID, Collection<String> partitions, Collection<String> columns,
                                     boolean wholeTable, boolean unpartitioned) {
        this.tableUUID = tableUUID;
        this.unpartitioned = unpartitioned;
        this.wholeTable = wholeTable;
        this.partitions = partitions.stream().distinct().sorted().collect(Collectors.toUnmodifiableList());
        this.columns = columns.stream().distinct().sorted().collect(Collectors.toUnmodifiableList());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ExternalStatisticsRequest)) {
            return false;
        }
        ExternalStatisticsRequest request = (ExternalStatisticsRequest) other;
        return wholeTable == request.wholeTable && unpartitioned == request.unpartitioned
                && tableUUID.equals(request.tableUUID)
                && partitions.equals(request.partitions) && columns.equals(request.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUUID, partitions, columns, wholeTable, unpartitioned);
    }
}

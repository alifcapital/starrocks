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

import java.util.Objects;

/** One partition of an external table in the per-partition statistics cache. */
public class ExternalPartitionStatsKey {
    public final String tableUUID;
    public final String partitionName;

    public ExternalPartitionStatsKey(String tableUUID, String partitionName) {
        this.tableUUID = tableUUID;
        this.partitionName = partitionName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ExternalPartitionStatsKey)) {
            return false;
        }
        ExternalPartitionStatsKey other = (ExternalPartitionStatsKey) o;
        return tableUUID.equals(other.tableUUID) && partitionName.equals(other.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableUUID, partitionName);
    }

    @Override
    public String toString() {
        return tableUUID + "/" + partitionName;
    }
}

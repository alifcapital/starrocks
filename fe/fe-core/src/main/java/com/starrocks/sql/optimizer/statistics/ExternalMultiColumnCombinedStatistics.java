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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Multi-column statistics of one external table as kept by the statistics cache: one entry per
 * collected column group. Column groups are identified by column names because external columns
 * have no stable numeric id.
 */
public class ExternalMultiColumnCombinedStatistics {
    public static final ExternalMultiColumnCombinedStatistics EMPTY = new ExternalMultiColumnCombinedStatistics();

    public static class Group {
        // Order of the tuple components in the most common values.
        private final List<String> columnNames;
        // Rows of the table when the statistics were collected; the MCV counts are shares of it.
        private final long rowCount;
        private final long ndv;
        private final List<MultiColumnCombinedStats.McvEntry> mcv;

        public Group(List<String> columnNames, long rowCount, long ndv, List<MultiColumnCombinedStats.McvEntry> mcv) {
            this.columnNames = columnNames;
            this.rowCount = rowCount;
            this.ndv = ndv;
            this.mcv = mcv;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public long getRowCount() {
            return rowCount;
        }

        public long getNdv() {
            return ndv;
        }

        public List<MultiColumnCombinedStats.McvEntry> getMcv() {
            return mcv;
        }
    }

    private final List<Group> groups;

    private ExternalMultiColumnCombinedStatistics() {
        this.groups = Collections.emptyList();
    }

    public ExternalMultiColumnCombinedStatistics(List<Group> groups) {
        this.groups = new ArrayList<>(groups);
    }

    public List<Group> getGroups() {
        return groups;
    }

    public boolean isEmpty() {
        return groups.isEmpty();
    }
}

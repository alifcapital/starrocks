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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;

/** Full-table metadata only: never enumerate files or issue a statistics query to measure growth. */
public record JoinStatisticsTableState(double rows, long version) {
    public static final JoinStatisticsTableState UNKNOWN = new JoinStatisticsTableState(-1, Long.MIN_VALUE);

    static JoinStatisticsTableState read(Table table, TvrVersionRange range) {
        try {
            if (table instanceof OlapTable nativeTable) {
                // Tablet row counters can lag behind visible versions; this is a best-effort estimate.
                double rows = nativeTable.getRowCount();
                return new JoinStatisticsTableState(rows > 0 ? rows : -1, nativeVersion(nativeTable));
            }
            if (table instanceof IcebergTable iceberg) {
                var nativeTable = iceberg.getNativeTable();
                var snapshot = range == null ? nativeTable.currentSnapshot()
                        : range.end().map(nativeTable::snapshot).orElse(null);
                if (snapshot == null) {
                    return UNKNOWN;
                }
                var summary = snapshot.summary();
                double rows = -1;
                if (summary != null && !hasDeletes(summary, "total-position-deletes")
                        && !hasDeletes(summary, "total-equality-deletes")) {
                    String count = summary.get("total-records");
                    rows = count == null ? -1 : Long.parseLong(count);
                }
                return new JoinStatisticsTableState(rows, snapshot.snapshotId());
            }
        } catch (RuntimeException ignored) {
            // Missing/expired metadata must not break planning or trigger a metadata refresh here.
        }
        return UNKNOWN;
    }

    private static boolean hasDeletes(java.util.Map<String, String> summary, String key) {
        return Long.parseLong(summary.getOrDefault(key, "0")) != 0;
    }

    public static long nativeVersion(OlapTable table) {
        var digest = com.google.common.hash.Hashing.sha256().newHasher();
        var schema = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        digest.putLong(table.getId()).putLong(table.getBaseIndexMetaId()).putLong(schema.getSchemaId())
                .putInt(schema.getSchemaVersion()).putInt(schema.getSchemaHash());
        table.getPhysicalPartitions().stream().sorted(java.util.Comparator.comparingLong(p -> p.getId()))
                .forEach(partition -> digest.putLong(partition.getId()).putLong(partition.getVisibleVersion()));
        return digest.hash().asLong() & Long.MAX_VALUE;
    }

    boolean matches(JoinStatisticsData.Source source) {
        return version != Long.MIN_VALUE && version == source.getSnapshot();
    }

    double scale(JoinStatisticsData.Source source) {
        if (matches(source) || !Double.isFinite(rows) || rows < 0) {
            return 1;
        }
        if (source.getRows() == 0) {
            return rows == 0 ? 1 : Double.NaN; // An empty sample says nothing about newly inserted keys.
        }
        return rows / source.getRows();
    }
}

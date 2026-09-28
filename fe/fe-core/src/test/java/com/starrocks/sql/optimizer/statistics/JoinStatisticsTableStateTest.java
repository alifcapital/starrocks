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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.tvr.TvrTableSnapshot;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

class JoinStatisticsTableStateTest {
    @Test
    void icebergUsesRequestedSnapshotMetadataWithoutPlanningFilesOrRefreshing() {
        var table = Mockito.mock(IcebergTable.class);
        var nativeTable = Mockito.mock(org.apache.iceberg.Table.class);
        var snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(table.getNativeTable()).thenReturn(nativeTable);
        Mockito.when(nativeTable.snapshot(42L)).thenReturn(snapshot);
        Mockito.when(snapshot.snapshotId()).thenReturn(42L);
        Mockito.when(snapshot.summary()).thenReturn(Map.of("total-records", "1000000"));
        Assertions.assertEquals(new JoinStatisticsTableState(1_000_000, 42),
                JoinStatisticsTableState.read(table, TvrTableSnapshot.of(42L)));
        Mockito.verify(nativeTable).snapshot(42L);
        Mockito.verifyNoMoreInteractions(nativeTable);
        Mockito.when(snapshot.summary()).thenReturn(Map.of("total-records", "1000000", "total-equality-deletes", "10"));
        Assertions.assertEquals(-1, JoinStatisticsTableState.read(table, TvrTableSnapshot.of(42L)).rows());
        Mockito.when(snapshot.summary()).thenReturn(Map.of("total-records", "1000000", "total-position-deletes", "10"));
        Assertions.assertEquals(-1, JoinStatisticsTableState.read(table, TvrTableSnapshot.of(42L)).rows());
        Mockito.when(snapshot.summary()).thenReturn(Map.of());
        Assertions.assertEquals(-1, JoinStatisticsTableState.read(table, TvrTableSnapshot.of(42L)).rows());
        Mockito.when(snapshot.summary()).thenReturn(Map.of("total-records", "broken"));
        Assertions.assertEquals(JoinStatisticsTableState.UNKNOWN,
                JoinStatisticsTableState.read(table, TvrTableSnapshot.of(42L)));
    }

    @Test
    void nativeUsesWholeTableCounterAndVersionNotFilteredScanEstimate() {
        var table = Mockito.mock(OlapTable.class, Mockito.RETURNS_DEEP_STUBS);
        var partition = Mockito.mock(PhysicalPartition.class);
        Mockito.when(table.getRowCount()).thenReturn(1_000_000L);
        Mockito.when(table.getPhysicalPartitions()).thenReturn(List.of(partition));
        Mockito.when(partition.getVisibleVersion()).thenReturn(7L);
        var first = JoinStatisticsTableState.read(table, null);
        Assertions.assertEquals(1_000_000, first.rows());
        Mockito.when(partition.getVisibleVersion()).thenReturn(8L);
        Assertions.assertNotEquals(first.version(), JoinStatisticsTableState.read(table, null).version());
        Mockito.when(table.getRowCount()).thenReturn(0L);
        Assertions.assertEquals(-1, JoinStatisticsTableState.read(table, null).rows(),
                "Unreported native tablet counters must not claim that the table is empty");
    }
}

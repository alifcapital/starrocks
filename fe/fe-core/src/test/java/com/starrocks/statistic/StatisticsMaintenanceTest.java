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


package com.starrocks.statistic;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class StatisticsMaintenanceTest {
    private AnalyzeMgr analyze;
    private MetadataMgr metadata;
    private ConnectorMetadata connector;

    @BeforeEach
    void setup() {
        analyze = spy(new AnalyzeMgr());
        metadata = mock(MetadataMgr.class);
        connector = mock(ConnectorMetadata.class);
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        when(state.getVariableMgr()).thenReturn(new VariableMgr());
        when(state.getAnalyzeMgr()).thenReturn(analyze);
        when(state.getMetadataMgr()).thenReturn(metadata);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return state;
            }
        };
        when(metadata.getOptionalMetadata(anyString())).thenReturn(Optional.of(connector));
        doNothing().when(analyze).dropExternalBasicStatsMetaAndData(anyString(), anyString(), anyString());
        doNothing().when(analyze).dropExternalHistogramStatsMetaAndData(anyString(), anyString(), anyString());
    }

    private void tracked(String catalog, String db, String table) {
        analyze.getExternalBasicStatsMetaMap().put(new AnalyzeMgr.StatsMetaKey(catalog, db, table),
                mock(ExternalBasicStatsMeta.class));
    }

    @Test
    void listsEachDatabaseOnceAndRemovesOnlyAbsentTables() {
        tracked("iceberg", "db", "keep");
        tracked("iceberg", "db", "gone");
        tracked("iceberg", "other", "keep");
        when(connector.listTableNames(any(), anyString())).thenReturn(List.of("keep"));
        analyze.clearStatisticFromExternalDroppedTable();
        verify(connector, times(1)).listTableNames(any(), eq("db"));
        verify(connector, times(1)).listTableNames(any(), eq("other"));
        verify(connector, never()).tableExists(any(), anyString(), anyString());
        verify(analyze).dropExternalBasicStatsMetaAndData("iceberg", "db", "gone");
        verify(analyze).dropExternalHistogramStatsMetaAndData("iceberg", "db", "gone");
        verify(analyze, never()).dropExternalBasicStatsMetaAndData(anyString(), anyString(), eq("keep"));
    }

    @Test
    void failedListingPreservesDatabaseAndContinuesOthers() {
        tracked("iceberg", "failed", "a");
        tracked("iceberg", "failed", "b");
        tracked("iceberg", "healthy", "gone");
        when(connector.listTableNames(any(), eq("failed"))).thenThrow(new RuntimeException("Glue throttled"));
        when(connector.listTableNames(any(), eq("healthy"))).thenReturn(List.of());
        analyze.clearStatisticFromExternalDroppedTable();
        verify(connector, times(1)).listTableNames(any(), eq("failed"));
        verify(analyze, never()).dropExternalBasicStatsMetaAndData(anyString(), eq("failed"), anyString());
        verify(analyze, never()).dropExternalHistogramStatsMetaAndData(anyString(), eq("failed"), anyString());
        verify(analyze).dropExternalBasicStatsMetaAndData("iceberg", "healthy", "gone");
    }

    @Test
    void partialListingCannotDeleteStatistics() {
        tracked("iceberg", "db", "gone");
        when(connector.listTableNames(any(), eq("db"))).thenReturn(new AbstractList<String>() {
            @Override
            public String get(int index) {
                if (index == 0) {
                    return "another-table";
                }
                throw new IllegalStateException("Next catalog page failed");
            }

            @Override
            public int size() {
                return 2;
            }
        });
        analyze.clearStatisticFromExternalDroppedTable();
        verify(analyze, never()).dropExternalBasicStatsMetaAndData(anyString(), anyString(), anyString());
        verify(analyze, never()).dropExternalHistogramStatsMetaAndData(anyString(), anyString(), anyString());
    }

    @Test
    void unresolvedCatalogIsNotAnEmptyDatabase() {
        tracked("missing", "db", "keep");
        when(metadata.getOptionalMetadata("missing")).thenReturn(Optional.empty());
        analyze.clearStatisticFromExternalDroppedTable();
        verifyNoInteractions(connector);
        verify(analyze, never()).dropExternalBasicStatsMetaAndData(anyString(), anyString(), anyString());
    }

    @Test
    void cleanupWaitsForCollectionFlagAndWindowIncludingFirstPass() {
        boolean oldEnabled = Config.enable_statistic_collect;
        boolean oldUnitTest = FeConstants.runningUnitTest;
        boolean[] inWindow = {false};
        new MockUp<StatisticAutoCollector>() {
            @Mock
            public boolean checkoutAnalyzeTime() {
                return inWindow[0];
            }
        };
        new MockUp<StatisticUtils>() {
            @Mock
            public boolean checkStatisticTableStateNormal() {
                return true;
            }
        };
        doNothing().when(analyze).clearStatisticFromDroppedPartition();
        doNothing().when(analyze).clearStatisticFromDroppedTable();
        StatisticAutoCollector collector = spy(new StatisticAutoCollector());
        doNothing().when(collector).prepareDefaultJob();
        doReturn(List.of()).when(collector).runJobs();
        try {
            FeConstants.runningUnitTest = false;
            Config.enable_statistic_collect = true;
            collector.runAfterCatalogReady();
            verify(analyze, never()).clearStatisticFromDroppedPartition();
            verify(analyze, never()).clearStatisticFromDroppedTable();
            inWindow[0] = true;
            Config.enable_statistic_collect = false;
            collector.runAfterCatalogReady();
            verify(analyze, never()).clearStatisticFromDroppedPartition();
            verify(analyze, never()).clearStatisticFromDroppedTable();
            Config.enable_statistic_collect = true;
            collector.runAfterCatalogReady();
            verify(analyze).clearStatisticFromDroppedPartition();
            verify(analyze).clearStatisticFromDroppedTable();
            verify(analyze, never()).clearExpiredAnalyzeStatus();
        } finally {
            Config.enable_statistic_collect = oldEnabled;
            FeConstants.runningUnitTest = oldUnitTest;
        }
    }
}

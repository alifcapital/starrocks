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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ScopedExternalStatisticsTest {
    private boolean oldSync;
    private ConnectContext context;
    private StatisticStorage storage;
    private Table table;
    private final ColumnRefOperator ref = new ColumnRefOperator(1, IntegerType.BIGINT, "c", true);
    private final ExternalStatisticsRequest request = new ExternalStatisticsRequest("iceberg.db.t.uuid", List.of("p=1"),
            List.of("c"));

    @BeforeAll
    static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    void beforeEach() {
        oldSync = Config.enable_sync_statistics_load;
        context = UtFrameUtils.createDefaultCtx();
        context.setThreadLocalInfo();
        storage = mock(StatisticStorage.class);
        table = mock(Table.class);
        when(table.getName()).thenReturn("t");
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StatisticStorage getStatisticStorage() {
                return storage;
            }
        };
    }

    @AfterEach
    void afterEach() {
        Config.enable_sync_statistics_load = oldSync;
        ConnectContext.remove();
    }

    private ExternalStatisticsAggregate aggregate() {
        ExternalStatisticsAggregate.Builder builder = new ExternalStatisticsAggregate.Builder(request);
        builder.add(Map.of(new ExternalStatisticsCacheKey(request.tableUUID, "p=1", "c"), Optional.of(
                new ExternalColumnStatistics.Partition(
                        ExternalPartitionStatisticsTest.row("p=1", "c", 100, 10, 5, "1", "10"), IntegerType.BIGINT))));
        return builder.build();
    }

    private Statistics estimate(OptimizerContext optimizer) {
        MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
        return Deencapsulation.invoke(metadata, "computeScopedExternalStatistics", optimizer, "missing_catalog", table,
                Map.of(ref, new Column("c", IntegerType.BIGINT)), List.of(),
                new BinaryPredicateOperator(BinaryType.GE, ref, ConstantOperator.createBigint(1)),
                -1L, TvrTableSnapshot.empty(), request);
    }

    @Test
    void completePartitionStatsDoNotLoadGlobalBasicOrMcv() {
        Config.enable_sync_statistics_load = true;
        when(storage.loadExternalStatistics(any())).thenReturn(CompletableFuture.completedFuture(aggregate()));
        OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
        Statistics first = estimate(optimizer);
        Assertions.assertEquals(100, first.getOutputRowCount());
        Assertions.assertEquals(10, first.getColumnStatistic(ref).getDistinctValuesCount());
        Assertions.assertTrue(first.isPartitionRestricted());
        Assertions.assertTrue(Statistics.buildFrom(first).build().isPartitionRestricted());
        Assertions.assertTrue(first.withOutputRowCount(50).isPartitionRestricted());
        Assertions.assertSame(first, StatisticsCalcUtils.withExternalMcvStats(table, first,
                Map.of(ref, new Column("c", IntegerType.BIGINT))));
        estimate(optimizer);
        verify(storage, times(1)).loadExternalStatistics(request);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void wholeTableAndLimitSkipPartitionEnumerationWhilePartitionFiltersUseCache() {
        context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(true);
        IcebergTable iceberg = mock(IcebergTable.class);
        when(iceberg.getId()).thenReturn(123456L);
        when(iceberg.getUUID()).thenReturn(request.tableUUID);
        when(iceberg.isIcebergTable()).thenReturn(true);
        when(iceberg.getCatalogDBName()).thenReturn("db");
        when(iceberg.getCatalogTableName()).thenReturn("t");
        when(iceberg.getPartitionColumnNames()).thenReturn(List.of("p"));
        MetadataMgr metadata = mock(MetadataMgr.class, CALLS_REAL_METHODS);
        doReturn(List.of("p=1", "p=2")).when(metadata).listPartitionNames(anyString(), anyString(), anyString(), any());
        ConnectorMetadata connector = mock(ConnectorMetadata.class);
        doReturn(Optional.of(connector)).when(metadata).getOptionalMetadata("iceberg");
        when(connector.getScannedPartitionNames(any(), any(), anyLong(), any())).thenReturn(List.of("p=1"));
        OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
        ColumnRefOperator p = new ColumnRefOperator(2, IntegerType.BIGINT, "p", true);
        Map<ColumnRefOperator, Column> columns = Map.of(ref, new Column("c", IntegerType.BIGINT),
                p, new Column("p", IntegerType.BIGINT));
        BinaryPredicateOperator ordinary = new BinaryPredicateOperator(BinaryType.GE, ref, ConstantOperator.createBigint(1));
        ExternalStatisticsRequest whole = metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, ordinary, -1, TvrTableSnapshot.of(123L));
        Assertions.assertTrue(whole.wholeTable);
        Assertions.assertTrue(whole.partitions.isEmpty());
        ExternalStatisticsRequest historical = metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, ordinary, -1, TvrTableDelta.of(Optional.empty(), Optional.of(122L)));
        Assertions.assertTrue(historical.wholeTable, "AS OF reads a whole snapshot, not an incremental delta");
        Assertions.assertEquals(whole.partitions, historical.partitions);
        verify(metadata, org.mockito.Mockito.never()).listPartitionNames(anyString(), anyString(), anyString(), any());
        Assertions.assertTrue(metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, ordinary, 100, TvrTableSnapshot.of(123L)).wholeTable);
        BinaryPredicateOperator partition = new BinaryPredicateOperator(BinaryType.EQ, p, ConstantOperator.createBigint(1));
        ExternalStatisticsRequest selected = metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, partition, -1, TvrTableSnapshot.of(123L));
        Assertions.assertFalse(selected.wholeTable);
        Assertions.assertEquals(List.of("p=1"), selected.partitions);
        Assertions.assertNull(metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, ordinary, -1, TvrTableDelta.of(122, 123)));
        context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(false);
        ExternalStatisticsRequest disabled = metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, partition, -1, TvrTableSnapshot.of(123L));
        Assertions.assertTrue(disabled.wholeTable, "The switch changes scope, not the backing cache");
        Assertions.assertTrue(disabled.partitions.isEmpty());
        context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(true);
        when(iceberg.hasPartitionTransformedEvolution()).thenReturn(true);
        Assertions.assertTrue(metadata.prepareExternalStatisticsRequest(optimizer, "iceberg", iceberg,
                columns, null, ordinary, -1, TvrTableSnapshot.of(123L)).wholeTable);
        verify(connector, times(1)).getScannedPartitionNames(any(), any(), anyLong(), any());
    }

    @Test
    void unknownPartitionDomainUsesConnectorWithoutLoadingWholeTableAnalyze() {
        IcebergTable iceberg = mock(IcebergTable.class);
        when(iceberg.getId()).thenReturn(123456L);
        when(iceberg.isIcebergTable()).thenReturn(true);
        when(iceberg.getPartitionColumnNames()).thenReturn(List.of("c"));
        MetadataMgr metadata = mock(MetadataMgr.class, CALLS_REAL_METHODS);
        ConnectorMetadata connector = mock(ConnectorMetadata.class);
        doReturn(Optional.of(connector)).when(metadata).getOptionalMetadata("iceberg");
        when(connector.getScannedPartitionNames(any(), any(), anyLong(), any())).thenReturn(null);
        Statistics manifest = Statistics.builder().setOutputRowCount(123)
                .addColumnStatistic(ref, ColumnStatistic.unknown())
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        when(connector.getTableStatistics(any(), any(), any(), any(), any(), anyLong(), any())).thenReturn(manifest);
        boolean oldUnit = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
            Statistics actual = Deencapsulation.invoke(metadata, "computeTableStatistics", optimizer, "iceberg", iceberg,
                    Map.of(ref, new Column("c", IntegerType.BIGINT)), List.of(),
                    new BinaryPredicateOperator(BinaryType.EQ, ref, ConstantOperator.createBigint(1)),
                    -1L, TvrTableSnapshot.of(123L));
            Assertions.assertEquals(123, actual.getOutputRowCount());
            Assertions.assertTrue(actual.isPartitionRestricted());
            Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, actual.getStatsSource());
            Assertions.assertSame(actual, StatisticsCalcUtils.withExternalMcvStats(iceberg, actual,
                    Map.of(ref, new Column("c", IntegerType.BIGINT))));
            verifyNoMoreInteractions(storage);
        } finally {
            FeConstants.runningUnitTest = oldUnit;
        }
    }

    @Test
    void incrementalReadsUseDeltaFileStatisticsWithoutLoadingAnalyzeOrAttachingMcv() {
        when(table.isIcebergTable()).thenReturn(true);
        when(table.getId()).thenReturn(123456L);
        MetadataMgr metadata = mock(MetadataMgr.class, CALLS_REAL_METHODS);
        ConnectorMetadata connector = mock(ConnectorMetadata.class);
        doReturn(Optional.of(connector)).when(metadata).getOptionalMetadata("iceberg");
        Statistics delta = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(ref, ColumnStatistic.unknown()).setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        when(connector.getTableStatistics(any(), any(), any(), any(), any(), anyLong(), any())).thenReturn(delta);
        boolean oldUnit = FeConstants.runningUnitTest;
        try {
            FeConstants.runningUnitTest = false;
            for (boolean scoped : List.of(false, true)) {
                context.getSessionVariable().setCboEnablePartitionAwareExternalStatistics(scoped);
                for (boolean unpartitioned : List.of(false, true)) {
                    when(table.isUnPartitioned()).thenReturn(unpartitioned);
                    OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
                    Statistics actual = Deencapsulation.invoke(metadata, "computeTableStatistics", optimizer, "iceberg",
                            table, Map.of(ref, new Column("c", IntegerType.BIGINT)), List.of(), ConstantOperator.TRUE,
                            -1L, TvrTableDelta.of(122, 123));
                    Assertions.assertEquals(1000, actual.getOutputRowCount());
                    Assertions.assertTrue(actual.getColumnStatistic(ref).isUnknown());
                    Assertions.assertTrue(actual.isPartitionRestricted());
                    Assertions.assertSame(actual, StatisticsCalcUtils.withExternalMcvStats(table, actual,
                            Map.of(ref, new Column("c", IntegerType.BIGINT))));
                }
            }
        } finally {
            FeConstants.runningUnitTest = oldUnit;
        }
        verify(connector, times(4)).getTableStatistics(any(), any(), any(), any(), any(), anyLong(), any());
        verifyNoMoreInteractions(storage);
    }

    @Test
    void limitedPartitionScanDoesNotClaimWholeTableDistribution() {
        Config.enable_sync_statistics_load = true;
        when(storage.loadExternalStatistics(any())).thenReturn(CompletableFuture.completedFuture(aggregate()));
        OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
        Statistics result = Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getMetadataMgr(),
                "computeScopedExternalStatistics", optimizer, "missing_catalog", table,
                Map.of(ref, new Column("c", IntegerType.BIGINT)), List.of(), ConstantOperator.TRUE,
                100L, TvrTableSnapshot.empty(), request);
        Assertions.assertTrue(result.isPartitionRestricted());
        Assertions.assertSame(result, StatisticsCalcUtils.withExternalMcvStats(table, result,
                Map.of(ref, new Column("c", IntegerType.BIGINT))));
        verify(storage).loadExternalStatistics(request);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void changedSourceTypeInvalidatesPreparedBoundsInsteadOfReinterpretingThem() {
        Config.enable_sync_statistics_load = true;
        when(table.getUUID()).thenReturn(request.tableUUID);
        when(storage.loadExternalStatistics(any())).thenReturn(CompletableFuture.completedFuture(aggregate()));
        OptimizerContext optimizer = OptimizerFactory.initContext(context, new ColumnRefFactory());
        Statistics result = Deencapsulation.invoke(GlobalStateMgr.getCurrentState().getMetadataMgr(),
                "computeScopedExternalStatistics", optimizer, "missing_catalog", table,
                Map.of(ref, new Column("c", DateType.DATE)), List.of(),
                new BinaryPredicateOperator(BinaryType.GE, ref, ConstantOperator.createBigint(1)),
                -1L, TvrTableSnapshot.empty(), request);
        Assertions.assertTrue(result.getColumnStatistic(ref).isUnknown());
        verify(storage).invalidateConnectorTableColumnStatistics(request.tableUUID, List.of("c"));
        verify(storage).loadExternalStatistics(request);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void asynchronousPlanningKeepsItsFirstViewAndNextQueryCanUseCompletedLoad() {
        Config.enable_sync_statistics_load = false;
        CompletableFuture<ExternalStatisticsAggregate> pending = new CompletableFuture<>();
        when(storage.loadExternalStatistics(any())).thenReturn(pending);
        OptimizerContext firstQuery = OptimizerFactory.initContext(context, new ColumnRefFactory());
        Statistics first = estimate(firstQuery);
        Assertions.assertTrue(first.getColumnStatistic(ref).isUnknown());
        pending.complete(aggregate());
        Assertions.assertTrue(estimate(firstQuery).getColumnStatistic(ref).isUnknown());
        OptimizerContext nextQuery = OptimizerFactory.initContext(context, new ColumnRefFactory());
        Assertions.assertEquals(100, estimate(nextQuery).getOutputRowCount());
        verify(storage, times(2)).loadExternalStatistics(request);
        verifyNoMoreInteractions(storage);
    }
}

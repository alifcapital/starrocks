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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.CacheRelaxDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ConnectorDictionaryEligibilityTest {
    private final SessionVariable session = new SessionVariable();
    private final StatisticStorage storage = mock(StatisticStorage.class);
    private final Table table = mock(Table.class);
    private final ColumnRefOperator ref = new ColumnRefOperator(1, VarcharType.VARCHAR, "region", true);
    private Optional<ColumnDict> cached = Optional.empty();
    private boolean allowed = true;
    private int loads;
    private int peeks;
    private boolean oldMockDict;

    @BeforeEach
    void setUp() {
        oldMockDict = FeConstants.USE_MOCK_DICT_MANAGER;
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        session.setCboEnablePartitionAwareExternalStatistics(true);
        assertFalse(session.isAlwaysCollectDictOnLake());
        when(table.getUUID()).thenReturn("ice.db.sales");
        when(storage.getCachedConnectorTableColumnStatistic(any(), anyString())).thenReturn(ColumnStatistic.unknown());
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StatisticStorage getStatisticStorage() {
                return storage;
            }
        };
        new MockUp<CacheRelaxDictManager>() {
            @Mock
            public Optional<ColumnDict> getCachedGlobalDict(String uuid, String column) {
                return cached;
            }

            @Mock
            public boolean hasGlobalDict(String uuid, String column) {
                peeks++;
                return allowed;
            }

            @Mock
            public Optional<ColumnDict> getGlobalDict(String uuid, String column) {
                loads++;
                return Optional.empty();
            }
        };
    }

    @AfterEach
    void tearDown() {
        FeConstants.USE_MOCK_DICT_MANAGER = oldMockDict;
    }

    private Pair<Boolean, Optional<ColumnDict>> check(ColumnStatistic statistic, boolean restricted, boolean where) {
        PhysicalScanOperator scan = mock(PhysicalScanOperator.class);
        if (where) {
            when(scan.getPredicate()).thenReturn(BinaryPredicateOperator.eq(ref, ConstantOperator.createVarchar("TJ")));
        }
        OptExpression expression = OptExpression.create(scan);
        if (statistic != null) {
            expression.setStatistics(Statistics.builder().setOutputRowCount(100).setPartitionRestricted(restricted)
                    .addColumnStatistic(ref, statistic).build());
        }
        return Deencapsulation.invoke(new DecodeCollector(session, true), "checkConnectorGlobalDict",
                scan, table, ref, expression);
    }

    private static ColumnStatistic ndv(double ndv) {
        return ColumnStatistic.builder().setDistinctValuesCount(ndv).build();
    }

    @Test
    void firstFilteredQueryStartsCollectionForBothWholeAndSelectedPartitions() {
        for (boolean restricted : new boolean[] {false, true}) {
            for (boolean where : new boolean[] {false, true}) {
                assertFalse(check(ndv(2), restricted, where).first);
            }
        }
        assertEquals(4, loads);
        verify(storage, times(4)).getCachedConnectorTableColumnStatistic(table, "region");
        verifyNoMoreInteractions(storage);
    }

    @Test
    void readyWholeTableNdvWinsOverScanEstimate() {
        when(storage.getCachedConnectorTableColumnStatistic(table, "region")).thenReturn(ndv(1000));
        check(ndv(2), true, true);
        assertEquals(0, loads);
        when(storage.getCachedConnectorTableColumnStatistic(table, "region")).thenReturn(ndv(2));
        check(ndv(1000), true, true);
        check(ColumnStatistic.unknown(), true, true);
        check(null, false, true);
        assertEquals(3, loads);
        verify(storage, times(4)).getCachedConnectorTableColumnStatistic(table, "region");
        verifyNoMoreInteractions(storage);
    }

    @Test
    void missingStatisticsKeepExistingAlwaysCollectPolicy() {
        check(ColumnStatistic.unknown(), true, true);
        check(null, false, true);
        assertEquals(0, loads);
        Deencapsulation.setField(session, "alwaysCollectDictOnLake", true);
        check(ColumnStatistic.unknown(), true, true);
        check(null, false, true);
        assertEquals(2, loads);
        check(ndv(CacheDictManager.LOW_CARDINALITY_THRESHOLD + 1), true, true);
        assertEquals(2, loads, "Always-collect does not override a known high NDV");
    }

    @Test
    void thresholdAndNegativeDictionaryDecisionRemainEffective() {
        check(ndv(CacheDictManager.LOW_CARDINALITY_THRESHOLD + 1), true, true);
        assertEquals(0, peeks);
        check(ndv(CacheDictManager.LOW_CARDINALITY_THRESHOLD), true, true);
        assertEquals(1, loads);
        allowed = false;
        check(ndv(2), true, true);
        assertEquals(1, loads);
    }

    @Test
    void warmDictionaryIsReusedWithoutAnyBasicLookup() {
        ColumnDict dictionary = new ColumnDict(ImmutableMap.of(ByteBuffer.wrap(new byte[] {1}), 1), 0);
        cached = Optional.of(dictionary);
        var result = check(ColumnStatistic.unknown(), true, true);
        assertTrue(result.first);
        assertSame(dictionary, result.second.orElseThrow());
        assertEquals(0, loads);
        verifyNoInteractions(storage);
    }
}

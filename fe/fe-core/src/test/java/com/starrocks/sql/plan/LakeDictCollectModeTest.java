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

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.statistics.CacheRelaxDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

// low_cardinality_optimize_on_lake collects and uses the global dicts of lake columns.
// low_cardinality_collect_dict_on_lake only starts the collection; the plan keeps reading strings.
public class LakeDictCollectModeTest extends ConnectorPlanTestBase {
    private static final String SQL = "select count(*) from iceberg0.unpartitioned_db.t0 group by data";
    private static final ColumnDict DICT = new ColumnDict(ImmutableMap.<ByteBuffer, Integer>builder()
            .put(ByteBuffer.wrap("a".getBytes()), 1)
            .put(ByteBuffer.wrap("b".getBytes()), 2)
            .build(), 0, 0);

    private final AtomicInteger dictLoads = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        // The lake visitors of DecodeCollector run only for queries; the plan harness does not set it.
        connectContext.getState().setIsQuery(true);
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.USE_MOCK_DICT_MANAGER = false;
        connectContext.getState().setIsQuery(false);
    }

    @BeforeEach
    public void setUp() {
        dictLoads.set(0);
        new MockUp<CacheRelaxDictManager>() {
            @Mock
            public boolean hasGlobalDict(String tableUUID, String columnName) {
                return true;
            }

            @Mock
            public Optional<ColumnDict> getCachedGlobalDict(String tableUUID, String columnName) {
                return Optional.empty();
            }

            @Mock
            public Optional<ColumnDict> getGlobalDict(String tableUUID, String columnName) {
                dictLoads.incrementAndGet();
                return Optional.of(DICT);
            }
        };
    }

    @AfterEach
    public void tearDown() {
        SessionVariable sv = connectContext.getSessionVariable();
        sv.setUseLowCardinalityOptimizeOnLake(false);
        sv.setCollectLowCardinalityDictOnLake(false);
        connectContext.setStatisticsConnection(false);
        connectContext.setLakeDictCollection(false);
    }

    @Test
    public void testOptimizeModeUsesDict() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
        String plan = getVerboseExplain(SQL);
        assertContains(plan, "dict_col=data");
        Assertions.assertTrue(dictLoads.get() > 0);
    }

    @Test
    public void testCollectModeLoadsDictButPlansStrings() throws Exception {
        connectContext.getSessionVariable().setCollectLowCardinalityDictOnLake(true);
        String plan = getVerboseExplain(SQL);
        assertNotContains(plan, "dict_col=");
        assertNotContains(plan, "DictDecode");
        Assertions.assertTrue(dictLoads.get() > 0);
    }

    @Test
    public void testBothModesOffDoNotLoadDict() throws Exception {
        String plan = getVerboseExplain(SQL);
        assertNotContains(plan, "dict_col=");
        Assertions.assertEquals(0, dictLoads.get());
    }

    @Test
    public void testStatisticsQueryDoesNotCollectOrUseDict() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
        connectContext.getSessionVariable().setCollectLowCardinalityDictOnLake(true);
        connectContext.setStatisticsConnection(true);
        String plan = getVerboseExplain(SQL);
        assertNotContains(plan, "dict_col=");
        Assertions.assertEquals(0, dictLoads.get());
    }

    @Test
    public void testDictCollectionQueryDoesNotCollectOrUseDict() throws Exception {
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
        connectContext.getSessionVariable().setCollectLowCardinalityDictOnLake(true);
        connectContext.setLakeDictCollection(true);
        String plan = getVerboseExplain(SQL);
        assertNotContains(plan, "dict_col=");
        Assertions.assertEquals(0, dictLoads.get());
    }
}

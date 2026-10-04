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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

// MIN/MAX on a LIST partition column may be answered from partition metadata only when the answer is exact.
// The values a partition allows are not the values it stores, so we expect the rewrite to fold only from
// non-empty single-value partitions and to keep the scan otherwise.
// mockDML advances partition versions without writing rows, so each test knows which partitions hold data
// and which values were inserted, and checks only the plan.
class PartitionListMinMaxCorrectnessTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    private static final class FixtureState implements AutoCloseable {
        private final boolean priorUnitTest = FeConstants.runningUnitTest;
        private final boolean priorRewrite = connectContext.getSessionVariable().isEnableRewritePartitionColumnMinMax();
        private final boolean priorMeta = connectContext.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();

        private FixtureState() {
            // Otherwise every partition reports data, including untouched partitions.
            FeConstants.runningUnitTest = false;
            connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
            UtFrameUtils.mockDML();
        }

        @Override
        public void close() throws Exception {
            try {
                setRewrite(priorRewrite);
            } finally {
                connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(priorMeta);
                FeConstants.runningUnitTest = priorUnitTest;
            }
        }
    }

    private static void execute(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.executeSql(sql);
        assertFalse(connectContext.getState().isError(), connectContext.getState().getErrorMessage());
    }

    private static void setRewrite(boolean enabled) throws Exception {
        execute("set " + SessionVariable.ENABLE_REWRITE_PARTITION_COLUMN_MINMAX + "=" + enabled);
        assertEquals(enabled, connectContext.getSessionVariable().isEnableRewritePartitionColumnMinMax());
    }

    private static OlapTable create(String name, String partitions) throws Exception {
        starRocksAssert.withTable("create table " + name + " (k int null) duplicate key(k) "
                + "partition by list(k) (" + partitions + ") distributed by hash(k) buckets 1 "
                + "properties('replication_num'='1')");
        return (OlapTable) starRocksAssert.getTable("test", name);
    }

    private static Set<String> nonEmpty(OlapTable table) {
        assertFalse(FeConstants.runningUnitTest);
        return table.getNonEmptyPartitions().stream().map(Partition::getName).collect(Collectors.toSet());
    }

    private static void insert(OlapTable table, String partitionName, String literal) throws Exception {
        Map<Long, Long> before = new HashMap<>();
        for (Partition partition : table.getPartitions()) {
            before.put(partition.getId(), partition.getDefaultPhysicalPartition().getVisibleVersion());
        }
        // Without a target partition mockDML marks every partition as written, so we always name it.
        execute("insert into " + table.getName() + " partition(" + partitionName + ") values (" + literal + ")");
        for (Partition partition : table.getPartitions()) {
            var physical = partition.getDefaultPhysicalPartition();
            if (partition.getName().equals(partitionName)) {
                assertTrue(physical.getVisibleVersion() > before.get(partition.getId()));
                assertTrue(physical.getDataVersion() > 1);
            } else {
                assertEquals(before.get(partition.getId()).longValue(), physical.getVisibleVersion());
            }
        }
    }

    private ExecPlan plan(String sql, boolean enabled) throws Exception {
        setRewrite(enabled);
        return getExecPlan(sql);
    }

    private static void collect(OptExpression node, List<ConstantOperator> constants,
                                List<PhysicalOlapScanOperator> scans) {
        if (node.getOp() instanceof PhysicalValuesOperator values) {
            for (List<ScalarOperator> row : values.getRows()) {
                for (ScalarOperator value : row) {
                    assertTrue(value instanceof ConstantOperator);
                    constants.add((ConstantOperator) value);
                }
            }
        } else if (node.getOp() instanceof PhysicalOlapScanOperator scan) {
            scans.add(scan);
        }
        for (OptExpression child : node.getInputs()) {
            collect(child, constants, scans);
        }
    }

    private static List<ConstantOperator> constants(ExecPlan plan) {
        List<ConstantOperator> constants = new ArrayList<>();
        collect(plan.getPhysicalPlan(), constants, new ArrayList<>());
        return constants;
    }

    private static Set<Long> scanPartitions(ExecPlan plan) {
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        collect(plan.getPhysicalPlan(), new ArrayList<>(), scans);
        assertEquals(1, scans.size(), "we expect exactly one olap scan when the rewrite does not fold");
        return new HashSet<>(scans.get(0).getSelectedPartitionId());
    }

    private void assertFallback(String sql, Set<Long> expectedPartitions) throws Exception {
        ExecPlan disabled = plan(sql, false);
        ExecPlan enabled = plan(sql, true);
        assertTrue(constants(disabled).isEmpty());
        assertTrue(constants(enabled).isEmpty(), "allowed partition values must not become result constants");
        assertEquals(expectedPartitions, scanPartitions(disabled));
        assertEquals(expectedPartitions, scanPartitions(enabled),
                "the rewrite must not prune partitions by their allowed values");
        assertEquals(disabled.getOutputExprs().size(), enabled.getOutputExprs().size());
    }

    private List<ConstantOperator> assertFold(String sql, List<String> expected) throws Exception {
        ExecPlan disabled = plan(sql, false);
        assertTrue(constants(disabled).isEmpty());
        scanPartitions(disabled);
        ExecPlan enabled = plan(sql, true);
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        List<ConstantOperator> values = new ArrayList<>();
        collect(enabled.getPhysicalPlan(), values, scans);
        assertTrue(scans.isEmpty());
        assertEquals(expected, values.stream().map(ConstantOperator::toString).toList());
        return values;
    }

    @Test
    void absentAllowedMinimumDoesNotFold() throws Exception {
        var table = create("list_min_absent", "partition p1 values in ('1','10')");
        try (var ignored = new FixtureState()) {
            assertTrue(nonEmpty(table).isEmpty());
            insert(table, "p1", "10");
            assertEquals(Set.of("p1"), nonEmpty(table));
            // Only 10 was inserted, so MIN is 10 and not the allowed lower value 1.
            assertFallback("select min(k) from list_min_absent", Set.of(table.getPartition("p1").getId()));
            assertFallback("select min(k), max(k) from list_min_absent", Set.of(table.getPartition("p1").getId()));
        }
    }

    @Test
    void overlappingValueEnvelopesRetainBothPartitionsForMinimum() throws Exception {
        var table = create("list_overlap_min", "partition p1 values in ('1','10'), "
                + "partition p2 values in ('2','9')");
        try (var ignored = new FixtureState()) {
            assertTrue(nonEmpty(table).isEmpty());
            insert(table, "p1", "10");
            insert(table, "p2", "2");
            assertEquals(Set.of("p1", "p2"), nonEmpty(table));
            // The allowed values point at p1 for the minimum, but the inserted minimum 2 is in p2.
            Set<Long> both = Set.of(table.getPartition("p1").getId(), table.getPartition("p2").getId());
            assertFallback("select min(k) from list_overlap_min", both);
            assertFallback("select min(k), max(k) from list_overlap_min", both);
        }
    }

    @Test
    void populatedNullPartitionIsIgnoredWhenNonnullValueExists() throws Exception {
        var table = create("list_null_and_value", "partition p_null values in (NULL), "
                + "partition p_value values in ('7')");
        try (var ignored = new FixtureState()) {
            insert(table, "p_null", "NULL");
            assertEquals(Set.of("p_null"), nonEmpty(table));
            insert(table, "p_value", "7");
            assertEquals(Set.of("p_null", "p_value"), nonEmpty(table));
            assertFold("select min(k) from list_null_and_value", List.of("7"));
            assertFold("select max(k) from list_null_and_value", List.of("7"));
            assertFold("select min(k), max(k) from list_null_and_value", List.of("7", "7"));
        }
    }

    @Test
    void populatedAllNullPartitionFoldsTypedNullForBothAggregates() throws Exception {
        var table = create("list_all_null", "partition p_null values in (NULL)");
        try (var ignored = new FixtureState()) {
            insert(table, "p_null", "NULL");
            List<ConstantOperator> values = assertFold("select min(k), max(k) from list_all_null",
                    List.of("null", "null"));
            for (ConstantOperator value : values) {
                assertTrue(value.isNull());
                assertEquals(IntegerType.INT, value.getType());
            }
        }
    }

    private void assertRestrictedQuery(String sql, Set<Long> expectedPartitions,
                                       List<String> expectedConstants) throws Exception {
        ExecPlan disabled = plan(sql, false);
        assertEquals(expectedPartitions, scanPartitions(disabled));
        ExecPlan enabled = plan(sql, true);
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        List<ConstantOperator> values = new ArrayList<>();
        collect(enabled.getPhysicalPlan(), values, scans);
        if (scans.isEmpty()) {
            assertEquals(expectedConstants, values.stream().map(ConstantOperator::toString).toList(),
                    "a restricted input must not fold extrema from other partitions");
        } else {
            assertEquals(expectedPartitions, scanPartitions(enabled));
            assertTrue(values.isEmpty());
        }
    }

    @Test
    void partitionPredicatesAndExplicitPartitionDoNotFoldOtherPartitions() throws Exception {
        var table = create("list_restricted_values", "partition p_low values in ('2'), "
                + "partition p_value values in ('7'), partition p_null values in (NULL)");
        try (var ignored = new FixtureState()) {
            insert(table, "p_low", "2");
            insert(table, "p_value", "7");
            insert(table, "p_null", "NULL");
            assertEquals(Set.of("p_low", "p_value", "p_null"), nonEmpty(table));
            assertRestrictedQuery("select min(k), max(k) from list_restricted_values where k=7",
                    Set.of(table.getPartition("p_value").getId()), List.of("7", "7"));
            assertRestrictedQuery("select min(k), max(k) from list_restricted_values where k is null",
                    Set.of(table.getPartition("p_null").getId()), List.of("null", "null"));
            assertFallback("select min(k), max(k) from list_restricted_values partition(p_value)",
                    Set.of(table.getPartition("p_value").getId()));
        }
    }
}

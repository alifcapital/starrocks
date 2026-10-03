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

package com.starrocks.planner;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.MultiColumnCombinedStats;
import com.starrocks.sql.optimizer.statistics.RuntimeFilterStatistics;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class RuntimeFilterKeyStatisticsTest {
    private static final ColumnRefOperator COLUMN = new ColumnRefOperator(1, IntegerType.INT, "key", false);
    private static final SlotRef SLOT = new SlotRef(new SlotId(1));

    private static class Node extends PlanNode {
        Node() {
            super(new PlanNodeId(1), "test");
        }

        @Override
        protected void toThrift(TPlanNode message) {
        }
    }

    private Node input() {
        Node node = new Node();
        node.computeStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(COLUMN, new ColumnStatistic(0, 100, 0, 4, 100)).build());
        return node;
    }

    @Test
    public void testDoesNotBorrowStatisticsAcrossSemanticOperators() {
        Node input = input();
        Node parent = new Node();
        parent.addChild(input);
        Assertions.assertEquals(-1, parent.getColumnNdv(SLOT));
        SelectNode filter = new SelectNode(new PlanNodeId(2), input, List.of());
        Assertions.assertEquals(-1, filter.getColumnNdv(SLOT));
        filter.computeStatistics(Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(COLUMN, new ColumnStatistic(0, 5, 0, 4, 5)).build());
        Assertions.assertEquals(5, filter.getColumnNdv(SLOT));
    }

    @Test
    public void testExchangeBorrowsOnlyWhenOwnStatisticsAreAbsent() {
        Node input = input();
        new PlanFragment(new PlanFragmentId(0), input, DataPartition.UNPARTITIONED);
        ExchangeNode exchange = new ExchangeNode(new PlanNodeId(2), input, DataPartition.UNPARTITIONED);
        Assertions.assertEquals(100, exchange.getColumnNdv(SLOT));
        exchange.computeStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(COLUMN, ColumnStatistic.unknown()).build());
        Assertions.assertEquals(-1, exchange.getColumnNdv(SLOT));
    }

    @Test
    public void testNdvIsBoundedByOperatorRows() {
        Node input = input();
        input.computeStatistics(Statistics.builder().setOutputRowCount(3)
                .addColumnStatistic(COLUMN, new ColumnStatistic(0, 100, 0, 4, 100)).build());
        Assertions.assertEquals(3, input.getColumnNdv(SLOT));
    }
    @Test
    public void testOnlyCompositeKeysSendTheirCurrentNdvToBackend() {
        RuntimeFilterDescription filter = new RuntimeFilterDescription(new com.starrocks.qe.SessionVariable());
        filter.setJoinMode(JoinNode.DistributionMode.PARTITIONED);
        filter.setBuildKeyStatistics(input().getRuntimeFilterStatistics(SLOT));
        filter.setEqualCount(1);
        Assertions.assertFalse(filter.toThrift().isSetEstimated_build_ndv());
        filter.setEqualCount(2);
        Assertions.assertEquals(100, filter.toThrift().getEstimated_build_ndv());
        Node filtered = input();
        filtered.computeStatistics(Statistics.builder().setOutputRowCount(7)
                .addColumnStatistic(COLUMN, new ColumnStatistic(0, 100, 0, 4, 100)).build());
        filter.setBuildKeyStatistics(filtered.getRuntimeFilterStatistics(SLOT));
        Assertions.assertEquals(7, filter.toThrift().getEstimated_build_ndv());
        filter.setBuildKeyStatistics(null);
        Assertions.assertFalse(filter.toThrift().isSetEstimated_build_ndv());
    }

    @Test
    public void testLocalAndSmallBuildFiltersRespectKnownMembership() {
        SessionVariable session = new SessionVariable();
        session.setGlobalRuntimeFilterProbeMinSize(1);
        session.setGlobalRuntimeFilterBuildMinSize(131072);
        Node probe = new Node();
        probe.computeStatistics(Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(COLUMN, new ColumnStatistic(1, 1000, 0, 4, 1000))
                .addMultiColumnStatistics(Set.of(COLUMN), new MultiColumnCombinedStats(1000, 1_000_000,
                        List.of(COLUMN), List.of(new MultiColumnCombinedStats.McvEntry(List.of("1"), 900000))))
                .build());
        RuntimeFilterDescription filter = new RuntimeFilterDescription(session);
        filter.setJoinMode(JoinNode.DistributionMode.PARTITIONED);
        filter.setBuildCardinality(10);
        for (boolean remote : List.of(false, true)) {
            if (remote) {
                filter.enterExchangeNode();
            }
            for (String value : List.of("1", "2")) {
                filter.setBuildKeyStatistics(RuntimeFilterStatistics.from(COLUMN,
                        new ColumnStatistic(1, 2, 0, 4, 1),
                        List.of(new MultiColumnCombinedStats(1, 10, List.of(COLUMN),
                                List.of(new MultiColumnCombinedStats.McvEntry(List.of(value), 10)))), 10));
                Assertions.assertEquals(value.equals("2"), filter.canProbeUse(probe, SLOT, null),
                        "A small/local build matching the hot key must not bypass its 90% passing fraction");
            }
        }
    }

    @Test
    public void testKnownUniformMembershipAndExplicitForce() {
        SessionVariable session = new SessionVariable();
        session.setGlobalRuntimeFilterProbeMinSize(1);
        RuntimeFilterDescription filter = new RuntimeFilterDescription(session);
        filter.setBuildKeyStatistics(input().getRuntimeFilterStatistics(SLOT));
        filter.setBuildCardinality(1000);
        Assertions.assertFalse(filter.canProbeUse(input(), SLOT, null));
        session.setGlobalRuntimeFilterProbeMinSize(0);
        Assertions.assertTrue(filter.canProbeUse(input(), SLOT, null));
        filter.enterExchangeNode();
        Assertions.assertTrue(filter.canProbeUse(input(), SLOT, null));
    }

    @Test
    public void testUnknownMembershipRetainsLocalAndSmallBuildFallbacks() {
        SessionVariable session = new SessionVariable();
        session.setGlobalRuntimeFilterProbeMinSize(1);
        session.setGlobalRuntimeFilterBuildMinSize(10);
        RuntimeFilterDescription filter = new RuntimeFilterDescription(session);
        filter.setJoinMode(JoinNode.DistributionMode.PARTITIONED);
        filter.setBuildCardinality(900);
        Assertions.assertTrue(filter.canProbeUse(input(), SLOT, null));
        filter.enterExchangeNode();
        Assertions.assertFalse(filter.canProbeUse(input(), SLOT, null));
        filter.setBuildCardinality(10);
        Assertions.assertTrue(filter.canProbeUse(input(), SLOT, null));
    }

    @Test
    public void testCastUsesCurrentOperatorStatisticsAndPreservesUnknown() {
        ColumnRefOperator text = new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true);
        SlotRef slot = new SlotRef(new SlotId(1));
        slot.setType(VarcharType.VARCHAR);
        CastExpr cast = new CastExpr(IntegerType.BIGINT, slot);
        Node input = new Node();
        input.computeStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(text, ColumnStatistic.builder().setDistinctValuesCount(100).build()).build());
        Assertions.assertEquals(100, input.getColumnNdv(cast));
        Node parent = new Node();
        parent.addChild(input);
        Assertions.assertEquals(-1, parent.getColumnNdv(cast));
        parent.computeStatistics(Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(text, ColumnStatistic.builder().setDistinctValuesCount(3).build()).build());
        Assertions.assertEquals(3, parent.getColumnNdv(cast));
        parent.computeStatistics(Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(text, ColumnStatistic.unknown()).build());
        Assertions.assertEquals(-1, parent.getColumnNdv(cast));
    }

}

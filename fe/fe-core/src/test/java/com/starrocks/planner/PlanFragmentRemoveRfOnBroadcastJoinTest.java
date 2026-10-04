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

import com.google.common.collect.Lists;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class PlanFragmentRemoveRfOnBroadcastJoinTest {

    private static class TestNode extends PlanNode {
        TestNode(int id, List<PlanNode> children) {
            super(new PlanNodeId(id), "TEST");
            this.children.addAll(children);
        }

        @Override
        protected void toThrift(TPlanNode msg) {
        }
    }

    private static class TreeBuilder {
        private final Random random;
        private int nextNodeId = 1;
        private int nextFilterId = 1;
        private final Map<PlanNode, PlanFragmentId> fragmentOf = new IdentityHashMap<>();
        private final List<RuntimeFilterDescription> filters = new ArrayList<>();
        private final List<PlanNode> nodes = new ArrayList<>();

        TreeBuilder(long seed) {
            this.random = new Random(seed);
        }

        PlanNode build(int depth, int fragment) {
            PlanNode node;
            int kind = depth <= 0 ? 0 : random.nextInt(5);
            if (kind == 0) {
                node = new TestNode(nextNodeId++, List.of());
            } else if (kind == 1) {
                node = new TestNode(nextNodeId++, List.of(build(depth - 1, fragment)));
            } else if (kind == 2) {
                // A child in another fragment, like the input of an exchange node.
                node = new TestNode(nextNodeId++, List.of(build(depth - 1, fragment + 1 + random.nextInt(3))));
            } else {
                PlanNode left = build(depth - 1, fragment);
                PlanNode right = build(depth - 1, fragment);
                HashJoinNode join = new HashJoinNode(new PlanNodeId(nextNodeId++), left, right,
                        JoinOperator.INNER_JOIN, List.of(), List.of());
                join.setDistributionMode(random.nextBoolean() ? JoinNode.DistributionMode.BROADCAST :
                        JoinNode.DistributionMode.PARTITIONED);
                int rfCount = random.nextInt(3);
                for (int i = 0; i < rfCount; i++) {
                    RuntimeFilterDescription rf = new RuntimeFilterDescription(new SessionVariable());
                    rf.setFilterId(nextFilterId++);
                    rf.setHasRemoteTargets(random.nextInt(3) != 0);
                    join.getBuildRuntimeFilters().add(rf);
                    filters.add(rf);
                }
                node = join;
            }
            fragmentOf.put(node, new PlanFragmentId(fragment));
            nodes.add(node);
            return node;
        }

        void assignProbeFilters() {
            for (PlanNode node : nodes) {
                List<RuntimeFilterDescription> probes = new ArrayList<>();
                for (RuntimeFilterDescription rf : filters) {
                    if (random.nextInt(3) == 0) {
                        probes.add(rf);
                    }
                }
                node.setProbeRuntimeFilters(probes);
            }
        }

        void applyFragmentIds() {
            fragmentOf.forEach(PlanNode::setFragmentId);
        }
    }

    private static Map<Integer, List<Integer>> probeFilterIds(PlanNode root) {
        Map<Integer, List<Integer>> result = new LinkedHashMap<>();
        collectProbeFilterIds(root, result);
        return result;
    }

    private static void collectProbeFilterIds(PlanNode node, Map<Integer, List<Integer>> result) {
        result.put(node.getId().asInt(), node.getProbeRuntimeFilters().stream()
                .map(RuntimeFilterDescription::getFilterId).collect(Collectors.toList()));
        for (PlanNode child : node.getChildren()) {
            collectProbeFilterIds(child, result);
        }
    }

    // The code of PlanFragment.removeRfOnRightOffspringsOfBroadcastJoin before it was changed.
    private static RoaringBitmap oldCollectNonBroadcastRfIds(PlanNode root) {
        RoaringBitmap filterIds = root.getChildren().stream()
                .filter(child -> child.getFragmentId().equals(root.getFragmentId()))
                .map(PlanFragmentRemoveRfOnBroadcastJoinTest::oldCollectNonBroadcastRfIds)
                .reduce(RoaringBitmap.bitmapOf(), (a, b) -> RoaringBitmap.or(a, b));
        if (root instanceof HashJoinNode) {
            HashJoinNode joinNode = (HashJoinNode) root;
            if (!joinNode.isBroadcast()) {
                joinNode.getBuildRuntimeFilters().forEach(rf -> filterIds.add(rf.getFilterId()));
            }
        }
        return filterIds;
    }

    private static RoaringBitmap oldCollectLocalRightOffsprings(PlanNode root, RoaringBitmap localRightOffsprings) {
        List<RoaringBitmap> localOffspringsPerChild = root.getChildren().stream()
                .filter(child -> child.getFragmentId().equals(root.getFragmentId()))
                .map(child -> oldCollectLocalRightOffsprings(child, localRightOffsprings))
                .collect(Collectors.toList());
        RoaringBitmap localOffsprings =
                localOffspringsPerChild.stream().reduce(RoaringBitmap.bitmapOf(), (a, b) -> RoaringBitmap.or(a, b));
        localOffsprings.add(root.getId().asInt());
        if (root instanceof HashJoinNode) {
            HashJoinNode hashJoinNode = (HashJoinNode) root;
            boolean hasGlobalRuntimeFilter = hashJoinNode.getBuildRuntimeFilters()
                    .stream().anyMatch(RuntimeFilterDescription::isHasRemoteTargets);
            if (hashJoinNode.isBroadcast() && hasGlobalRuntimeFilter && !localOffspringsPerChild.isEmpty()) {
                localRightOffsprings.or(localOffspringsPerChild.get(1));
            }
        }
        return localOffsprings;
    }

    private static void oldRemoveRfOfRightOffspring(PlanNode root, RoaringBitmap targetRightOffsprings,
                                                    RoaringBitmap filterIds) {
        if (targetRightOffsprings.contains(root.getId().asInt())) {
            List<RuntimeFilterDescription> reservedRuntimeFilters = root.getProbeRuntimeFilters()
                    .stream()
                    .filter(rf -> !filterIds.contains(rf.getFilterId()))
                    .collect(Collectors.toList());
            root.setProbeRuntimeFilters(reservedRuntimeFilters);
        }

        root.getChildren()
                .stream()
                .filter(child -> child.getFragmentId().equals(root.getFragmentId()))
                .forEach(child -> oldRemoveRfOfRightOffspring(child, targetRightOffsprings, filterIds));
    }

    private static void oldRemoveRfOnRightOffspringsOfBroadcastJoin(PlanNode root) {
        RoaringBitmap localRightOffsprings = RoaringBitmap.bitmapOf();
        oldCollectLocalRightOffsprings(root, localRightOffsprings);

        RoaringBitmap filterIds = oldCollectNonBroadcastRfIds(root);
        if (localRightOffsprings.isEmpty() || filterIds.isEmpty()) {
            return;
        }

        oldRemoveRfOfRightOffspring(root, localRightOffsprings, filterIds);
    }

    @Test
    public void testSameResultAsBefore() {
        int changed = 0;
        for (int seed = 0; seed < 3000; seed++) {
            TreeBuilder oldBuilder = new TreeBuilder(seed);
            PlanNode oldRoot = oldBuilder.build(6, 0);
            oldBuilder.assignProbeFilters();
            oldBuilder.applyFragmentIds();

            TreeBuilder newBuilder = new TreeBuilder(seed);
            PlanNode newRoot = newBuilder.build(6, 0);
            newBuilder.assignProbeFilters();
            PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), newRoot, DataPartition.UNPARTITIONED);
            newBuilder.applyFragmentIds();

            Map<Integer, List<Integer>> before = probeFilterIds(newRoot);
            Assertions.assertEquals(before, probeFilterIds(oldRoot));

            oldRemoveRfOnRightOffspringsOfBroadcastJoin(oldRoot);
            fragment.removeRfOnRightOffspringsOfBroadcastJoin();

            Map<Integer, List<Integer>> expected = probeFilterIds(oldRoot);
            Assertions.assertEquals(expected, probeFilterIds(newRoot), "seed " + seed);
            if (!expected.equals(before)) {
                changed++;
            }
        }
        // Make sure the generated trees reach the removal.
        Assertions.assertTrue(changed > 100, "changed " + changed);
    }

    @Test
    public void testRemovesFilterOfNonBroadcastJoinOnBroadcastRightSide() {
        PlanNode leftLeaf = new TestNode(2, Lists.newArrayList());
        PlanNode rightLeaf = new TestNode(4, Lists.newArrayList());
        PlanNode right = new TestNode(3, Lists.newArrayList(rightLeaf));
        HashJoinNode broadcast = new HashJoinNode(new PlanNodeId(1), leftLeaf, right, JoinOperator.INNER_JOIN,
                List.of(), List.of());
        broadcast.setDistributionMode(JoinNode.DistributionMode.BROADCAST);
        RuntimeFilterDescription global = new RuntimeFilterDescription(new SessionVariable());
        global.setFilterId(10);
        global.setHasRemoteTargets(true);
        broadcast.getBuildRuntimeFilters().add(global);

        PlanNode nonBroadcastLeaf = new TestNode(6, Lists.newArrayList());
        HashJoinNode shuffle = new HashJoinNode(new PlanNodeId(5), broadcast, nonBroadcastLeaf,
                JoinOperator.INNER_JOIN, List.of(), List.of());
        shuffle.setDistributionMode(JoinNode.DistributionMode.PARTITIONED);
        RuntimeFilterDescription shuffleRf = new RuntimeFilterDescription(new SessionVariable());
        shuffleRf.setFilterId(20);
        shuffle.getBuildRuntimeFilters().add(shuffleRf);

        leftLeaf.setProbeRuntimeFilters(Lists.newArrayList(shuffleRf, global));
        right.setProbeRuntimeFilters(Lists.newArrayList(shuffleRf, global));
        rightLeaf.setProbeRuntimeFilters(Lists.newArrayList(shuffleRf, global));

        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), shuffle, DataPartition.UNPARTITIONED);
        fragment.removeRfOnRightOffspringsOfBroadcastJoin();

        Assertions.assertEquals(List.of(20, 10), probeFilterIds(leftLeaf).get(2));
        Assertions.assertEquals(List.of(10), probeFilterIds(right).get(3));
        Assertions.assertEquals(List.of(10), probeFilterIds(rightLeaf).get(4));
    }
}

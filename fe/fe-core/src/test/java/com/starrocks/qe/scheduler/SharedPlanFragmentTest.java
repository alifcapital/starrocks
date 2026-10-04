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

package com.starrocks.qe.scheduler;

import com.google.common.collect.Maps;
import com.starrocks.planner.ExportSink;
import com.starrocks.planner.MultiCastPlanFragment;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.TpchSQL;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.THdfsProperties;
import com.starrocks.thrift.TPlanFragment;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

/**
 * The instances of a fragment share one thrift plan. Each test serializes the request of every instance built with
 * the shared plan and built with a plan of its own, and expects the same bytes.
 */
public class SharedPlanFragmentTest extends SchedulerTestBase {
    private static final TProtocolFactory[] PROTOCOLS = {new TBinaryProtocol.Factory(), new TCompactProtocol.Factory()};

    private static final String CTE_SQL = "with\n" +
            "    w1 as (select * from lineitem),\n" +
            "    w2 as (select count(1) as cnt, L_ORDERKEY, L_PARTKEY from lineitem group by L_ORDERKEY, L_PARTKEY)\n" +
            "select /*+SET_VAR(cbo_cte_reuse=true,cbo_cte_reuse_rate=0)*/ " +
            "count(1) as cnt2, v1.L_ORDERKEY, v1.L_PARTKEY, v3.cnt\n" +
            "from\n" +
            "    w1 v1\n" +
            "    join w1 v2 on (v1.L_ORDERKEY = v2.L_ORDERKEY)\n" +
            "    join w2 v3 on (v1.L_PARTKEY = v3.L_PARTKEY)\n" +
            "    join w2 v4 on (v1.L_ORDERKEY = v4.cnt)\n" +
            "GROUP BY v1.L_ORDERKEY, v1.L_PARTKEY, v3.cnt";

    private static Stream<Arguments> tpchSource() {
        return TpchSQL.getAllSQL().entrySet().stream().map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    @ParameterizedTest(name = "Tpch.{0}")
    @MethodSource("tpchSource")
    public void testTpch(String name, String sql) throws Exception {
        Result result = check(sql, false);
        Assertions.assertTrue(result.sharedFragments > 0, name);
    }

    @Test
    public void testSetOperations() throws Exception {
        Assertions.assertTrue(check("select L_ORDERKEY from lineitem union all select L_ORDERKEY from lineitem0 " +
                "union select L_PARTKEY from lineitem1", false).sharedFragments > 0);
        Assertions.assertTrue(check("select L_ORDERKEY from lineitem intersect select L_ORDERKEY from lineitem0 " +
                "except select L_PARTKEY from lineitem2", false).sharedFragments > 0);
    }

    @Test
    public void testWindowAndTopN() throws Exception {
        Assertions.assertTrue(check("select L_ORDERKEY, rank() over (partition by L_PARTKEY order by L_SHIPDATE) " +
                "from lineitem order by L_ORDERKEY limit 10", false).sharedFragments > 0);
    }

    @Test
    public void testInsert() throws Exception {
        Assertions.assertTrue(check("insert into lineitem select * from lineitem", false).sharedFragments > 0);
        check("insert into nation select * from nation union all select * from nation", false);
    }

    @Test
    public void testMultiCast() throws Exception {
        Result result = check(CTE_SQL, false);
        Assertions.assertTrue(result.multiCastFragments > 0);
        Assertions.assertTrue(result.sharedFragments > 0);
    }

    @Test
    public void testExportSink() throws Exception {
        Result result = check("select * from lineitem", true);
        Assertions.assertTrue(result.exportFragments > 0);
    }

    private static class Result {
        int sharedFragments = 0;
        int multiCastFragments = 0;
        int exportFragments = 0;
    }

    /**
     * Plans the query and builds the request of every instance in three ways: with a thrift plan of its own, with
     * {@link TFragmentInstanceFactory#create(ExecutionFragment, List, TDescriptorTable, int, int)}, and with
     * {@link Deployer#createFragmentExecStates}. Expects the same bytes, and expects the plan to be shared only when
     * the fragment is neither a multi cast fragment nor has an export sink.
     *
     * @param exportSink put an export sink on every fragment with more than one instance below the root
     */
    private Result check(String sql, boolean exportSink) throws Exception {
        DefaultCoordinator coordinator = getSchedulerWithQueryId(sql);
        coordinator.prepareExec();
        CoordinatorPreprocessor prepareInfo = coordinator.getPrepareInfo();
        List<ExecutionFragment> fragments = prepareInfo.getFragmentsInPreorder();
        TDescriptorTable descTable = prepareInfo.getDescriptorTable();
        TFragmentInstanceFactory factory = prepareInfo.createTFragmentInstanceFactory();

        if (exportSink) {
            for (ExecutionFragment fragment : fragments.subList(1, fragments.size())) {
                if (fragment.getInstances().size() > 1 && !(fragment.getPlanFragment() instanceof MultiCastPlanFragment)) {
                    fragment.getPlanFragment().setSink(new ExportSink("hdfs://127.0.0.1:9000/export/", "prefix_", ",",
                            "\n", new BrokerDesc(Maps.newHashMap()), new THdfsProperties()));
                }
            }
        }
        // An export sink appends the index of each instance to its file prefix, so every pass starts from the
        // original prefix.
        Map<ExportSink, String> exportPrefixes = new IdentityHashMap<>();
        for (ExecutionFragment fragment : fragments) {
            if (fragment.getPlanFragment().getSink() instanceof ExportSink sink) {
                exportPrefixes.put(sink, sink.getFileNamePrefix());
            }
        }

        Result result = new Result();
        for (ExecutionFragment fragment : fragments) {
            PlanFragment planFragment = fragment.getPlanFragment();
            List<FragmentInstance> instances = fragment.getInstances();
            boolean canShare = !(planFragment instanceof MultiCastPlanFragment) &&
                    !(planFragment.getSink() instanceof ExportSink);
            if (instances.size() > 1) {
                if (planFragment instanceof MultiCastPlanFragment) {
                    result.multiCastFragments++;
                } else if (planFragment.getSink() instanceof ExportSink) {
                    result.exportFragments++;
                } else {
                    result.sharedFragments++;
                }
            }

            boolean tableSinkDop = coordinator.getJobSpec().isEnablePipeline() && planFragment.hasTableSink();
            int totalTableSinkDop = tableSinkDop ?
                    instances.stream().mapToInt(FragmentInstance::getTableSinkDop).sum() : 0;

            restore(exportPrefixes);
            List<TExecPlanFragmentParams> own = new ArrayList<>();
            int accTabletSinkDop = 0;
            for (FragmentInstance instance : instances) {
                own.add(factory.create(instance, descTable, accTabletSinkDop, totalTableSinkDop));
                if (tableSinkDop) {
                    accTabletSinkDop += instance.getTableSinkDop();
                }
            }

            restore(exportPrefixes);
            List<TExecPlanFragmentParams> shared = factory.create(fragment, instances, descTable, 0, totalTableSinkDop);

            Assertions.assertEquals(own.size(), shared.size());
            for (int i = 0; i < own.size(); i++) {
                assertSameBytes(own.get(i), shared.get(i));
            }
            assertSharing(canShare, shared);
        }

        // The deployer builds the requests in the order of its stages, which is also the order of the export prefixes.
        restore(exportPrefixes);
        Deployer deployer = new Deployer(connectContext, coordinator.getJobSpec(), coordinator.getExecutionDAG(),
                prepareInfo.getCoordAddress(), (status, execution, failure) -> {
                }, false);
        DeployState deployState = deployer.createFragmentExecStates(fragments);
        List<FragmentInstanceExecState> executions = new ArrayList<>();
        deployState.getThreeStageExecutionsToDeploy().forEach(executions::addAll);
        Assertions.assertEquals(fragments.stream().mapToInt(f -> f.getInstances().size()).sum(), executions.size());

        restore(exportPrefixes);
        Map<ExecutionFragment, List<TExecPlanFragmentParams>> requestsByFragment = new IdentityHashMap<>();
        List<TExecPlanFragmentParams> requests = new ArrayList<>();
        for (FragmentInstanceExecState execution : executions) {
            TExecPlanFragmentParams request = execution.getRequestToDeploy();
            FragmentInstance instance = execution.getFragmentInstance();
            boolean tableSinkDop = request.getParams().isSetPipeline_sink_dop();
            TExecPlanFragmentParams own = factory.create(instance, request.getDesc_tbl(),
                    tableSinkDop ? request.getParams().getSender_id() : 0,
                    tableSinkDop ? request.getParams().getNum_senders() : 0);
            assertSameBytes(own, request);
            requestsByFragment.computeIfAbsent(instance.getExecFragment(), k -> new ArrayList<>()).add(request);
            requests.add(request);
        }
        for (Map.Entry<ExecutionFragment, List<TExecPlanFragmentParams>> entry : requestsByFragment.entrySet()) {
            PlanFragment planFragment = entry.getKey().getPlanFragment();
            assertSharing(!(planFragment instanceof MultiCastPlanFragment) &&
                    !(planFragment.getSink() instanceof ExportSink), entry.getValue());
        }

        assertConcurrentSerialization(requests);
        return result;
    }

    private static void restore(Map<ExportSink, String> exportPrefixes) {
        exportPrefixes.forEach(ExportSink::setFileNamePrefix);
    }

    private static void assertSharing(boolean canShare, List<TExecPlanFragmentParams> requests) {
        Set<TPlanFragment> plans = Collections.newSetFromMap(new IdentityHashMap<>());
        requests.forEach(request -> plans.add(request.getFragment()));
        Assertions.assertEquals(canShare ? 1 : requests.size(), plans.size());
    }

    private static void assertSameBytes(TExecPlanFragmentParams expected, TExecPlanFragmentParams actual)
            throws TException {
        for (TProtocolFactory protocol : PROTOCOLS) {
            Assertions.assertArrayEquals(new TSerializer(protocol).serialize(expected),
                    new TSerializer(protocol).serialize(actual));
        }
    }

    /**
     * The deployer may serialize the requests of one fragment on several threads at once, so they read the shared
     * plan at the same time. Expects the same bytes as one thread.
     */
    private static void assertConcurrentSerialization(List<TExecPlanFragmentParams> requests) throws Exception {
        List<byte[]> expected = new ArrayList<>();
        for (TExecPlanFragmentParams request : requests) {
            expected.add(new TSerializer(new TBinaryProtocol.Factory()).serialize(request));
        }
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            for (int round = 0; round < 4; round++) {
                List<Future<byte[]>> futures = new ArrayList<>();
                for (TExecPlanFragmentParams request : requests) {
                    futures.add(executor.submit(() -> new TSerializer(new TBinaryProtocol.Factory()).serialize(request)));
                }
                for (int i = 0; i < futures.size(); i++) {
                    Assertions.assertArrayEquals(expected.get(i), futures.get(i).get());
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}

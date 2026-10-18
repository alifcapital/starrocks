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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.Type;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

/**
 * Measures what an MCV list of a six-column group with 300 head tuples adds to the planning time of queries that
 * filter the table on the columns of the group, and counts the scan derivations and head evaluations of one planning.
 * The heavy query repeats a filtered scan in several CTEs and joins it, as the reports of BI tools do. We expect the
 * numbers to vary between hosts, so the test logs them and asserts only a generous bound.
 */
public class McvPlanningCostTest extends ConnectorPlanTestBase {
    private static final int WARM_UP = 20;
    private static final int RUNS = 40;
    private static final long TABLE_ROWS = 600_037_902L;
    private static final List<String> COLUMNS = List.of("l_returnflag", "l_linenumber", "l_suppkey", "l_partkey",
            "l_quantity", "l_tax");

    private interface Body {
        void run() throws Exception;
    }

    /**
     * 300 distinct tuples of the six columns with a skewed frequency, covering 99.98% of the rows, with the component
     * counts the collection writes.
     */
    static ExternalMcvStatistics.Group group() {
        Random random = new Random(20261014);
        String[] flags = {"A", "N", "R"};
        Set<List<String>> tuples = new LinkedHashSet<>();
        while (tuples.size() < 300) {
            tuples.add(List.of(flags[skewed(random, 3)], Integer.toString(1 + skewed(random, 7)),
                    Integer.toString(skewed(random, 40)), Integer.toString(skewed(random, 30)),
                    (1 + skewed(random, 50)) + ".00", "0.0" + skewed(random, 9)));
        }
        long head = (long) (TABLE_ROWS * 0.9998);
        double[] weights = new double[tuples.size()];
        double total = 0;
        for (int i = 0; i < weights.length; i++) {
            weights[i] = 1.0 / Math.pow(i + 1, 1.1);
            total += weights[i];
        }
        List<List<String>> ordered = new ArrayList<>(tuples);
        long[] counts = new long[ordered.size()];
        List<Map<String, Long>> marginals = new ArrayList<>();
        for (int i = 0; i < COLUMNS.size(); i++) {
            marginals.add(new HashMap<>());
        }
        for (int t = 0; t < ordered.size(); t++) {
            counts[t] = Math.max(1, (long) (head * weights[t] / total));
            for (int i = 0; i < COLUMNS.size(); i++) {
                marginals.get(i).merge(ordered.get(t).get(i), counts[t], Long::sum);
            }
        }
        List<MultiColumnCombinedStats.McvEntry> entries = new ArrayList<>();
        for (int t = 0; t < ordered.size(); t++) {
            List<Long> components = new ArrayList<>();
            for (int i = 0; i < COLUMNS.size(); i++) {
                components.add(marginals.get(i).get(ordered.get(t).get(i)));
            }
            entries.add(new MultiColumnCombinedStats.McvEntry(ordered.get(t), counts[t], components));
        }
        return new ExternalMcvStatistics.Group(COLUMNS, TABLE_ROWS, 896, entries, List.of(),
                List.of(0L, 0L, 0L, 0L, 0L, 0L));
    }

    // A value in [0, n) where small values are much more frequent.
    private static int skewed(Random random, int n) {
        return Math.min(n - 1, (int) Math.floor(n * Math.pow(random.nextDouble(), 3)));
    }

    static void withStatistics(ExternalMcvStatistics statistics, Body body) throws Exception {
        StatisticStorage storage = GlobalStateMgr.getCurrentState().getStatisticStorage();
        StatisticStorage proxy = (StatisticStorage) Proxy.newProxyInstance(StatisticStorage.class.getClassLoader(),
                new Class<?>[] {StatisticStorage.class}, (instance, method, arguments) -> {
                    if (method.getName().equals("getExternalMcvStatistics")) {
                        return statistics;
                    }
                    try {
                        return method.invoke(storage, arguments);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        GlobalStateMgr.getCurrentState().setStatisticStorage(proxy);
        try {
            body.run();
        } finally {
            GlobalStateMgr.getCurrentState().setStatisticStorage(storage);
        }
    }

    // The conditions of the heavy query on the columns of the group, with a range outside of it that changes per run.
    static String predicate(int i) {
        return "l_returnflag = 'R' and l_linenumber not in (5) and l_suppkey in (0, 1, 2, 3) "
                + "and l_partkey not in (0, 1, 5) "
                + "and not ((l_partkey = 2 and l_quantity in (1, 25) and l_comment like '5058%ALF%') "
                + "or (l_partkey = 2 and l_tax = 0.04)) "
                + "and l_shipdate between '" + LocalDate.of(1995, 1, 1).plusDays(i % 300) + "' and '1997-12-31'";
    }

    static Map<String, IntFunction<String>> queries() {
        Map<String, IntFunction<String>> queries = new LinkedHashMap<>();
        queries.put("scan", i -> "select count(*) from hive0.tpch.lineitem where " + predicate(i));
        queries.put("join of 3", i -> "select o.o_orderdate, sum(l.l_extendedprice) from hive0.tpch.lineitem l "
                + "join hive0.tpch.orders o on l.l_orderkey = o.o_orderkey "
                + "join hive0.tpch.supplier s on l.l_suppkey = s.s_suppkey where " + predicate(i)
                + " group by o.o_orderdate");
        queries.put("CTE report", i -> "with t as (select l_orderkey, l_partkey, l_suppkey, l_extendedprice, "
                + "l_discount, l_shipdate from hive0.tpch.lineitem where " + predicate(i) + "), "
                + "o as (select o_orderkey, o_custkey from hive0.tpch.orders where o_orderstatus = 'F'), "
                + "by_customer as (select c.c_nationkey, sum(t.l_extendedprice) revenue from t "
                + "join o on t.l_orderkey = o.o_orderkey join hive0.tpch.customer c on o.o_custkey = c.c_custkey "
                + "group by c.c_nationkey), "
                + "by_supplier as (select s.s_nationkey, sum(t.l_extendedprice * (1 - t.l_discount)) revenue from t "
                + "join hive0.tpch.supplier s on t.l_suppkey = s.s_suppkey group by s.s_nationkey), "
                + "by_part as (select p.p_brand, count(*) cnt from t join hive0.tpch.part p "
                + "on t.l_partkey = p.p_partkey group by p.p_brand), "
                + "by_day as (select t.l_shipdate, count(*) cnt from t join o on t.l_orderkey = o.o_orderkey "
                + "group by t.l_shipdate) "
                + "select n.n_name, cast(sum(bc.revenue) as double) a, cast(sum(bs.revenue) as double) b "
                + "from by_customer bc join hive0.tpch.nation n on bc.c_nationkey = n.n_nationkey "
                + "join by_supplier bs on bs.s_nationkey = n.n_nationkey group by n.n_name "
                + "union all select p_brand, cast(cnt as double), 0 from by_part "
                + "union all select cast(l_shipdate as varchar), cast(cnt as double), 0 from by_day");
        return queries;
    }

    // [median, p90] in microseconds of the timed body.
    private static double[] measure(Body body) throws Exception {
        for (int i = 0; i < WARM_UP; i++) {
            body.run();
        }
        long[] times = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            body.run();
            times[i] = System.nanoTime() - start;
        }
        Arrays.sort(times);
        return new double[] {times[RUNS / 2] / 1000.0, times[(int) (RUNS * 0.9)] / 1000.0};
    }

    /**
     * Samples the stack of the planning thread about every millisecond and counts, per method of the planner, the
     * samples it is on the stack and on top of it.
     */
    private static final class Sampler implements AutoCloseable {
        private final Thread target = Thread.currentThread();
        private final Map<String, Integer> inclusive = new java.util.concurrent.ConcurrentHashMap<>();
        private final Map<String, Integer> self = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.atomic.AtomicInteger samples =
                new java.util.concurrent.atomic.AtomicInteger();
        private volatile boolean running = true;
        private final Thread thread;

        Sampler() {
            thread = new Thread(() -> {
                while (running) {
                    StackTraceElement[] stack = target.getStackTrace();
                    if (stack.length > 0) {
                        samples.incrementAndGet();
                        self.merge(frame(stack[0]), 1, Integer::sum);
                        Set<String> seen = new java.util.HashSet<>();
                        for (StackTraceElement element : stack) {
                            if (!element.getClassName().startsWith("com.starrocks.")
                                    || element.getClassName().contains("Test")) {
                                continue;
                            }
                            String frame = frame(element);
                            if (seen.add(frame)) {
                                inclusive.merge(frame, 1, Integer::sum);
                            }
                        }
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        private static String frame(StackTraceElement element) {
            String name = element.getClassName();
            return name.substring(name.lastIndexOf('.') + 1) + "." + element.getMethodName();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.join();
        }

        List<String> report(String title, int top) {
            List<String> lines = new ArrayList<>();
            int total = Math.max(1, samples.get());
            lines.add(title + ": " + total + " samples");
            for (Map<String, Integer> counts : List.of(inclusive, self)) {
                lines.add(counts == inclusive ? "  inclusive" : "  self");
                counts.entrySet().stream()
                        .sorted((a, b) -> b.getValue() - a.getValue()).limit(top)
                        .forEach(entry -> lines.add(String.format("    %5.1f%%  %s", 100.0 * entry.getValue() / total,
                                entry.getKey())));
            }
            return lines;
        }
    }

    /** The calls of one planning: scan derivations, head evaluations and conversions of a stored value. */
    private static final class Counts {
        final AtomicLong scans = new AtomicLong();
        final AtomicLong estimates = new AtomicLong();
        final AtomicLong filters = new AtomicLong();
        final AtomicLong completeHeads = new AtomicLong();
        final AtomicLong matches = new AtomicLong();
        final AtomicLong casts = new AtomicLong();

        Counts copy() {
            Counts copy = new Counts();
            copy.scans.set(scans.get());
            copy.estimates.set(estimates.get());
            copy.filters.set(filters.get());
            copy.completeHeads.set(completeHeads.get());
            copy.matches.set(matches.get());
            copy.casts.set(casts.get());
            return copy;
        }

        Counts minus(Counts other) {
            Counts result = copy();
            result.scans.addAndGet(-other.scans.get());
            result.estimates.addAndGet(-other.estimates.get());
            result.filters.addAndGet(-other.filters.get());
            result.completeHeads.addAndGet(-other.completeHeads.get());
            result.matches.addAndGet(-other.matches.get());
            result.casts.addAndGet(-other.casts.get());
            return result;
        }

        @Override
        public String toString() {
            return String.format("scan derivations %d, MCV estimates %d, filters %d, complete heads %d, "
                    + "component matches %d, casts %d", scans.get(), estimates.get(), filters.get(),
                    completeHeads.get(), matches.get(), casts.get());
        }
    }

    // The mock-ups count the calls until the end of the test.
    private static void countCalls(Counts counts) {
        new MockUp<StatisticsCalculator>() {
            @Mock
            public Void visitLogicalHiveScan(Invocation invocation, LogicalHiveScanOperator node,
                                             ExpressionContext context) {
                counts.scans.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            public Void visitPhysicalHiveScan(Invocation invocation, PhysicalHiveScanOperator node,
                                              ExpressionContext context) {
                counts.scans.incrementAndGet();
                return invocation.proceed();
            }
        };
        new MockUp<MultiColumnMcvEstimator>() {
            @Mock
            public Optional<MultiColumnMcvEstimator.Result> estimate(Invocation invocation,
                                                                     List<ScalarOperator> conjuncts,
                                                                     Statistics statistics) {
                counts.estimates.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            Optional<Boolean> matchesComponent(Invocation invocation, ScalarOperator conjunct,
                                               ColumnRefOperator column, String value,
                                               LikePatternEstimator.Work work) {
                counts.matches.incrementAndGet();
                return invocation.proceed();
            }
        };
        new MockUp<McvStatisticsPropagation>() {
            @Mock
            Statistics filter(Invocation invocation, ScalarOperator predicate, Statistics input, Statistics output) {
                counts.filters.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            java.util.OptionalDouble completeHeadRows(Invocation invocation, ScalarOperator predicate,
                                                      Statistics statistics) {
                counts.completeHeads.incrementAndGet();
                return invocation.proceed();
            }
        };
        new MockUp<StatisticsCastEvaluator>() {
            @Mock
            Optional<ConstantOperator> cast(Invocation invocation, ConstantOperator value, Type target) {
                counts.casts.incrementAndGet();
                return invocation.proceed();
            }
        };
    }

    @Test
    public void testCallCounts() throws Exception {
        ExternalMcvStatistics withMcv = new ExternalMcvStatistics(List.of(group()));
        Counts counts = new Counts();
        countCalls(counts);
        List<String> lines = new ArrayList<>();
        int run = 0;
        for (Map.Entry<String, IntFunction<String>> query : queries().entrySet()) {
            String sql = query.getValue().apply(run++);
            Counts before = counts.copy();
            withStatistics(withMcv, () -> getCostExplain(sql));
            lines.add(query.getKey() + ": " + counts.minus(before));
        }
        // The MCV list must take part in the estimate of the scan, or the numbers measure nothing.
        String sql = queries().get("scan").apply(0);
        String[] plans = new String[2];
        withStatistics(ExternalMcvStatistics.EMPTY, () -> plans[0] = getCostExplain(sql));
        withStatistics(withMcv, () -> plans[1] = getCostExplain(sql));
        lines.add("scan cardinality without MCV " + scanCardinality(plans[0]) + ", with MCV "
                + scanCardinality(plans[1]));
        String report = String.join("\n", lines);
        System.out.println("MCV planning calls\n" + report);
        write("mcv-planning-calls.txt", report);
        Assertions.assertTrue(counts.scans.get() > 0, report);
        Assertions.assertNotEquals(scanCardinality(plans[0]), scanCardinality(plans[1]), report);
    }

    private static long scanCardinality(String plan) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("cardinality: (\\d+)")
                .matcher(plan);
        Assertions.assertTrue(matcher.find(plan.indexOf("HdfsScanNode")), plan);
        return Long.parseLong(matcher.group(1));
    }

    private static void write(String file, String report) {
        try {
            Path target = Path.of("target");
            Files.createDirectories(target);
            Files.writeString(target.resolve(file), report + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            // The numbers are in the output of the test as well.
        }
    }

    @Test
    public void testPlanningCost() throws Exception {
        ExternalMcvStatistics withMcv = new ExternalMcvStatistics(List.of(group()));
        List<String> lines = new ArrayList<>();
        lines.add(String.format("%-12s %12s %12s %12s %12s %12s %12s", "query, us", "plain median", "plain p90",
                "mcv median", "mcv p90", "added median", "added p90"));
        int[] run = {0};
        double plainTotal = 0;
        double mcvTotal = 0;
        for (Map.Entry<String, IntFunction<String>> query : queries().entrySet()) {
            double[][] result = new double[2][];
            withStatistics(ExternalMcvStatistics.EMPTY, () -> result[0] = measure(() -> getCostExplain(
                    query.getValue().apply(run[0]++))));
            withStatistics(withMcv, () -> result[1] = measure(() -> getCostExplain(
                    query.getValue().apply(run[0]++))));
            plainTotal += result[0][0];
            mcvTotal += result[1][0];
            lines.add(String.format("%-12s %12.0f %12.0f %12.0f %12.0f %12.0f %12.0f", query.getKey(), result[0][0],
                    result[0][1], result[1][0], result[1][1], result[1][0] - result[0][0],
                    result[1][1] - result[0][1]));
        }
        // Where the planning of the report spends its time with the MCV list.
        IntFunction<String> report = queries().get("CTE report");
        withStatistics(withMcv, () -> {
            try (Sampler sampler = new Sampler()) {
                for (int i = 0; i < RUNS; i++) {
                    getCostExplain(report.apply(run[0]++));
                }
                lines.add("");
                lines.addAll(sampler.report("CTE report with MCV", 60));
            }
        });
        String text = String.join("\n", lines);
        System.out.println("MCV planning cost\n" + text);
        write("mcv-planning-cost.txt", text);
        // A generous bound: the MCV list may not make planning several times slower.
        Assertions.assertTrue(mcvTotal < 3 * plainTotal + 100_000, text);
    }
}

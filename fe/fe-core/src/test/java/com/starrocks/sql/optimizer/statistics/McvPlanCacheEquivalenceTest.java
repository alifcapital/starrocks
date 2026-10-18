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
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Queries on a table with an MCV list of six columns plan the same, with the same costs and row counts, whether the
 * MCV statistics attached to a scan and the estimate of the predicate of a scan are kept for the query or computed on
 * every derivation of the scan. The predicates are random conditions on the columns of the group, the queries scan
 * the table once, join it and read it in several copies of a CTE.
 */
public class McvPlanCacheEquivalenceTest extends ConnectorPlanTestBase {
    private static final String[] FLAGS = {"'A'", "'N'", "'R'", "'X'"};

    private static String value(Random random, String column) {
        switch (column) {
            case "l_returnflag":
                return FLAGS[random.nextInt(FLAGS.length)];
            case "l_linenumber":
                return Integer.toString(1 + random.nextInt(8));
            case "l_quantity":
                return (1 + random.nextInt(12)) + ".00";
            case "l_tax":
                return "0.0" + random.nextInt(9);
            default:
                return Integer.toString(random.nextInt(12));
        }
    }

    private static String condition(Random random) {
        String column = List.of("l_returnflag", "l_linenumber", "l_suppkey", "l_partkey", "l_quantity", "l_tax")
                .get(random.nextInt(6));
        switch (random.nextInt(8)) {
            case 0:
            case 1:
                return column + " = " + value(random, column);
            case 2:
                return column + " != " + value(random, column);
            case 3:
                return column + (random.nextBoolean() ? " < " : " >= ") + value(random, column);
            case 4:
                return column + (random.nextBoolean() ? " in (" : " not in (") + value(random, column) + ", "
                        + value(random, column) + ")";
            case 5:
                return column + (random.nextBoolean() ? " is null" : " is not null");
            case 6:
                return "(" + condition(random) + " or " + condition(random) + ")";
            default:
                return "not (" + condition(random) + " and " + condition(random) + ")";
        }
    }

    private static String predicate(Random random) {
        List<String> conditions = new ArrayList<>();
        for (int i = 1 + random.nextInt(5); i > 0; i--) {
            conditions.add(condition(random));
        }
        if (random.nextBoolean()) {
            conditions.add("l_shipdate >= '1995-0" + (1 + random.nextInt(9)) + "-01'");
        }
        return String.join(" and ", conditions);
    }

    private static List<String> queries() {
        Random random = new Random(20261014);
        List<String> queries = new ArrayList<>();
        McvPlanningCostTest.queries().values().forEach(query -> queries.add(query.apply(0)));
        for (int i = 0; i < 30; i++) {
            String where = predicate(random);
            switch (i % 3) {
                case 0:
                    queries.add("select count(*) from hive0.tpch.lineitem where " + where);
                    break;
                case 1:
                    queries.add("select s.s_name, count(*) from hive0.tpch.lineitem l join hive0.tpch.supplier s "
                            + "on l.l_suppkey = s.s_suppkey join hive0.tpch.orders o on l.l_orderkey = o.o_orderkey "
                            + "where " + where + " group by s.s_name");
                    break;
                default:
                    queries.add("with t as (select * from hive0.tpch.lineitem where " + where + ") "
                            + "select count(*) from t a join t b on a.l_orderkey = b.l_orderkey "
                            + "join hive0.tpch.part p on a.l_partkey = p.p_partkey "
                            + "union all select count(*) from t where l_linenumber = 1");
            }
        }
        return queries;
    }

    @Test
    public void testPlansAreTheSameWithoutTheKeptScanEstimates() throws Exception {
        ExternalMcvStatistics statistics = new ExternalMcvStatistics(List.of(McvPlanningCostTest.group()));
        List<String> queries = queries();
        AtomicBoolean bypass = new AtomicBoolean();
        AtomicLong calls = new AtomicLong();
        AtomicLong computed = new AtomicLong();
        AtomicLong attachCalls = new AtomicLong();
        AtomicLong attachComputed = new AtomicLong();
        new MockUp<ExpressionStatisticsCache>() {
            @Mock
            Statistics scanEstimate(Invocation invocation, ScalarOperator predicate, Statistics input,
                                    Supplier<Statistics> compute) {
                if (bypass.get()) {
                    // Every derivation estimates the predicate of the scan again.
                    return compute.get();
                }
                calls.incrementAndGet();
                Supplier<Statistics> counted = () -> {
                    computed.incrementAndGet();
                    return compute.get();
                };
                return invocation.proceed(predicate, input, counted);
            }

            @Mock
            Statistics mcvAttachment(Invocation invocation, Table table, ExternalMcvStatistics mcv,
                                     Map<ColumnRefOperator, Column> columns, Statistics input,
                                     Supplier<Statistics> compute) {
                if (bypass.get()) {
                    // Every derivation attaches the MCV statistics again.
                    return compute.get();
                }
                attachCalls.incrementAndGet();
                Supplier<Statistics> counted = () -> {
                    attachComputed.incrementAndGet();
                    return compute.get();
                };
                return invocation.proceed(table, mcv, columns, input, counted);
            }
        };
        List<String> kept = new ArrayList<>();
        McvPlanningCostTest.withStatistics(statistics, () -> {
            for (String query : queries) {
                kept.add(getCostExplain(query));
            }
        });
        // The plans read kept estimates and attachments, or the comparison proves nothing.
        Assertions.assertTrue(computed.get() > 0 && calls.get() > computed.get(), calls + " " + computed);
        Assertions.assertTrue(attachComputed.get() > 0 && attachCalls.get() > attachComputed.get(),
                attachCalls + " " + attachComputed);
        bypass.set(true);
        McvPlanningCostTest.withStatistics(statistics, () -> {
            for (int i = 0; i < queries.size(); i++) {
                Assertions.assertEquals(kept.get(i), getCostExplain(queries.get(i)), queries.get(i));
            }
        });
    }

    // The MCV statistics are attached once per scan of a query: every attach of one planning reads the table through
    // other column references, while the scans are derived many times.
    @Test
    public void testMcvStatisticsAreAttachedOncePerScan() throws Exception {
        ExternalMcvStatistics statistics = new ExternalMcvStatistics(List.of(McvPlanningCostTest.group()));
        AtomicLong derivations = new AtomicLong();
        List<Set<Integer>> attached = new ArrayList<>();
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            Statistics withExternalMcvStats(Invocation invocation, Table table, Statistics input,
                                            Map<ColumnRefOperator, Column> columns, OptimizerContext context) {
                derivations.incrementAndGet();
                return invocation.proceed();
            }

            @Mock
            Statistics attachExternalMcvStats(Invocation invocation, Statistics input, ExternalMcvStatistics mcv,
                                              Map<ColumnRefOperator, Column> columns) {
                Set<Integer> ids = new TreeSet<>();
                columns.keySet().forEach(column -> ids.add(column.getId()));
                attached.add(ids);
                return invocation.proceed();
            }
        };
        for (IntFunction<String> query : McvPlanningCostTest.queries().values()) {
            derivations.set(0);
            attached.clear();
            McvPlanningCostTest.withStatistics(statistics, () -> getCostExplain(query.apply(0)));
            Assertions.assertFalse(attached.isEmpty());
            Assertions.assertEquals(new HashSet<>(attached).size(), attached.size(), attached.toString());
            Assertions.assertTrue(derivations.get() > attached.size(), derivations + " " + attached);
        }
    }
}

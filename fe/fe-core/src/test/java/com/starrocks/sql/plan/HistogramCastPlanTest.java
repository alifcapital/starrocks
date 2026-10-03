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
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class HistogramCastPlanTest extends PlanTestBase {
    @Test
    public void nativeScanCastsKeepUsableEstimatesThroughPlanning() throws Exception {
        starRocksAssert.withTable("CREATE TABLE m8_cast (id int, s varchar(40), dt varchar(40)) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')");
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        StatisticStorage old = state.getStatisticStorage();
        OlapTable table = (OlapTable) starRocksAssert.getTable("test", "m8_cast");
        setTableStatistics(table, 10_000_000);
        table.getAllPartitions().forEach(p -> UtFrameUtils.setPartitionVersion(p, 2));
        Histogram strings = load("10", "900000");
        Histogram dates = load("2021-01-01", "2025-01-01");
        state.setStatisticStorage(new StatisticStorage() {
            @Override
            public ColumnStatistic getColumnStatistic(Table ignored, String column) {
                return ColumnStatistic.builder().setMinValue(Double.NEGATIVE_INFINITY)
                        .setMaxValue(Double.POSITIVE_INFINITY).setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(1_000_000).build();
            }

            @Override
            public List<ColumnStatistic> getColumnStatistics(Table t, List<String> columns) {
                return columns.stream().map(c -> getColumnStatistic(t, c)).toList();
            }

            @Override
            public void addColumnStatistic(Table t, String c, ColumnStatistic statistic) {
            }

            @Override
            public Map<String, Histogram> getHistogramStatistics(Table t, List<String> columns) {
                return Map.of("s", strings, "dt", dates);
            }
        });
        try {
            for (String op : List.of("<", "<=", ">", ">=")) {
                for (String predicate : List.of("cast(s as bigint) " + op + " 500000",
                        "cast(dt as date) " + op + " cast('2024-01-01' as date)",
                        "cast(dt as datetime) " + op + " cast('2024-01-01' as datetime)")) {
                    String sql = "select id from m8_cast where " + predicate;
                    ExecPlan plan = getExecPlan(sql);
                    Assertions.assertEquals(5_000_000, plan.getScanNodes().get(0).getCardinality(),
                            sql + "\n" + plan.getExplainString(com.starrocks.thrift.TExplainLevel.COSTS));
                }
            }
            for (String sql : List.of(
                    "select id from (select id, cast(s as bigint) n from m8_cast) t where n < 500000",
                    "select id from m8_cast where cast(cast(s as bigint) as largeint) < 500000")) {
                Assertions.assertEquals(5_000_000, getExecPlan(sql).getScanNodes().get(0).getCardinality(), sql);
            }
            Assertions.assertEquals(750, getExecPlan("select id from m8_cast where s = '10'")
                    .getScanNodes().get(0).getCardinality());
            Assertions.assertEquals(1500, getExecPlan("select id from m8_cast where s in ('10', '900000')")
                    .getScanNodes().get(0).getCardinality());
        } finally {
            state.setStatisticStorage(old);
            starRocksAssert.dropTable("m8_cast");
        }
    }

    private static Histogram load(String first, String second) throws Exception {
        String json = "{\"buckets\":[[\"Infinity\",\"Infinity\",\"9998500\",\"0\"]],\"mcv\":[[\""
                + first + "\",\"750\"],[\"" + second + "\",\"750\"]]}";
        return new Histogram(HistogramUtils.convertBuckets(json, VarcharType.VARCHAR), HistogramUtils.convertMCV(json));
    }
}

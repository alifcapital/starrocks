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

package com.starrocks.statistic;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ExternalHistogramStatisticsCollectJobTest {
    @ParameterizedTest
    @EnumSource(value = StatsConstants.ScheduleType.class, names = {"ONCE", "SCHEDULE"})
    void ordinaryHistogramJobsHonorSampleRatio(StatsConstants.ScheduleType schedule) throws Exception {
        var state = Mockito.mock(GlobalStateMgr.class, Mockito.RETURNS_DEEP_STUBS);
        try (var global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            var table = new Table(1, "events", Table.TableType.ICEBERG,
                    List.of(new Column("amount", IntegerType.BIGINT)));
            var context = Mockito.mock(ConnectContext.class);
            Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
            var status = Mockito.mock(AnalyzeStatus.class);
            for (String ratio : List.of("0.01", "1.0")) {
                var job = Mockito.spy(new ExternalHistogramStatisticsCollectJob("iceberg", new Database(1, "db"), table,
                        List.of("amount"), List.of(IntegerType.BIGINT), StatsConstants.AnalyzeType.HISTOGRAM,
                        schedule, Map.of(StatsConstants.HISTOGRAM_SAMPLE_RATIO, ratio,
                                StatsConstants.HISTOGRAM_BUCKET_NUM, "64", StatsConstants.HISTOGRAM_MCV_SIZE, "20")));
                var statements = new ArrayList<String>();
                Mockito.doAnswer(call -> { statements.add(call.getArgument(0)); return null; })
                        .when(job).collectStatisticSync(Mockito.anyString(), Mockito.eq(context), Mockito.eq(status));
                var queries = new ArrayList<String>();
                try (var executors = Mockito.mockConstruction(StatisticExecutor.class, (executor, ignored) -> {
                    Mockito.when(executor.queryMCV(Mockito.eq(context), Mockito.anyString())).thenAnswer(call -> {
                        queries.add(call.getArgument(1));
                        return List.of(new TStatisticData().setColumnName("7").setHistogram("10"));
                    });
                    Mockito.when(executor.dropExternalHistogramRawColumn(Mockito.any(), Mockito.anyString(),
                            Mockito.anyString())).thenReturn(true);
                })) {
                    job.collect(context, status);
                    Assertions.assertEquals(1, queries.size());
                    Assertions.assertTrue(queries.get(0).contains("group by `amount`"), queries.toString());
                    Assertions.assertTrue(queries.get(0).contains("order by column_value desc limit 20"));
                    Assertions.assertFalse(queries.get(0).contains("rand()"));
                    Assertions.assertEquals(1, statements.size());
                    String sql = statements.get(0);
                    Assertions.assertTrue(sql.contains("histogram(`column_key`, cast(64 as int), cast(" + ratio
                            + " as double))"), sql);
                    Assertions.assertTrue(sql.contains("rand() <= " + ratio), sql);
                    Assertions.assertTrue(sql.contains("not in (7)"), sql);
                    Assertions.assertTrue(sql.contains("LIMIT " + Config.histogram_max_sample_row_count), sql);
                    Assertions.assertFalse(sql.contains("histogram_by_bounds"));
                    Assertions.assertFalse(sql.contains("ds_"));
                    Mockito.verify(executors.constructed().get(0)).dropExternalHistogramRawColumn(
                            context, table.getUUID(), "amount");
                }
            }
            Mockito.verify(status, Mockito.times(2)).setProgress(100);
        }
    }
}

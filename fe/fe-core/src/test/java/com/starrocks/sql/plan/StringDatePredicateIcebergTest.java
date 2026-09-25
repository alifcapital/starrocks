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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.connector.PartitionCastPredicatePruner;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Utils;
import org.apache.iceberg.expressions.Expression;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StringDatePredicateIcebergTest extends ConnectorPlanTestBase {
    private TimeZone savedJvmZone;
    private SessionVariable saved;

    @BeforeEach
    public void saveVariables() {
        savedJvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        saved = connectContext.getSessionVariable();
        connectContext.setSessionVariable((SessionVariable) saved.clone());
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void restoreVariables() {
        TimeZone.setDefault(savedJvmZone);
        connectContext.setSessionVariable(saved);
    }

    private Expression storageFilter(String sql) throws Exception {
        IcebergScanNode scan = (IcebergScanNode) getExecPlan(sql).getScanNodes().get(0);
        // Match IcebergMetadata's split before conversion, including its existing CAST guard.
        var residual = PartitionCastPredicatePruner.split(
                Utils.extractConjuncts(scan.getIcebergJobPlanningPredicate()), Set.of("date"));
        return new ScalarOperatorToIcebergExpr().convert(residual.pushable,
                new ScalarOperatorToIcebergExpr.IcebergContext(scan.getIcebergTable().getNativeTable().schema().asStruct()));
    }

    @ParameterizedTest
    @EnumSource(StringDateFormat.class)
    public void testOnlyDeclaredStringBoundsReachIceberg(StringDateFormat format) throws Exception {
        var variables = connectContext.getSessionVariable();
        variables.setStringDatePredicateFormat(format.getSqlFormat());
        String sql = "select id from iceberg0.partitioned_db.t1 "
                + "where date_format(cast(`date` as date), '%Y%m') = '202403'";
        variables.setEnableStringDatePredicatePushdown(false);
        Expression off = storageFilter(sql);
        assertEquals(Expression.Operation.TRUE, off.op());
        variables.setEnableStringDatePredicatePushdown(true);
        Expression on = storageFilter(sql);
        assertNotEquals(Expression.Operation.TRUE, on.op());
        assertContains(on.toString(), format.format(LocalDateTime.of(2024, 3, 1, 0, 0)),
                format.format(LocalDateTime.of(2024, 4, 1, 0, 0)));
        String plan = getFragmentPlan(sql);
        assertContains(plan, "IcebergScanNode", "CAST(");
        String output = System.getProperty("string.date.plan.dir");
        if (output != null) {
            Path directory = Path.of(output);
            Files.createDirectories(directory);
            Files.writeString(directory.resolve("iceberg-" + format.name() + ".txt"),
                    sql + "\nOFF: " + off + "\nON: " + on + "\n" + plan);
        }
    }

    @Test
    public void testUtcMicrosecondsAndNamedZoneBoundsReachIceberg() throws Exception {
        var variables = connectContext.getSessionVariable();
        variables.setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s.%fZ");
        variables.setEnableStringDatePredicatePushdown(true);
        String sql = "select id from iceberg0.partitioned_db.t1 where "
                + "convert_tz(cast(`date` as datetime), 'UTC', 'Asia/Dushanbe') >= '2026-09-25 00:00:00'";
        assertContains(storageFilter(sql).toString(), "2026-09-24T19:00:00.000000Z");
    }

    @Test
    public void testNullableParsedDateCannotBecomeStringNullCheck() throws Exception {
        connectContext.getSessionVariable().setEnableStringDatePredicatePushdown(false);
        assertEquals(Expression.Operation.TRUE, storageFilter(
                "select id from iceberg0.partitioned_db.t1 where cast(`date` as date) is null").op());
    }
}

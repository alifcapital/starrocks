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

import com.starrocks.common.FeConstants;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Files;
import java.nio.file.Path;

public class MonotonicJoinTypeMatrixTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        // A VARCHAR source of dates needs the format its Iceberg table declares.
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
        for (String type : new String[] {"date", "int", "varchar"}) {
            String sqlType = type.equals("varchar") ? "varchar(20)" : type;
            if (!type.equals("varchar")) {
                starRocksAssert.withTable("CREATE TABLE matrix_source_" + type
                        + " (id bigint, k " + sqlType + ") DUPLICATE KEY(id) "
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
            }
            StringBuilder partitions = new StringBuilder();
            for (int month = 1; month <= 6; month++) {
                if (month > 1) {
                    partitions.append(",");
                }
                String value = type.equals("date") ? String.format("2024-%02d-01", month)
                        : String.format("2024%02d", month);
                String next = type.equals("date") ? String.format("2024-%02d-01", month + 1)
                        : String.format("2024%02d", month + 1);
                partitions.append("PARTITION p").append(month);
                if (type.equals("varchar")) {
                    partitions.append(" VALUES IN ('").append(value).append("')");
                } else {
                    partitions.append(" VALUES [('").append(value).append("'), ('").append(next).append("'))");
                }
            }
            starRocksAssert.withTable("CREATE TABLE matrix_target_" + type
                    + " (id bigint, k " + sqlType + ") DUPLICATE KEY(id,k) PARTITION BY "
                    + (type.equals("varchar") ? "LIST" : "RANGE") + "(k) (" + partitions + ") "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES (\"replication_num\"=\"1\")");
        }
    }

    @ParameterizedTest(name = "source={0}, target={1}, comparison={2}")
    @CsvSource({
            "date,date,decimal", "date,int,decimal", "date,varchar,decimal",
            "int,date,decimal", "int,int,decimal", "int,varchar,decimal",
            "varchar,date,decimal", "varchar,int,decimal", "varchar,varchar,decimal",
            "date,date,varchar", "date,int,varchar", "date,varchar,varchar",
            "int,date,varchar", "int,int,varchar", "int,varchar,varchar",
            "varchar,date,varchar", "varchar,int,varchar", "varchar,varchar,varchar"
    })
    public void testDayToMonthJoin(String source, String target, String comparison) throws Exception {
        var variables = connectContext.getSessionVariable();
        String oldComparison = variables.getCboEqBaseType();
        boolean oldMove = variables.isEnableMonotonicPredicateMoveAround();
        boolean oldRewrite = variables.isEnableMonotonicPredicateRewrite();
        boolean oldFormat = variables.isEnableStringDateFormatProperty();
        String key = source.equals("varchar") ? "e.s" : "e.k";
        String sourceTable = source.equals("varchar") ? "iceberg0.string_date_db.strdate_compact"
                : "matrix_source_" + source;
        String date = source.equals("date") ? key : "cast(" + key + " as date)";
        String expression = target.equals("date") ? "date_trunc('month', " + date + ")"
                : "date_format(" + date + ", '%Y%m')";
        String range = source.equals("date") ? "'2024-03-05' AND '2024-04-10'"
                : source.equals("int") ? "20240305 AND 20240410" : "'20240305' AND '20240410'";
        String sql = "SELECT f.id FROM matrix_target_" + target + " f JOIN " + sourceTable
                + " e ON f.id=e.id AND f.k=" + expression + " WHERE " + key + " BETWEEN " + range;
        String name = source + "-" + target + "-" + comparison;
        try {
            variables.setCboEqBaseType(comparison);
            variables.setEnableMonotonicPredicateRewrite(true);
            variables.setEnableMonotonicPredicateMoveAround(false);
            String off = getFragmentPlan(sql);
            assertContains(off, "partitions=6/6");
            variables.setEnableMonotonicPredicateMoveAround(true);
            String on = getFragmentPlan(sql);
            savePlans(name, sql, off, on);
            // String comparison over an INT range keeps the partition touching the lower bound.
            int expected = target.equals("int") && comparison.equals("varchar") ? 3 : 2;
            assertContains(on, "partitions=" + expected + "/6");
            if (source.equals("varchar")) {
                variables.setEnableStringDateFormatProperty(false);
                assertContains(getFragmentPlan(sql), "partitions=6/6");
            }
        } finally {
            variables.setCboEqBaseType(oldComparison);
            variables.setEnableMonotonicPredicateMoveAround(oldMove);
            variables.setEnableMonotonicPredicateRewrite(oldRewrite);
            variables.setEnableStringDateFormatProperty(oldFormat);
        }
    }

    private static void savePlans(String name, String sql, String off, String on) throws Exception {
        String output = System.getProperty("monotonic.plan.dir");
        if (output != null) {
            Path directory = Path.of(output);
            Files.createDirectories(directory);
            Files.writeString(directory.resolve(name + ".sql"), sql);
            Files.writeString(directory.resolve(name + ".off.plan"), off);
            Files.writeString(directory.resolve(name + ".on.plan"), on);
        }
    }
}

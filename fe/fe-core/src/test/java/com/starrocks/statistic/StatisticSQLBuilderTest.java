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

import com.google.common.collect.ImmutableList;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// external_column_statistics / external_histogram_statistics store table_uuid hashed
// (StatisticUtils.hashTableUuidForPkStorage) to stay within BE's primary_key_limit_size.
// These tests confirm every query/delete builder that filters by table_uuid matches both
// the hashed and the raw value, so historical rows written before hashing was introduced
// stay visible until they naturally age out.
class StatisticSQLBuilderTest {

    private static final String TABLE_UUID =
            "iceberg.udp_abx_etl_db1_datawarehouse.tenant.account_buying_group.d6cfa1ed-0000-0000-0000-000000000000";

    @Test
    void dropMcvGroupUsesCollectionKeyAndPreservesOtherGroups() {
        String all = StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID);
        String selected = StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID, List.of("a", "b"));
        Assertions.assertEquals(all + " and column_ids = '"
                + ExternalMcvStatisticsCollectJob.buildColumnIds(List.of("a", "b")) + "'", selected);
        Assertions.assertEquals(selected,
                StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID, List.of("b", "a")));
        Assertions.assertNotEquals(selected,
                StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID, List.of("a")));
        Assertions.assertNotEquals(selected,
                StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID, List.of("a", "b", "c")));
        Assertions.assertTrue(selected.contains(StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID)));
        Assertions.assertTrue(selected.contains(TABLE_UUID));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StatisticSQLBuilder.buildDropExternalMcvStatisticsSQL(TABLE_UUID, List.of()));
    }

    @Test
    void partitionRequestsPreserveMissingPairsAndGroupEqualColumnSets() {
        Map<String, Set<String>> request = new LinkedHashMap<>();
        request.put("p=1", Set.of("a"));
        request.put("p=2", Set.of("a"));
        request.put("p=3", Set.of("b"));
        String sql = StatisticSQLBuilder.buildQueryExternalPartitionStatisticsSQL(TABLE_UUID, request, false);
        Assertions.assertTrue(sql.contains("(partition_name IN ('p=1', 'p=2') AND column_name IN ('a')) OR "
                + "(partition_name IN ('p=3') AND column_name IN ('b'))"), sql);
        Assertions.assertEquals(1, StringUtils.countMatches(sql, "FROM _statistics_.external_column_statistics"));
        Assertions.assertFalse(sql.contains("row_number()"));
        String escaped = StatisticSQLBuilder.buildQueryExternalPartitionStatisticsSQL(TABLE_UUID,
                Map.of("p='x", Set.of("c'1")), false);
        Assertions.assertTrue(escaped.contains("'p=''x'"), escaped);
        Assertions.assertTrue(escaped.contains("'c''1'"), escaped);
        Assertions.assertTrue(StatisticSQLBuilder.buildQueryExternalPartitionStatisticsSQL(TABLE_UUID, Map.of(), false)
                .contains("AND (FALSE)"));
    }

    @Test
    void unpartitionedRequestsUseCanonicalRowsWithoutRanking() {
        String sql = StatisticSQLBuilder.buildQueryExternalPartitionStatisticsSQL(TABLE_UUID,
                Map.of("", Set.of("a")), true);
        Assertions.assertTrue(sql.contains("as INT), '', column_name, row_count, data_size, hll_serialize(ndv)"), sql);
        Assertions.assertFalse(sql.contains("row_number()"), sql);
        Assertions.assertTrue(sql.contains("column_name IN ('a')"), sql);
        Assertions.assertFalse(sql.contains("partition_name"), sql);
        Assertions.assertFalse(sql.contains("rn = 1"), sql);
    }

    @Test
    void mixedTypePredicatesEscapeColumnNames() {
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(TABLE_UUID,
                List.of("a\"b", "c\\d"), List.of(IntegerType.BIGINT, VarcharType.VARCHAR));
        Assertions.assertTrue(sql.contains("column_name in (\"a\\\"b\")"), sql);
        Assertions.assertTrue(sql.contains("column_name in (\"c\\\\d\")"), sql);
    }

    @Test
    void buildQueryExternalFullStatisticsSQLUsesCanonicalUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(sql.contains("table_uuid = '" + hashed + "'"), sql);
        Assertions.assertFalse(sql.contains(TABLE_UUID), sql);
    }

    @Test
    void buildDropExternalStatSQLUsesCanonicalUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL(TABLE_UUID);
        Assertions.assertTrue(sql.contains("table_uuid = '" + hashed + "'"),
                "delete predicate must match the canonical table_uuid: " + sql);
    }

    @Test
    void buildQueryConnectorHistogramStatisticsSQLMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        List<String> columnNames = ImmutableList.of("col1");
        String sql = StatisticSQLBuilder.buildQueryConnectorHistogramStatisticsSQL(TABLE_UUID, columnNames);
        Assertions.assertTrue(sql.contains("table_uuid in ('" + hashed + "', '" + TABLE_UUID + "')"),
                "query predicate must match both hashed and raw table_uuid: " + sql);
    }

    @Test
    void buildDropExternalHistogramSQLMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQL(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains("table_uuid in ('" + hashed + "', '" + TABLE_UUID + "')"),
                "delete predicate must match both hashed and raw table_uuid: " + sql);
    }

    @Test
    void buildQueryExternalFullStatisticsSQLGroupsByColumnNameOnly() {
        // Regression test: grouping by table_uuid too would split a table's rows into two groups
        // whenever both the hashed and raw representations are present, silently dropping one
        // group's aggregated data downstream (see the collect-vs-read consistency discussion).
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(sql.endsWith("GROUP BY column_name"), "must group by column_name only: " + sql);
        Assertions.assertFalse(sql.contains("GROUP BY table_uuid"), "must not group by table_uuid: " + sql);
    }

    @Test
    void buildDropExternalHistogramSQLForRawUuidOnlyMatchesRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQLForRawUuid(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains("table_uuid = '" + TABLE_UUID + "'"),
                "cleanup delete must target only the raw uuid: " + sql);
        Assertions.assertFalse(sql.contains(hashed),
                "cleanup delete must never also match the hashed uuid (it holds the fresh data): " + sql);
    }

    @Test
    void canonicalBasicStatisticsDoesNotRankDuplicateRepresentations() {
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertFalse(sql.contains("row_number()"), sql);
        Assertions.assertTrue(sql.endsWith("GROUP BY column_name"), sql);
    }

    @Test
    void buildQueryConnectorHistogramStatisticsSQLDedupsByLatestUpdateTime() {
        String sql = StatisticSQLBuilder.buildQueryConnectorHistogramStatisticsSQL(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains(
                "row_number() over ( partition by column_name order by update_time desc) as rn"), sql);
        Assertions.assertTrue(sql.contains(") dedup_t WHERE rn = 1"), sql);
    }

    @Test
    void tableUUIDPredicatesEscapeQuotesAndBackslashes() {
        // table_uuid is derived from catalog/db/table names (Table.getUUID()), so it must be
        // escaped like any other untrusted value before being embedded into a SQL string literal.
        // StarRocks decodes backslash escapes inside string literals, so doubling quotes alone
        // (the old StringEscapeUtils.escapeSql behavior) is not sufficient - see SqlUtils.escapeSqlString.
        String doubleQuoteTrickyUUID = "iceberg.db.o\"brien\\table.uuid"; // contains " and \
        String doubleQuoted = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                doubleQuoteTrickyUUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(doubleQuoted.contains(StatisticUtils.hashTableUuidForPkStorage(doubleQuoteTrickyUUID)));

        String singleQuoteTrickyUUID = "iceberg.db.o'brien\\table.uuid"; // contains ' and \
        String singleQuoted = StatisticSQLBuilder.buildDropExternalHistogramSQL(singleQuoteTrickyUUID, List.of("c"));
        Assertions.assertTrue(singleQuoted.contains("iceberg.db.o''brien\\\\table.uuid'"), singleQuoted);
    }

    @Test
    void nameBasedDeletesEscapeNames() {
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL("ice'berg", "d'b", "o'brien\\table");
        Assertions.assertTrue(sql.contains("CATALOG_NAME = 'ice''berg'"), sql);
        Assertions.assertTrue(sql.contains("DB_NAME = 'd''b'"), sql);
        Assertions.assertTrue(sql.contains("TABLE_NAME = 'o''brien\\\\table'"), sql);

        String histogramSql = StatisticSQLBuilder.buildDropExternalHistogramSQL("ice'berg", "d'b", "o'brien",
                ImmutableList.of("c'1"));
        Assertions.assertTrue(histogramSql.contains("catalog_name = 'ice''berg'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("db_name = 'd''b'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("table_name = 'o''brien'"), histogramSql);
        Assertions.assertTrue(histogramSql.contains("column_name in ('c''1')"), histogramSql);
    }
    @Test
    void partitionBlocksKeepExactMembershipLatestRowsAndTypedMinMax() {
        var a = com.starrocks.sql.optimizer.statistics.ExternalStatisticsCacheKey.block(
                TABLE_UUID, "a", List.of("p='1", "p=3"));
        var b = com.starrocks.sql.optimizer.statistics.ExternalStatisticsCacheKey.block(
                TABLE_UUID, "b", List.of("p=2"));
        String sql = StatisticSQLBuilder.buildQueryExternalPartitionBlocksSQL(TABLE_UUID, List.of(a, b),
                Map.of("a", IntegerType.LARGEINT, "b", com.starrocks.type.DateType.DATETIME));
        Assertions.assertTrue(sql.contains("'p=''1', 'p=3'"), sql);
        Assertions.assertTrue(sql.contains("column_name IN ('b') AND partition_name IN ('p=2')"), sql);
        Assertions.assertFalse(sql.contains("row_number()"), sql);
        Assertions.assertTrue(sql.contains("min(cast(nullif(min, '') as " + IntegerType.LARGEINT.toSql() + "))"), sql);
        Assertions.assertTrue(sql.toLowerCase(java.util.Locale.ROOT).contains("as datetime"), sql);
        Assertions.assertTrue(sql.contains("json_array(partition_name, row_count)"), sql);
        Assertions.assertEquals(2, StringUtils.countMatches(sql, "hll_serialize(hll_union(ndv))"));
        Assertions.assertFalse(sql.contains("group_concat"), "coverage must not be truncated by group_concat_max_len");
        Assertions.assertEquals(4, StringUtils.countMatches(sql, "coalesce("),
                "All-NULL numeric/date blocks must emit empty bounds on the external statistics wire");
    }

}

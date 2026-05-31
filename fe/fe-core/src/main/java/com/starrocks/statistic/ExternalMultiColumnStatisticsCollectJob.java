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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.COLUMN_ID_SEPARATOR;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME;

// Collects multi-column combined NDV statistics for external (connector) tables and persists them to the
// _statistics_.external_multi_column_statistics table. Mirrors MultiColumnHyperStatisticsCollectJob (combined NDV via
// ndv(murmur_hash3_32(...))) but runs the query against the connector table through a 3-part name and stores rows keyed
// by table UUID, the way ExternalFullStatisticsCollectJob does for single-column external stats.
public class ExternalMultiColumnStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalMultiColumnStatisticsCollectJob.class);

    private final String catalogName;
    private final List<String> sqlBuffer = Lists.newArrayList();
    private final List<List<Expr>> rowsBuffer = Lists.newArrayList();

    public ExternalMultiColumnStatisticsCollectJob(String catalogName, Database db, Table table,
                                                   List<String> columnNames, List<Type> columnTypes,
                                                   StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                                   Map<String, String> properties, List<StatisticsType> statisticsTypes,
                                                   List<List<String>> columnGroups) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties, statisticsTypes, columnGroups);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalMultiColumn";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long finished = 0;
        long total = Math.max(1, columnGroups.size());
        for (List<String> columnGroup : columnGroups) {
            String sql = buildCollectSQL(columnGroup);
            collectStatisticSync(sql, context, analyzeStatus);
            finished++;
            analyzeStatus.setProgress(finished * 100 / total);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
        }
        flushInsertStatisticsData(context);
    }

    // SELECT cast($version as INT), '<col ids>', cast(ndv(murmur_hash3_32(coalesce(c1, ''), ...)) as BIGINT)
    // FROM `catalog`.`db`.`table`
    // The combined NDV is computed over the whole table (no partition split): cross-column correlation is a
    // table-level property. The result is serialized by BE into TStatisticData (columnName / countDistinct) exactly
    // like the native multi-column path, so no BE change is needed.
    private String buildCollectSQL(List<String> columnGroup) {
        List<String> coalesces = columnGroup.stream()
                .map(col -> "coalesce(" + StatisticUtils.quoting(table, col) + ", '')")
                .collect(Collectors.toList());
        String ndvFunction = "ndv(murmur_hash3_32(" + String.join(", ", coalesces) + "))";
        String columnIds = String.join(COLUMN_ID_SEPARATOR, columnGroup);
        return "SELECT cast(" + StatsConstants.STATISTIC_MULTI_COLUMN_VERSION + " as INT)"
                + ", '" + StringEscapeUtils.escapeSql(columnIds) + "'"
                + ", cast(" + ndvFunction + " as BIGINT)"
                + " FROM `" + catalogName + "`.`" + db.getOriginName() + "`.`" + table.getName() + "`";
    }

    @Override
    public void collectStatisticSync(String sql, ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        calculateAndSetRemainingTimeout(context, analyzeStatus);
        LOG.debug("external multi-column statistics collect sql : {}", sql);

        StatisticExecutor executor = new StatisticExecutor();
        setDefaultSessionVariable(context);
        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);

        for (TStatisticData data : dataList) {
            // For external tables column_ids holds the column names joined by '#' (external columns have no stable
            // numeric unique id), so column_names is the same list joined by ','.
            String columnIds = data.getColumnName();
            String columnNames = columnIds.replace(COLUMN_ID_SEPARATOR, ",");

            List<String> params = Lists.newArrayList();
            params.add("'" + table.getUUID() + "'");
            params.add("'" + StringEscapeUtils.escapeSql(columnIds) + "'");
            params.add("'" + catalogName + "'");
            params.add("'" + db.getOriginName() + "'");
            params.add("'" + table.getName() + "'");
            params.add("'" + StringEscapeUtils.escapeSql(columnNames) + "'");
            params.add(String.valueOf(data.getCountDistinct()));
            params.add("now()");

            List<Expr> row = Lists.newArrayList();
            row.add(new StringLiteral(table.getUUID()));
            row.add(new StringLiteral(columnIds));
            row.add(new StringLiteral(catalogName));
            row.add(new StringLiteral(db.getOriginName()));
            row.add(new StringLiteral(table.getName()));
            row.add(new StringLiteral(columnNames));
            row.add(new IntLiteral(data.getCountDistinct(), IntegerType.BIGINT));
            row.add(nowFn());

            rowsBuffer.add(row);
            sqlBuffer.add("(" + String.join(", ", params) + ")");
        }
    }

    private void flushInsertStatisticsData(ConnectContext context) throws Exception {
        if (rowsBuffer.isEmpty()) {
            return;
        }

        int count = 0;
        int maxRetryTimes = 5;
        StatementBase insertStmt = createInsertStmt();
        do {
            LOG.debug("external multi-column statistics insert sql size: {}", rowsBuffer.size());
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, insertStmt);
            context.setExecutor(executor);
            context.setQueryId(UUIDUtil.genUUID());
            context.setStartTime();
            executor.execute();

            if (context.getState().getStateType() == QueryState.MysqlStateType.ERR) {
                LOG.warn("external multi-column statistics collect fail | {} | Error Message [{}]",
                        DebugUtil.printId(context.getQueryId()), context.getState().getErrorMessage());
                if (org.apache.commons.lang3.StringUtils.contains(context.getState().getErrorMessage(),
                        "Too many versions")) {
                    Thread.sleep(Config.statistic_collect_too_many_version_sleep);
                    count++;
                } else {
                    throw new DdlException(context.getState().getErrorMessage());
                }
            } else {
                sqlBuffer.clear();
                rowsBuffer.clear();
                return;
            }
        } while (count < maxRetryTimes);

        throw new DdlException(context.getState().getErrorMessage());
    }

    private StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME)
                .stream().map(ColumnDef::getName).collect(Collectors.toList());

        String sql = "INSERT INTO " + EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME + "("
                + String.join(", ", targetColumnNames) + ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList("_statistics_", EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME)),
                null, NodePosition.ZERO);
        InsertStmt insert = new InsertStmt(tableRef, qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }
}

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.StatisticsType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Journaled record of one MCV statistics collection on an external table: which column
 * group was collected, when and with what parameters. The statistics themselves live in
 * _statistics_.external_mcv_statistics.
 */
public class ExternalMcvStatsMeta implements Writable {
    @SerializedName("catalogName")
    private String catalogName;

    @SerializedName("dbName")
    private String dbName;

    @SerializedName("tableName")
    private String tableName;

    @SerializedName("columnNames")
    private List<String> columnNames;

    @SerializedName("analyzeType")
    private StatsConstants.AnalyzeType analyzeType;

    @SerializedName("statisticsTypes")
    private List<StatisticsType> statisticsTypes;

    @SerializedName("updateTime")
    private LocalDateTime updateTime;

    @SerializedName("properties")
    private Map<String, String> properties;

    // The table UUID resolved on the leader at analyze time. It keys the statistics cache, so followers
    // can invalidate their cache during journal replay without resolving external table metadata.
    @SerializedName("tableUUID")
    private String tableUUID;

    public ExternalMcvStatsMeta(String catalogName, String dbName, String tableName,
                                        List<String> columnNames, StatsConstants.AnalyzeType analyzeType,
                                        List<StatisticsType> statisticsTypes, LocalDateTime updateTime,
                                        Map<String, String> properties) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.analyzeType = analyzeType;
        this.statisticsTypes = statisticsTypes;
        this.updateTime = updateTime;
        this.properties = properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public StatsConstants.AnalyzeType getAnalyzeType() {
        return analyzeType;
    }

    public List<StatisticsType> getStatisticsTypes() {
        return statisticsTypes;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public void setTableUUID(String tableUUID) {
        this.tableUUID = tableUUID;
    }
}

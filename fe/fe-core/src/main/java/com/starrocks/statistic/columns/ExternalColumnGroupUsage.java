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

package com.starrocks.statistic.columns;

import com.google.gson.Gson;
import com.starrocks.catalog.Table;
import com.starrocks.statistic.StatisticUtils;

import java.time.LocalDateTime;
import java.util.List;

/** An observed column set, not an assertion that its columns are statistically correlated. */
public record ExternalColumnGroupUsage(String tableUuid, String catalogName, String dbName, String tableName,
                                       List<String> columns, ColumnUsage.UseCase useCase, LocalDateTime lastUsed) {
    private static final Gson GSON = new Gson();

    public ExternalColumnGroupUsage {
        columns = columns.stream().distinct().sorted().toList();
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("A usage group needs at least one column");
        }
    }

    public static ExternalColumnGroupUsage of(Table table, List<String> columns, ColumnUsage.UseCase useCase,
                                              LocalDateTime now) {
        return new ExternalColumnGroupUsage(StatisticUtils.hashTableUuidForPkStorage(table.getUUID()),
                table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName(), columns, useCase, now);
    }

    public int estimatedMemoryBytes() {
        long bytes = 256L + 2L * (tableUuid.length() + catalogName.length() + dbName.length() + tableName.length());
        for (String column : columns) {
            bytes += 48L + 2L * column.length();
        }
        return (int) Math.min(Integer.MAX_VALUE, bytes);
    }

    public String columnsJson() {
        return GSON.toJson(columns);
    }

    public String groupId() {
        // JSON preserves boundaries even when names contain separators, quotes or non-ASCII characters.
        return StatisticUtils.hashTableUuidForPkStorage(useCase.name() + columnsJson());
    }

    public String key() {
        return tableUuid + ":" + groupId();
    }

    public static ExternalColumnGroupUsage newest(ExternalColumnGroupUsage left, ExternalColumnGroupUsage right) {
        return left.lastUsed.isAfter(right.lastUsed) ? left : right;
    }
}

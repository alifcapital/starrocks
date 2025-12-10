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

package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.connector.iceberg.CachingIcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HTTP API action to retrieve information about cached Iceberg tables.
 *
 * Usage:
 *   GET /api/iceberg/cache_info
 *   GET /api/iceberg/cache_info?catalog=<catalog_name>
 */
public class IcebergCacheInfoAction extends RestBaseAction {
    private static final String CATALOG_PARAM = "catalog";

    public IcebergCacheInfoAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/iceberg/cache_info",
                new IcebergCacheInfoAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        response.setContentType("application/json");

        String catalogFilter = request.getSingleParameter(CATALOG_PARAM);

        ConnectorTableMetadataProcessor processor =
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor();
        Map<String, IcebergCatalog> catalogs = processor.getCachingIcebergCatalogs();

        Map<String, Object> result = new HashMap<>();
        Map<String, Object> catalogsInfo = new HashMap<>();

        for (Map.Entry<String, IcebergCatalog> entry : catalogs.entrySet()) {
            String catalogName = entry.getKey();

            // Apply catalog filter if specified
            if (catalogFilter != null && !catalogFilter.isEmpty() && !catalogFilter.equals(catalogName)) {
                continue;
            }

            IcebergCatalog catalog = entry.getValue();
            if (catalog instanceof CachingIcebergCatalog) {
                CachingIcebergCatalog cachingCatalog = (CachingIcebergCatalog) catalog;
                List<CachingIcebergCatalog.IcebergCachedTableInfo> tablesInfo = cachingCatalog.getCachedTablesInfo();

                Map<String, Object> catalogInfo = new HashMap<>();
                List<Map<String, Object>> tablesList = new ArrayList<>();

                for (CachingIcebergCatalog.IcebergCachedTableInfo tableInfo : tablesInfo) {
                    Map<String, Object> tableMap = new HashMap<>();
                    tableMap.put("dbName", tableInfo.getDbName());
                    tableMap.put("tableName", tableInfo.getTableName());
                    tableMap.put("snapshotId", tableInfo.getSnapshotId());
                    tableMap.put("latestAccessTime", tableInfo.getLatestAccessTime());
                    tableMap.put("latestRefreshTime", tableInfo.getLatestRefreshTime());
                    tableMap.put("latestSnapshotTime", tableInfo.getLatestSnapshotTime());
                    tablesList.add(tableMap);
                }

                catalogInfo.put("tables", tablesList);
                catalogInfo.put("tablesCount", tablesInfo.size());
                catalogsInfo.put(catalogName, catalogInfo);
            }
        }

        result.put("catalogs", catalogsInfo);

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        response.getContent().append(gson.toJson(result));
        sendResult(request, response);
    }
}

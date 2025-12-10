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

import com.starrocks.connector.hive.ConnectorTableMetadataProcessor;
import com.starrocks.connector.iceberg.CachingIcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Map;
import java.util.concurrent.Executors;

/**
 * HTTP API action to manually refresh an Iceberg table cache.
 *
 * Usage:
 *   POST /api/iceberg/refresh_table?catalog=xxx&db=yyy&table=zzz
 */
public class IcebergRefreshTableAction extends RestBaseAction {
    private static final String CATALOG_PARAM = "catalog";
    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "table";

    public IcebergRefreshTableAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/iceberg/refresh_table",
                new IcebergRefreshTableAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String catalogName = request.getSingleParameter(CATALOG_PARAM);
        String dbName = request.getSingleParameter(DB_PARAM);
        String tableName = request.getSingleParameter(TABLE_PARAM);

        if (catalogName == null || catalogName.isEmpty()) {
            response.appendContent("Missing required parameter: catalog");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        if (dbName == null || dbName.isEmpty()) {
            response.appendContent("Missing required parameter: db");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        if (tableName == null || tableName.isEmpty()) {
            response.appendContent("Missing required parameter: table");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        ConnectorTableMetadataProcessor processor =
                GlobalStateMgr.getCurrentState().getConnectorTableMetadataProcessor();
        Map<String, IcebergCatalog> catalogs = processor.getCachingIcebergCatalogs();

        IcebergCatalog catalog = catalogs.get(catalogName);
        if (catalog == null) {
            response.appendContent("Catalog not found: " + catalogName);
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }

        if (!(catalog instanceof CachingIcebergCatalog)) {
            response.appendContent("Catalog is not a caching catalog: " + catalogName);
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        CachingIcebergCatalog cachingCatalog = (CachingIcebergCatalog) catalog;
        cachingCatalog.refreshTable(dbName, tableName, Executors.newSingleThreadExecutor());

        RestBaseResult result = new RestBaseResult();
        result.addResultEntry("message", "Refresh triggered for " + catalogName + "." + dbName + "." + tableName);
        sendResult(request, response, result);
    }
}

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

package com.starrocks.authorization.ranger.starrocks;

import com.google.common.collect.Maps;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ColumnAccessKind;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.RangerAccessController;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.pipe.PipeName;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerSecurityZoneMatcher;
import org.apache.ranger.plugin.service.RangerAuthContext;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerStarRocksAccessController extends RangerAccessController {
    public RangerStarRocksAccessController() {
        super("starrocks", null);
    }

    private RangerStarRocksAccessController(boolean initInBackground) {
        super("starrocks", null, initInBackground);
    }

    /**
     * The controller of the default catalog. The FE start does other work while it loads its policies, and
     * waits for it with awaitInitialized().
     */
    public static RangerStarRocksAccessController startingInBackground() {
        return new RangerStarRocksAccessController(true);
    }

    @Override
    public void checkSystemAction(ConnectContext context, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setSystem().build(),
                context,
                privilegeType);
    }

    @Override
    public void checkUserAction(ConnectContext context, UserIdentity impersonateUser,
                                PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setUser(impersonateUser.getUser()).build(),
                context,
                privilegeType);
    }

    @Override
    public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnCatalog(ConnectContext context, String catalogName)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkDbAction(ConnectContext context, String catalogName, String db,
                              PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build(),
                context,
                PrivilegeType.ANY);
    }

    /**
     * We want to answer this check with few Ranger requests: system tables run it for every row.
     * For an ANY request on a database, Ranger also matches the policies of the tables, views,
     * materialized views, functions and pipes in it. So one request on the database usually gives
     * the answer of all the object checks.
     * <p>
     * It can deny where an object check allows. A policy that denies all access types on the whole
     * database denies the database request, but it does not match the request for a table in it, so
     * an allow policy on that table still allows the table. So when a policy set the deny, we ask
     * about each object as before. When no policy set it, we expect the object checks to deny too:
     * a policy that allows an object in the database also allows the ANY request on the database.
     * Then only the check of all pipes is left, because it asks about database "*", not this one.
     * <p>
     * The tests compare this with the object checks for many policy shapes, without security zones
     * and with tag policies that cannot allow. A zone changes which policies a request sees, and a
     * tag policy that allows can allow an object without matching the database request. So we ask
     * about each object when one of them may apply.
     */
    @Override
    public void checkAnyActionOnOrInDb(ConnectContext context, String catalogName, String db)
            throws AccessDeniedException {
        RangerAccessResourceImpl database =
                RangerStarRocksResource.builder().setCatalog(catalogName).setDatabase(db).build();
        AccessDecision decision =
                decideAccess(database, context, PrivilegeType.ANY, EnumSet.noneOf(ColumnAccessKind.class));
        if (decision.allowed()) {
            return;
        }
        if (decision.decidedByPolicy() || mayUseZonesOrTags(database)) {
            AccessController.checkAnyActionOnOrInDbOneByOne(this, context, catalogName, db);
            return;
        }
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            checkAnyActionOnPipe(context, new PipeName("*", "*"));
            return;
        }
        throw new AccessDeniedException();
    }

    // The one-step answer only turns "no policy set the answer" into a deny, so a tag policy matters
    // here only if it can allow. Deny, masking and row filter tag policies cannot. The tag answer is
    // kept per policy version, and the zone lookup returns at once when the service has no zones.
    private boolean mayUseZonesOrTags(RangerAccessResourceImpl database) {
        try {
            if (tagPolicyMayAllow()) {
                return true;
            }
            RangerAuthContext authContext = plugin().getCurrentRangerAuthContext();
            RangerSecurityZoneMatcher zoneMatcher = authContext == null ? null : authContext.getZoneMatcher();
            if (zoneMatcher == null) {
                return true;
            }
            Set<String> zones = zoneMatcher.getZonesForResourceAndChildren(database);
            return zones != null && !zones.isEmpty();
        } catch (RuntimeException e) {
            // We cannot tell, so we take the path that does not depend on the answer.
            return true;
        }
    }

    @Override
    public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnTable(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        String catalog = tableName.getCatalog() == null ? InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();

        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(catalog)
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyTable(ConnectContext context, String catalog, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalog, db);
        if (database == null) {
            // A database that does not exist has no tables, so the user has no action on any of them.
            throw new AccessDeniedException();
        }
        for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(database.getId())) {
            try {
                hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog(catalog)
                                .setDatabase(database.getFullName())
                                .setTable(table.getName())
                                .build(),
                        context,
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType) throws AccessDeniedException {
        checkColumnAction(context, tableName, column, privilegeType, EnumSet.noneOf(ColumnAccessKind.class));
    }

    @Override
    public void checkColumnAction(ConnectContext context, TableName tableName,
                                  String column, PrivilegeType privilegeType,
                                  EnumSet<ColumnAccessKind> usage) throws AccessDeniedException {
        hasPermission(RangerStarRocksResource.builder()
                        .setCatalog(tableName.getCatalog())
                        .setDatabase(tableName.getDb())
                        .setTable(tableName.getTbl())
                        .setColumn(column)
                        .build(),
                context, privilegeType, usage);
    }

    @Override
    public void checkViewAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setView(tableName.getTbl())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyView(ConnectContext context, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(db);
        if (database == null) {
            // A database that does not exist has no views, so the user has no action on any of them.
            throw new AccessDeniedException();
        }
        for (Table table : database.getViews()) {
            try {
                hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setView(table.getName())
                                .build(),
                        context,
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                            PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnMaterializedView(ConnectContext context, TableName tableName)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(tableName.getDb())
                        .setMaterializedView(tableName.getTbl())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyMaterializedView(ConnectContext context, String db)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(db);
        if (database == null) {
            // A database that does not exist has no materialized views, so the user has no action on any of them.
            throw new AccessDeniedException();
        }
        for (Table table : database.getMaterializedViews()) {
            try {
                hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setMaterializedView(table.getName())
                                .build(),
                        context,
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkFunctionAction(ConnectContext context, Database database, Function function,
                                    PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database.getFullName())
                        .setFunction(function.getSignature())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnFunction(ConnectContext context, String database, Function function)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(database)
                        .setFunction(function.getSignature())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkAnyActionOnAnyFunction(ConnectContext context, String db) throws AccessDeniedException {
        Database database = GlobalStateMgr.getServingState().getLocalMetastore().getDb(db);
        if (database == null) {
            // A database that does not exist has no functions, so the user has no action on any of them.
            throw new AccessDeniedException();
        }
        for (Function function : database.getFunctions()) {
            try {
                hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                                .setDatabase(database.getFullName())
                                .setFunction(function.getSignature())
                                .build(),
                        context,
                        PrivilegeType.ANY);
            } catch (AccessDeniedException e) {
                continue;
            }
            return;
        }
        throw new AccessDeniedException();
    }

    @Override
    public void checkGlobalFunctionAction(ConnectContext context, Function function,
                                          PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnGlobalFunction(ConnectContext context, Function function)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setGlobalFunction(function.getSignature())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    /**
     * Check whether current user has specified privilege action on any object(table/view/mv) in the db.
     */
    @Override
    public void checkActionInDb(ConnectContext context, String db, PrivilegeType privilegeType)
            throws AccessDeniedException {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db);
        if (database == null) {
            // A database that does not exist has no tables, so the user has no action on any of them.
            throw new AccessDeniedException();
        }
        for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(database.getId())) {
            if (table.isOlapView()) {
                checkViewAction(context, new TableName(database.getFullName(), table.getName()), privilegeType);
            } else if (table.isMaterializedView()) {
                checkMaterializedViewAction(context,
                        new TableName(database.getFullName(), table.getName()), privilegeType);
            } else {
                checkTableAction(context, new TableName(database.getFullName(), table.getName()), privilegeType);
            }
        }
    }

    @Override
    public void checkResourceAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnResource(ConnectContext context, String name) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setResource(name)
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkResourceGroupAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setResourceGroup(name)
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkPipeAction(ConnectContext context, PipeName name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        // A pipe policy has the resources catalog, database and pipe. Ranger does not match such a
        // policy, even with catalog "*", when the request has no catalog, so we set the internal
        // catalog, which holds all pipes.
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(name.getDbName())
                        .setPipe(name.getPipeName())
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnPipe(ConnectContext context, PipeName pipeName)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)
                        .setDatabase(pipeName.getDbName())
                        .setPipe(pipeName.getPipeName())
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public void checkStorageVolumeAction(ConnectContext context, String storageVolume,
                                         PrivilegeType privilegeType) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnStorageVolume(ConnectContext context, String storageVolume)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setStorageVolume(storageVolume)
                        .build(),
                context,
                PrivilegeType.ANY);
    }

    @Override
    public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName, List<Column> columns) {
        Map<String, Expr> maskingExprMap = Maps.newHashMap();
        for (Column column : columns) {
            Expr columnMaskingExpression = getColumnMaskingExpression(RangerStarRocksResource.builder()
                    .setCatalog(tableName.getCatalog())
                    .setDatabase(tableName.getDb())
                    .setTable(tableName.getTbl())
                    .setColumn(column.getName())
                    .build(), column, context);
            if (columnMaskingExpression != null) {
                maskingExprMap.put(column.getName(), columnMaskingExpression);
            }
        }

        return maskingExprMap;
    }

    @Override
    public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
        return getRowAccessExpression(RangerStarRocksResource.builder()
                .setCatalog(tableName.getCatalog())
                .setDatabase(tableName.getDb())
                .setTable(tableName.getTbl())
                .build(), context);
    }

    @Override
    public String convertToAccessType(PrivilegeType privilegeType) {
        return privilegeType.name().toLowerCase(ENGLISH);
    }

    @Override
    public void checkWarehouseAction(ConnectContext context, String name, PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setWarehouse(name)
                        .build(),
                context,
                privilegeType);
    }

    @Override
    public void checkAnyActionOnWarehouse(ConnectContext context, String name) throws AccessDeniedException {
        hasPermission(
                RangerStarRocksResource.builder()
                        .setWarehouse(name)
                        .build(),
                context,
                PrivilegeType.ANY);
    }
}
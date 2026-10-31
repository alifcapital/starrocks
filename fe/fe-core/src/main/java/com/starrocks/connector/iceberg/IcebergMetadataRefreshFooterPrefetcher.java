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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.datacache.DataCacheSelectExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

// Fires a CACHE SELECT against the freshly-refreshed Iceberg table to warm BE block_cache with
// parquet/orc footers. The feature is gated by the public session variable
// enable_iceberg_metadata_refresh_footer_prefetch on the caller's ConnectContext (FE-only knob,
// not propagated to BE).
//
// The actual block_cache populate happens through the same scan path real queries use:
// DataCacheSelectExecutor → StmtExecutor → Coordinator → HDFSBackendSelector → CacheSelectScanner.
// Inside buildContext we force the INVISIBLE session var cache_select_footer_only=true on the
// cloned SessionVariable; that is the only signal BE reads (via TQueryOptions). CacheSelectScanner
// then stops after reader->init in _fetch_parquet/_fetch_orc (which populates the footer through
// CacheInputStream) and skips Iceberg delete files. Placement matches what real SELECT queries on
// this table will compute, by construction.
//
// The two knobs are deliberately split so a user opting into the public feature does not
// accidentally turn their own ad-hoc CACHE SELECT into a footer-only no-op.
//
// A big table with frequent commits must not get a footer pass over every file on each commit. The
// refresh passes the snapshot the cache held before it, and the INVISIBLE session var
// cache_select_footer_base_snapshot_id carries it to Iceberg scan planning, which then plans only the
// data files added after that snapshot.
//
// Submitted fire-and-forget to the caller-provided single-thread executor so the refresh thread
// returns immediately; a failed warm-up only logs a warning and never propagates.
public class IcebergMetadataRefreshFooterPrefetcher {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataRefreshFooterPrefetcher.class);

    // Keep one queued request per table and warehouse. A refresh during a running warmup
    // replaces the pending request; it needs another scan because planning may have finished.
    private static final Map<List<String>, WarmupTask> INFLIGHT = new HashMap<>();

    // A request warms the data files added after baseSnapshotId, the snapshot the table cache held before
    // the refresh. SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET warms every file.
    private static final class WarmupRequest {
        private final long baseSnapshotId;
        private final SessionVariable sessionVariable;

        private WarmupRequest(long baseSnapshotId, SessionVariable sessionVariable) {
            this.baseSnapshotId = baseSnapshotId;
            this.sessionVariable = sessionVariable;
        }
    }

    private static final class WarmupTask implements Runnable {
        // catalog, database, table and warehouse
        private final List<String> key;
        private final ExecutorService executor;
        private WarmupRequest pending;

        private WarmupTask(List<String> key, ExecutorService executor, WarmupRequest pending) {
            this.key = key;
            this.executor = executor;
            this.pending = pending;
        }

        private void submit() {
            try {
                executor.execute(this);
            } catch (RuntimeException e) {
                synchronized (INFLIGHT) {
                    INFLIGHT.remove(key, this);
                }
                LOG.warn("FooterPrefetch: executor rejected warmup for {}", key, e);
            }
        }

        @Override
        public void run() {
            WarmupRequest request;
            synchronized (INFLIGHT) {
                request = pending;
                pending = null;
            }
            try {
                runWarmup(key.get(0), key.get(1), key.get(2), request.sessionVariable, key.get(3),
                        request.baseSnapshotId);
            } finally {
                boolean resubmit;
                synchronized (INFLIGHT) {
                    resubmit = pending != null;
                    if (!resubmit) {
                        INFLIGHT.remove(key, this);
                    }
                }
                if (resubmit) {
                    // Requeue at the tail so other tables can warm between consecutive refreshes.
                    submit();
                }
            }
        }
    }

    // baseSnapshotId is the snapshot the table cache held before the refresh that calls this, or
    // SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET when the table had none. We expect the files of that
    // snapshot to be warm already, so the warmup covers only the files added after it.
    public static void warmup(String catalogName, String dbName, String tableName, long baseSnapshotId,
                              ConnectContext callerCtx, ExecutorService executor) {
        if (executor == null || catalogName == null || dbName == null || tableName == null) {
            return;
        }
        SessionVariable callerSv = callerCtx != null ? callerCtx.getSessionVariable() : null;
        if (callerSv == null || !callerSv.isEnableIcebergMetadataRefreshFooterPrefetch()) {
            return;
        }
        // Capture the caller's warehouse so the cache_select runs on the same warehouse the user
        // queries will hit. Falling back to default would warm a different warehouse and leave
        // the caller's warehouse cold on first user query.
        final String warehouseName = callerCtx.getCurrentWarehouseName() != null
                ? callerCtx.getCurrentWarehouseName()
                : WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        final List<String> key = ImmutableList.of(catalogName, dbName, tableName, warehouseName);
        final SessionVariable clonedSv = callerSv.clone();
        WarmupTask task;
        synchronized (INFLIGHT) {
            task = INFLIGHT.get(key);
            if (task != null) {
                // The pending request has not started, so the files added by its refresh are still cold.
                // We fear that the later base would leave them cold, so the merged request covers the files
                // of both requests. Without a pending request a warmup is running and may have planned
                // already, so the new request keeps its own base.
                long mergedBase = task.pending != null
                        ? coveringBase(task.pending.baseSnapshotId, baseSnapshotId) : baseSnapshotId;
                task.pending = new WarmupRequest(mergedBase, clonedSv);
                return;
            }
            task = new WarmupTask(key, executor, new WarmupRequest(baseSnapshotId, clonedSv));
            INFLIGHT.put(key, task);
        }
        task.submit();
    }

    // The pending request came from the earlier refresh, so its base covers the files of both requests,
    // unless either of them warms every file.
    private static long coveringBase(long pendingBase, long newBase) {
        if (pendingBase == SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET
                || newBase == SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET) {
            return SessionVariable.CACHE_SELECT_FOOTER_BASE_NOT_SET;
        }
        return pendingBase;
    }

    private static void runWarmup(String catalogName, String dbName, String tableName,
                                  SessionVariable clonedSv, String warehouseName, long baseSnapshotId) {
        try {
            ConnectContext ctx = buildContext(catalogName, dbName, clonedSv, warehouseName, baseSnapshotId);
            DataCacheSelectStatement stmt = buildStatement(catalogName, dbName, tableName);
            DataCacheSelectExecutor.cacheSelect(stmt, ctx);
        } catch (Throwable t) {
            LOG.warn("FooterPrefetch: warmup failed for {}.{}.{}",
                    catalogName, dbName, tableName, t);
        } finally {
            ConnectContext.remove();
        }
    }

    private static ConnectContext buildContext(String catalogName, String dbName,
                                                SessionVariable clonedSv, String warehouseName,
                                                long baseSnapshotId) {
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        // Force the internal footer-only signal on for this cache_select run so
        // CacheSelectScanner skips column data and Iceberg delete-file fetches. This is a
        // private, INVISIBLE session var — users opting into the feature with the public
        // enable_iceberg_metadata_refresh_footer_prefetch knob must not see footer-only mode
        // leak into their own CACHE SELECT statements.
        clonedSv.setCacheSelectFooterOnly(true);
        // We expect the footers of the files that existed at the base snapshot to be warm, so scan planning
        // keeps only the files added after it. Like cache_select_footer_only, this variable is set only
        // here and read only by FE planning.
        clonedSv.setCacheSelectFooterBaseSnapshotId(baseSnapshotId);
        // Stamp the warehouse name onto the cloned session var directly. We must NOT call
        // ctx.setCurrentWarehouse(): that helper replaces sessionVariable with a fresh default,
        // discarding cacheSelectFooterOnly and every caller setting we just cloned.
        clonedSv.setWarehouseName(warehouseName);
        ctx.setSessionVariable(clonedSv);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setQualifiedUser(UserIdentity.ROOT.getUser());
        // ROOT user's UserPrivilegeCollection has no direct grants; all privileges flow through
        // ROOT_ROLE_ID. AuthorizationMgr.loadPrivilegeCollection does retainAll(currentRoleIds) on
        // user roles — an empty set here would silently strip ROOT_ROLE_ID and warmup would auth-
        // fail through native access control (Ranger has a ROOT bypass; native does not).
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setCurrentCatalog(catalogName);
        ctx.setDatabase(dbName);
        UUID queryId = UUIDUtil.genUUID();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private static DataCacheSelectStatement buildStatement(String catalogName, String dbName, String tableName) {
        TableName tn = new TableName(catalogName, dbName, tableName);
        TableRelation tableRelation = new TableRelation(tn);
        SelectListItem starItem = new SelectListItem((TableName) null);
        SelectList selectList = new SelectList(ImmutableList.of(starItem), false);
        SelectRelation queryRelation = new SelectRelation(
                selectList, tableRelation, null, null, null, NodePosition.ZERO);
        QueryStatement queryStatement = new QueryStatement(queryRelation);
        InsertStmt insertStmt = new InsertStmt(queryStatement, NodePosition.ZERO);
        DataCacheSelectStatement statement = new DataCacheSelectStatement(insertStmt, new HashMap<>(), NodePosition.ZERO);
        // The internal executor analyzes the INSERT, but takes its session catalog from this statement.
        statement.setCatalog(catalogName);
        return statement;
    }
}

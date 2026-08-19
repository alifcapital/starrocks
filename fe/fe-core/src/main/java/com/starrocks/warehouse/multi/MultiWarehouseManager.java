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

package com.starrocks.warehouse.multi;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.AlterWarehouseStmt;
import com.starrocks.sql.ast.warehouse.CreateWarehouseStmt;
import com.starrocks.sql.ast.warehouse.DropWarehouseStmt;
import com.starrocks.sql.ast.warehouse.ResumeWarehouseStmt;
import com.starrocks.sql.ast.warehouse.SuspendWarehouseStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.ComputeResourceProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link WarehouseManager} that really creates warehouses, instead of rejecting the DDL.
 *
 * <p>Each warehouse owns one StarMgr worker group. {@code CREATE WAREHOUSE} allocates the group,
 * {@code ALTER SYSTEM ADD COMPUTE NODE ... INTO WAREHOUSE} puts nodes in it. Tables and their metadata
 * are shared across warehouses.
 *
 * <p>Enable it with {@code enable_multi_warehouse = true} in fe.conf. It requires shared-data mode.
 *
 * <p>Not supported here: CNGROUPs, SUSPEND/RESUME, and changing a warehouse's worker group size after
 * creation (the StarMgr client exposes no size update).
 */
public class MultiWarehouseManager extends WarehouseManager {
    private static final Logger LOG = LogManager.getLogger(MultiWarehouseManager.class);

    /**
     * Worker group size class handed to StarMgr, e.g. "x1". The accepted values are defined by StarMgr, not
     * by the FE; whatever StarMgr rejects surfaces as the DdlException from createWorkerGroup.
     */
    public static final String PROPERTY_SIZE = "size";
    public static final String DEFAULT_SIZE = "x1";

    /**
     * Number of StarMgr worker replicas for the group.
     */
    public static final String PROPERTY_REPLICA_NUMBER = "replica_number";
    public static final int DEFAULT_REPLICA_NUMBER = 1;

    private static final Set<String> MUTABLE_PROPERTIES = Sets.newHashSet(PROPERTY_REPLICA_NUMBER);

    public MultiWarehouseManager() {
        super();
    }

    public MultiWarehouseManager(ComputeResourceProvider computeResourceProvider) {
        super(computeResourceProvider, new ArrayList<>());
    }

    // ------------------------------------------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------------------------------------------

    /**
     * DDL entry points are serialized on the manager: the existence check, the StarMgr call and the journal
     * write must not interleave with a concurrent warehouse DDL on the same leader. Warehouse DDL is rare, so
     * a coarse lock costs nothing; the in-memory maps are still guarded by the inherited {@code rwLock}.
     */
    @Override
    public synchronized void createWarehouse(CreateWarehouseStmt stmt) throws DdlException {
        checkSharedDataMode();

        final String name = stmt.getWarehouseName();
        if (warehouseExists(name)) {
            if (stmt.isSetIfNotExists()) {
                LOG.info("Warehouse {} already exists, skip creating", name);
                return;
            }
            ErrorReport.reportDdlException(ErrorCode.ERR_WAREHOUSE_EXISTS, name);
        }

        final Map<String, String> properties = stmt.getProperties() == null
                ? Maps.newHashMap() : Maps.newHashMap(stmt.getProperties());

        // Validate on a scratch copy: the declared properties are kept verbatim on the warehouse so that
        // SHOW WAREHOUSES and ALTER WAREHOUSE see what the user asked for.
        final Map<String, String> scratch = Maps.newHashMap(properties);
        final String size = extractSize(scratch);
        final int replicaNumber = extractReplicaNumber(scratch);
        checkNoUnknownProperties(scratch);

        final long workerGroupId = getStarOSAgent().createWorkerGroup(size, replicaNumber);
        boolean succeeded = false;
        try {
            final long id = allocateId();
            final MultiWarehouse warehouse = new MultiWarehouse(id, name, stmt.getComment(), workerGroupId,
                    properties, System.currentTimeMillis());
            logEdit(OperationType.OP_CREATE_WAREHOUSE, warehouse, wal -> replayCreateWarehouse((Warehouse) wal));
            succeeded = true;
            LOG.info("Created warehouse {}, id: {}, workerGroupId: {}", name, id, workerGroupId);
        } finally {
            if (!succeeded) {
                // The warehouse never became visible, so the group it would have owned is unreachable.
                deleteWorkerGroupQuietly(workerGroupId);
            }
        }
    }

    @Override
    public synchronized void dropWarehouse(DropWarehouseStmt stmt) throws DdlException {
        checkSharedDataMode();

        final String name = stmt.getWarehouseName();
        if (DEFAULT_WAREHOUSE_NAME.equalsIgnoreCase(name)) {
            throw new DdlException("Can't drop the " + DEFAULT_WAREHOUSE_NAME);
        }

        final Warehouse warehouse = getWarehouseAllowNull(name);
        if (warehouse == null) {
            if (stmt.isSetIfExists()) {
                LOG.info("Warehouse {} does not exist, skip dropping", name);
                return;
            }
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", name));
            return;
        }

        final List<Long> nodeIds = new ArrayList<>();
        if (name.equals(Config.lake_background_warehouse) || name.equals(Config.lake_compaction_warehouse)
                || name.equals(Config.statistic_collect_warehouse)) {
            throw new DdlException("Warehouse " + name + " is configured for background work. Change the FE configuration first");
        }
        for (ComputeNode node : nodesOf(warehouse)) {
            nodeIds.add(node.getId());
        }
        if (!nodeIds.isEmpty()) {
            throw new DdlException(String.format(
                    "Warehouse %s still has %d node(s) %s. Drop them first, e.g. "
                            + "ALTER SYSTEM DROP COMPUTE NODE \"<host>:<port>\" FROM WAREHOUSE %s",
                    name, nodeIds.size(), nodeIds, name));
        }

        // A resource group binds warehouses by NAME (ResourceGroup#getWarehouses), and that name is only
        // validated when the group is created or altered. Dropping a bound warehouse would therefore leave a
        // dangling binding that silently stops matching in
        // ResourceGroupMgr#isResourceGroupMatchWarehouse - and a group bound only to this warehouse would
        // become unreachable with no error anywhere. Refuse instead, and name the groups to fix.
        final List<String> boundGroups = boundResourceGroupNames(name, fetchAllResourceGroups());
        if (!boundGroups.isEmpty()) {
            throw new DdlException(String.format(
                    "Warehouse %s is still bound by resource group(s) %s. Remove the binding first, e.g. "
                            + "ALTER RESOURCE GROUP <group> SET (\"warehouses\" = \"<remaining>\"), "
                            + "or drop those resource groups.",
                    name, boundGroups));
        }

        final List<Long> workerGroupIds = new ArrayList<>(warehouse.getWorkerGroupIds());

        // Journal first: if the StarMgr cleanup below fails, an orphaned worker group is recoverable, while a
        // deleted group behind a still-visible warehouse would silently break every query routed to it.
        logEdit(OperationType.OP_DROP_WAREHOUSE, new DropWarehouseLog(name),
                wal -> replayDropWarehouse((DropWarehouseLog) wal));
        for (Long workerGroupId : workerGroupIds) {
            deleteWorkerGroupQuietly(workerGroupId);
        }
        LOG.info("Dropped warehouse {}, worker groups: {}", name, workerGroupIds);
    }

    @Override
    public synchronized void alterWarehouse(AlterWarehouseStmt stmt) throws DdlException {
        checkSharedDataMode();

        final String name = stmt.getWarehouseName();
        final Warehouse warehouse = getWarehouseAllowNull(name);
        if (warehouse == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", name));
            return;
        }
        if (!(warehouse instanceof MultiWarehouse)) {
            throw new DdlException("Warehouse " + name + " can not be altered");
        }
        final MultiWarehouse multiWarehouse = (MultiWarehouse) warehouse;

        final Map<String, String> changes = stmt.getProperties() == null
                ? Maps.newHashMap() : Maps.newHashMap(stmt.getProperties());
        if (changes.isEmpty()) {
            throw new DdlException("No property to alter");
        }
        for (String key : changes.keySet()) {
            if (!MUTABLE_PROPERTIES.contains(key)) {
                throw new DdlException(String.format(
                        "Property '%s' can not be altered. Alterable properties: %s", key, MUTABLE_PROPERTIES));
            }
        }

        final Map<String, String> merged = Maps.newHashMap(multiWarehouse.getProperties());
        merged.putAll(changes);
        final int replicaNumber = extractReplicaNumber(Maps.newHashMap(merged));
        getStarOSAgent().updateWorkerGroup(multiWarehouse.getAnyWorkerGroupId(), replicaNumber);

        final MultiWarehouse updated = new MultiWarehouse(multiWarehouse.getId(), multiWarehouse.getName(),
                multiWarehouse.getComment(), multiWarehouse.getAnyWorkerGroupId(), merged,
                multiWarehouse.getCreatedTime());
        updated.setUpdatedTime(System.currentTimeMillis());
        logEdit(OperationType.OP_ALTER_WAREHOUSE, updated, wal -> replayAlterWarehouse((Warehouse) wal));
        LOG.info("Altered warehouse {}, properties: {}", name, merged);
    }

    @Override
    public void suspendWarehouse(SuspendWarehouseStmt stmt) throws DdlException {
        throw new DdlException("SUSPEND WAREHOUSE is not supported: a warehouse's compute nodes are managed "
                + "with ALTER SYSTEM ADD/DROP COMPUTE NODE, not by suspending the warehouse");
    }

    @Override
    public void resumeWarehouse(ResumeWarehouseStmt stmt) throws DdlException {
        throw new DdlException("RESUME WAREHOUSE is not supported: a warehouse's compute nodes are managed "
                + "with ALTER SYSTEM ADD/DROP COMPUTE NODE, not by resuming the warehouse");
    }

    // ------------------------------------------------------------------------------------------------------
    // Background work
    // ------------------------------------------------------------------------------------------------------

    /**
     * Honour {@code lake_background_warehouse}. The base class always returns {@code default_warehouse}, which
     * would leave every background job (vacuum, dictionary refresh, the leader daemons, ...) without nodes once
     * all compute nodes have been moved into user-created warehouses.
     */
    @Override
    public Warehouse getBackgroundWarehouse() {
        return Config.enable_multi_warehouse ? resolveConfiguredWarehouse(Config.lake_background_warehouse)
                : super.getBackgroundWarehouse();
    }

    /**
     * Compaction uses its configured warehouse. An unavailable warehouse delays the job.
     */
    @Override
    public ComputeResource getCompactionComputeResource(long tableId) {
        if (!Config.enable_multi_warehouse) {
            return DEFAULT_RESOURCE;
        }
        final Warehouse warehouse = Strings.isNullOrEmpty(Config.lake_compaction_warehouse)
                ? getBackgroundWarehouse() : resolveConfiguredWarehouse(Config.lake_compaction_warehouse);
        return acquireComputeResource(CRAcquireContext.of(warehouse.getId()));
    }

    /**
     * An empty setting selects the default warehouse. Invalid names must not redirect work to another pool.
     */
    private Warehouse resolveConfiguredWarehouse(String configuredName) {
        return getWarehouse(Strings.isNullOrEmpty(configuredName) ? DEFAULT_WAREHOUSE_NAME : configuredName);
    }

    // ------------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------------

    /**
     * Write one journal entry and apply it in-memory inside the WAL fence. Seam for tests.
     */
    protected void logEdit(short op, Object payload, WALApplier applier) {
        GlobalStateMgr.getCurrentState().getEditLog().logJsonObject(op, payload, applier);
    }

    /**
     * Allocate a catalog id. Seam for tests.
     */
    protected long allocateId() {
        return GlobalStateMgr.getCurrentState().getNextId();
    }

    private static void checkSharedDataMode() throws DdlException {
        if (!RunMode.isSharedDataMode()) {
            throw new DdlException("Multi-warehouse is only supported in shared_data mode");
        }
    }

    /**
     * Every resource group currently defined, or an empty list when the manager is not available. Seam for
     * tests. Read through the public {@code ResourceGroupMgr} API so its own lock is taken and released per
     * call - this must not be invoked while holding {@code rwLock}, to keep the lock order one-directional.
     */
    protected List<ResourceGroup> fetchAllResourceGroups() {
        final ResourceGroupMgr resourceGroupMgr = GlobalStateMgr.getCurrentState().getResourceGroupMgr();
        if (resourceGroupMgr == null) {
            return new ArrayList<>();
        }
        final List<ResourceGroup> groups = new ArrayList<>();
        // getAllResourceGroupNames() hands back the manager's live key set, not a copy, so snapshot it before
        // the per-name lookups below rather than iterating it across them.
        for (String groupName : new ArrayList<>(resourceGroupMgr.getAllResourceGroupNames())) {
            final ResourceGroup group = resourceGroupMgr.getResourceGroup(groupName);
            if (group != null) {
                groups.add(group);
            }
        }
        return groups;
    }

    /**
     * Names of the groups whose {@code warehouses} property references {@code warehouseName}, sorted so the
     * error message is deterministic.
     *
     * <p>Matching is case-sensitive on purpose: it mirrors
     * {@code ResourceGroupMgr#isResourceGroupMatchWarehouse}, which compares with {@code List#contains}. A
     * binding that differs only in case never matched any query, so it is not a reference worth blocking a
     * drop for.
     */
    static List<String> boundResourceGroupNames(String warehouseName, List<ResourceGroup> groups) {
        final List<String> bound = new ArrayList<>();
        for (ResourceGroup group : groups) {
            final List<String> warehouses = group.getWarehouses();
            if (warehouses != null && warehouses.contains(warehouseName)) {
                bound.add(group.getName());
            }
        }
        Collections.sort(bound);
        return bound;
    }

    /**
     * The StarMgr client used for worker-group operations. Seam for tests.
     */
    protected StarOSAgent getStarOSAgent() throws DdlException {
        StarOSAgent agent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        if (agent == null) {
            throw new DdlException("StarOSAgent is not available");
        }
        return agent;
    }

    private List<ComputeNode> nodesOf(Warehouse warehouse) {
        if (warehouse instanceof MultiWarehouse) {
            return ((MultiWarehouse) warehouse).getNodes();
        }
        return new ArrayList<>();
    }

    private void deleteWorkerGroupQuietly(long workerGroupId) {
        try {
            getStarOSAgent().deleteWorkerGroup(workerGroupId);
        } catch (Exception e) {
            LOG.warn("Failed to delete starMgr worker group {}, it may be leaked: {}",
                    workerGroupId, e.getMessage());
        }
    }

    private static String extractSize(Map<String, String> properties) {
        String size = properties.remove(PROPERTY_SIZE);
        if (Strings.isNullOrEmpty(size)) {
            return DEFAULT_SIZE;
        }
        return size;
    }

    private static int extractReplicaNumber(Map<String, String> properties) throws DdlException {
        String value = properties.remove(PROPERTY_REPLICA_NUMBER);
        if (Strings.isNullOrEmpty(value)) {
            return DEFAULT_REPLICA_NUMBER;
        }
        int replicaNumber;
        try {
            replicaNumber = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new DdlException("Property '" + PROPERTY_REPLICA_NUMBER + "' must be an integer, got: " + value);
        }
        if (replicaNumber < 1) {
            throw new DdlException("Property '" + PROPERTY_REPLICA_NUMBER + "' must be >= 1, got: " + replicaNumber);
        }
        return replicaNumber;
    }

    private static void checkNoUnknownProperties(Map<String, String> leftovers) throws DdlException {
        if (!leftovers.isEmpty()) {
            throw new DdlException(String.format("Unknown warehouse properties: %s. Supported: %s, %s",
                    leftovers.keySet(), PROPERTY_SIZE, PROPERTY_REPLICA_NUMBER));
        }
    }
}

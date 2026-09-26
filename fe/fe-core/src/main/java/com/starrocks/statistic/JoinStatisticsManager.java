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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.metric.StatisticsCacheMetrics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

/** Coordinates collection, durable publication and prepared generation loading. */
public final class JoinStatisticsManager implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(JoinStatisticsManager.class);

    public record Status(String state, String error) {
    }

    private static final class Job {
        private final JoinStatisticsRegistry.Collection ticket;
        private final ConnectContext context;
        private final AtomicInteger phase = new AtomicInteger();
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private Runnable task;

        private Job(JoinStatisticsRegistry.Collection ticket, ConnectContext context) {
            this.ticket = ticket;
            this.context = context;
        }
    }

    private final JoinStatisticsRegistry registry;
    private final JoinStatisticsStorage storage = new JoinStatisticsStorage();
    private final ThreadPoolExecutor collections = ThreadPoolManager.newDaemonThreadPool(1, 1, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(32), new ThreadPoolExecutor.AbortPolicy(), "join-statistics-collect", true);
    private final ThreadPoolExecutor loads = ThreadPoolManager.newDaemonThreadPool(2, 2, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(64), new ThreadPoolExecutor.AbortPolicy(), "join-statistics-load", true);
    private final Map<Long, Job> jobs = new ConcurrentHashMap<>();
    private final Map<Long, Status> statuses = new ConcurrentHashMap<>();
    private final JoinStatisticsCache cache;
    private long lastCleanup;

    public JoinStatisticsManager(JoinStatisticsRegistry registry) {
        this.registry = registry;
        cache = new JoinStatisticsCache(registry, meta -> {
            ConnectContext context = StatisticUtils.buildConnectContext();
            context.getSessionVariable().setQueryTimeoutS(60);
            try (ConnectContext.ScopeGuard ignored = context.bindScope()) {
                return storage.load(context, meta);
            }
        }, loads, Config.statistic_join_cache_max_bytes, 2);
        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(
                () -> cache.setMaximumBytes(Config.statistic_join_cache_max_bytes));
        ThreadPoolManager.newDaemonScheduledThreadPool(1, "join-statistics-cancel", true)
                .scheduleWithFixedDelay(this::cancelRevokedQueries, 1, 1, TimeUnit.SECONDS);
    }

    public void create(JoinStatisticsDefinition definition, boolean asynchronous, LongConsumer started) throws DdlException {
        checkLeader();
        registry.create(GlobalStateMgr.getCurrentState().getNextId(), definition);
        if (Config.enable_trigger_analyze_job_immediate) {
            analyze(definition.getName(), asynchronous, started);
        }
    }

    public void analyze(String name, boolean asynchronous, LongConsumer started) throws DdlException {
        checkLeader();
        JoinStatisticsRegistry.Collection ticket = registry.begin(name, GlobalStateMgr.getCurrentState().getNextId());
        ConnectContext context = StatisticUtils.buildConnectContext();
        Job job = new Job(ticket, context);
        job.task = () -> {
            if (!job.phase.compareAndSet(0, 1)) {
                return;
            }
            try {
                run(job);
                job.result.complete(null);
            } catch (Throwable e) {
                job.result.completeExceptionally(e);
            } finally {
                job.phase.set(2);
            }
        };
        long id = ticket.getPrevious().getId();
        jobs.put(id, job);
        started.accept(ticket.getGeneration());
        status(job, "PENDING", "");
        try {
            collections.execute(job.task);
            if (job.phase.get() == 2) {
                collections.remove(job.task);
            }
        } catch (RuntimeException e) {
            status(job, "FAILED", e.getMessage());
            jobs.remove(id, job);
            registry.finish(ticket);
            throw new DdlException("Cannot schedule JOIN statistics collection: " + e.getMessage());
        }
        if (!asynchronous) {
            try {
                job.result.get(Config.statistic_collect_query_timeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                cancel(job);
                Thread.currentThread().interrupt();
                throw new DdlException("JOIN statistics collection was interrupted");
            } catch (TimeoutException e) {
                cancel(job);
                throw new DdlException("JOIN statistics collection timed out");
            } catch (ExecutionException e) {
                throw new DdlException("JOIN statistics collection failed: " + e.getCause().getMessage());
            } catch (CancellationException e) {
                throw new DdlException("JOIN statistics collection was cancelled");
            }
        }
    }

    private void run(Job job) {
        JoinStatisticsRegistry.Collection ticket = job.ticket;
        long id = ticket.getPrevious().getId();
        boolean published = false;
        try (ConnectContext.ScopeGuard ignored = job.context.bindScope();
                JoinStatisticsCollector collector = new JoinStatisticsCollector(job.context, ticket, this::checkLeader)) {
            check(job);
            if (!StatisticUtils.checkStatisticTables(java.util.List.of(StatsConstants.JOIN_STATISTICS_TABLE_NAME))) {
                throw new IllegalStateException("JOIN statistics storage is not ready; retry ANALYZE after initialization");
            }
            status(job, "COLLECTING", "");
            JoinStatisticsData data = collector.collect();
            check(job);
            status(job, "SAVING", "");
            JoinStatisticsStorage.Manifest manifest = storage.write(job.context, data, () -> check(job));
            check(job);
            published = registry.publish(ticket, manifest.parts(), manifest.bytes(), manifest.checksum(),
                    System.currentTimeMillis());
            if (!published) {
                throw new CancellationException("JOIN statistics publication was revoked");
            }
            cache.invalidate(id);
            JoinStatisticsMeta current = registry.get(ticket.getPrevious().getDefinition().getName());
            if (current != null) {
                cache.put(current, data);
            }
            status(job, "READY", "");
            if (ticket.getPrevious().getGeneration() != 0) {
                deletePayload(id, ticket.getPrevious().getGeneration());
            }
        } catch (Exception e) {
            status(job, ticket.isCancelled() ? "CANCELLED" : "FAILED",
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            LOG.warn("JOIN statistics collection failed for {} generation {}", id, ticket.getGeneration(), e);
            throw new CompletionException(e);
        } finally {
            if (!published) {
                deletePayload(id, ticket.getGeneration());
            }
            registry.finish(ticket);
            jobs.remove(id, job);
            JoinStatisticsMeta current = registry.get(ticket.getPrevious().getDefinition().getName());
            if (current == null || current.getId() != id) {
                statuses.remove(id);
            }
        }
    }

    public void drop(String name, boolean ifExists) {
        checkLeader();
        JoinStatisticsMeta removed = registry.drop(name, ifExists);
        if (removed == null) {
            return;
        }
        cancel(removed.getId());
        cache.invalidate(removed.getId());
        statuses.remove(removed.getId());
        deletePayload(removed.getId(), null);
    }

    public void cancel(long id) {
        Job job = jobs.get(id);
        if (job != null) {
            cancel(job);
        }
    }

    /** KILL belongs to one statement, not to a later refresh of the same named object. */
    public void cancelCollection(long generation) {
        jobs.values().stream().filter(job -> job.ticket.getGeneration() == generation).findFirst().ifPresent(this::cancel);
    }

    private void status(Job job, String state, String error) {
        jobs.computeIfPresent(job.ticket.getPrevious().getId(), (id, current) -> {
            if (current == job) {
                statuses.put(id, new Status(state, error));
            }
            return current;
        });
    }

    private void cancel(Job job) {
        long id = job.ticket.getPrevious().getId();
        registry.cancel(job.ticket);
        if (job.phase.compareAndSet(0, 2)) {
            // A queued synchronous request must not wait for the preceding object's collection to finish.
            collections.remove(job.task);
            registry.finish(job.ticket);
            status(job, "CANCELLED", "JOIN statistics collection was cancelled");
            jobs.remove(id, job);
            job.result.completeExceptionally(new CancellationException("JOIN statistics collection was cancelled"));
        } else if (job.context.getExecutor() != null) {
            job.context.getExecutor().cancel("JOIN statistics collection was cancelled");
        }
    }

    private void cancelRevokedQueries() {
        // Cancellation may race with installing the next internal coordinator. Keep cancelling until the job exits.
        for (Job job : jobs.values()) {
            if (job.ticket.isCancelled()) {
                try {
                    if (job.context.getExecutor() != null) {
                        job.context.getExecutor().cancel("JOIN statistics collection was cancelled");
                    }
                } catch (RuntimeException e) {
                    LOG.warn("Cannot cancel revoked JOIN statistics query", e);
                }
            }
        }
    }

    public void revokeCollections() {
        registry.revokeCollections();
        jobs.forEach((id, job) -> cancel(id));
    }

    public StatisticsCacheMetrics getCacheMetrics() {
        return cache.metrics();
    }

    public Optional<JoinStatisticsData> get(JoinStatisticsMeta meta, long timeoutMillis) {
        ConnectContext context = ConnectContext.get();
        if (context != null && context.isStatisticsJob()) {
            return Optional.empty();
        }
        return cache.get(meta, Config.enable_sync_statistics_load, timeoutMillis);
    }

    /** Journal replay retires cached generations on follower FEs as well as on the collecting leader. */
    public void invalidateCache(long objectId) {
        cache.invalidate(objectId);
    }

    public Status status(JoinStatisticsMeta meta) {
        return statuses.getOrDefault(meta.getId(), new Status(meta.getGeneration() == 0 ? "EMPTY" : "READY", ""));
    }

    private void check(Job job) {
        checkLeader();
        if (job.ticket.isCancelled()) {
            throw new CancellationException("JOIN statistics collection was cancelled");
        }
    }

    private void checkLeader() {
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            throw new CancellationException("JOIN statistics collection requires the leader FE");
        }
    }

    private void deletePayload(long id, Long generation) {
        if (!GlobalStateMgr.getCurrentState().isLeader()) {
            return;
        }
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.getSessionVariable().setQueryTimeoutS(60);
        context.getSessionVariable().setInsertTimeoutS(60);
        try (ConnectContext.ScopeGuard ignored = context.bindScope()) {
            storage.delete(context, id, generation);
        } catch (Exception e) {
            LOG.warn("Cannot remove obsolete JOIN statistics payload {} generation {}", id, generation, e);
        }
    }

    /** Best-effort leader cleanup also handles an FE crash between payload writes and publication. */
    public synchronized void cleanOrphans() {
        if (!GlobalStateMgr.getCurrentState().isLeader() || System.currentTimeMillis() - lastCleanup < 60000) {
            return;
        }
        lastCleanup = System.currentTimeMillis();
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(StatsConstants.STATISTICS_DB_NAME);
        if (database == null) {
            return;
        }
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.getSessionVariable().setQueryTimeoutS(30);
        context.getSessionVariable().setInsertTimeoutS(30);
        try (ConnectContext.ScopeGuard ignored = context.bindScope()) {
            int cleaned = 0;
            for (Table table : database.getTables()) {
                String name = table.getName();
                if (!name.matches("_join_collect_[0-9]+_[0-9]+_[0-9]+")) {
                    continue;
                }
                String[] identity = name.substring("_join_collect_".length()).split("_");
                long id = Long.parseLong(identity[0]);
                long generation = Long.parseLong(identity[1]);
                if (active(id, generation)) {
                    continue;
                }
                checkLeader();
                JoinStatisticsStorage.execute(context, "DROP TABLE IF EXISTS default_catalog."
                        + StatsConstants.STATISTICS_DB_NAME + ".`" + name + "` FORCE");
                if (++cleaned >= 32) {
                    break;
                }
            }
            List<List<String>> payloads = new StatisticExecutor().executeStatisticJsonDQL(context,
                    "SELECT object_id, generation FROM " + JoinStatisticsStorage.TABLE + " GROUP BY object_id, generation");
            for (List<String> row : payloads) {
                long id = Long.parseLong(row.get(0));
                long generation = Long.parseLong(row.get(1));
                // Read the current manifest after the active-job check: publication precedes removing the job.
                if (active(id, generation) || registry.snapshot().stream()
                        .anyMatch(meta -> meta.getId() == id && meta.getGeneration() == generation)) {
                    continue;
                }
                checkLeader();
                storage.delete(context, id, generation);
                if (++cleaned >= 64) {
                    break;
                }
            }
        } catch (Exception e) {
            LOG.warn("Cannot clean obsolete JOIN statistics data", e);
        }
    }

    private boolean active(long id, long generation) {
        Job job = jobs.get(id);
        return job != null && job.ticket.getGeneration() == generation;
    }

    @Override
    public long estimateSize() {
        return cache.estimatedSize();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return Map.of("JoinStatistics", cache.entryCount());
    }
}

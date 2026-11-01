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

import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// Runs the collection queries of one analyze task with up to statistic_collect_parallel of them at the same
// time. Each query that runs next to others gets its own ConnectContext (see
// StatisticUtils#buildCollectQueryContext), registered with AnalyzeMgr so that KILL ANALYZE stops all of them.
// The caller keeps the state that the queries share thread-safe.
public final class CollectQueryRunner {
    private static final Logger LOG = LogManager.getLogger(CollectQueryRunner.class);

    // We do not want the collection queries of all running analyze tasks to start an unbounded number of
    // threads, so they share this pool. The calling thread takes queries from its task's list too, so a task
    // makes progress when every pool thread is busy with other tasks; the pool only adds concurrency.
    private static final int MAX_POOL_THREADS = 64;
    private static final ThreadPoolExecutor POOL = createPool();

    private static ThreadPoolExecutor createPool() {
        ThreadPoolExecutor pool = ThreadPoolManager.newDaemonThreadPool(MAX_POOL_THREADS, MAX_POOL_THREADS,
                60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadPoolExecutor.AbortPolicy(),
                "statistics-collect-query-pool", true);
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @FunctionalInterface
    public interface QueryTask<T> {
        void run(T item, ConnectContext queryContext) throws Exception;
    }

    private CollectQueryRunner() {
    }

    // Calls `task` once for every item, with up to `parallelism` calls at the same time. The first exception
    // stops the run: the queries still running are cancelled, and the exception is thrown here once all
    // calls have returned. With one call at a time every item runs in the calling thread on `taskContext`.
    public static <T> void run(ConnectContext taskContext, long analyzeId, int parallelism, List<T> items,
                               QueryTask<T> task) throws Exception {
        int workers = Math.min(parallelism, items.size());
        if (workers <= 1) {
            for (T item : items) {
                task.run(item, taskContext);
            }
            return;
        }
        new Execution<>(analyzeId, items, task).run(taskContext, workers);
    }

    private static final class Execution<T> {
        private final long analyzeId;
        private final List<T> items;
        private final QueryTask<T> task;
        private final AtomicInteger nextItem = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final List<ConnectContext> contexts = new ArrayList<>();

        Execution(long analyzeId, List<T> items, QueryTask<T> task) {
            this.analyzeId = analyzeId;
            this.items = items;
            this.task = task;
        }

        void run(ConnectContext taskContext, int workers) throws Exception {
            AnalyzeMgr analyzeMgr = GlobalStateMgr.getCurrentState().getAnalyzeMgr();
            List<Worker> poolWorkers = new ArrayList<>();
            try {
                // All contexts are copied here, before any query starts, so nothing changes the task context
                // while it is copied.
                for (int i = 0; i < workers; i++) {
                    ConnectContext context = StatisticUtils.buildCollectQueryContext(taskContext);
                    contexts.add(context);
                    analyzeMgr.registerQueryConnection(analyzeId, context);
                }
                for (int i = 1; i < workers; i++) {
                    Worker worker = new Worker(contexts.get(i));
                    poolWorkers.add(worker);
                    try {
                        POOL.execute(worker);
                    } catch (RejectedExecutionException e) {
                        LOG.warn("statistics collect query pool rejected a worker of analyze {}", analyzeId, e);
                        worker.skipOrAwait();
                    }
                }
                work(contexts.get(0));
                for (Worker worker : poolWorkers) {
                    worker.skipOrAwait();
                }
            } finally {
                for (ConnectContext context : contexts) {
                    analyzeMgr.unregisterQueryConnection(analyzeId, context);
                }
            }

            Throwable t = failure.get();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof Exception) {
                throw (Exception) t;
            } else if (t != null) {
                throw new RuntimeException(t);
            }
        }

        private void work(ConnectContext context) {
            try (var guard = context.bindScope()) {
                while (failure.get() == null) {
                    int index = nextItem.getAndIncrement();
                    if (index >= items.size()) {
                        return;
                    }
                    try {
                        task.run(items.get(index), context);
                    } catch (Throwable t) {
                        abort(t, context);
                        return;
                    }
                }
            }
        }

        // The first failure fails the whole run. We do not want the other queries to keep the backends busy
        // for results nobody uses, so they are cancelled.
        private void abort(Throwable t, ConnectContext failedContext) {
            if (!failure.compareAndSet(null, t)) {
                return;
            }
            for (ConnectContext context : contexts) {
                if (context != failedContext) {
                    context.kill(false, "statistics collect query cancelled: another query of the task failed");
                }
            }
        }

        private final class Worker implements Runnable {
            private final ConnectContext context;
            private final AtomicBoolean claimed = new AtomicBoolean();
            private final CountDownLatch done = new CountDownLatch(1);

            Worker(ConnectContext context) {
                this.context = context;
            }

            @Override
            public void run() {
                if (!claimed.compareAndSet(false, true)) {
                    return;
                }
                try {
                    work(context);
                } finally {
                    done.countDown();
                }
            }

            // Called by the calling thread once it takes no more items. A worker that has not started by then
            // would find nothing to do, so it never starts; a worker that has started is waited for.
            void skipOrAwait() {
                if (claimed.compareAndSet(false, true)) {
                    return;
                }
                Uninterruptibles.awaitUninterruptibly(done);
            }
        }
    }
}

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

package com.starrocks.connector.share.iceberg;

import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.iceberg.aws.HttpClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

// HTTP connection pools for the S3 and STS clients that IcebergAwsClientFactory builds, one pool per catalog. We
// expect a new S3 client for every load of a table, because a FileIO is built for every load, and we do not want
// each of them to open a connection and make a full TLS handshake for its first request. So the S3 clients of a
// catalog take their connections from the pool of the catalog. A connection carries no identity: S3 checks the
// signature of each request, made with the credentials of the S3 client that sends it, so the clients with the
// credentials that a REST catalog vends for a table share the pool of their catalog too. Catalogs do not share a
// pool: a slow endpoint of one catalog must not take the connections of another, and the cookies that the
// endpoints of a catalog set stay in the pool of that catalog.
//
// The FE puts the pool into the properties of the catalog (IcebergConnector), and they go with the FileIO of a
// table to a CN, so a CN keeps one pool per catalog too. A REST catalog server can override any property of the
// FileIO of a table, so the pool is named by the catalog and an id that the FE makes for it and does not send to
// the server: a server cannot name the pool of another catalog. A FileIO without these properties, a table
// serialized by a FE of an older version for example, gets an HTTP client of its own, which its S3 client closes.
//
// An SDK client does not close an HTTP client it was given, so closing a FileIO or an S3 client keeps the pool. A
// pool is never closed, because a FileIO in a table cache can use it at any time. The SDK closes the connections of
// a pool after they are idle for a minute, so the pool of a dropped catalog, or of a catalog whose pool properties
// were changed, keeps none. To bound the memory of such pools a process builds at most MAX_POOLS of them; after
// that an S3 client gets an HTTP client of its own.
//
// Every S3 or STS client that gets an HTTP client of its own is counted by the reason, and the first one of a
// reason in a minute is logged, since such a client opens connections and makes TLS handshakes of its own.
public final class SharedS3HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(SharedS3HttpClient.class);

    public static final String POOL_CATALOG = "starrocks.s3-http-pool.catalog";
    public static final String POOL_ID = "starrocks.s3-http-pool.id";
    // The names Iceberg gives these settings of its own Apache HTTP client.
    public static final String MAX_CONNECTIONS = HttpClientProperties.APACHE_MAX_CONNECTIONS;
    public static final String CONNECTION_TIME_TO_LIVE_MS = HttpClientProperties.APACHE_CONNECTION_TIME_TO_LIVE_MS;

    public static final int DEFAULT_MAX_CONNECTIONS = 256;
    public static final int MAX_CONNECTIONS_LIMIT = 4096;
    static final int MAX_POOLS = 256;
    // MAX_POOLS; a test lowers it.
    static volatile int maxPools = MAX_POOLS;
    private static final long UNPOOLED_LOG_INTERVAL_MS = 60_000;

    private static final ConcurrentHashMap<PoolKey, Pool> POOLS = new ConcurrentHashMap<>();
    // Keyed by the catalog name, so the pools that a catalog had over time report as one.
    private static final ConcurrentHashMap<String, PoolMetrics> METRICS = new ConcurrentHashMap<>();
    // Guarded by SharedS3HttpClient.class.
    private static Consumer<PoolMetrics> metricsListener;

    // Why an S3 or STS client has an HTTP client of its own.
    public enum UnpooledReason {
        // The properties of the FileIO name no pool: a table serialized by a FE of an older version, for example.
        NO_POOL,
        // A pool setting is bad; the FE checks them when a catalog is created, so it came from a REST catalog server.
        BAD_SETTING,
        // The process already built MAX_POOLS pools.
        POOL_LIMIT;

        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final EnumMap<UnpooledReason, AtomicLong> UNPOOLED_CLIENTS = new EnumMap<>(UnpooledReason.class);
    private static final EnumMap<UnpooledReason, AtomicLong> UNPOOLED_LOG_MS = new EnumMap<>(UnpooledReason.class);

    static {
        for (UnpooledReason reason : UnpooledReason.values()) {
            UNPOOLED_CLIENTS.put(reason, new AtomicLong());
            UNPOOLED_LOG_MS.put(reason, new AtomicLong());
        }
    }

    private SharedS3HttpClient() {
    }

    public static int parseMaxConnections(String value) {
        int maxConnections;
        try {
            maxConnections = Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            maxConnections = 0;
        }
        if (maxConnections < 1 || maxConnections > MAX_CONNECTIONS_LIMIT) {
            throw new IllegalArgumentException(
                    MAX_CONNECTIONS + " must be an integer from 1 to " + MAX_CONNECTIONS_LIMIT + ": " + value);
        }
        return maxConnections;
    }

    // 0 is no limit.
    public static long parseConnectionTimeToLiveMs(String value) {
        long timeToLiveMs;
        try {
            timeToLiveMs = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            timeToLiveMs = -1;
        }
        if (timeToLiveMs < 0) {
            throw new IllegalArgumentException(
                    CONNECTION_TIME_TO_LIVE_MS + " must be a non-negative integer of milliseconds: " + value);
        }
        return timeToLiveMs;
    }

    // Returns null when the properties name no pool. Throws IllegalArgumentException for a bad setting.
    public static PoolKey poolKey(Map<String, String> properties) {
        String catalog = properties.get(POOL_CATALOG);
        String id = properties.get(POOL_ID);
        if (catalog == null || catalog.isEmpty() || id == null || id.isEmpty()) {
            return null;
        }
        String maxConnections = properties.get(MAX_CONNECTIONS);
        String timeToLiveMs = properties.get(CONNECTION_TIME_TO_LIVE_MS);
        return new PoolKey(catalog, id,
                maxConnections == null ? DEFAULT_MAX_CONNECTIONS : parseMaxConnections(maxConnections),
                timeToLiveMs == null ? 0 : parseConnectionTimeToLiveMs(timeToLiveMs), 0);
    }

    // The pool of an S3 or STS client built by a factory whose properties gave this key, or the error of a bad
    // setting; the catalog is null when the properties name none. Returns null when the client gets an HTTP client
    // of its own, and counts and logs that.
    public static Pool poolForClient(PoolKey key, String catalog, String settingError) {
        if (key != null) {
            Pool pool = pool(key);
            if (pool == null) {
                recordUnpooled(UnpooledReason.POOL_LIMIT, catalog,
                        "the process already built " + POOLS.size() + " pools, the most it builds");
            }
            return pool;
        }
        if (settingError != null) {
            recordUnpooled(UnpooledReason.BAD_SETTING, catalog, settingError);
        } else {
            recordUnpooled(UnpooledReason.NO_POOL, catalog, "the properties of its FileIO name no pool");
        }
        return null;
    }

    private static void recordUnpooled(UnpooledReason reason, String catalog, String detail) {
        long clients = UNPOOLED_CLIENTS.get(reason).incrementAndGet();
        AtomicLong lastLogMs = UNPOOLED_LOG_MS.get(reason);
        long now = System.currentTimeMillis();
        long last = lastLogMs.get();
        if (now - last >= UNPOOLED_LOG_INTERVAL_MS && lastLogMs.compareAndSet(last, now)) {
            LOG.warn("An S3 client of catalog {} has an HTTP client of its own, not a pool of the catalog: {}."
                            + " Since start: {} S3 and STS clients without a pool for this reason ({}), {} pools built",
                    catalog == null ? "<unknown>" : catalog, detail, clients, reason.label(), POOLS.size());
        }
    }

    public static long unpooledClients(UnpooledReason reason) {
        return UNPOOLED_CLIENTS.get(reason).get();
    }

    public static int poolCount() {
        return POOLS.size();
    }

    // Returns null when the process already has maxPools pools.
    public static Pool pool(PoolKey key) {
        Pool pool = POOLS.get(key);
        if (pool != null) {
            return pool;
        }
        synchronized (SharedS3HttpClient.class) {
            pool = POOLS.get(key);
            if (pool != null) {
                return pool;
            }
            if (POOLS.size() >= maxPools) {
                return null;
            }
            PoolMetrics metrics = METRICS.get(key.catalog);
            if (metrics == null) {
                metrics = new PoolMetrics(key.catalog);
                METRICS.put(key.catalog, metrics);
                if (metricsListener != null) {
                    metricsListener.accept(metrics);
                }
            }
            ApacheHttpClient.Builder builder = ApacheHttpClient.builder()
                    .maxConnections(key.maxConnections)
                    // Without a limit a busy pool keeps its connections, and with them the S3 addresses resolved
                    // when they were opened. A limit makes the pool open new connections, which resolve again.
                    .connectionTimeToLive(Duration.ofMillis(key.connectionTimeToLiveMs));
            if (key.connectionAcquisitionTimeoutMs > 0) {
                builder.connectionAcquisitionTimeout(Duration.ofMillis(key.connectionAcquisitionTimeoutMs));
            }
            pool = new Pool(new TimeoutCountingHttpClient(builder.build(), metrics), metrics);
            POOLS.put(key, pool);
            LOG.info("Built the S3 HTTP pool of catalog {} with max connections {} and connection time to live {} ms",
                    key.catalog, key.maxConnections, key.connectionTimeToLiveMs);
            return pool;
        }
    }

    // The listener gets the metrics of every catalog that has a pool, now and later; the FE exports them.
    public static synchronized void setMetricsListener(Consumer<PoolMetrics> listener) {
        metricsListener = listener;
        METRICS.values().forEach(listener);
    }

    // Serializable, as the AwsClientFactory that holds it is.
    public static final class PoolKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String catalog;
        private final String id;
        private final int maxConnections;
        private final long connectionTimeToLiveMs;
        // 0 is the SDK default; only tests set it.
        private final long connectionAcquisitionTimeoutMs;

        PoolKey(String catalog, String id, int maxConnections, long connectionTimeToLiveMs,
                long connectionAcquisitionTimeoutMs) {
            this.catalog = catalog;
            this.id = id;
            this.maxConnections = maxConnections;
            this.connectionTimeToLiveMs = connectionTimeToLiveMs;
            this.connectionAcquisitionTimeoutMs = connectionAcquisitionTimeoutMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PoolKey)) {
                return false;
            }
            PoolKey other = (PoolKey) o;
            return maxConnections == other.maxConnections && connectionTimeToLiveMs == other.connectionTimeToLiveMs
                    && connectionAcquisitionTimeoutMs == other.connectionAcquisitionTimeoutMs
                    && catalog.equals(other.catalog) && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(catalog, id, maxConnections, connectionTimeToLiveMs, connectionAcquisitionTimeoutMs);
        }
    }

    public static final class Pool {
        private final SdkHttpClient httpClient;
        private final PoolMetrics metrics;

        private Pool(SdkHttpClient httpClient, PoolMetrics metrics) {
            this.httpClient = httpClient;
            this.metrics = metrics;
        }

        public SdkHttpClient httpClient() {
            return httpClient;
        }

        public PoolMetrics metrics() {
            return metrics;
        }
    }

    // The Apache client throws ConnectionPoolTimeoutException from call() when a request does not get a connection
    // within the acquisition timeout, so this counts every attempt that timed out, retries included.
    private static final class TimeoutCountingHttpClient implements SdkHttpClient {
        private final SdkHttpClient delegate;
        private final PoolMetrics metrics;

        private TimeoutCountingHttpClient(SdkHttpClient delegate, PoolMetrics metrics) {
            this.delegate = delegate;
            this.metrics = metrics;
        }

        @Override
        public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
            ExecutableHttpRequest executable = delegate.prepareRequest(request);
            return new ExecutableHttpRequest() {
                @Override
                public HttpExecuteResponse call() throws IOException {
                    try {
                        return executable.call();
                    } catch (ConnectionPoolTimeoutException e) {
                        metrics.acquireTimeouts.incrementAndGet();
                        throw e;
                    }
                }

                @Override
                public void abort() {
                    executable.abort();
                }
            };
        }

        @Override
        public String clientName() {
            return delegate.clientName();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    // The SDK reports the state of the pool after a call that got its response, so the gauges show the pool as the
    // last such call of any pool of the catalog saw it. The counters only grow. The FE exports them as metrics; a CN
    // does not export the metrics of its JVM, so a slow acquire is also written to the log, at most once a minute
    // per catalog.
    public static final class PoolMetrics implements MetricPublisher {
        // A request that waits this long or longer for a connection counts as a slow acquire.
        static final Duration SLOW_ACQUIRE = Duration.ofMillis(100);
        static final long SLOW_ACQUIRE_LOG_INTERVAL_MS = 60_000;

        private final String catalog;
        private volatile int max;
        private volatile int leased;
        private volatile int available;
        private volatile int pending;
        private final AtomicLong acquires = new AtomicLong();
        private final AtomicLong acquireWaitMicros = new AtomicLong();
        private final AtomicLong slowAcquires = new AtomicLong();
        private final AtomicLong acquireTimeouts = new AtomicLong();
        private final AtomicLong apiCalls = new AtomicLong();
        private final AtomicLong failedApiCalls = new AtomicLong();
        private final AtomicLong retries = new AtomicLong();
        private final AtomicLong lastSlowAcquireLogMs = new AtomicLong();

        PoolMetrics(String catalog) {
            this.catalog = catalog;
        }

        @Override
        public void publish(MetricCollection collection) {
            Duration slowestAcquire = record(collection, Duration.ZERO);
            if (slowestAcquire.compareTo(SLOW_ACQUIRE) >= 0) {
                logSlowAcquire(slowestAcquire, System.currentTimeMillis());
            }
        }

        // Returns the longest wait for a connection in the collection and its children, at least slowest.
        private Duration record(MetricCollection collection, Duration slowest) {
            for (MetricRecord<?> metricRecord : collection) {
                Object value = metricRecord.value();
                if (value == null) {
                    continue;
                }
                if (metricRecord.metric() == HttpMetric.MAX_CONCURRENCY) {
                    max = (Integer) value;
                } else if (metricRecord.metric() == HttpMetric.LEASED_CONCURRENCY) {
                    leased = (Integer) value;
                } else if (metricRecord.metric() == HttpMetric.AVAILABLE_CONCURRENCY) {
                    available = (Integer) value;
                } else if (metricRecord.metric() == HttpMetric.PENDING_CONCURRENCY_ACQUIRES) {
                    pending = (Integer) value;
                } else if (metricRecord.metric() == HttpMetric.CONCURRENCY_ACQUIRE_DURATION) {
                    Duration wait = (Duration) value;
                    recordAcquire(wait);
                    if (wait.compareTo(slowest) > 0) {
                        slowest = wait;
                    }
                } else if (metricRecord.metric() == CoreMetric.API_CALL_SUCCESSFUL) {
                    apiCalls.incrementAndGet();
                    if (!((Boolean) value)) {
                        failedApiCalls.incrementAndGet();
                    }
                } else if (metricRecord.metric() == CoreMetric.RETRY_COUNT) {
                    retries.addAndGet((Integer) value);
                }
            }
            for (MetricCollection child : collection.children()) {
                slowest = record(child, slowest);
            }
            return slowest;
        }

        // Returns whether it wrote the log line, which happens at most once per SLOW_ACQUIRE_LOG_INTERVAL_MS.
        boolean logSlowAcquire(Duration wait, long nowMs) {
            long last = lastSlowAcquireLogMs.get();
            if (nowMs - last < SLOW_ACQUIRE_LOG_INTERVAL_MS || !lastSlowAcquireLogMs.compareAndSet(last, nowMs)) {
                return false;
            }
            LOG.warn("S3 HTTP pool of catalog {} is busy: a request waited {} ms for a connection. Pool max {},"
                            + " leased {}, available {}, pending {}. Since start: acquires {}, wait {} ms,"
                            + " slow acquires {}, acquire timeouts {}, api calls {}, failed {}, retries {}",
                    catalog, wait.toMillis(), max, leased, available, pending, acquires.get(),
                    acquireWaitMicros.get() / 1000, slowAcquires.get(), acquireTimeouts.get(), apiCalls.get(),
                    failedApiCalls.get(), retries.get());
            return true;
        }

        // The SDK reports the wait also when it ends in a timeout, so a timed-out request is a slow acquire too.
        void recordAcquire(Duration wait) {
            acquires.incrementAndGet();
            acquireWaitMicros.addAndGet(wait.toNanos() / 1000);
            if (wait.compareTo(SLOW_ACQUIRE) >= 0) {
                slowAcquires.incrementAndGet();
            }
        }

        @Override
        public void close() {
        }

        public String catalog() {
            return catalog;
        }

        public int max() {
            return max;
        }

        public int leased() {
            return leased;
        }

        public int available() {
            return available;
        }

        public int pending() {
            return pending;
        }

        public long acquires() {
            return acquires.get();
        }

        public long acquireWaitMicros() {
            return acquireWaitMicros.get();
        }

        public long slowAcquires() {
            return slowAcquires.get();
        }

        public long acquireTimeouts() {
            return acquireTimeouts.get();
        }

        public long apiCalls() {
            return apiCalls.get();
        }

        public long failedApiCalls() {
            return failedApiCalls.get();
        }

        public long retries() {
            return retries.get();
        }
    }
}

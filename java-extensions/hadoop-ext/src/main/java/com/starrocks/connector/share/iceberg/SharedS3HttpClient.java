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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

// One HTTP client, with one connection pool, for every S3 and STS client that IcebergAwsClientFactory builds in
// this process. We expect a new S3 client for every load of a table, because a FileIO is built for every load, and
// we do not want each of them to open a connection and make a full TLS handshake for its first request. A
// connection carries no identity: S3 checks the signature of each request, made with the credentials of the S3
// client that sends it. So clients with different credentials, regions and endpoints share the pool, and the pool
// keeps the connections of each host apart. An SDK client does not close an HTTP client it was given, so closing
// a FileIO or an S3 client keeps the pool.
public final class SharedS3HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(SharedS3HttpClient.class);

    // The FE sets both from its config. A CN does not call configure, and its metadata scanners share this pool, so
    // the default size is the FE default and not the 50 connections of one SDK client.
    private static volatile int maxConnections = 256;
    private static volatile long connectionTimeToLiveSeconds = 0;
    private static volatile SdkHttpClient httpClient;

    private static final PoolMetrics METRICS = new PoolMetrics();

    private SharedS3HttpClient() {
    }

    // The pool is built on first use with the values set here before it; a later call does not change it.
    public static synchronized void configure(int maxConnections, long connectionTimeToLiveSeconds) {
        if (httpClient != null) {
            if (maxConnections != SharedS3HttpClient.maxConnections
                    || connectionTimeToLiveSeconds != SharedS3HttpClient.connectionTimeToLiveSeconds) {
                LOG.warn("The shared S3 HTTP client is built with max connections {} and connection time to live {} s;"
                                + " max connections {} and time to live {} s take effect after a restart",
                        SharedS3HttpClient.maxConnections, SharedS3HttpClient.connectionTimeToLiveSeconds,
                        maxConnections, connectionTimeToLiveSeconds);
            }
            return;
        }
        SharedS3HttpClient.maxConnections = Math.max(1, maxConnections);
        SharedS3HttpClient.connectionTimeToLiveSeconds = Math.max(0, connectionTimeToLiveSeconds);
    }

    public static SdkHttpClient get() {
        SdkHttpClient client = httpClient;
        if (client != null) {
            return client;
        }
        synchronized (SharedS3HttpClient.class) {
            if (httpClient == null) {
                // Without a limit a busy pool keeps its connections, and with them the S3 addresses resolved when
                // they were opened. A positive time to live makes the pool open new connections, which resolve the
                // S3 endpoint again.
                httpClient = ApacheHttpClient.builder()
                        .maxConnections(maxConnections)
                        .connectionTimeToLive(Duration.ofSeconds(connectionTimeToLiveSeconds))
                        .build();
                LOG.info("Built the shared S3 HTTP client with max connections {} and connection time to live {} s",
                        maxConnections, connectionTimeToLiveSeconds);
            }
            return httpClient;
        }
    }

    public static MetricPublisher metricPublisher() {
        return METRICS;
    }

    public static PoolMetrics metrics() {
        return METRICS;
    }

    // The SDK reports the state of the pool when a request takes a connection, so the gauges show the pool as the
    // last request saw it. The counters only grow. The FE exports them as metrics; a CN does not export the metrics
    // of its JVM, so a slow acquire is also written to the log, at most once a minute.
    public static final class PoolMetrics implements MetricPublisher {
        // A request that waits longer than this for a connection counts as a slow acquire.
        static final Duration SLOW_ACQUIRE = Duration.ofMillis(100);
        static final long SLOW_ACQUIRE_LOG_INTERVAL_MS = 60_000;
        static final Duration ACQUIRE_TIMEOUT =
                SdkHttpConfigurationOption.GLOBAL_HTTP_DEFAULTS.get(SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT);

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
            LOG.warn("Shared S3 HTTP pool is busy: a request waited {} ms for a connection. Pool max {}, leased {},"
                            + " available {}, pending {}. Since start: acquires {}, wait {} ms, slow acquires {},"
                            + " acquire timeouts {}, api calls {}, failed {}, retries {}",
                    wait.toMillis(), max, leased, available, pending, acquires.get(),
                    acquireWaitMicros.get() / 1000, slowAcquires.get(), acquireTimeouts.get(), apiCalls.get(),
                    failedApiCalls.get(), retries.get());
            return true;
        }

        // The SDK reports the wait also when it ends in a timeout, so a wait as long as the timeout is one.
        void recordAcquire(Duration wait) {
            acquires.incrementAndGet();
            acquireWaitMicros.addAndGet(wait.toNanos() / 1000);
            if (wait.compareTo(SLOW_ACQUIRE) >= 0) {
                slowAcquires.incrementAndGet();
            }
            if (wait.compareTo(ACQUIRE_TIMEOUT) >= 0) {
                acquireTimeouts.incrementAndGet();
            }
        }

        @Override
        public void close() {
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

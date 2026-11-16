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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;

public class SharedS3HttpClientTest {

    // An HTTP server that answers every request with 200 on a kept-alive connection and counts the connections, so a
    // test sees whether two requests shared one. It holds the answer to a request while held is set.
    private static final class Server implements AutoCloseable {
        private final ServerSocket socket;
        private final AtomicInteger connections = new AtomicInteger();
        private final AtomicInteger requests = new AtomicInteger();
        private volatile CountDownLatch held;
        private volatile CountDownLatch received;

        Server() throws IOException {
            socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(() -> {
                while (!socket.isClosed()) {
                    try {
                        Socket connection = socket.accept();
                        connections.incrementAndGet();
                        Thread handler = new Thread(() -> serve(connection));
                        handler.setDaemon(true);
                        handler.start();
                    } catch (IOException e) {
                        return;
                    }
                }
            });
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private void serve(Socket connection) {
            try (connection) {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), StandardCharsets.ISO_8859_1));
                OutputStream out = connection.getOutputStream();
                while (true) {
                    String line = in.readLine();
                    if (line == null) {
                        return;
                    }
                    while (!line.isEmpty()) {
                        line = in.readLine();
                        if (line == null) {
                            return;
                        }
                    }
                    requests.incrementAndGet();
                    CountDownLatch receivedLatch = received;
                    if (receivedLatch != null) {
                        receivedLatch.countDown();
                    }
                    CountDownLatch heldLatch = held;
                    if (heldLatch != null) {
                        heldLatch.await(30, TimeUnit.SECONDS);
                    }
                    out.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
                    out.flush();
                }
            } catch (IOException | InterruptedException e) {
                // the client closed the connection
            }
        }

        String endpoint() {
            return "http://127.0.0.1:" + socket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private Server server;

    @BeforeEach
    public void startServer() throws IOException {
        server = new Server();
    }

    @AfterEach
    public void stopServer() throws IOException {
        server.close();
    }

    private Map<String, String> properties(String accessKey) {
        Map<String, String> properties = new HashMap<>();
        properties.put(AWS_S3_ACCESS_KEY, accessKey);
        properties.put(AWS_S3_SECRET_KEY, "secret");
        properties.put(AWS_S3_REGION, "us-east-1");
        properties.put(AWS_S3_ENDPOINT, server.endpoint());
        properties.put(AWS_S3_ENABLE_PATH_STYLE_ACCESS, "true");
        return properties;
    }

    private Map<String, String> catalogProperties(String catalog, String id, String accessKey) {
        Map<String, String> properties = properties(accessKey);
        properties.put(SharedS3HttpClient.POOL_CATALOG, catalog);
        properties.put(SharedS3HttpClient.POOL_ID, id);
        properties.put(SharedS3HttpClient.MAX_CONNECTIONS, "8");
        return properties;
    }

    private static S3Client s3Client(Map<String, String> properties) {
        IcebergAwsClientFactory factory = new IcebergAwsClientFactory();
        factory.initialize(properties);
        return factory.s3();
    }

    private static void head(S3Client client) {
        client.headObject(HeadObjectRequest.builder().bucket("bucket").key("metadata/v1.json").build());
    }

    private static void headAndClose(Map<String, String> properties) {
        try (S3Client client = s3Client(properties)) {
            head(client);
        }
    }

    private static String unique(String name) {
        return name + "-" + UUID.randomUUID();
    }

    // The S3 clients of a catalog, with different credentials as a REST catalog vends them for tables, reuse a
    // connection after the one that opened it is closed, as a closed or collected FileIO is. Another catalog, or the
    // same catalog name with another id, opens a connection of its own.
    @Test
    public void clientsOfACatalogShareItsPoolAndCatalogsDoNot() {
        String catalog = unique("a");
        String id = unique("id");
        headAndClose(catalogProperties(catalog, id, "key-a"));
        headAndClose(catalogProperties(catalog, id, "key-b"));
        Assertions.assertEquals(2, server.requests.get());
        Assertions.assertEquals(1, server.connections.get());

        headAndClose(catalogProperties(unique("b"), id, "key-a"));
        Assertions.assertEquals(2, server.connections.get());
        headAndClose(catalogProperties(catalog, unique("id"), "key-a"));
        Assertions.assertEquals(3, server.connections.get());

        SharedS3HttpClient.PoolKey key = SharedS3HttpClient.poolKey(catalogProperties(catalog, id, "key-c"));
        SharedS3HttpClient.Pool pool = SharedS3HttpClient.pool(key);
        Assertions.assertSame(pool, SharedS3HttpClient.pool(SharedS3HttpClient.poolKey(
                catalogProperties(catalog, id, "key-d"))));
        // The metrics of a catalog are the sum of its pools: the one with the second id made one call.
        SharedS3HttpClient.PoolMetrics metrics = pool.metrics();
        Assertions.assertEquals(catalog, metrics.catalog());
        Assertions.assertEquals(3, metrics.apiCalls());
        Assertions.assertEquals(3, metrics.acquires());
        Assertions.assertEquals(0, metrics.acquireTimeouts());
        Assertions.assertEquals(8, metrics.max());
        Assertions.assertEquals(0, metrics.leased());
    }

    // A FileIO whose properties name no pool, or a bad pool setting from a REST catalog server, gives each S3
    // client an HTTP client of its own, as before the pools, and each such client is counted by the reason.
    @Test
    public void withoutAPoolEachClientHasItsOwnConnections() {
        long noPool = SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.NO_POOL);
        long badSetting = SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.BAD_SETTING);
        Assertions.assertNull(SharedS3HttpClient.poolKey(properties("key-a")));
        headAndClose(properties("key-a"));
        headAndClose(properties("key-a"));
        Assertions.assertEquals(2, server.connections.get());
        Assertions.assertEquals(noPool + 2, SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.NO_POOL));

        Map<String, String> bad = catalogProperties(unique("c"), unique("id"), "key-a");
        bad.put(SharedS3HttpClient.MAX_CONNECTIONS, "0");
        headAndClose(bad);
        headAndClose(bad);
        Assertions.assertEquals(4, server.connections.get());
        Assertions.assertEquals(badSetting + 2,
                SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.BAD_SETTING));
    }

    // Past the limit of pools a new catalog's clients have HTTP clients of their own and are counted, while a
    // catalog that has a pool keeps using it.
    @Test
    public void pastThePoolLimitClientsHaveTheirOwnConnections() {
        Map<String, String> pooled = catalogProperties(unique("f"), unique("id"), "key-a");
        headAndClose(pooled);
        long poolLimit = SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.POOL_LIMIT);
        int pools = SharedS3HttpClient.poolCount();
        SharedS3HttpClient.maxPools = pools;
        try {
            Map<String, String> unpooled = catalogProperties(unique("g"), unique("id"), "key-a");
            headAndClose(unpooled);
            headAndClose(unpooled);
            headAndClose(pooled);
            Assertions.assertEquals(3, server.connections.get());
            Assertions.assertEquals(pools, SharedS3HttpClient.poolCount());
            Assertions.assertEquals(poolLimit + 2,
                    SharedS3HttpClient.unpooledClients(SharedS3HttpClient.UnpooledReason.POOL_LIMIT));
        } finally {
            SharedS3HttpClient.maxPools = SharedS3HttpClient.MAX_POOLS;
        }
    }

    // A request that finds the only connection of the pool taken waits for the acquisition timeout and fails; the
    // pool counts it as a timeout, and as a slow acquire, since it waited that long.
    @Test
    public void aRequestThatTimesOutWaitingForAConnectionIsCounted() throws Exception {
        SharedS3HttpClient.Pool pool = SharedS3HttpClient.pool(
                new SharedS3HttpClient.PoolKey(unique("d"), unique("id"), 1, 0, 300));
        try (S3Client client = S3Client.builder()
                .httpClient(pool.httpClient())
                .overrideConfiguration(c -> c.addMetricPublisher(pool.metrics())
                        .retryStrategy(AwsRetryStrategy.doNotRetry()))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")))
                .region(Region.US_EAST_1)
                .endpointOverride(URI.create(server.endpoint()))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            CountDownLatch held = new CountDownLatch(1);
            CountDownLatch received = new CountDownLatch(1);
            server.held = held;
            server.received = received;
            Thread holder = new Thread(() -> head(client));
            holder.start();
            Assertions.assertTrue(received.await(10, TimeUnit.SECONDS));

            long start = System.nanoTime();
            SdkClientException e = Assertions.assertThrows(SdkClientException.class, () -> head(client));
            Duration waited = Duration.ofNanos(System.nanoTime() - start);
            held.countDown();
            holder.join(10_000);

            Assertions.assertTrue(waited.compareTo(Duration.ofMillis(250)) >= 0, waited.toString());
            Assertions.assertEquals(1, pool.metrics().acquireTimeouts(), e.toString());
            Assertions.assertEquals(1, pool.metrics().slowAcquires());
            Assertions.assertEquals(1, pool.metrics().failedApiCalls());
        } finally {
            server.held = null;
            server.received = null;
        }
    }

    @Test
    public void badPoolSettingsAreRejected() {
        Assertions.assertEquals(1, SharedS3HttpClient.parseMaxConnections(" 1 "));
        Assertions.assertEquals(SharedS3HttpClient.MAX_CONNECTIONS_LIMIT,
                SharedS3HttpClient.parseMaxConnections(String.valueOf(SharedS3HttpClient.MAX_CONNECTIONS_LIMIT)));
        for (String value : new String[] {"0", "-1", "4097", "x", ""}) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> SharedS3HttpClient.parseMaxConnections(value),
                    value);
        }
        Assertions.assertEquals(0, SharedS3HttpClient.parseConnectionTimeToLiveMs("0"));
        Assertions.assertEquals(Long.MAX_VALUE,
                SharedS3HttpClient.parseConnectionTimeToLiveMs(String.valueOf(Long.MAX_VALUE)));
        for (String value : new String[] {"-1", "1.5", "x", "9223372036854775808"}) {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> SharedS3HttpClient.parseConnectionTimeToLiveMs(value), value);
        }
    }

    // The longest time to live builds a pool; the SDK and the Apache pool do not overflow on it.
    @Test
    public void theLongestTimeToLiveBuildsAPool() {
        Map<String, String> properties = catalogProperties(unique("e"), unique("id"), "key-a");
        properties.put(SharedS3HttpClient.CONNECTION_TIME_TO_LIVE_MS, String.valueOf(Long.MAX_VALUE));
        headAndClose(properties);
        headAndClose(properties);
        Assertions.assertEquals(1, server.connections.get());
    }

    @Test
    public void acquireWaitsAreCounted() {
        SharedS3HttpClient.PoolMetrics metrics = new SharedS3HttpClient.PoolMetrics("catalog");
        metrics.recordAcquire(Duration.ofMillis(1));
        metrics.recordAcquire(SharedS3HttpClient.PoolMetrics.SLOW_ACQUIRE);
        Assertions.assertEquals(2, metrics.acquires());
        Assertions.assertEquals(1, metrics.slowAcquires());
        Assertions.assertEquals(1_000 + 100_000, metrics.acquireWaitMicros());
    }

    @Test
    public void slowAcquireIsLoggedOncePerInterval() {
        SharedS3HttpClient.PoolMetrics metrics = new SharedS3HttpClient.PoolMetrics("catalog");
        long interval = SharedS3HttpClient.PoolMetrics.SLOW_ACQUIRE_LOG_INTERVAL_MS;
        long start = interval * 10;
        Assertions.assertTrue(metrics.logSlowAcquire(Duration.ofMillis(200), start));
        Assertions.assertFalse(metrics.logSlowAcquire(Duration.ofMillis(200), start + interval - 1));
        Assertions.assertTrue(metrics.logSlowAcquire(Duration.ofMillis(200), start + interval));
    }
}

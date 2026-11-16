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

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;

public class SharedS3HttpClientTest {

    private static S3Client s3Client(int port, String accessKey) {
        IcebergAwsClientFactory factory = new IcebergAwsClientFactory();
        factory.initialize(Map.of(
                AWS_S3_ACCESS_KEY, accessKey,
                AWS_S3_SECRET_KEY, "secret",
                AWS_S3_REGION, "us-east-1",
                AWS_S3_ENDPOINT, "http://127.0.0.1:" + port,
                AWS_S3_ENABLE_PATH_STYLE_ACCESS, "true"));
        return factory.s3();
    }

    private static void head(S3Client client) {
        client.headObject(HeadObjectRequest.builder().bucket("bucket").key("metadata/v1.json").build());
    }

    // Clients with different credentials take their connections from one pool, and closing a client, as a closed
    // or collected FileIO does, leaves the pool to the others.
    @Test
    public void closingAClientKeepsTheSharedPool() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.incrementAndGet();
            exchange.getResponseHeaders().add("Content-Length", "5");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            SharedS3HttpClient.PoolMetrics metrics = SharedS3HttpClient.metrics();
            long acquiresBefore = metrics.acquires();
            long callsBefore = metrics.apiCalls();

            S3Client first = s3Client(port, "key-a");
            head(first);
            first.close();
            S3Client second = s3Client(port, "key-b");
            head(second);
            second.close();

            Assertions.assertEquals(2, requests.get());
            Assertions.assertEquals(2, metrics.acquires() - acquiresBefore);
            Assertions.assertEquals(2, metrics.apiCalls() - callsBefore);
            Assertions.assertTrue(metrics.max() > 0);
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void acquireWaitsAreCounted() {
        SharedS3HttpClient.PoolMetrics metrics = new SharedS3HttpClient.PoolMetrics();
        metrics.recordAcquire(Duration.ofMillis(1));
        metrics.recordAcquire(SharedS3HttpClient.PoolMetrics.SLOW_ACQUIRE);
        metrics.recordAcquire(SharedS3HttpClient.PoolMetrics.ACQUIRE_TIMEOUT);
        Assertions.assertEquals(3, metrics.acquires());
        Assertions.assertEquals(2, metrics.slowAcquires());
        Assertions.assertEquals(1, metrics.acquireTimeouts());
        Assertions.assertEquals(1_000 + 100_000 + SharedS3HttpClient.PoolMetrics.ACQUIRE_TIMEOUT.toNanos() / 1000,
                metrics.acquireWaitMicros());
    }

    @Test
    public void slowAcquireIsLoggedOncePerInterval() {
        SharedS3HttpClient.PoolMetrics metrics = new SharedS3HttpClient.PoolMetrics();
        long interval = SharedS3HttpClient.PoolMetrics.SLOW_ACQUIRE_LOG_INTERVAL_MS;
        long start = interval * 10;
        Assertions.assertTrue(metrics.logSlowAcquire(Duration.ofMillis(200), start));
        Assertions.assertFalse(metrics.logSlowAcquire(Duration.ofMillis(200), start + interval - 1));
        Assertions.assertTrue(metrics.logSlowAcquire(Duration.ofMillis(200), start + interval));
    }
}

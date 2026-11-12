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

import com.starrocks.connector.share.iceberg.SerializableTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class DeserializedTableCacheTest {

    private static final int NO_EVICTION = 1024;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    // #2 single-flight: many threads racing on the same key must trigger exactly one load and all
    // share the one resulting instance. The barrier + delayed loader widen the miss window so a
    // broken single-flight would actually be observed (multiple loads / distinct instances).
    @Test
    public void testSingleFlightDeserializesOncePerKey() throws Exception {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        int threads = 64;
        AtomicInteger loadCount = new AtomicInteger();
        CyclicBarrier gate = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Table>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    return cache.acquire("same-payload", () -> {
                        loadCount.incrementAndGet();
                        Thread.sleep(20);
                        return makeTable();
                    }).table();
                }));
            }
            Table first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<Table> future : futures) {
                assertSame(first, future.get(10, TimeUnit.SECONDS), "all callers must share one instance");
            }
            assertEquals(1, loadCount.get(), "loader must run exactly once per key");
        } finally {
            pool.shutdownNow();
        }
    }

    // #3 isolation: distinct payloads must not collide on the hashed key; a cache hit must not re-run
    // the loader and must return the instance stored for that exact payload.
    @Test
    public void testDistinctPayloadsAreIsolated() {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        Table a = cache.acquire("payload-A", DeserializedTableCacheTest::makeTable).table();
        Table b = cache.acquire("payload-B", DeserializedTableCacheTest::makeTable).table();
        assertNotSame(a, b);

        Table aHit = cache.acquire("payload-A", () -> {
            throw new AssertionError("loader must not run on a cache hit");
        }).table();
        assertSame(a, aHit);
    }

    // #4 eviction: with maxEntries=1 a second distinct payload evicts the first; the next access to
    // the first reloads it (loader runs again) rather than returning a corrupt or stale handle.
    @Test
    public void testEntryReloadsAfterSizeEviction() {
        DeserializedTableCache cache = new DeserializedTableCache(600, 1);
        AtomicInteger keepLoads = new AtomicInteger();

        cache.acquire("keep", () -> {
            keepLoads.incrementAndGet();
            return makeTable();
        });
        // maxEntries=1, so inserting another distinct payload evicts "keep" (LRU)
        cache.acquire("other", DeserializedTableCacheTest::makeTable);
        cache.acquire("keep", () -> {
            keepLoads.incrementAndGet();
            return makeTable();
        });

        assertEquals(2, keepLoads.get(), "evicted entry must be reloaded on next access");
    }

    // public acquire() path: a real serialized payload is deserialized once and then served from cache.
    @Test
    public void testGetDeserializesAndCachesRealPayload() {
        String payload = SerializationUtil.serializeToBase64(makeTable());
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        Table first = cache.acquire(payload).table();
        assertNotNull(first.schema());
        assertSame(first, cache.acquire(payload).table(), "second acquire must hit the cache");
    }

    // The FileIO of the shared table is closed once the entry left the cache and the last lease is closed, not
    // before: the other scan tasks of the table still read through it.
    @Test
    public void testFileIOClosedWhenRemovedAndNoLeaseHoldsIt() {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        CountingFileIO io = new CountingFileIO();
        DeserializedTableCache.Lease first = cache.acquire("payload", () -> makeTable(io));
        DeserializedTableCache.Lease second = cache.acquire("payload", () -> {
            throw new AssertionError("loader must not run on a cache hit");
        });
        assertSame(first.table(), second.table());

        first.close();
        assertEquals(0, io.closes.get(), "an entry in the cache keeps its FileIO open");
        cache.invalidateAll();
        assertEquals(0, io.closes.get(), "a lease still holds the removed entry");
        second.close();
        assertEquals(1, io.closes.get(), "the last lease of a removed entry closes its FileIO");
        // closing a lease again releases nothing more
        second.close();
        first.close();
        assertEquals(1, io.closes.get());
    }

    // An entry without leases closes its FileIO as it leaves the cache, also by size eviction.
    @Test
    public void testFileIOClosedOnEvictionWithoutLease() {
        DeserializedTableCache cache = new DeserializedTableCache(600, 1);
        CountingFileIO io = new CountingFileIO();
        cache.acquire("evicted", () -> makeTable(io)).close();
        assertEquals(0, io.closes.get());
        cache.acquire("other", DeserializedTableCacheTest::makeTable).close();
        assertEquals(1, io.closes.get());
    }

    // A lookup never hands out an entry that left the cache, whose FileIO may be closed: it loads a new one.
    @Test
    public void testRemovedEntryIsNotHandedOut() {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        AtomicInteger loads = new AtomicInteger();
        CountingFileIO io = new CountingFileIO();
        Table first = cache.acquire("payload", () -> {
            loads.incrementAndGet();
            return makeTable(io);
        }).table();
        cache.invalidateAll();
        Table second = cache.acquire("payload", () -> {
            loads.incrementAndGet();
            return makeTable();
        }).table();
        assertNotSame(first, second);
        assertEquals(2, loads.get());
    }

    private static final class CountingFileIO extends InMemoryFileIO {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
            super.close();
        }
    }

    private static Table makeTable() {
        return makeTable(new InMemoryFileIO());
    }

    private static Table makeTable(FileIO io) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "tbl"), SCHEMA, PartitionSpec.unpartitioned());
        return new SerializableTable(table, io);
    }
}

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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Serializes metadata publication and DROP; collection and payload I/O run outside its lock. */
public final class JoinStatisticsRegistry {
    @FunctionalInterface
    public interface Journal {
        void append(JoinStatisticsMeta meta, boolean drop, Runnable apply);
    }

    public static final class Collection {
        private final JoinStatisticsMeta previous;
        private final long generation;
        private volatile boolean cancelled;

        private Collection(JoinStatisticsMeta previous, long generation) {
            this.previous = previous;
            this.generation = generation;
        }

        public JoinStatisticsMeta getPrevious() {
            return previous;
        }

        public long getGeneration() {
            return generation;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    private final Journal journal;
    private final Map<String, JoinStatisticsMeta> definitions = new ConcurrentHashMap<>();
    private final Map<Long, Collection> collections = new HashMap<>();

    public JoinStatisticsRegistry(Journal journal) {
        this.journal = journal;
    }

    public JoinStatisticsMeta get(String name) {
        return definitions.get(name.toLowerCase(Locale.ROOT));
    }

    public List<JoinStatisticsMeta> snapshot() {
        return List.copyOf(definitions.values());
    }

    public synchronized void create(long id, JoinStatisticsDefinition definition) {
        if (get(definition.getName()) != null) {
            throw new IllegalArgumentException("JOIN statistics already exist: " + definition.getName());
        }
        JoinStatisticsMeta meta = new JoinStatisticsMeta(id, definition);
        journal.append(meta, false, () -> definitions.put(definition.getName(), meta));
    }

    public synchronized Collection begin(String name, long generation) {
        JoinStatisticsMeta current = get(name);
        if (current == null) {
            throw new IllegalArgumentException("Unknown JOIN statistics: " + name);
        }
        if (generation <= current.getGeneration()) {
            throw new IllegalArgumentException("JOIN statistics generation must increase");
        }
        if (collections.containsKey(current.getId())) {
            throw new IllegalStateException("JOIN statistics collection is already running: " + name);
        }
        Collection collection = new Collection(current, generation);
        collections.put(current.getId(), collection);
        return collection;
    }

    /** Returns false when DROP or a leader change has revoked the collector's publication token. */
    public synchronized boolean publish(Collection collection, int parts, long bytes, String checksum, long collectedAt) {
        JoinStatisticsMeta previous = collection.previous;
        JoinStatisticsMeta current = get(previous.getDefinition().getName());
        if (collection.cancelled || current != previous || collections.get(previous.getId()) != collection) {
            return false;
        }
        JoinStatisticsMeta published = new JoinStatisticsMeta(previous.getId(), previous.getDefinition(),
                collection.generation, parts, bytes, checksum, collectedAt);
        journal.append(published, false, () -> definitions.put(previous.getDefinition().getName(), published));
        collections.remove(previous.getId());
        return true;
    }

    public synchronized void finish(Collection collection) {
        collections.remove(collection.previous.getId(), collection);
    }

    public synchronized JoinStatisticsMeta drop(String name, boolean ifExists) {
        JoinStatisticsMeta current = get(name);
        if (current == null) {
            if (!ifExists) {
                throw new IllegalArgumentException("Unknown JOIN statistics: " + name);
            }
            return null;
        }
        journal.append(current, true, () -> definitions.remove(current.getDefinition().getName(), current));
        Collection collection = collections.remove(current.getId());
        if (collection != null) {
            collection.cancelled = true;
        }
        return current;
    }

    public synchronized boolean isCollecting(long id) {
        return collections.containsKey(id);
    }

    public synchronized void cancel(long id) {
        Collection collection = collections.get(id);
        if (collection != null) {
            collection.cancelled = true;
        }
    }

    public synchronized void cancel(Collection collection) {
        if (collections.get(collection.previous.getId()) == collection) {
            collection.cancelled = true;
        }
    }

    public synchronized void revokeCollections() {
        collections.values().forEach(collection -> collection.cancelled = true);
        collections.clear();
    }

    public synchronized void replay(JoinStatisticsMeta meta, boolean drop) {
        String name = meta.getDefinition().getName();
        if (drop) {
            JoinStatisticsMeta current = get(name);
            if (current != null && current.getId() == meta.getId()) {
                definitions.remove(name);
            }
        } else {
            definitions.put(name, new JoinStatisticsMeta(meta.getId(), meta.getDefinition().immutableCopy(),
                    meta.getGeneration(), meta.getParts(), meta.getPayloadBytes(), meta.getChecksum(), meta.getCollectedAt()));
        }
    }
}

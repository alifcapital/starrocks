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

package com.starrocks.authorization.ranger;

import com.starrocks.authorization.ColumnAccessKind;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Ranger answers of one statement. The ConnectContext of the statement holds this cache.
 * <p>
 * One statement can ask Ranger the same question many times. A system table checks the same
 * database for each of its rows, and the planner reads the masks and the row filter of a table
 * twice. Every evaluation costs CPU and memory and writes one audit event. So we keep the first
 * answer and reuse it for the same question in the same statement. A reused answer writes no
 * audit event.
 * <p>
 * The key holds every input of the Ranger request that can change the answer. If we cannot copy an
 * input into the key, we do not cache the request. The rest of the answer comes from the data the
 * plugin loads from Ranger Admin: policies, tags of resources, roles and users with their groups.
 * Each of them has a version, so an answer is reused only while all four versions stay the same.
 * <p>
 * The access time is not in the key or in a version. A policy with a validity schedule or a time
 * condition can change its answer at the same versions. While such a policy is loaded, we keep an
 * answer for at most {@link Config#ranger_decision_cache_time_dependent_ttl_ms}.
 */
public final class RangerDecisionCache {
    // A statement over a system table can ask about many different objects. We do not want one
    // statement to hold an unbounded map, so at this size we stop adding and ask Ranger each time.
    static final int MAX_ENTRIES = 10_000;

    enum Kind {
        ACCESS,
        MASK,
        ROW_FILTER
    }

    /**
     * The controller is part of the key, so two Ranger services, for example the StarRocks service
     * and the Hive service of a catalog, never share an answer.
     */
    record Key(RangerAccessController controller, Kind kind, Map<String, String> resource, String ownerUser,
               String user, String host, Set<String> groups, String accessType, Set<ColumnAccessKind> usage) {
    }

    private record Entry(Object value, RangerAccessController.DataVersions versions, long createdNanos) {
    }

    private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();

    /**
     * Builds the key from the same values that RangerStarRocksAccessRequest puts into the request.
     * Returns null when a value cannot be copied, and then the request is not cached.
     */
    static Key keyOf(RangerAccessController controller, Kind kind, RangerAccessResourceImpl resource,
                     UserIdentity user, Set<String> groups, String accessType, Set<ColumnAccessKind> usage) {
        try {
            Map<String, String> elements = new HashMap<>();
            for (Map.Entry<String, Object> element : resource.getAsMap().entrySet()) {
                if (!(element.getValue() instanceof String value)) {
                    return null;
                }
                elements.put(element.getKey(), value);
            }
            return new Key(controller, kind, Map.copyOf(elements), resource.getOwnerUser(),
                    user.getUser(), user.getHost(), groups == null ? null : Set.copyOf(groups), accessType,
                    usage == null ? Set.of() : Set.copyOf(usage));
        } catch (RuntimeException e) {
            // For example a null group name. Asking Ranger without the cache gives the same answer.
            return null;
        }
    }

    /**
     * Returns the kept answer for the key, or evaluates it and keeps it. The answer must not be null.
     */
    @SuppressWarnings("unchecked")
    <T> T get(Key key, Supplier<T> evaluate) {
        if (key == null || !Config.enable_ranger_decision_cache) {
            return evaluate.get();
        }
        // Read before the evaluation: if Ranger loads new data meanwhile, the entry has the old
        // versions and the next call evaluates again.
        RangerAccessController.DataVersions versions;
        boolean timeDependent;
        try {
            versions = key.controller().dataVersions();
            timeDependent = key.controller().hasTimeDependentPolicies();
        } catch (RuntimeException e) {
            return evaluate.get();
        }
        long maxAgeMs = Config.ranger_decision_cache_time_dependent_ttl_ms;
        if (timeDependent && maxAgeMs <= 0) {
            return evaluate.get();
        }
        long now = System.nanoTime();
        Entry entry = entries.get(key);
        if (entry != null && entry.versions().equals(versions)
                && (!timeDependent || now - entry.createdNanos() < TimeUnit.MILLISECONDS.toNanos(maxAgeMs))) {
            return (T) entry.value();
        }
        T value = evaluate.get();
        if (value != null && (entry != null || entries.size() < MAX_ENTRIES)) {
            entries.put(key, new Entry(value, versions, now));
        }
        return value;
    }

    int size() {
        return entries.size();
    }
}

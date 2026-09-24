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

import com.google.common.hash.Hashing;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Per-table calendar slots, persisted with the owning analyze job. */
public class AutoStatisticsSchedule {
    private static final long DAY_SECONDS = 86400;

    @SerializedName("tables")
    private ConcurrentMap<String, Entry> tables = new ConcurrentHashMap<>();

    // Replayed jobs resume future slots, but never execute the entire missed backlog on promotion.
    private transient Set<String> activated = new HashSet<>();
    private transient boolean dirty;

    private static class Entry {
        @SerializedName("next")
        private LocalDateTime next;
        @SerializedName("interval")
        private long interval;
        @SerializedName("window")
        private String window;

        private Entry(LocalDateTime next, long interval, String window) {
            this.next = next;
            this.interval = interval;
            this.window = window;
        }
    }

    public static LocalDateTime now() {
        return LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
    }

    synchronized void deactivate() {
        activated.clear();
    }

    synchronized boolean takeDirty() {
        boolean result = dirty;
        dirty = false;
        return result;
    }

    synchronized LocalDateTime getNext(String key) {
        Entry entry = tables.get(key);
        return entry == null ? null : entry.next;
    }

    synchronized Attempt due(String key, long interval, LocalDateTime now, Window window) {
        if (interval <= 0 || window.length == 0) {
            return null;
        }
        Entry entry = tables.get(key);
        boolean firstVisit = activated.add(key);
        if (entry == null || entry.interval != interval || !window.signature.equals(entry.window)
                || (firstVisit && !entry.next.isAfter(now))
                || (!window.sameWindow(entry.next, now) && !entry.next.isAfter(now))) {
            tables.put(key, new Entry(window.nextSlot(key, interval, now), interval, window.signature));
            dirty = true;
            return null;
        }
        if (entry.next.isAfter(now) || !window.contains(now)) {
            return null;
        }
        return new Attempt(this, key, entry, window);
    }

    static final class Attempt {
        private final AutoStatisticsSchedule owner;
        private final String key;
        private final Entry entry;
        private final Window window;

        private Attempt(AutoStatisticsSchedule owner, String key, Entry entry, Window window) {
            this.owner = owner;
            this.key = key;
            this.entry = entry;
            this.window = window;
        }

        void complete(LocalDateTime now) {
            synchronized (owner) {
                // A concurrent run or changed configuration must not overwrite the newer schedule.
                if (owner.tables.get(key) != entry) {
                    return;
                }
                owner.tables.put(key, new Entry(window.nextSlot(key, entry.interval, now),
                        entry.interval, entry.window));
                owner.dirty = true;
            }
        }
    }

    /** Daily permitted seconds, expressed on the same wall clock as checkoutAnalyzeTime(). */
    static final class Window {
        private final long start;
        private final long length;
        private final String signature;

        Window(long start, long length, String zone) {
            this.start = start;
            this.length = length;
            this.signature = start + ":" + length + ":" + zone;
        }

        static Window current() {
            String zone = TimeUtils.getTimeZone().getID();
            try {
                LocalTime from = LocalTime.parse(Config.statistic_auto_analyze_start_time.replaceAll("[\"']", ""),
                        DateUtils.TIME_FORMATTER);
                LocalTime to = LocalTime.parse(Config.statistic_auto_analyze_end_time.replaceAll("[\"']", ""),
                        DateUtils.TIME_FORMATTER);
                // The collector excludes both endpoints. Schedule on whole seconds strictly inside.
                long span = Math.floorMod(to.toSecondOfDay() - from.toSecondOfDay(), DAY_SECONDS);
                // Leave one polling interval before closing, so deadlines are not placed
                // in the tail that the next normal collector pass cannot reach.
                long headroom = Math.min(Math.max(1, Config.statistic_collect_interval_sec), Math.max(0, span / 2));
                return new Window(from.toSecondOfDay() + 1L, Math.max(0, span - 1 - headroom), zone);
            } catch (DateTimeParseException e) {
                // Match the existing collector's fail-open policy for malformed configuration.
                return new Window(0, DAY_SECONDS, zone);
            }
        }

        String signature() {
            return signature;
        }

        boolean contains(LocalDateTime time) {
            return Math.floorMod(seconds(time) - start, DAY_SECONDS) < length;
        }

        boolean sameWindow(LocalDateTime left, LocalDateTime right) {
            return length == DAY_SECONDS || Math.floorDiv(seconds(left) - start, DAY_SECONDS)
                    == Math.floorDiv(seconds(right) - start, DAY_SECONDS);
        }

        // Cumulative permitted seconds. Closed hours consume no scheduling slots.
        private long activeSeconds(long wallSeconds) {
            long shifted = wallSeconds - start;
            return Math.floorDiv(shifted, DAY_SECONDS) * length
                    + Math.min(Math.floorMod(shifted, DAY_SECONDS), length);
        }

        private long wallSeconds(long activeSeconds) {
            return Math.floorDiv(activeSeconds, length) * DAY_SECONDS
                    + Math.floorMod(activeSeconds, length) + start;
        }

        LocalDateTime nextSlot(String key, long interval, LocalDateTime after) {
            long afterSeconds = seconds(after);
            long cycle = Math.floorDiv(afterSeconds, interval);
            long hash = Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asLong();
            while (true) {
                long begin = Math.multiplyExact(cycle, interval);
                long end = Math.addExact(begin, interval);
                long first = activeSeconds(begin);
                long count = activeSeconds(end) - first;
                if (count > 0) {
                    long candidate = wallSeconds(first + Math.floorMod(hash, count));
                    if (candidate > afterSeconds) {
                        return LocalDateTime.ofEpochSecond(candidate, 0, ZoneOffset.UTC);
                    }
                }
                // Jump over closed hours, including when the interval is much shorter than a day.
                cycle = Math.max(cycle + 1, Math.floorDiv(wallSeconds(activeSeconds(end)), interval));
            }
        }

        private static long seconds(LocalDateTime time) {
            // UTC is only an arithmetic origin here, not a conversion of the configured wall clock.
            return time.toEpochSecond(ZoneOffset.UTC);
        }
    }
}

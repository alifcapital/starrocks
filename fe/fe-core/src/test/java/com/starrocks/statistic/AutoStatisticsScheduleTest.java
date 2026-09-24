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

import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AutoStatisticsScheduleTest {
    private static final long WEEK = 7 * 86400;
    private static final AutoStatisticsSchedule.Window NIGHT =
            new AutoStatisticsSchedule.Window(3601, 4 * 3600 - 1, "UTC");

    @Test
    public void oldAnalyzeJobsAcquireIndependentSchedulesAndPersistThem() {
        String nativeJson = "{\"clazz\":\"NativeAnalyzeJob\",\"id\":7,\"dbId\":-1,\"tableId\":-1}";
        String externalJson = "{\"clazz\":\"ExternalAnalyzeJob\",\"id\":8,\"catalogName\":\"iceberg\"}";
        LocalDateTime now = LocalDateTime.of(2026, 9, 25, 12, 0);
        for (String json : new String[] {nativeJson, externalJson}) {
            AnalyzeJob job = GsonUtils.GSON.fromJson(json, AnalyzeJob.class);
            job.getCollectSchedule().due("t1", WEEK, now, NIGHT);
            job.getCollectSchedule().due("t2", WEEK, now, NIGHT);
            AnalyzeJob copy = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(job, AnalyzeJob.class), AnalyzeJob.class);
            Assertions.assertEquals(job.getCollectSchedule().getNext("t1"), copy.getCollectSchedule().getNext("t1"));
            Assertions.assertEquals(job.getCollectSchedule().getNext("t2"), copy.getCollectSchedule().getNext("t2"));
            Assertions.assertNotEquals(copy.getCollectSchedule().getNext("t1"), copy.getCollectSchedule().getNext("t2"));
        }
    }

    @Test
    public void existingFleetStartsWithinWeekAndKeepsNightlyPhases() {
        LocalDateTime enabled = LocalDateTime.of(2026, 9, 25, 12, 0);
        AutoStatisticsSchedule schedule = new AutoStatisticsSchedule();
        Map<String, Integer> nights = new TreeMap<>();
        Set<LocalDateTime> distinct = new HashSet<>();
        for (int i = 0; i < 2500; i++) {
            String key = "existing-job:" + i;
            Assertions.assertNull(schedule.due(key, WEEK, enabled, NIGHT));
            LocalDateTime first = schedule.getNext(key);
            Assertions.assertTrue(first.isAfter(enabled));
            Assertions.assertFalse(first.isAfter(enabled.plusWeeks(1)));
            Assertions.assertTrue(first.toLocalTime().isAfter(LocalTime.of(1, 0)));
            Assertions.assertTrue(first.toLocalTime().isBefore(LocalTime.of(5, 0)));
            nights.merge(first.toLocalDate().toString(), 1, Integer::sum);
            distinct.add(first);
            AutoStatisticsSchedule.Attempt attempt = schedule.due(key, WEEK, first, NIGHT);
            Assertions.assertNotNull(attempt);
            attempt.complete(first.plusMinutes(10));
            Assertions.assertEquals(first.plusWeeks(1), schedule.getNext(key));
        }
        Assertions.assertEquals(7, nights.size());
        Assertions.assertTrue(distinct.size() > 2400, distinct.toString());
        for (int count : nights.values()) {
            Assertions.assertTrue(count > 280 && count < 440, nights.toString());
        }
        System.out.println("2500 weekly tables, 01:00-05:00: " + nights);
    }

    @Test
    public void replayDoesNotRunOverdueFleetTogether() {
        LocalDateTime enabled = LocalDateTime.of(2026, 9, 25, 12, 0);
        AutoStatisticsSchedule original = new AutoStatisticsSchedule();
        for (int i = 0; i < 2500; i++) {
            original.due("table:" + i, WEEK, enabled, NIGHT);
        }
        AutoStatisticsSchedule replayed = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(original), AutoStatisticsSchedule.class);
        LocalDateTime resumed = enabled.plusWeeks(2).withHour(1).withMinute(1);
        Set<String> nights = new HashSet<>();
        for (int i = 0; i < 2500; i++) {
            String key = "table:" + i;
            Assertions.assertNull(replayed.due(key, WEEK, resumed, NIGHT));
            LocalDateTime next = replayed.getNext(key);
            Assertions.assertTrue(next.isAfter(resumed));
            Assertions.assertFalse(next.isAfter(resumed.plusWeeks(1)));
            Assertions.assertEquals(original.getNext(key).getDayOfWeek(), next.getDayOfWeek());
            Assertions.assertEquals(original.getNext(key).toLocalTime(), next.toLocalTime());
            nights.add(next.toLocalDate().toString());
        }
        Assertions.assertTrue(nights.size() >= 7);
    }

    @Test
    public void futureScheduleSurvivesReplayAndStaleCompletionCannotOverwrite() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 25, 12, 0);
        AutoStatisticsSchedule schedule = new AutoStatisticsSchedule();
        schedule.due("table", WEEK, now, NIGHT);
        LocalDateTime first = schedule.getNext("table");
        AutoStatisticsSchedule replayed = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(schedule), AutoStatisticsSchedule.class);
        Assertions.assertNull(replayed.due("table", WEEK, now.plusMinutes(1), NIGHT));
        Assertions.assertEquals(first, replayed.getNext("table"));
        AutoStatisticsSchedule.Attempt attempt = replayed.due("table", WEEK, first, NIGHT);
        AutoStatisticsSchedule.Attempt concurrent = replayed.due("table", WEEK, first, NIGHT);
        Assertions.assertNotNull(attempt);
        attempt.complete(first.plusSeconds(5));
        concurrent.complete(first.plusDays(2));
        Assertions.assertEquals(first.plusWeeks(1), replayed.getNext("table"));
    }

    @Test
    public void noChangeConsumesSlotAndFailureDoesNot() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 25, 12, 0);
        AutoStatisticsSchedule schedule = new AutoStatisticsSchedule();
        schedule.due("table", WEEK, now, NIGHT);
        LocalDateTime first = schedule.getNext("table");
        Assertions.assertNotNull(schedule.due("table", WEEK, first, NIGHT));
        // Failure does not complete the attempt: retry remains eligible in the same window.
        AutoStatisticsSchedule.Attempt retry = schedule.due("table", WEEK, first.plusSeconds(1), NIGHT);
        Assertions.assertNotNull(retry);
        retry.complete(first.plusSeconds(2));
        Assertions.assertNull(schedule.due("table", WEEK, first.plusHours(1), NIGHT));
        Assertions.assertEquals(first.plusWeeks(1), schedule.getNext("table"));
    }

    @Test
    public void changedIntervalWindowAndEnablementRescheduleBeforeRunning() {
        LocalDateTime now = LocalDateTime.of(2026, 9, 25, 12, 0);
        AutoStatisticsSchedule schedule = new AutoStatisticsSchedule();
        schedule.due("table", WEEK, now, NIGHT);
        Assertions.assertNull(schedule.due("table", 86400, now, NIGHT));
        Assertions.assertFalse(schedule.getNext("table").isAfter(now.plusDays(1)));
        AutoStatisticsSchedule.Window evening = new AutoStatisticsSchedule.Window(20 * 3600, 3600, "UTC");
        Assertions.assertNull(schedule.due("table", WEEK, now, evening));
        Assertions.assertTrue(evening.contains(schedule.getNext("table")));
        schedule.deactivate();
        LocalDateTime resumed = now.plusWeeks(3);
        Assertions.assertNull(schedule.due("table", WEEK, resumed, evening));
        Assertions.assertTrue(schedule.getNext("table").isAfter(resumed));
    }

    @Test
    public void wrappingWindowAndShortIntervalsSkipClosedHours() {
        AutoStatisticsSchedule.Window midnight = new AutoStatisticsSchedule.Window(22 * 3600 + 1, 6 * 3600 - 1, "UTC");
        LocalDateTime now = LocalDateTime.of(2026, 9, 25, 12, 0);
        for (long interval : new long[] {1, 60, 3600, 12 * 3600, WEEK}) {
            LocalDateTime next = midnight.nextSlot("table", interval, now);
            Assertions.assertTrue(next.isAfter(now));
            Assertions.assertTrue(midnight.contains(next));
            Assertions.assertFalse(next.isAfter(now.plusSeconds(Math.max(interval, 86400))));
        }
        Assertions.assertTrue(midnight.sameWindow(now.withHour(23), now.plusDays(1).withHour(2)));
        Assertions.assertFalse(midnight.sameWindow(now.withHour(23), now.plusDays(1).withHour(23)));
    }

    @Test
    public void configWindowMatchesCollectorEndpointsAndMalformedFallback() {
        String start = Config.statistic_auto_analyze_start_time;
        String end = Config.statistic_auto_analyze_end_time;
        try {
            Config.statistic_auto_analyze_start_time = "'22:00:00'";
            Config.statistic_auto_analyze_end_time = "\"04:00:00\"";
            AutoStatisticsSchedule.Window window = AutoStatisticsSchedule.Window.current();
            LocalDateTime day = LocalDateTime.of(2026, 9, 25, 0, 0);
            Assertions.assertTrue(window.contains(day.plusHours(23)));
            Assertions.assertTrue(window.contains(day.plusHours(2)));
            Assertions.assertFalse(window.contains(day.plusHours(22)));
            Assertions.assertFalse(window.contains(day.plusHours(4)));
            Config.statistic_auto_analyze_end_time = "22:00:00";
            Assertions.assertNull(new AutoStatisticsSchedule().due("t", WEEK, day,
                    AutoStatisticsSchedule.Window.current()));
            Config.statistic_auto_analyze_end_time = "invalid";
            Assertions.assertTrue(AutoStatisticsSchedule.Window.current().contains(day.plusHours(12)));
        } finally {
            Config.statistic_auto_analyze_start_time = start;
            Config.statistic_auto_analyze_end_time = end;
        }
    }
}

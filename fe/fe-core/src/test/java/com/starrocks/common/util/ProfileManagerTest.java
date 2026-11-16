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
package com.starrocks.common.util;

import com.starrocks.common.Config;
import com.starrocks.thrift.TUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProfileManagerTest {

    public RuntimeProfile buildRuntimeProfile(String queryId, String queryType) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, queryType);

        profile.addChild(summaryProfile);

        return profile;
    }

    @Test
    public void testSingleton() {
        ProfileManager instance1 = ProfileManager.getInstance();
        ProfileManager instance2 = ProfileManager.getInstance();
        assertSame(instance1, instance2, "ProfileManager should be singleton");
    }

    @Test
    public void testProfileAddAndGet() {
        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile);

        String retrievedProfile = manager.getProfile("123");
        assertNotNull(retrievedProfile, "Retrieved profile should not be null");
        assertTrue(manager.hasProfile("123"), "Profile should exist");

        assertEquals(1, manager.getAllProfileElements().size());

        assertNotNull(manager.getProfileElement("123"), "Profile element should not be null");

        manager.clearProfiles();
    }

    @Test
    public void testRemoveProfile() {
        ProfileManager manager = ProfileManager.getInstance();
        RuntimeProfile profile = buildRuntimeProfile("124", "Load");
        manager.pushProfile(null, profile);

        String retrievedProfile = manager.getProfile("124");
        assertNotNull(retrievedProfile, "Retrieved profile should not be null");

        manager.removeProfile("124");
        assertNull(manager.getProfile("124"), "Profile should be removed");
        assertFalse(manager.hasProfile("124"), "Profile should not exist");
    }

    @Test
    public void testGetAllQueries() {
        ProfileManager manager = ProfileManager.getInstance();
        assertTrue(manager.getAllProfileElements().isEmpty());

        RuntimeProfile profile1 = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile1);

        RuntimeProfile profile2 = buildRuntimeProfile("124", "Load");
        manager.pushProfile(null, profile2);

        assertEquals(2, manager.getAllQueries(element -> true).size());

        manager.clearProfiles();
    }

    private RuntimeProfile buildLoadProfile(String queryId, String state) {
        RuntimeProfile profile = new RuntimeProfile("");
        RuntimeProfile summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, queryId);
        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Load");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, state);
        summaryProfile.addInfoString(ProfileManager.START_TIME, "2024-01-01 10:00:00");
        summaryProfile.addInfoString(ProfileManager.END_TIME, "2024-01-01 10:00:05");
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, "test_db");
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, "stream load label");
        summaryProfile.addInfoString(ProfileManager.USER, "root");
        summaryProfile.addInfoString(ProfileManager.WAREHOUSE_CNGROUP, "default_warehouse");
        profile.addChild(summaryProfile);
        return profile;
    }

    @Test
    public void testPrintLoadProfileToLog() {
        ProfileManager manager = ProfileManager.getInstance();
        boolean original = Config.enable_print_load_profile_to_log;
        Config.enable_print_load_profile_to_log = true;
        try {
            // Each state exercises buildLoadQueryDetail plus one branch of toLoadState:
            // Finished -> FINISHED, Aborted -> FAILED, CANCELLED -> CANCELLED, Running -> RUNNING (default).
            String[][] loads = {
                    {"load-finished", "Finished"},
                    {"load-aborted", "Aborted"},
                    {"load-cancelled", "CANCELLED"},
                    {"load-running", "Running"}};
            for (String[] load : loads) {
                manager.pushProfile(null, buildLoadProfile(load[0], load[1]));
                assertTrue(manager.hasProfile(load[0]), "Load profile should be stored");
            }

            // A query profile must not take the load-profile log path but is still stored normally.
            manager.pushProfile(null, buildRuntimeProfile("a-query", "Query"));
            assertTrue(manager.hasProfile("a-query"), "Query profile should be stored");
        } finally {
            Config.enable_print_load_profile_to_log = original;
            manager.clearProfiles();
        }
    }

    @Test
    public void testPushExceed() {
        ProfileManager manager = ProfileManager.getInstance();
        assertTrue(manager.getAllProfileElements().isEmpty());

        Config.profile_info_reserved_num = 1;

        RuntimeProfile profile1 = buildRuntimeProfile("123", "Query");
        manager.pushProfile(null, profile1);

        RuntimeProfile profile2 = buildRuntimeProfile("124", "Query");
        manager.pushProfile(null, profile2);

        assertEquals(1, manager.getAllQueries(element -> true).size());

        manager.clearProfiles();
    }

    // A fake running query: every call of buildExecutionProfile builds the execution profile once more.
    private static class CountingSource implements ProfileManager.RunningProfileSource {
        long version = 1;
        int builds = 0;
        long rows = 10;

        @Override
        public long version() {
            return version;
        }

        @Override
        public RuntimeProfile buildExecutionProfile() {
            builds++;
            RuntimeProfile execution = new RuntimeProfile("Execution");
            execution.addCounter("Rows", TUnit.UNIT, null).setValue(rows);
            return execution;
        }
    }

    // The profile of a running query is built when it is read, and once per report version.
    @Test
    public void testRunningProfileIsBuiltWhenRead() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();
        CountingSource source = new CountingSource();
        ProfileManager.RunningProfile running = new ProfileManager.RunningProfile(source);

        manager.pushRunningProfile(null, buildRuntimeProfile("running-1", "Query"), running);
        manager.pushRunningProfile(null, buildRuntimeProfile("running-1", "Query"), running);
        assertEquals(0, source.builds, "publishing does not build the profile");
        assertTrue(manager.hasProfile("running-1"));
        assertEquals("running-1", manager.getProfileElement("running-1").infoStrings.get(ProfileManager.QUERY_ID));

        String first = manager.getProfile("running-1");
        assertTrue(first.contains("Summary:"), first);
        assertTrue(first.contains("Rows: 10"), first);
        assertEquals(first, manager.getProfile("running-1"));
        assertEquals(1, source.builds, "the same report version is built once");

        source.version = 2;
        source.rows = 20;
        assertTrue(manager.getProfile("running-1").contains("Rows: 20"));
        assertEquals(2, source.builds);

        // the profile object is a copy parsed from the text of the same version
        RuntimeProfile profile = manager.getProfileElement("running-1").getRuntimeProfile();
        assertEquals(20, profile.getChild("Execution").getCounter("Rows").getValue());
        assertEquals(2, source.builds);
        profile.getChild("Execution").getCounter("Rows").setValue(0);
        assertTrue(manager.getProfile("running-1").contains("Rows: 20"));
        manager.clearProfiles();
    }

    // At the end of a query without a final profile the running profile is built once more and keeps no source.
    @Test
    public void testDetachedRunningProfileKeepsTheLastProfile() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();
        CountingSource source = new CountingSource();
        ProfileManager.RunningProfile running = new ProfileManager.RunningProfile(source);
        manager.pushRunningProfile(null, buildRuntimeProfile("running-2", "Query"), running);

        source.rows = 30;
        running.detach();
        assertEquals(1, source.builds);
        source.version = 5;
        source.rows = 40;
        String profileString = manager.getProfile("running-2");
        assertTrue(profileString.contains("Rows: 30"), profileString);
        assertEquals(30, manager.getProfileElement("running-2").getRuntimeProfile()
                .getChild("Execution").getCounter("Rows").getValue());
        assertEquals(1, source.builds, "a detached profile does not read the source");

        // a final profile replaces the running one
        manager.pushProfile(null, buildRuntimeProfile("running-2", "Query"));
        assertFalse(manager.getProfile("running-2").contains("Rows:"));
        manager.clearProfiles();
    }

    // A final profile replaces the running one, and the end of the query does not build the replaced one.
    @Test
    public void testReplacedRunningProfileIsNotBuiltAtTheEnd() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();
        CountingSource source = new CountingSource();
        ProfileManager.RunningProfile running = new ProfileManager.RunningProfile(source);
        manager.pushRunningProfile(null, buildRuntimeProfile("running-3", "Load"), running);
        manager.pushProfile(null, buildRuntimeProfile("running-3", "Load"));
        running.detach();
        assertEquals(0, source.builds);
        assertFalse(manager.getProfile("running-3").contains("Rows:"));
        manager.clearProfiles();
    }

    // A running profile removed from the cache, by a retry of the query or by eviction, is not built at the end.
    @Test
    public void testRemovedRunningProfileIsNotBuiltAtTheEnd() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();
        CountingSource source = new CountingSource();
        ProfileManager.RunningProfile running = new ProfileManager.RunningProfile(source);
        manager.pushRunningProfile(null, buildRuntimeProfile("running-4", "Query"), running);
        manager.removeProfile("running-4");
        running.detach();
        assertEquals(0, source.builds);
        assertNull(manager.getProfile("running-4"));
    }

    // A failure to build leaves the last built profile to the readers.
    @Test
    public void testFailedBuildServesTheLastProfile() {
        ProfileManager manager = ProfileManager.getInstance();
        manager.clearProfiles();
        CountingSource source = new CountingSource() {
            @Override
            public RuntimeProfile buildExecutionProfile() {
                if (version > 1) {
                    throw new IllegalStateException("broken profile");
                }
                return super.buildExecutionProfile();
            }
        };
        ProfileManager.RunningProfile running = new ProfileManager.RunningProfile(source);
        manager.pushRunningProfile(null, buildRuntimeProfile("running-5", "Query"), running);
        String built = manager.getProfile("running-5");
        assertTrue(built.contains("Rows: 10"), built);

        source.version = 2;
        source.rows = 11;
        assertEquals(built, manager.getProfile("running-5"));
        manager.clearProfiles();
    }
}

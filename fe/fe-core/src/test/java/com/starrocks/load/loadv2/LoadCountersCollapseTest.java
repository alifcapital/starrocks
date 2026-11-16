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

package com.starrocks.load.loadv2;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.load.EtlStatus;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// When a load job is over, the counters of its fragments are summed into one cell per load. Everything read from
// them stays the same.
public class LoadCountersCollapseTest {

    private static final TUniqueId FIRST_LOAD = new TUniqueId(1L, 1L);
    private static final TUniqueId SECOND_LOAD = new TUniqueId(2L, 2L);

    private static TReportExecStatusParams report(TUniqueId load, TUniqueId fragment, long base) {
        TReportExecStatusParams params = new TReportExecStatusParams();
        params.setQuery_id(load);
        params.setFragment_instance_id(fragment);
        params.setBackend_id(10001L);
        params.setLoaded_rows(base);
        params.setSink_load_bytes(base * 7);
        params.setSource_load_rows(base * 3);
        params.setSource_load_bytes(base * 5);
        params.setFiltered_rows(base % 3);
        params.setUnselected_rows(base % 5);
        params.setSource_scan_bytes(Long.MAX_VALUE - base);
        return params;
    }

    // Two loads, as a job that retried runs them, with 20 and 30 fragment instances.
    private static EtlStatus loadedStatus() {
        EtlStatus status = new EtlStatus();
        int base = 1;
        for (TUniqueId load : List.of(FIRST_LOAD, SECOND_LOAD)) {
            Set<TUniqueId> fragments = new LinkedHashSet<>();
            for (int i = 0; i < (load == FIRST_LOAD ? 20 : 30); i++) {
                fragments.add(new TUniqueId(load.hi, 1000L + i));
            }
            status.getLoadStatistic().initLoad(load, fragments, List.of(10001L));
            for (TUniqueId fragment : fragments) {
                status.getLoadStatistic().updateLoadProgress(report(load, fragment, base++));
            }
        }
        return status;
    }

    private static List<Object> observed(EtlStatus status) {
        EtlStatus.LoadStatistic statistic = status.getLoadStatistic();
        List<Object> values = new ArrayList<>();
        values.add(statistic.totalRows());
        values.add(statistic.totalSinkLoadRows());
        values.add(statistic.totalSourceLoadRows());
        values.add(statistic.totalSourceLoadBytes());
        values.add(statistic.totalFilteredRows());
        values.add(statistic.totalUnselectedRows());
        values.add(statistic.sourceScanBytes());
        values.add(statistic.toShowInfoStr());
        values.add(statistic.toRuntimeDetails());
        return values;
    }

    private static int cells(EtlStatus status) {
        JsonObject statistic = JsonParser.parseString(GsonUtils.GSON.toJson(status)).getAsJsonObject()
                .getAsJsonObject("l");
        int cells = 0;
        for (Map.Entry<String, com.google.gson.JsonElement> table : statistic.entrySet()) {
            if (table.getKey().endsWith("ounterTbl")) {
                cells += table.getValue().getAsJsonObject().getAsJsonArray("cells").size() / 3;
            }
        }
        return cells;
    }

    @Test
    public void collapsedCountersShowTheSame() {
        EtlStatus status = loadedStatus();
        List<Object> before = observed(status);
        Assertions.assertEquals(7 * 50, cells(status));

        status.getLoadStatistic().collapseFragments();
        Assertions.assertEquals(before, observed(status));
        Assertions.assertEquals(7 * 2, cells(status));

        status.getLoadStatistic().collapseFragments();
        Assertions.assertEquals(before, observed(status));

        // a report that comes after the job is over changes nothing
        status.getLoadStatistic().updateLoadProgress(report(FIRST_LOAD, new TUniqueId(1L, 1000L), 999_999L));
        Assertions.assertEquals(before, observed(status));

        // the persisted counters read back the same
        EtlStatus read = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(status), EtlStatus.class);
        Assertions.assertEquals(before, observed(read));
    }

    @Test
    public void theEndOfAJobCollapsesItsCounters() {
        EtlStatus status = loadedStatus();
        List<Object> before = observed(status);
        LoadJobFinalOperation operation =
                new LoadJobFinalOperation(1L, status, 100, 1L, 2L, JobState.FINISHED, null);
        Assertions.assertEquals(7 * 2, cells(operation.getLoadingStatus()));
        Assertions.assertEquals(before, observed(operation.getLoadingStatus()));
    }

    // An end operation in the journal from a frontend that does not collapse the counters is collapsed on replay.
    @Test
    public void replayCollapsesTheCountersOfAnEndOperation() {
        EtlStatus status = loadedStatus();
        List<Object> before = observed(status);
        JsonObject operationJson = JsonParser.parseString(GsonUtils.GSON.toJson(
                new LoadJobFinalOperation(1L, new EtlStatus(), 100, 1L, 2L, JobState.FINISHED, null)))
                .getAsJsonObject();
        operationJson.add("ls", JsonParser.parseString(GsonUtils.GSON.toJson(status)));
        LoadJobFinalOperation replayed =
                GsonUtils.GSON.fromJson(operationJson.toString(), LoadJobFinalOperation.class);
        Assertions.assertEquals(7 * 50, cells(replayed.getLoadingStatus()));

        InsertLoadJob job = new InsertLoadJob();
        job.unprotectReadEndOperation(replayed, true);
        Assertions.assertEquals(7 * 2, cells(job.loadingStatus));
        Assertions.assertEquals(before, observed(job.loadingStatus));
    }
}

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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.scheduler.MvTaskRunContext;
import com.starrocks.scheduler.TaskRunContext;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshNonPartitioner;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class MVPCTRefreshNonPartitionerTest {

    @Test
    public void testGetAdaptivePartitionRefreshNumber() throws MVAdaptiveRefreshException {
        MvTaskRunContext mvTaskRunContext = Mockito.mock(MvTaskRunContext.class);
        TaskRunContext taskRunContext = Mockito.mock(TaskRunContext.class);
        Database database = Mockito.mock(Database.class);
        MaterializedView mv = Mockito.mock(MaterializedView.class);
        MVRefreshParams mvRefreshParams = Mockito.mock(MVRefreshParams.class);
        MVPCTRefreshNonPartitioner job = new MVPCTRefreshNonPartitioner(mvTaskRunContext, taskRunContext,
                database, mv, mvRefreshParams);
        int result = job.getAdaptivePartitionRefreshNumber(PCellSortedSet.of());
        Assertions.assertEquals(0, result);
    }

    // A non-partitioned MV is refreshed as a whole, so the refresh does not map its partition to base table
    // partitions: the set stays as it is and is not cut into batches.
    @Test
    public void testRefreshScopeIsNotExpanded() {
        MvTaskRunContext mvTaskRunContext = Mockito.mock(MvTaskRunContext.class);
        MVPCTRefreshNonPartitioner job = new MVPCTRefreshNonPartitioner(mvTaskRunContext,
                Mockito.mock(TaskRunContext.class), Mockito.mock(Database.class), Mockito.mock(MaterializedView.class),
                Mockito.mock(MVRefreshParams.class));
        Mockito.clearInvocations(mvTaskRunContext);

        PCellSortedSet toRefresh = PCellSortedSet.of(List.of(PCellWithName.of("mv1", new PCellNone())));
        Assertions.assertFalse(job.addPotentialRefreshPartitions(toRefresh));
        Assertions.assertEquals(1, toRefresh.size());
        Assertions.assertTrue(job.getMVToRefreshPotentialPartitions().isEmpty());
        Mockito.verifyNoInteractions(mvTaskRunContext);
    }
}

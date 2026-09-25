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

package com.starrocks.qe;

import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.thrift.TUniqueId;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class QeProcessorObservabilityTest {
    @Test
    public void testInternalQueryKeepsRegisteredExecutionId(@Mocked Coordinator coordinator) throws Exception {
        ConnectContext context = new ConnectContext();
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.genTUniqueId());
        context.setStatisticsConnection(true);
        context.getState().setEof();
        TUniqueId registeredId = context.getExecutionId();
        String id = DebugUtil.printId(registeredId);
        new Expectations() {
            {
                coordinator.isDone();
                result = false;
                coordinator.getWarehouseName();
                result = "stats";
                coordinator.getResourceGroupName();
                result = "default_wg";
            }
        };
        QeProcessorImpl.INSTANCE.registerQuery(registeredId,
                new QeProcessorImpl.QueryInfo(context, "select count(*) from t", coordinator));
        try {
            context.setQueryId(UUIDUtil.genUUID());
            context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
            Map<String, QueryStatisticsItem> queries = QeProcessorImpl.INSTANCE.getQueryStatistics();
            Assertions.assertTrue(queries.containsKey(id));
            QueryStatisticsItem item = queries.get(id);
            Assertions.assertEquals(registeredId, item.getExecutionId());
            Assertions.assertEquals("stats", item.getWarehouseName());
            Assertions.assertEquals("Statistics", item.getQueryType());
            Assertions.assertEquals("RUNNING", item.getExecState());
            context.setPending(true);
            Assertions.assertEquals("PENDING", QeProcessorImpl.INSTANCE.getQueryStatistics().get(id).getExecState());
            new Expectations() {
                {
                    coordinator.isDone();
                    result = true;
                }
            };
            Assertions.assertFalse(QeProcessorImpl.INSTANCE.getQueryStatistics().containsKey(id));
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(registeredId);
        }
    }
}

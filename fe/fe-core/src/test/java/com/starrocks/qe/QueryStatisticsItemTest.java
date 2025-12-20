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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryStatisticsItemTest {

    @Test
    void testQueryStatisticsItem() {
        String queryId = "123";
        String warehouseName = "wh1";

        final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .customQueryId("abc")
                .queryId("123")
                .warehouseName("wh1").build();

        Assertions.assertEquals("wh1", item.getWarehouseName());
        Assertions.assertEquals("abc", item.getCustomQueryId());
        Assertions.assertEquals("123", item.getQueryId());
    }

    @Test
    void testQueryStatisticsItemWithCnGroup() {
        final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("456")
                .warehouseName("test_warehouse")
                .cnGroupName("analytics")
                .resourceGroupName("rg1")
                .build();

        Assertions.assertEquals("test_warehouse", item.getWarehouseName());
        Assertions.assertEquals("analytics", item.getCnGroupName());
        Assertions.assertEquals("rg1", item.getResourceGroupName());
    }

    @Test
    void testQueryStatisticsItemWithNullCnGroup() {
        // Test that null cnGroupName doesn't cause issues
        final QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("789")
                .warehouseName("wh2")
                .build();

        Assertions.assertEquals("wh2", item.getWarehouseName());
        Assertions.assertNull(item.getCnGroupName());
    }
}

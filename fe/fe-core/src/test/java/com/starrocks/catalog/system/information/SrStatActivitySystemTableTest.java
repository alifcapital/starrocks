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

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SrStatActivitySystemTableTest {

    @Test
    public void testSchema() {
        SystemTable table = SrStatActivitySystemTable.create();
        Assertions.assertEquals(SrStatActivitySystemTable.NAME, table.getName());
        Assertions.assertEquals(Table.TableType.SCHEMA, table.getType());

        Assertions.assertEquals(22, table.getColumns().size());
        Assertions.assertNotNull(table.getColumn("FE_IP"));
        Assertions.assertNotNull(table.getColumn("CONNECTION_ID"));
        Assertions.assertNotNull(table.getColumn("QUERY_ID"));
        Assertions.assertNotNull(table.getColumn("CUSTOM_QUERY_ID"));
        Assertions.assertNotNull(table.getColumn("USER"));
        Assertions.assertNotNull(table.getColumn("CATALOG"));
        Assertions.assertNotNull(table.getColumn("DB"));
        Assertions.assertNotNull(table.getColumn("WAREHOUSE"));
        Assertions.assertNotNull(table.getColumn("CNGROUP"));
        Assertions.assertNotNull(table.getColumn("RESOURCE_GROUP"));
        Assertions.assertNotNull(table.getColumn("STATE"));
        Assertions.assertNotNull(table.getColumn("RESULT_SINK_STATE"));
        Assertions.assertNotNull(table.getColumn("PROGRESS_PCT"));
        Assertions.assertNotNull(table.getColumn("CONNECT_TIME"));
        Assertions.assertNotNull(table.getColumn("QUERY_START_TIME"));
        Assertions.assertNotNull(table.getColumn("EXEC_TIME_MS"));
        Assertions.assertNotNull(table.getColumn("CPU_COST_NS"));
        Assertions.assertNotNull(table.getColumn("SCAN_BYTES"));
        Assertions.assertNotNull(table.getColumn("SCAN_ROWS"));
        Assertions.assertNotNull(table.getColumn("MEM_USAGE_BYTES"));
        Assertions.assertNotNull(table.getColumn("SPILL_BYTES"));
        Assertions.assertNotNull(table.getColumn("QUERY"));
    }
}



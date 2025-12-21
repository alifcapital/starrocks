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
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;

import static com.starrocks.catalog.system.SystemTable.NAME_CHAR_LEN;
import static com.starrocks.catalog.system.SystemTable.builder;

/**
 * sr_stat_activity - similar to PostgreSQL's pg_stat_activity
 * Shows current activity for all connections across all FEs.
 */
public class SrStatActivitySystemTable {
    public static final String NAME = "sr_stat_activity";

    public static SystemTable create() {
        return new SystemTable(SystemId.SR_STAT_ACTIVITY_ID,
                NAME,
                Table.TableType.SCHEMA,
                builder()
                        // Identification
                        .column("FE_IP", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("CONNECTION_ID", IntegerType.BIGINT)
                        .column("QUERY_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("CUSTOM_QUERY_ID", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        // User and context
                        .column("USER", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("CATALOG", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("DB", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("WAREHOUSE", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        .column("RESOURCE_GROUP", TypeFactory.createVarcharType(NAME_CHAR_LEN))
                        // State
                        .column("STATE", TypeFactory.createVarcharType(32))
                        .column("RESULT_SINK_STATE", TypeFactory.createVarcharType(32))
                        .column("PROGRESS_PCT", FloatType.DOUBLE)
                        // Timestamps
                        .column("CONNECT_TIME", TypeFactory.createDatetimeType())
                        .column("QUERY_START_TIME", TypeFactory.createDatetimeType())
                        .column("EXEC_TIME_MS", IntegerType.BIGINT)
                        // Metrics
                        .column("CPU_COST_NS", IntegerType.BIGINT)
                        .column("SCAN_BYTES", IntegerType.BIGINT)
                        .column("SCAN_ROWS", IntegerType.BIGINT)
                        .column("MEM_USAGE_BYTES", IntegerType.BIGINT)
                        .column("SPILL_BYTES", IntegerType.BIGINT)
                        // SQL
                        .column("QUERY", TypeFactory.createVarcharType(65535))
                        .build(), TSchemaTableType.SCH_SR_STAT_ACTIVITY);
    }
}

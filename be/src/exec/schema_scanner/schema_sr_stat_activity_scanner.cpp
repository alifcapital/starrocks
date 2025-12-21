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

#include "exec/schema_scanner/schema_sr_stat_activity_scanner.h"

#include <fmt/format.h>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/timezone_utils.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaSrStatActivityScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        // Identification
        {"FE_IP", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CONNECTION_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CUSTOM_QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        // User and context
        {"USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CATALOG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DB", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"WAREHOUSE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"RESOURCE_GROUP", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        // State
        {"STATE", TypeDescriptor::create_varchar_type(32), 32, true},
        {"RESULT_SINK_STATE", TypeDescriptor::create_varchar_type(32), 32, true},
        {"PROGRESS_PCT", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), true},
        // Timestamps
        {"CONNECT_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"QUERY_START_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"EXEC_TIME_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        // Metrics
        {"CPU_COST_NS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"SCAN_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"SCAN_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"MEM_USAGE_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"SPILL_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        // SQL
        {"QUERY", TypeDescriptor::create_varchar_type(65535), 65535, true},
};

SchemaSrStatActivityScanner::SchemaSrStatActivityScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaSrStatActivityScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TAuthInfo auth_info;
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetSrStatActivityRequest req;
    req.__set_auth_info(auth_info);
    RETURN_IF_ERROR(SchemaHelper::get_sr_stat_activity(_ss_state, req, &_response));
    return Status::OK();
}

Status SchemaSrStatActivityScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_idx >= _response.items.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaSrStatActivityScanner::fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TGetSrStatActivityItem& item = _response.items[_idx++];

    for (const auto& [slot_id, index] : slot_id_map) {
        if (slot_id < 1 || slot_id > 21) {
            return Status::InternalError(fmt::format("invalid slot id: {}", slot_id));
        }
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
        switch (slot_id) {
        case 1: { // FE_IP
            Slice v(item.fe_ip);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 2: { // CONNECTION_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.connection_id);
            break;
        }
        case 3: { // QUERY_ID
            Slice v(item.query_id);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 4: { // CUSTOM_QUERY_ID
            Slice v(item.custom_query_id);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 5: { // USER
            Slice v(item.user);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 6: { // CATALOG
            Slice v(item.catalog);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 7: { // DB
            Slice v(item.db);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 8: { // WAREHOUSE
            Slice v(item.warehouse);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 9: { // RESOURCE_GROUP
            Slice v(item.resource_group);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 10: { // STATE
            Slice v(item.state);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 11: { // RESULT_SINK_STATE
            Slice v(item.result_sink_state);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        case 12: { // PROGRESS_PCT
            fill_column_with_slot<TYPE_DOUBLE>(column, (void*)&item.progress_pct);
            break;
        }
        case 13: { // CONNECT_TIME
            DateTimeValue dt;
            if (dt.from_unixtime(item.connect_time / 1000, TimezoneUtils::default_time_zone)) {
                fill_column_with_slot<TYPE_DATETIME>(column, (void*)&dt);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 14: { // QUERY_START_TIME
            DateTimeValue dt;
            if (item.query_start_time > 0 && dt.from_unixtime(item.query_start_time / 1000, TimezoneUtils::default_time_zone)) {
                fill_column_with_slot<TYPE_DATETIME>(column, (void*)&dt);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 15: { // EXEC_TIME_MS
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.exec_time_ms);
            break;
        }
        case 16: { // CPU_COST_NS
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.cpu_cost_ns);
            break;
        }
        case 17: { // SCAN_BYTES
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.scan_bytes);
            break;
        }
        case 18: { // SCAN_ROWS
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.scan_rows);
            break;
        }
        case 19: { // MEM_USAGE_BYTES
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.mem_usage_bytes);
            break;
        }
        case 20: { // SPILL_BYTES
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&item.spill_bytes);
            break;
        }
        case 21: { // QUERY
            Slice v(item.query);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&v);
            break;
        }
        default:
            break;
        }
    }
    return Status::OK();
}

} // namespace starrocks

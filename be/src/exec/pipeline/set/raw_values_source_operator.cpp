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

#include "exec/pipeline/set/raw_values_source_operator.h"

#include <fmt/format.h>

#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "runtime/decimalv3.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"

namespace starrocks::pipeline {

namespace {

template <LogicalType LT>
void append_integers(Column* column, const std::vector<int64_t>& values) {
    auto* data = down_cast<RunTimeColumnType<LT>*>(column);
    for (int64_t value : values) {
        data->append(static_cast<RunTimeCppType<LT>>(value));
    }
}

template <LogicalType LT>
void append_strings(Column* column, const std::vector<std::string>& values) {
    auto* data = down_cast<RunTimeColumnType<LT>*>(column);
    for (const std::string& value : values) {
        data->append(Slice(value));
    }
}

template <LogicalType LT>
Status append_decimals(Column* column, const TypeDescriptor& type, const std::vector<std::string>& values) {
    using CppType = RunTimeCppType<LT>;
    auto* data = down_cast<RunTimeColumnType<LT>*>(column);
    for (const std::string& value : values) {
        CppType decimal;
        if (DecimalV3Cast::from_string<CppType>(&decimal, type.precision, type.scale, value.data(), value.size())) {
            return Status::InternalError(fmt::format("RawValues: invalid {} value '{}'", type.debug_string(), value));
        }
        data->append(decimal);
    }
    return Status::OK();
}

template <LogicalType LT>
Status append_dates(Column* column, const TypeDescriptor& type, const std::vector<std::string>& values) {
    auto* data = down_cast<RunTimeColumnType<LT>*>(column);
    for (const std::string& value : values) {
        RunTimeCppType<LT> date;
        if (!date.from_string(value.data(), value.size())) {
            return Status::InternalError(fmt::format("RawValues: invalid {} value '{}'", type.debug_string(), value));
        }
        data->append(date);
    }
    return Status::OK();
}

} // namespace

StatusOr<ColumnPtr> RawValuesSourceOperatorFactory::build_column(const TypeDescriptor& type, bool nullable,
                                                                 const std::vector<int64_t>& long_values,
                                                                 const std::vector<std::string>& string_values) {
    bool is_integer = type.type == TYPE_TINYINT || type.type == TYPE_SMALLINT || type.type == TYPE_INT ||
                      type.type == TYPE_BIGINT;
    if (is_integer ? !string_values.empty() : !long_values.empty()) {
        return Status::InternalError(fmt::format("RawValues: unexpected values for {}", type.debug_string()));
    }
    size_t num_rows = is_integer ? long_values.size() : string_values.size();

    MutableColumnPtr column = ColumnHelper::create_column(type, false);
    column->reserve(num_rows);
    switch (type.type) {
    case TYPE_TINYINT:
        append_integers<TYPE_TINYINT>(column.get(), long_values);
        break;
    case TYPE_SMALLINT:
        append_integers<TYPE_SMALLINT>(column.get(), long_values);
        break;
    case TYPE_INT:
        append_integers<TYPE_INT>(column.get(), long_values);
        break;
    case TYPE_BIGINT:
        append_integers<TYPE_BIGINT>(column.get(), long_values);
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        append_strings<TYPE_VARCHAR>(column.get(), string_values);
        break;
    case TYPE_DECIMAL32:
        RETURN_IF_ERROR(append_decimals<TYPE_DECIMAL32>(column.get(), type, string_values));
        break;
    case TYPE_DECIMAL64:
        RETURN_IF_ERROR(append_decimals<TYPE_DECIMAL64>(column.get(), type, string_values));
        break;
    case TYPE_DECIMAL128:
        RETURN_IF_ERROR(append_decimals<TYPE_DECIMAL128>(column.get(), type, string_values));
        break;
    case TYPE_DATE:
        RETURN_IF_ERROR(append_dates<TYPE_DATE>(column.get(), type, string_values));
        break;
    case TYPE_DATETIME:
        RETURN_IF_ERROR(append_dates<TYPE_DATETIME>(column.get(), type, string_values));
        break;
    default:
        return Status::NotSupported(fmt::format("RawValues does not support {}", type.debug_string()));
    }

    if (nullable) {
        return ColumnPtr(NullableColumn::create(std::move(column), NullColumn::create(num_rows, 0)));
    }
    return ColumnPtr(std::move(column));
}

Status RawValuesSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));
    const SlotDescriptor* slot = _dst_slots[0];
    ASSIGN_OR_RETURN(_values, build_column(slot->type(), slot->is_nullable(), _long_values, _string_values));
    // The lists are not used once the column holds the values
    std::vector<int64_t>().swap(_long_values);
    std::vector<std::string>().swap(_string_values);
    return Status::OK();
}

StatusOr<ChunkPtr> RawValuesSourceOperator::pull_chunk(RuntimeState* state) {
    DCHECK(_next_processed_row_index < _rows_total);
    size_t rows_count = std::min(static_cast<size_t>(state->chunk_size()), _rows_total - _next_processed_row_index);

    MutableColumnPtr column = _values->clone_empty();
    column->append(*_values, _start_index + _next_processed_row_index, rows_count);

    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(column), _slot_id);
    _next_processed_row_index += rows_count;

    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

} // namespace starrocks::pipeline

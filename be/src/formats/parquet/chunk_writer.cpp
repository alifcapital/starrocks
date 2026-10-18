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

#include "formats/parquet/chunk_writer.h"

#include <fmt/format.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include <algorithm>
#include <numeric>
#include <utility>

#include "column/chunk.h"
#include "common/compiler_util.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "formats/parquet/column_chunk_writer.h"
#include "formats/parquet/level_builder.h"

namespace starrocks::parquet {

ChunkWriter::ChunkWriter(::parquet::RowGroupWriter* rg_writer, std::vector<TypeDescriptor> type_descs,
                         std::shared_ptr<::parquet::schema::GroupNode> schema,
                         std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> eval_func, std::string timezone,
                         bool use_legacy_decimal_encoding, bool use_int96_timestamp_encoding)
        : _rg_writer(rg_writer),
          _type_descs(std::move(type_descs)),
          _schema(std::move(schema)),
          _eval_func(std::move(eval_func)),
          _timezone(std::move(timezone)),
          _use_legacy_decimal_encoding(use_legacy_decimal_encoding),
          _use_int96_timestamp_encoding(use_int96_timestamp_encoding) {
    int num_columns = rg_writer->num_columns();
    _estimated_buffered_bytes.resize(num_columns);
    std::fill(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), 0);
}

Status ChunkWriter::write(Chunk* chunk) {
    return call_catching_parquet_exception("write chunk error", [&]() { return _write(chunk); });
}

Status ChunkWriter::_write(Chunk* chunk) {
    // We fear a schema that has fewer fields than there are types, because the loops below index the schema by the
    // position of the type.
    if (UNLIKELY(static_cast<size_t>(_schema->field_count()) != _type_descs.size())) {
        return Status::InternalError(fmt::format("Parquet schema has {} fields, but there are {} column types",
                                                 _schema->field_count(), _type_descs.size()));
    }

    LevelBuilderContext ctx(chunk->num_rows());

    // Writes out all leaf parquet columns to the RowGroupWriter. Each leaf column is written fully before
    // the next column is written. Columns are written in DFS order.
    int leaf_column_idx = 0;
    const int num_leaf_columns = _rg_writer->num_columns();
    Status leaf_status;

    auto write_leaf_column = [&](const LevelBuilderResult& result) {
        if (!leaf_status.ok()) {
            return;
        }
        // For an index past the last column, the row group writer returns nullptr instead of throwing. We fear more
        // leaf columns from the level builder than the file schema has, so we remember an error and stop writing.
        ::parquet::ColumnWriter* column_writer =
                leaf_column_idx < num_leaf_columns ? _rg_writer->column(leaf_column_idx) : nullptr;
        if (column_writer == nullptr) {
            leaf_status = Status::InternalError(
                    fmt::format("The chunk has more leaf columns than the {} of the parquet schema", num_leaf_columns));
            return;
        }
        auto leaf_column_writer = ColumnChunkWriter(column_writer);
        leaf_column_writer.write(result);
        _estimated_buffered_bytes[leaf_column_idx] = leaf_column_writer.estimated_buffered_value_bytes();
        ++leaf_column_idx;
    };

    // Check required fields before writing any column: writing is column-at-a-time, so failing
    // midway would leave earlier columns already appended to the row group. Only a required
    // column or a column with a required nested field is evaluated up front; the other columns
    // cannot fail this check and stay lazy so we do not hold every evaluated column alive at once.
    std::vector<ColumnPtr> checked_columns(_type_descs.size());
    for (size_t i = 0; i < _type_descs.size(); i++) {
        const auto& field = _schema->field(i);
        const bool has_required_nested_field = has_required_descendant(_type_descs[i], field);
        if (!field->is_required() && !has_required_nested_field) {
            continue;
        }
        ASSIGN_OR_RETURN(checked_columns[i], _eval_func(chunk, i));
        if (field->is_required() && checked_columns[i]->has_null()) {
            return Status::DataQualityError(fmt::format("NULL value in non-nullable column '{}'", field->name()));
        }
        if (has_required_nested_field) {
            RETURN_IF_ERROR(check_required_descendants(_type_descs[i], field, checked_columns[i], field->name()));
        }
    }

    for (size_t i = 0; i < _type_descs.size(); i++) {
        ColumnPtr col;
        if (checked_columns[i] != nullptr) {
            col = std::move(checked_columns[i]);
        } else {
            ASSIGN_OR_RETURN(col, _eval_func(chunk, i));
        }
        auto level_builder = LevelBuilder(_type_descs[i], _schema->field(i), _timezone, _use_legacy_decimal_encoding,
                                          _use_int96_timestamp_encoding);
        RETURN_IF_ERROR(level_builder.init());
        RETURN_IF_ERROR(level_builder.write(ctx, col, write_leaf_column));
        RETURN_IF_ERROR(leaf_status);
    }

    // We fear fewer leaf columns than the file schema has, because the row group then holds columns of different
    // lengths and fails when it is closed.
    if (leaf_column_idx != num_leaf_columns) {
        return Status::InternalError(fmt::format("The chunk has {} leaf columns, but the parquet schema has {}",
                                                 leaf_column_idx, num_leaf_columns));
    }

    return Status::OK();
}

void ChunkWriter::close() {
    _rg_writer->Close();
}

// The current row group written bytes = total_bytes_written + total_compressed_bytes + estimated_bytes.
// total_bytes_written: total bytes written by the page writer
// total_compressed_types: total bytes still compressed but not written
// estimated_bytes: estimated size of all column chunk uncompressed values that are not written to a page yet. it
// mainly includes value buffer size and repetition buffer size and definition buffer value for each column.
int64_t ChunkWriter::estimated_buffered_bytes() const {
    if (_rg_writer == nullptr) {
        return 0;
    }

    auto buffered_bytes =
            std::accumulate(_estimated_buffered_bytes.begin(), _estimated_buffered_bytes.end(), int64_t{0});
    return _rg_writer->total_compressed_bytes_written() + _rg_writer->total_compressed_bytes() + buffered_bytes;
}

} // namespace starrocks::parquet

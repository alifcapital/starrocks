
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

#pragma once

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <fmt/format.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stddef.h>
#include <stdint.h>

#include <exception>
#include <functional>
#include <memory>
#include <new>
#include <string_view>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "fs/fs.h"
#include "runtime/types.h"

namespace parquet {
class RowGroupWriter;

namespace schema {
class GroupNode;
} // namespace schema
} // namespace parquet
namespace starrocks {
class Chunk;
template <typename T>
class StatusOr;
} // namespace starrocks

namespace starrocks::parquet {

// parquet-cpp reports an error, such as a failed allocation or a failed write to the output stream, by throwing.
// Nothing above the writer catches it, so we fear that it terminates the BE. We run every call into the parquet
// writer through this function and return the exception as a Status. We do not convert std::bad_alloc: the memory
// limit of a query is reported with it, and the caller handles it. An arrow out-of-memory status becomes a memory
// limit error.
template <typename Fn>
Status call_catching_parquet_exception(std::string_view action, Fn&& fn) {
    try {
        return fn();
    } catch (const std::bad_alloc&) {
        throw;
    } catch (const ::parquet::ParquetStatusException& e) {
        if (e.status().IsOutOfMemory()) {
            return Status::MemoryLimitExceeded(fmt::format("{}: {}", action, e.what()));
        }
        return Status::IOError(fmt::format("{}: {}", action, e.what()));
    } catch (const std::exception& e) {
        return Status::IOError(fmt::format("{}: {}", action, e.what()));
    }
}

// Wraps parquet::RowGroupWriter.
// Write chunks into buffer. Flush on closing.
class ChunkWriter {
public:
    ChunkWriter(::parquet::RowGroupWriter* rg_writer, std::vector<TypeDescriptor> type_descs,
                std::shared_ptr<::parquet::schema::GroupNode> schema,
                std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> eval_func, std::string timezone,
                bool use_legacy_decimal_encoding = false, bool use_int96_timestamp_encoding = false);

    Status write(Chunk* chunk);

    void close();

    int64_t estimated_buffered_bytes() const;

private:
    Status _write(Chunk* chunk);

    ::parquet::RowGroupWriter* _rg_writer;
    std::vector<TypeDescriptor> _type_descs;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> _eval_func;
    std::vector<int64_t> _estimated_buffered_bytes;
    std::string _timezone;
    bool _use_legacy_decimal_encoding = false;
    bool _use_int96_timestamp_encoding = false;
};

} // namespace starrocks::parquet

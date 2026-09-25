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
#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

// Which dict-aware wrapper a global-dict-encoded slot ended up with. Reported back
// by the dict-wrapping create() overload so the GroupReader can attribute the
// applied slot to the right counter (Parquet has no force-encode fallback like
// OLAP; the two truthful paths are LowCardColumnReader for all-page-dict columns
// and LowRowsColumnReader when row count fits the FE dictionary).
enum class GlobalDictReaderKind {
    kNone,          // no wrapper selected (factory returned GlobalDictNotMatch)
    kDictCode,      // LowCardColumnReader: reader consumes Parquet dict codes directly
    kLowRowsEncode, // LowRowsColumnReader: reader reads strings then encodes per row
};

class ColumnReaderFactory {
public:
    // create a column reader
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type);

    // Create a column reader with iceberg schema
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type,
                                            const TIcebergSchemaField* lake_schema_field);

    // Wrap an already-built scalar reader with a dict-aware reader for the given
    // FE global dictionary.  If out_kind is non-null, it is set to identify which
    // wrapper was picked so the caller can publish profile counters for the
    // dict-code vs. low-rows-encode path.  Out_kind is left at kNone on failure
    // (the function returns a GlobalDictNotMatch status in that case).
    static StatusOr<ColumnReaderPtr> create(ColumnReaderPtr raw_reader, const GlobalDictMap* dict, const SlotId slot_id,
                                            int64_t num_rows, GlobalDictReaderKind* out_kind = nullptr);

private:
    // for struct type without schema change
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, std::vector<int32_t>& pos);

    // for schema changed
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, const TIcebergSchemaField* lake_schema_field,
                                                  std::vector<int32_t>& pos,
                                                  std::vector<const TIcebergSchemaField*>& lake_schema_subfield);

    static bool _has_valid_subfield_column_reader(
            const std::map<std::string, std::unique_ptr<ColumnReader>>& children_readers);

    static StatusOr<ColumnReaderPtr> create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                  const ParquetField* variant_field);
};

} // namespace starrocks::parquet
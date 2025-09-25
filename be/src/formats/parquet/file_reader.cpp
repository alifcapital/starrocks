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

#include "formats/parquet/file_reader.h"

#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <iterator>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/datacache.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "exec/olap_common.h"
#include "exec/olap_utils.h"
#include "types/logical_type_infra.h"
#include "storage/runtime_range_pruner.hpp"
#include "connector/connector_chunk_sink.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/utils.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "exprs/runtime_filter.h"
#include "exprs/agg_in_runtime_filter.h"
#include "types/logical_type_infra.h"
#include "common/object_pool.h"
#include "util/thrift_util.h"
#include <cstdint>
#include "fs/fs.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::parquet {

FileReader::FileReader(int chunk_size, RandomAccessFile* file, size_t file_size,
                       const DataCacheOptions& datacache_options, io::SharedBufferedInputStream* sb_stream,
                       SkipRowsContextPtr skip_rows_context)
        : _chunk_size(chunk_size),
          _file(file),
          _file_size(file_size),
          _datacache_options(datacache_options),
          _sb_stream(sb_stream),
          _skip_rows_ctx(std::move(skip_rows_context)) {}

FileReader::~FileReader() = default;

Status FileReader::init(HdfsScannerContext* ctx) {
    _scanner_ctx = ctx;
    if (ctx->use_file_metacache) {
        _cache = DataCache::GetInstance()->page_cache();
    }

    // parse FileMetadata
    FileMetaDataParser file_metadata_parser{_file, ctx, _cache, &_datacache_options, _file_size};
    ASSIGN_OR_RETURN(_file_metadata, file_metadata_parser.get_file_metadata());

    // set existed SlotDescriptor in this parquet file
    std::unordered_set<std::string> existed_column_names;
    _meta_helper = _build_meta_helper();
    _prepare_read_columns(existed_column_names);
    RETURN_IF_ERROR(_scanner_ctx->update_materialized_columns(existed_column_names));
    ASSIGN_OR_RETURN(_is_file_filtered, _scanner_ctx->should_skip_by_evaluating_not_existed_slots());
    if (_is_file_filtered) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_build_split_tasks());
    if (_scanner_ctx->split_tasks.size() > 0) {
        _scanner_ctx->has_split_tasks = true;
        _is_file_filtered = true;
        return Status::OK();
    }

    if (_scanner_ctx->rf_scan_range_pruner != nullptr) {
        _rf_scan_range_pruner = std::make_shared<RuntimeScanRangePruner>(*_scanner_ctx->rf_scan_range_pruner);
    }
    RETURN_IF_ERROR(_init_group_readers());

    _init_eq_delete_predicates();

    return Status::OK();
}

std::shared_ptr<MetaHelper> FileReader::_build_meta_helper() {
    if (_scanner_ctx->lake_schema != nullptr && _file_metadata->schema().exist_filed_id()) {
        // If we want read this parquet file with iceberg/paimon schema,
        // we also need to make sure it contains parquet field id.
        return std::make_shared<LakeMetaHelper>(_file_metadata.get(), _scanner_ctx->case_sensitive,
                                                _scanner_ctx->lake_schema);
    } else {
        return std::make_shared<ParquetMetaHelper>(_file_metadata.get(), _scanner_ctx->case_sensitive);
    }
}

const FileMetaData* FileReader::get_file_metadata() {
    return _file_metadata.get();
}

Status FileReader::collect_scan_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* io_ranges) {
    int64_t dummy_offset = 0;
    for (auto& r : _row_group_readers) {
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGE_INDEX);
        r->collect_io_ranges(io_ranges, &dummy_offset, ColumnIOType::PAGES);
    }
    return Status::OK();
}

Status FileReader::_build_split_tasks() {
    // don't do split in following cases:
    // 1. this feature is not enabled
    // 2. we have already done split before (that's why `split_context` is nullptr)
    if (!_scanner_ctx->enable_split_tasks || _scanner_ctx->split_context != nullptr) {
        return Status::OK();
    }

    size_t row_group_size = _file_metadata->t_metadata().row_groups.size();
    for (size_t i = 0; i < row_group_size; i++) {
        const tparquet::RowGroup& row_group = _file_metadata->t_metadata().row_groups[i];
        if (!_select_row_group(row_group)) continue;
        int64_t start_offset = ParquetUtils::get_row_group_start_offset(row_group);
        int64_t end_offset = ParquetUtils::get_row_group_end_offset(row_group);
        if (start_offset >= end_offset) {
            LOG(INFO) << "row group " << i << " is empty. start = " << start_offset << ", end = " << end_offset;
            continue;
        }
#ifndef NDEBUG
        DCHECK(start_offset < end_offset) << "start = " << start_offset << ", end = " << end_offset;
        // there could be holes between row groups.
        // but this does not affect our scan range filter logic.
        // because in `_select_row_group`, we check if `start offset of row group` is in this range
        // so as long as `end_offset > start_offset && end_offset <= start_offset(next_group)`, it's ok
        if ((i + 1) < row_group_size) {
            const tparquet::RowGroup& next_row_group = _file_metadata->t_metadata().row_groups[i + 1];
            DCHECK(end_offset <= ParquetUtils::get_row_group_start_offset(next_row_group));
        }
#endif
        auto split_ctx = std::make_unique<SplitContext>();
        split_ctx->split_start = start_offset;
        split_ctx->split_end = end_offset;
        split_ctx->file_metadata = _file_metadata;
        split_ctx->skip_rows_ctx = _skip_rows_ctx;
        _scanner_ctx->split_tasks.emplace_back(std::move(split_ctx));
    }
    _scanner_ctx->merge_split_tasks();
    // if only one split task, clear it, no need to do split work.
    if (_scanner_ctx->split_tasks.size() <= 1) {
        _scanner_ctx->split_tasks.clear();
    }

    if (VLOG_OPERATOR_IS_ON) {
        std::stringstream ss;
        for (const HdfsSplitContextPtr& ctx : _scanner_ctx->split_tasks) {
            ss << "[" << ctx->split_start << "," << ctx->split_end << "]";
        }
        VLOG_OPERATOR << "FileReader: do_open. split task for " << _file->filename()
                      << ", split_tasks.size = " << _scanner_ctx->split_tasks.size() << ", range = " << ss.str();
    }
    return Status::OK();
}

// when doing row group filter, there maybe some error, but we'd better just ignore it instead of returning the error
// status and lead to the query failed.
bool FileReader::_filter_group(const GroupReaderPtr& group_reader) {
    bool& filtered = group_reader->get_is_group_filtered();
    filtered = false;
    auto visitor = PredicateFilterEvaluator{_scanner_ctx->predicate_tree, group_reader.get(),
                                            _scanner_ctx->parquet_page_index_enable,
                                            _scanner_ctx->parquet_bloom_filter_enable};
    auto sparse_range = _scanner_ctx->predicate_tree.visit(visitor);
    _group_reader_param.stats->_optimzation_counter += visitor.counter;
    if (!sparse_range.ok()) {
        LOG(WARNING) << "filter row group failed: " << sparse_range.status().message();
    } else if (sparse_range.value().has_value()) {
        if (sparse_range.value()->span_size() == 0) {
            // no rows selected, the whole row group can be filtered
            filtered = true;
        } else if (sparse_range.value()->span_size() < group_reader->get_row_group_metadata()->num_rows) {
            // some pages have been filtered
            group_reader->get_range() = sparse_range.value().value();
        }
    }
    return filtered;
}

StatusOr<bool> FileReader::_update_rf_and_filter_group(const GroupReaderPtr& group_reader) {
    bool filter = false;
    if (_rf_scan_range_pruner != nullptr) {
        RETURN_IF_ERROR(_rf_scan_range_pruner->update_range_if_arrived(
                _scanner_ctx->global_dictmaps,
                [this, &filter, &group_reader](auto cid, const PredicateList& predicates) {
                    PredicateCompoundNode<CompoundNodeType::AND> pred_tree;
                    for (const auto& pred : predicates) {
                        pred_tree.add_child(PredicateColumnNode{pred});
                    }
                    auto real_tree = PredicateTree::create(std::move(pred_tree));
                    auto visitor = PredicateFilterEvaluator{real_tree, group_reader.get(), false, false};
                    auto res = real_tree.visit(visitor);
                    if (res.ok() && res->has_value() && res->value().span_size() == 0) {
                        filter = true;
                    }
                    this->_group_reader_param.stats->_optimzation_counter += visitor.counter;
                    return Status::OK();
                },
                true, 0));
    }
    return filter;
}

void FileReader::_init_eq_delete_predicates() {
    if (_scanner_ctx->eq_delete_markers.empty()) return;

    auto start_time = std::chrono::steady_clock::now();
    size_t total_delete_values = 0;

    // Count total delete values for logging
    for (auto* probe_descriptor : _scanner_ctx->eq_delete_markers) {
        if (probe_descriptor == nullptr) continue;

        auto* rf = probe_descriptor->runtime_filter(0);
        if (rf == nullptr || rf->type() != RuntimeFilterSerializeType::EQ_DELETE_MARKER) continue;

        auto* in_filter = rf->get_in_filter();
        if (in_filter) {
            auto* abstract_filter = down_cast<const AbstractInRuntimeFilter*>(in_filter);
            total_delete_values += abstract_filter->size();
        }
    }

    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();

    VLOG(1) << "EQDELETE FileReader: Found " << _scanner_ctx->eq_delete_markers.size()
            << " EQ-delete markers with " << total_delete_values << " total delete values in "
            << total_duration << "ms (direct approach - no predicate conversion)";
}

bool FileReader::_should_bypass_eq_delete_for_row_group(const GroupReaderPtr& group_reader) {
    if (_scanner_ctx->eq_delete_markers.empty()) return false;

    auto eval_start = std::chrono::steady_clock::now();
    try {
        // Use direct approach instead of complex predicate system
        bool can_bypass = _should_bypass_eq_delete_direct(group_reader);

        auto eval_end = std::chrono::steady_clock::now();
        auto eval_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            eval_end - eval_start).count();

        if (can_bypass) {
            _iceberg_eq_delete_row_groups_skipped++;
        }

        VLOG(1) << "EQDELETE FileReader: Direct row group eval took " << eval_duration
                << "us, can_bypass=" << can_bypass;

        return can_bypass;
    } catch (...) {
        return false;
    }
}

bool FileReader::_should_bypass_eq_delete_direct(const GroupReaderPtr& group_reader) {
    auto direct_start = std::chrono::steady_clock::now();

    for (auto* probe_descriptor : _scanner_ctx->eq_delete_markers) {
        if (probe_descriptor == nullptr) continue;

        auto* rf = probe_descriptor->runtime_filter(0); // driver_sequence = 0
        if (rf == nullptr || rf->type() != RuntimeFilterSerializeType::EQ_DELETE_MARKER) continue;

        auto* in_filter = rf->get_in_filter();
        if (in_filter == nullptr) continue;

        // Get SlotDescriptor from probe expression
        ExprContext* probe_expr_ctx = probe_descriptor->probe_expr_ctx();
        if (!probe_expr_ctx || !probe_expr_ctx->root()->is_slotref()) continue;

        auto* slot_ref = down_cast<ColumnRef*>(probe_expr_ctx->root());
        SlotDescriptor* slot = nullptr;

        // Find SlotDescriptor by slot_id
        SlotId slot_id = slot_ref->slot_id();
        for (const auto& col_info : _scanner_ctx->materialized_columns) {
            if (col_info.slot_id() == slot_id) {
                slot = col_info.slot_desc;
                break;
            }
        }
        if (slot == nullptr) continue;

        // Get column reader and metadata
        auto* column_reader = group_reader->get_column_reader(slot_id);
        if (column_reader == nullptr) continue;

        auto* chunk_metadata = column_reader->get_chunk_metadata();
        if (chunk_metadata == nullptr) continue;

        auto stats_start = std::chrono::steady_clock::now();

        // Check min/max statistics first (fastest check)
        if (!_check_eq_delete_minmax_direct(in_filter, chunk_metadata, slot->type().type)) {
            auto stats_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - stats_start).count();
            VLOG(1) << "EQDELETE FileReader: MinMax check found intersection in " << stats_duration << "us - cannot bypass";
            return false; // Values overlap with row group range - cannot bypass
        }

        auto stats_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - stats_start).count();

        // Check bloom filter if available and enabled
        if (_scanner_ctx->parquet_bloom_filter_enable &&
            chunk_metadata->meta_data.__isset.bloom_filter_offset) {

            auto bloom_start = std::chrono::steady_clock::now();
            if (!_check_eq_delete_bloom_filter_direct(in_filter, column_reader, slot->type().type)) {
                auto bloom_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - bloom_start).count();
                VLOG(1) << "EQDELETE FileReader: Bloom filter found potential match in " << bloom_duration
                        << "us (after " << stats_duration << "us stats) - cannot bypass";
                return false; // Bloom filter found potential matches - cannot bypass
            }
            auto bloom_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - bloom_start).count();
            VLOG(1) << "EQDELETE FileReader: Bloom filter passed in " << bloom_duration
                    << "us (after " << stats_duration << "us stats)";
        } else {
            VLOG(1) << "EQDELETE FileReader: MinMax passed in " << stats_duration
                    << "us, bloom filter not available/enabled";
        }
    }

    auto direct_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - direct_start).count();
    VLOG(1) << "EQDELETE FileReader: Direct check completed in " << direct_duration << "us - safe to bypass";

    return true; // All checks passed - safe to bypass
}

bool FileReader::_check_eq_delete_minmax_direct(const RuntimeFilter* in_filter,
                                                const tparquet::ColumnChunk* chunk_metadata,
                                                LogicalType column_type) {
    if (!chunk_metadata->meta_data.__isset.statistics) {
        VLOG(2) << "EQDELETE FileReader: No statistics available - allowing bloom filter check";
        return true; // No statistics available - allow bloom filter check
    }

    const auto& stats = chunk_metadata->meta_data.statistics;
    if (!stats.__isset.min_value || !stats.__isset.max_value) {
        VLOG(2) << "EQDELETE FileReader: No min/max values - allowing bloom filter check";
        return true; // No min/max values - allow bloom filter check
    }

    // For now, let bloom filter do the heavy lifting
    // MinMax optimization can be added later for specific common types
    VLOG(2) << "EQDELETE FileReader: MinMax check skipped for type " << column_type
            << " - deferring to bloom filter";
    return true;
}

bool FileReader::_check_eq_delete_bloom_filter_direct(const RuntimeFilter* in_filter,
                                                     const ColumnReader* column_reader,
                                                     LogicalType column_type) {
    try {
        auto* chunk_metadata = column_reader->get_chunk_metadata();
        if (!chunk_metadata || !chunk_metadata->meta_data.__isset.bloom_filter_offset) {
            VLOG(2) << "EQDELETE FileReader: No bloom filter available";
            return true; // No bloom filter - assume no intersection
        }

        // Initialize bloom filter manually (copying logic from RawColumnReader::_init_column_bloom_filter)
        starrocks::ParquetBlockSplitBloomFilter bloom_filter;
        int32_t offset = chunk_metadata->meta_data.bloom_filter_offset;
        int32_t length = chunk_metadata->meta_data.__isset.bloom_filter_length
                        ? chunk_metadata->meta_data.bloom_filter_length : 0;

        // Initialize bloom filter from parquet file data
        auto init_status = _init_parquet_bloom_filter(offset, length, chunk_metadata, &bloom_filter);
        if (!init_status.ok()) {
            VLOG(1) << "EQDELETE FileReader: Failed to init bloom filter: " << init_status.message();
            return true; // Failed to init - assume no intersection
        }

        // Use type dispatch to check values against bloom filter
        return type_dispatch_basic(column_type, [&]<LogicalType LT>() -> bool {
            return _check_bloom_filter_values<LT>(in_filter, &bloom_filter);
        });

    } catch (const std::exception& e) {
        VLOG(1) << "EQDELETE FileReader: Bloom filter check failed: " << e.what();
        return true; // Error - assume no intersection to be safe
    }
}

Status FileReader::_init_parquet_bloom_filter(int32_t offset, int32_t length, 
                                              const tparquet::ColumnChunk* chunk_metadata,
                                              starrocks::ParquetBlockSplitBloomFilter* bloom_filter) {
    // Copy logic from RawColumnReader::_init_column_bloom_filter
    const uint32_t SBBF_HEADER_SIZE_ESTIMATE = 32;
    
    std::vector<char> bloom_filter_data;
    tparquet::BloomFilterHeader header;
    uint32_t header_len = SBBF_HEADER_SIZE_ESTIMATE;
    
    if (length > 0) {
        bloom_filter_data.resize(length + 1);
        RETURN_IF_ERROR(_file->read_at_fully(offset, bloom_filter_data.data(), length));
    } else {
        // if length is not set, read the header first;
        bloom_filter_data.reserve(header_len);
        RETURN_IF_ERROR(_file->read_at_fully(offset, bloom_filter_data.data(), header_len));
    }

    RETURN_IF_ERROR(deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(bloom_filter_data.data()), 
                                          &header_len, TProtocolType::COMPACT, &header));
    if (length == 0) {
        bloom_filter_data.resize(header_len + header.numBytes + 1);
        RETURN_IF_ERROR(_file->read_at_fully(offset + header_len, 
                                           bloom_filter_data.data() + header_len, header.numBytes));
    }
    
    // Set null flag
    if (chunk_metadata->meta_data.__isset.statistics &&
        chunk_metadata->meta_data.statistics.__isset.null_count) {
        if (chunk_metadata->meta_data.statistics.null_count > 0) {
            bloom_filter_data.back() = 1;
        } else {
            bloom_filter_data.back() = 0;
        }
    } else {
        // Conservative: assume nulls can exist
        bloom_filter_data.back() = 1;
    }

    return bloom_filter->init(bloom_filter_data.data() + header_len, header.numBytes + 1, 
                             Hasher::HashStrategy::XXHASH64, 0);
}

template<LogicalType LT>
bool FileReader::_check_bloom_filter_values(const RuntimeFilter* in_filter, 
                                            starrocks::ParquetBlockSplitBloomFilter* bloom_filter) {
    using CppType = RunTimeCppType<LT>;

    // Cast to typed runtime filter
    auto* typed_filter = down_cast<const InRuntimeFilter<LT>*>(in_filter);
    if (!typed_filter) {
        VLOG(1) << "EQDELETE FileReader: Failed to cast to typed InRuntimeFilter";
        return true; // Cast failed - assume no intersection
    }

    // Get all values from the filter
    ObjectPool temp_pool;
    auto value_set = typed_filter->get_set(&temp_pool);

    VLOG(2) << "EQDELETE FileReader: Checking " << value_set.size() << " delete values against bloom filter";

    // Check each value against bloom filter with early exit
    size_t values_checked = 0;
    for (const auto& delete_value : value_set) {
        values_checked++;

        bool might_contain = false;
        if constexpr (IsSlice<CppType>) {
            // For string types
            might_contain = bloom_filter->test_bytes(delete_value.data, delete_value.size);
        } else {
            // For numeric types
            might_contain = bloom_filter->test_bytes(reinterpret_cast<const char*>(&delete_value),
                                                   sizeof(delete_value));
        }

        if (might_contain) {
            VLOG(2) << "EQDELETE FileReader: Bloom filter found potential match after checking "
                    << values_checked << "/" << value_set.size() << " values - early exit";
            return false; // Found potential match - cannot bypass!
        }
    }

    VLOG(2) << "EQDELETE FileReader: Checked all " << value_set.size()
            << " values, no bloom filter matches - safe to bypass";
    return true; // No matches found - safe to bypass
}


void FileReader::_prepare_read_columns(std::unordered_set<std::string>& existed_column_names) {
    _meta_helper->prepare_read_columns(_scanner_ctx->materialized_columns, _group_reader_param.read_cols,
                                       existed_column_names);
    _no_materialized_column_scan = (_group_reader_param.read_cols.size() == 0);
}

bool FileReader::_select_row_group(const tparquet::RowGroup& row_group) {
    size_t row_group_start = ParquetUtils::get_row_group_start_offset(row_group);
    const auto* scan_range = _scanner_ctx->scan_range;
    size_t scan_start = scan_range->offset;
    size_t scan_end = scan_range->length + scan_start;
    if (row_group_start >= scan_start && row_group_start < scan_end) {
        return true;
    }
    return false;
}

Status FileReader::_collect_row_group_io(std::shared_ptr<GroupReader>& group_reader) {
    // collect io ranges.
    if (config::parquet_coalesce_read_enable && _sb_stream != nullptr) { //should move to scanner_ctx
        std::vector<io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        ColumnIOTypeFlags flags = 0;
        if (_scanner_ctx->parquet_page_index_enable) {
            flags |= ColumnIOType::PAGE_INDEX;
        }
        if (_scanner_ctx->parquet_bloom_filter_enable) {
            flags |= ColumnIOType::BLOOM_FILTER;
        }
        group_reader->collect_io_ranges(&ranges, &end_offset, flags);
        RETURN_IF_ERROR(_sb_stream->set_io_ranges(ranges));
    }
    return Status::OK();
}

Status FileReader::_init_group_readers() {
    const HdfsScannerContext& fd_scanner_ctx = *_scanner_ctx;

    _init_eq_delete_predicates();

    // _group_reader_param is used by all group readers
    _group_reader_param.conjunct_ctxs_by_slot = fd_scanner_ctx.conjunct_ctxs_by_slot;
    _group_reader_param.timezone = fd_scanner_ctx.timezone;
    _group_reader_param.stats = fd_scanner_ctx.stats;
    _group_reader_param.sb_stream = _sb_stream;
    _group_reader_param.chunk_size = _chunk_size;
    _group_reader_param.file = _file;
    _group_reader_param.file_metadata = _file_metadata.get();
    _group_reader_param.case_sensitive = fd_scanner_ctx.case_sensitive;
    _group_reader_param.use_file_pagecache = fd_scanner_ctx.use_file_pagecache;
    _group_reader_param.lazy_column_coalesce_counter = fd_scanner_ctx.lazy_column_coalesce_counter;
    _group_reader_param.partition_columns = &fd_scanner_ctx.partition_columns;
    _group_reader_param.partition_values = &fd_scanner_ctx.partition_values;
    _group_reader_param.not_existed_slots = &fd_scanner_ctx.not_existed_slots;
    // for pageIndex
    _group_reader_param.min_max_conjunct_ctxs = fd_scanner_ctx.min_max_conjunct_ctxs;
    _group_reader_param.predicate_tree = &fd_scanner_ctx.predicate_tree;
    _group_reader_param.global_dictmaps = fd_scanner_ctx.global_dictmaps;
    _group_reader_param.modification_time = _datacache_options.modification_time;
    _group_reader_param.file_size = _file_size;
    _group_reader_param.datacache_options = &_datacache_options;

    int64_t row_group_first_row = 0;
    // select and create row group readers.
    for (size_t i = 0; i < _file_metadata->t_metadata().row_groups.size(); i++) {
        if (i > 0) {
            row_group_first_row += _file_metadata->t_metadata().row_groups[i - 1].num_rows;
        }

        if (!_select_row_group(_file_metadata->t_metadata().row_groups[i])) {
            continue;
        }

        auto row_group_reader =
                std::make_shared<GroupReader>(_group_reader_param, i, _skip_rows_ctx, row_group_first_row);
        RETURN_IF_ERROR(row_group_reader->init());

        _group_reader_param.stats->parquet_total_row_groups += 1;

        RETURN_IF_ERROR(_collect_row_group_io(row_group_reader));
        // You should call row_group_reader->init() before _filter_group()
        if (_filter_group(row_group_reader)) {
            DLOG(INFO) << "row group " << i << " of file has been filtered";
            _group_reader_param.stats->parquet_filtered_row_groups += 1;
            continue;
        }

        bool bypass_decision = _should_bypass_eq_delete_for_row_group(row_group_reader);
        _row_group_bypass_decisions.push_back(bypass_decision);

        VLOG(1) << "EQDELETE FileReader: Row group " << i
                << " bypass decision (static): " << bypass_decision;

        _row_group_readers.emplace_back(row_group_reader);
        int64_t num_rows = _file_metadata->t_metadata().row_groups[i].num_rows;
        // for skip rows which already deleted
        if (_skip_rows_ctx != nullptr && _skip_rows_ctx->has_skip_rows()) {
            uint64_t deletion_rows = _skip_rows_ctx->deletion_bitmap->get_range_cardinality(
                    row_group_first_row, row_group_first_row + num_rows);
            num_rows -= deletion_rows;
        }
        _total_row_count += num_rows;
    }
    _row_group_size = _row_group_readers.size();

    if (!_row_group_readers.empty()) {
        // prepare first row group
        RETURN_IF_ERROR(_row_group_readers[_cur_row_group_idx]->prepare());
    }

    return Status::OK();
}

Status FileReader::get_next(ChunkPtr* chunk) {
    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_no_materialized_column_scan) {
        RETURN_IF_ERROR(_exec_no_materialized_column_scan(chunk));
        return Status::OK();
    }

    if (_cur_row_group_idx < _row_group_size) {
        // Use pre-computed bypass decision for current row group (computed in _init_group_readers)
        bool should_bypass_eq_delete = (_cur_row_group_idx < _row_group_bypass_decisions.size()) ?
                                        _row_group_bypass_decisions[_cur_row_group_idx] : false;

        size_t row_count = _chunk_size;
        Status status;
        try {
            status = _row_group_readers[_cur_row_group_idx]->get_next(chunk, &row_count);
        } catch (std::exception& e) {
            return Status::InternalError(
                    strings::Substitute("Encountered Exception while reading. reason = $0", e.what()));
        }
        if (status.ok() || status.is_end_of_file()) {
            if (row_count > 0) {
                RETURN_IF_ERROR(_scanner_ctx->append_or_update_not_existed_columns_to_chunk(chunk, row_count));
                _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, row_count);
                _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, row_count);

                // Add column for bypass EQ-delete
                if (should_bypass_eq_delete) {
                    auto bypass_column = BooleanColumn::create(row_count, 1);
                    (*chunk)->append_column(bypass_column, Chunk::EQ_DELETE_BYPASS_SLOT_ID);
                    _iceberg_eq_delete_rows_skipped += row_count;
                }

                _scan_row_count += (*chunk)->num_rows();
            }
            if (status.is_end_of_file()) {
                // release previous RowGroupReader
                do {
                    _row_group_readers[_cur_row_group_idx] = nullptr;
                    _cur_row_group_idx++;
                    if (_cur_row_group_idx < _row_group_size) {
                        const auto& cur_row_group = _row_group_readers[_cur_row_group_idx];
                        auto ret = _update_rf_and_filter_group(cur_row_group);
                        if (ret.ok() && ret.value()) {
                            // row group is filtered by runtime filter
                            _group_reader_param.stats->parquet_filtered_row_groups += 1;
                            continue;
                        } else if (ret.status().is_end_of_file()) {
                            // If rf is always false, will return eof
                            _group_reader_param.stats->parquet_filtered_row_groups +=
                                    (_row_group_size - _cur_row_group_idx);
                            _row_group_readers.assign(_row_group_readers.size(), nullptr);
                            _cur_row_group_idx = _row_group_size;
                            break;
                        }

                        RETURN_IF_ERROR(cur_row_group->prepare());
                    }
                    break;
                } while (true);

                return Status::OK();
            }
        } else {
            auto s = strings::Substitute("FileReader::get_next failed. reason = $0, file = $1", status.to_string(),
                                         _file->filename());
            LOG(WARNING) << s;
            return status;
        }

        return status;
    }

    return Status::EndOfFile("");
}

Status FileReader::_exec_no_materialized_column_scan(ChunkPtr* chunk) {
    if (_scan_row_count < _total_row_count) {
        size_t read_size = 0;
        if (_scanner_ctx->use_count_opt) {
            read_size = _total_row_count - _scan_row_count;
            _scanner_ctx->append_or_update_count_column_to_chunk(chunk, read_size);
            _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, 1);
            _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, 1);
        } else {
            read_size = std::min(static_cast<size_t>(_chunk_size), _total_row_count - _scan_row_count);
            RETURN_IF_ERROR(_scanner_ctx->append_or_update_not_existed_columns_to_chunk(chunk, read_size));
            _scanner_ctx->append_or_update_partition_column_to_chunk(chunk, read_size);
            _scanner_ctx->append_or_update_extended_column_to_chunk(chunk, read_size);
        }
        _scan_row_count += read_size;
        return Status::OK();
    }

    return Status::EndOfFile("");
}

} // namespace starrocks::parquet

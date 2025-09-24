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

#include <cstring>
#include <iterator>

#include "column/fixed_length_column.h"
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/datacache.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/utils.h"
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

    VLOG(1) << "EQDELETE FileReader: init() called, scanner_ctx->rf_scan_range_pruner=" << (_scanner_ctx->rf_scan_range_pruner != nullptr);
    if (_scanner_ctx->rf_scan_range_pruner != nullptr) {
        _rf_scan_range_pruner = std::make_shared<RuntimeScanRangePruner>(*_scanner_ctx->rf_scan_range_pruner);
        VLOG(1) << "EQDELETE FileReader: Created _rf_scan_range_pruner, ptr=" << _rf_scan_range_pruner.get();
    } else {
        VLOG(1) << "EQDELETE FileReader: rf_scan_range_pruner is NULL, no RF processing!";
    }
    RETURN_IF_ERROR(_init_group_readers());
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
    VLOG(1) << "EQDELETE FileReader: _update_rf_and_filter_group called, _rf_scan_range_pruner=" << (_rf_scan_range_pruner != nullptr);
    if (_rf_scan_range_pruner != nullptr) {
        RETURN_IF_ERROR(_rf_scan_range_pruner->update_range_if_arrived(
                _scanner_ctx->global_dictmaps,
                [this, &filter, &group_reader](auto cid, const PredicateList& predicates, const RuntimeFilterProbeDescriptor* desc) {
                    VLOG(1) << "EQDELETE FileReader: lambda called, desc=" << (desc != nullptr)
                            << ", filter_id=" << (desc ? desc->filter_id() : -1)
                            << ", is_eq_delete=" << (desc ? desc->is_iceberg_eq_delete_filter() : false)
                            << ", predicates_size=" << predicates.size();

                    // Debug: check what type of RF we have
                    if (desc != nullptr) {
                        const RuntimeFilter* rf = desc->runtime_filter(0); // driver_sequence = 0
                        if (rf) {
                            VLOG(1) << "EQDELETE FileReader: RF type info, filter_id=" << desc->filter_id()
                                    << ", has_in_filter=" << (rf->get_in_filter() != nullptr)
                                    << ", has_min_max_filter=" << (rf->get_min_max_filter() != nullptr)
                                    << ", has_membership_filter=" << (rf->get_membership_filter() != nullptr);
                        }
                    }
                    // For Iceberg equality delete: apply RF normally, but set bypass flag instead of filtering
                    bool is_eq_delete = (desc != nullptr && desc->is_iceberg_eq_delete_filter());

                    PredicateCompoundNode<CompoundNodeType::AND> pred_tree;
                    for (const auto& pred : predicates) {
                        pred_tree.add_child(PredicateColumnNode{pred});
                    }

                    auto real_tree = PredicateTree::create(std::move(pred_tree));
                    // For EQ-delete: disable page index to get row-group level filtering only
                    bool enable_page_index = is_eq_delete ? false : _scanner_ctx->parquet_page_index_enable;
                    auto visitor = PredicateFilterEvaluator{real_tree, group_reader.get(),
                                                             enable_page_index,
                                                             _scanner_ctx->parquet_bloom_filter_enable};
                    auto res = real_tree.visit(visitor);

                    if (is_eq_delete) {
                        // For EQ-delete: bypass only if RF completely filters row group (no match with delete files)
                        if (res.ok() && res->has_value() && res->value().span_size() == 0) {
                            _iceberg_eq_delete_skip_probe = true;
                            _iceberg_eq_delete_row_groups_skipped++;
                            if (_group_reader_param.stats != nullptr) {
                                _group_reader_param.stats->iceberg_eq_delete_row_groups_skipped++;
                            }
                            VLOG(1) << "EQDELETE FileReader: row group bypassed (no match), filter_id=" << desc->filter_id();
                        } else {
                            VLOG(1) << "EQDELETE FileReader: row group NOT bypassed (potential match), filter_id=" << desc->filter_id()
                                    << ", span_size=" << (res.ok() && res->has_value() ? res->value().span_size() : -1);
                        }
                        // Don't set filter=true for EQ-delete - we handle it via bypass mechanism
                    } else {
                        // Normal runtime filter: filter the entire row group if RF matches
                        if (res.ok() && res->has_value() && res->value().span_size() == 0) {
                            filter = true;
                        }
                    }
                    this->_group_reader_param.stats->_optimzation_counter += visitor.counter;
                    return Status::OK();
                },
                true, 0));
    }
    return filter;
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
    VLOG(1) << "EQDELETE FileReader: get_next() called, _is_file_filtered=" << _is_file_filtered
            << ", _no_materialized_column_scan=" << _no_materialized_column_scan
            << ", _cur_row_group_idx=" << _cur_row_group_idx << ", _row_group_size=" << _row_group_size;

    if (_is_file_filtered) {
        return Status::EndOfFile("");
    }
    if (_no_materialized_column_scan) {
        RETURN_IF_ERROR(_exec_no_materialized_column_scan(chunk));
        return Status::OK();
    }

    if (_cur_row_group_idx < _row_group_size) {
        // Reset bypass flag for each row group
        _iceberg_eq_delete_skip_probe = false;

        // Check runtime filters for current row group BEFORE reading data
        const auto& cur_row_group = _row_group_readers[_cur_row_group_idx];
        VLOG(1) << "EQDELETE FileReader: About to call _update_rf_and_filter_group for initial check, row_group_idx=" << _cur_row_group_idx;
        auto ret = _update_rf_and_filter_group(cur_row_group);
        if (ret.ok() && ret.value()) {
            // row group is filtered by runtime filter - skip to next
            _group_reader_param.stats->parquet_filtered_row_groups += 1;
            _cur_row_group_idx++;
            return get_next(chunk); // Recurse to try next row group
        } else if (ret.status().is_end_of_file()) {
            // If rf is always false, skip all remaining row groups
            _group_reader_param.stats->parquet_filtered_row_groups += (_row_group_size - _cur_row_group_idx);
            for (size_t i = _cur_row_group_idx; i < _row_group_size; i++) {
                _row_group_readers[i] = nullptr;
            }
            _cur_row_group_idx = _row_group_size;
            return Status::EndOfFile("");
        }

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

                // Add EQ delete skip probe column if needed
                if (_iceberg_eq_delete_skip_probe) {
                    auto skip_probe_column = BooleanColumn::create();
                    skip_probe_column->resize(row_count);
                    // Mark all rows in this row group to skip hash table probe (since runtime filter determined they can't match EQ deletes)
                    std::fill(skip_probe_column->get_data().begin(), skip_probe_column->get_data().end(), 1);
                    (*chunk)->append_column(skip_probe_column, Chunk::EQ_DELETE_BYPASS_SLOT_ID);
                    _iceberg_eq_delete_rows_skipped += row_count;
                    if (_group_reader_param.stats != nullptr) {
                        _group_reader_param.stats->iceberg_eq_delete_rows_skipped += row_count;
                    }
                    VLOG(1) << "EQDELETE FileReader: Added bypass column, row_count=" << row_count
                              << ", slot_id=" << Chunk::EQ_DELETE_BYPASS_SLOT_ID
                              << ", chunk_columns=" << (*chunk)->num_columns()
                              << ", has_bypass_after_add=" << (*chunk)->is_slot_exist(Chunk::EQ_DELETE_BYPASS_SLOT_ID)
                              << ", total_skipped=" << _iceberg_eq_delete_rows_skipped;
                }

                _scan_row_count += (*chunk)->num_rows();
            }
            if (status.is_end_of_file()) {
                // release previous RowGroupReader
                do {
                    _row_group_readers[_cur_row_group_idx] = nullptr;
                    _cur_row_group_idx++;
                    if (_cur_row_group_idx < _row_group_size) {
                        // reset per-row-group bypass flag for next row group
                        _iceberg_eq_delete_skip_probe = false;
                        RETURN_IF_ERROR(_row_group_readers[_cur_row_group_idx]->prepare());
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

        // Add Iceberg EQ delete bypass column if needed
        if (_iceberg_eq_delete_skip_probe) {
            auto bypass_column = BooleanColumn::create(read_size, 1); // All rows skip probe
            (*chunk)->append_column(bypass_column, Chunk::EQ_DELETE_BYPASS_SLOT_ID);
            _iceberg_eq_delete_rows_skipped += read_size;
            VLOG(1) << "EQDELETE FileReader: Added bypass column (no_materialized), read_size=" << read_size
                      << ", total_skipped=" << _iceberg_eq_delete_rows_skipped;
        }

        _scan_row_count += read_size;
        return Status::OK();
    }

    return Status::EndOfFile("");
}


} // namespace starrocks::parquet
